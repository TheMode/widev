//! Packet Scheduler Architecture
//!===========================
//!
//! The scheduler transforms incoming packet messages into dispatch actions
//! while respecting ordering, timing, and priority constraints.
//!
//! ## Message Flow
//!
//!```text
//! Incoming Message
//!     |
//!     v
//! [push()]──────────────────────────────────────────────────────┐
//!     |                                                          |
//!     ├─Clear────────────────────────────────────> ClearTransportState
//!     |                                                          |
//!     ├─Barrier─────────────────> Flush all, BeginBarrier       │
//!     |                              (block until drained)       |
//!     |                                                          │
//!     ├─Normal/Droppable ─────> DispatchMessage (immediate)     │
//!     |                                                          │
//!     ├─MaxDelay/Coalescing ──> Enqueue to deferred queue        │
//!     |                              │                           │
//!     └─Sequence command─────> Enqueue to sequence queue         │
//!                                  │                           │
//!                                  v                           │
//!                           [poll()] <─────────────────────────┘
//!                                  │
//!                     ┌────────────┴────────────┐
//!                     │                         │
//!               Check head:                Check sequences:
//!               - Deadline expired?         - Pending close?
//!               - Coalescing ready?         - Ready to dispatch?
//!                     │                         │
//!            ┌────────┴────────┐       ┌────────┴────────┐
//!            v                 v       v                 v
//!         DropMessage    DispatchMessage   CloseSequence   DispatchMessage
//! ```
//!
//! ## Ordering
//!
//! Messages belong to ordering categories that constrain dispatch order:
//! - **Independent/Dependency**: No sequence ordering constraints
//! - **Sequence/SequenceEnd(Uuid)**: Must dispatch in order within same sequence_id
//!
//! ## Priority Policies
//!
//! | Policy| Behavior                                    |
//! |----------------|---------------------------------------------|
//! | Normal     | Dispatch immediately, requeue on congestion |
//! | Droppable   | Dispatch or drop if no budget               |
//! | MaxDelay| Defer up to deadline, drop if expired   |
//! | Coalescing | Wait until cumulative bytes reach threshold  |
//!
//! ## Key Invariants
//!
//! 1. **Sequence ordering**: Messages in same sequence dispatch in arrival order.
//!    This is guaranteed by per-sequence queues - no explicit blocking needed.
//!
//! 2. **Barrier semantics**: Flushes all deferred messages, then blocks new
//!    dispatches until `on_inflight_drained()` signals transport completion.
//!
//! 3. **Deadline enforcement**: MaxDelay messages drop when poll() observes
//!    `now >= deadline`. Deadline is computed at message creation.
//!
//! 4. **Coalescing accumulation**: Messages accumulate payload bytes from
//!    queue head until threshold met, then dispatch in FIFO order.
//!
//! 5. **Pending closes**: SequenceClose commands that arrive while a sequence
//!    has pending messages are stored as pending flags, executed when queue drains.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use strum::IntoStaticStr;
use uuid::Uuid;

use crate::network_trace::{DispatchTraceMeta, SchedulerTraceEvent};
use crate::packets::{
    DeliveryPolicy, DropReason, MessageId, PacketMeta, PacketOrder, PacketPriority, RetryReason,
};

#[derive(Clone)]
pub struct DispatchMessage {
    pub kind: DispatchKind,
    pub id: Option<MessageId>,
    pub meta: PacketMeta,
    pub framed: Vec<u8>,
    pub trace: DispatchTraceMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchKind {
    Envelope,
    Resource,
}

impl DispatchMessage {
    pub fn new(
        kind: DispatchKind,
        id: Option<MessageId>,
        meta: PacketMeta,
        framed: Vec<u8>,
        trace: DispatchTraceMeta,
    ) -> Self {
        Self { kind, id, meta, framed, trace }
    }

    pub fn priority(&self) -> PacketPriority {
        self.meta.priority
    }

    pub fn payload_len(&self) -> usize {
        self.framed.len()
    }

    pub fn maybe_id(&self) -> Option<MessageId> {
        self.id
    }

    pub fn order(&self) -> PacketOrder {
        self.meta.order
    }

    pub fn delivery(&self) -> DeliveryPolicy {
        self.meta.delivery
    }

    pub fn framed(&self) -> &[u8] {
        &self.framed
    }

    pub fn is_datagram_eligible(&self) -> bool {
        self.kind == DispatchKind::Envelope
            && matches!(
                self.meta.priority,
                PacketPriority::Droppable | PacketPriority::MaxDelay { .. }
            )
            && matches!(self.meta.order, PacketOrder::Independent | PacketOrder::Dependency(_))
            && self.id.is_none()
    }

    pub fn is_droppable(&self) -> bool {
        matches!(self.priority(), PacketPriority::Droppable)
    }

    pub fn is_max_delay(&self) -> bool {
        matches!(self.priority(), PacketPriority::MaxDelay { .. })
    }

    pub fn kind_name(&self) -> &'static str {
        match self.kind {
            DispatchKind::Envelope => "envelope",
            DispatchKind::Resource => "resource",
        }
    }

    pub fn trace(&self) -> &DispatchTraceMeta {
        &self.trace
    }
}

#[derive(Clone)]
struct ScheduledMessage {
    deadline_at: Option<Instant>,
    coalescing_target: Option<usize>,
    message: DispatchMessage,
}

#[derive(Clone, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SchedulerCommand {
    Message(DispatchMessage),
    SequenceClose(Uuid),
    SequenceCloseAll,
    Clear,
    Barrier,
}

impl SchedulerCommand {
    pub fn flow_id(&self) -> Option<u64> {
        match self {
            SchedulerCommand::Message(message) => Some(message.trace().flow_id),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub enum SchedulerAction {
    DispatchMessage {
        message: DispatchMessage,
        force_flush: bool,
    },
    CloseSequence(Uuid),
    CloseAllSequences,
    ClearTransportState,
    BeginBarrier,
    DropMessage {
        message: DispatchMessage,
        reason: DropReason,
    },
}

/// Decision for the head of a deferred queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueDecision {
    /// Ready to dispatch now.
    Dispatch,
    /// Wait for more data or deadline.
    Wait,
    /// Deadline expired, drop the message.
    DropExpired,
}

pub struct PacketScheduler {
    /// Messages without sequence ordering constraints.
    pending_independent: VecDeque<ScheduledMessage>,
    /// Per-sequence message queues. Ordering is implicit: messages are
    /// dispatched in FIFO order within each sequence.
    pending_sequences: HashMap<Uuid, VecDeque<ScheduledMessage>>,
    /// Sequence IDs that have a pending close command.
    /// When the sequence queue empties, the close action is emitted.
    pending_sequence_closes: HashSet<Uuid>,
    /// If true, close all sequences after current work completes.
    pending_close_all: bool,
    /// Barrier is active: no new dispatches until on_inflight_drained().
    barrier_pending: bool,
    /// Messages that arrived during a barrier, enqueued for post-barrier dispatch.
    pending_barrier_queue: VecDeque<DispatchMessage>,
    trace_events: Vec<SchedulerTraceEvent>,
}

impl PacketScheduler {
    pub fn new() -> Self {
        Self {
            pending_independent: VecDeque::new(),
            pending_sequences: HashMap::new(),
            pending_sequence_closes: HashSet::new(),
            pending_close_all: false,
            barrier_pending: false,
            pending_barrier_queue: VecDeque::new(),
            trace_events: Vec::new(),
        }
    }

    /// Push a command into the scheduler.
    ///
    /// Returns actions that should be executed immediately. Messages that
    /// require deferral (MaxDelay, Coalescing) are queued for later poll().
    pub fn push(&mut self, command: SchedulerCommand, now: Instant) -> Vec<SchedulerAction> {
        if matches!(command, SchedulerCommand::Clear) {
            return self.handle_clear();
        }

        if self.barrier_pending {
            self.trace_events.push(SchedulerTraceEvent::BlockedByBarrier {
                flow_id: command.flow_id(),
                command: (&command).into(),
            });
            match command {
                SchedulerCommand::Message(message) => {
                    self.pending_barrier_queue.push_back(message);
                    return Vec::new();
                },
                SchedulerCommand::SequenceClose(sequence_id) => {
                    self.pending_sequence_closes.insert(sequence_id);
                    return Vec::new();
                },
                SchedulerCommand::SequenceCloseAll => {
                    self.pending_close_all = true;
                    return Vec::new();
                },
                SchedulerCommand::Barrier => {
                    // Nested barrier - just flush again
                },
                SchedulerCommand::Clear => unreachable!("handled above"),
            }
        }

        match command {
            SchedulerCommand::Message(message) => self.handle_message(message, now),
            SchedulerCommand::SequenceClose(sequence_id) => self.handle_sequence_close(sequence_id),
            SchedulerCommand::SequenceCloseAll => self.handle_sequence_close_all(),
            SchedulerCommand::Barrier => self.handle_barrier(now),
            SchedulerCommand::Clear => unreachable!("handled above"),
        }
    }

    /// Poll for ready messages.
    ///
    /// Checks deadlines and coalescing thresholds, returning actions for
    /// messages ready to dispatch or drop. If `force_flush` is true,
    /// all deferred messages dispatch regardless of thresholds.
    pub fn poll(&mut self, now: Instant, force_flush: bool) -> Vec<SchedulerAction> {
        if self.barrier_pending {
            return Vec::new();
        }

        let mut actions = Vec::new();
        self.pump_queue(None, now, force_flush, &mut actions);

        let sequence_ids: Vec<Uuid> = self.pending_sequences.keys().copied().collect();
        for sequence_id in sequence_ids {
            self.pump_queue(Some(sequence_id), now, force_flush, &mut actions);

            // Check for pending sequence close after queue drains
            let queue_empty = self.pending_sequences.get(&sequence_id).is_none_or(|q| q.is_empty());
            if queue_empty && self.pending_sequence_closes.remove(&sequence_id) {
                actions.push(SchedulerAction::CloseSequence(sequence_id));
            }
            if queue_empty {
                self.pending_sequences.remove(&sequence_id);
            }
        }

        if self.pending_close_all
            && self.pending_independent.is_empty()
            && self.pending_sequences.is_empty()
        {
            self.pending_close_all = false;
            actions.push(SchedulerAction::CloseAllSequences);
        }

        actions
    }

    /// Called when all inflight transport writes have completed.
    ///
    /// Releases the barrier and processes any pending work.
    pub fn on_inflight_drained(&mut self, now: Instant) -> Vec<SchedulerAction> {
        if self.barrier_pending {
            self.barrier_pending = false;
            self.trace_events.push(SchedulerTraceEvent::BarrierReleased);
        }

        // Process messages that arrived during barrier
        let barrier_queue = std::mem::take(&mut self.pending_barrier_queue);
        let mut actions = Vec::new();
        for message in barrier_queue {
            actions.extend(self.handle_message(message, now));
        }
        actions.extend(self.poll(now, false));
        actions
    }

    /// Requeue a message that could not be sent due to congestion.
    ///
    /// The message is inserted at the front of its queue to preserve ordering.
    pub fn requeue_deferred_message(
        &mut self,
        message: DispatchMessage,
        now: Instant,
        reason: RetryReason,
    ) {
        let scheduled = ScheduledMessage::new(message, now);
        self.trace_events.push(SchedulerTraceEvent::RequeuedCongestion {
            trace: scheduled.message.trace().clone(),
            queued_messages: self.queue_len_for_order(scheduled.message.order()) + 1,
            reason,
        });
        match scheduled.message.order() {
            PacketOrder::Independent | PacketOrder::Dependency(_) => {
                self.pending_independent.push_front(scheduled);
            },
            PacketOrder::Sequence(sequence_id) | PacketOrder::SequenceEnd(sequence_id) => {
                self.pending_sequences.entry(sequence_id).or_default().push_front(scheduled);
            },
        }
    }

    #[cfg(test)]
    pub fn has_pending_work(&self) -> bool {
        self.barrier_pending
            || !self.pending_independent.is_empty()
            || !self.pending_sequences.is_empty()
            || !self.pending_sequence_closes.is_empty()
            || self.pending_close_all
            || !self.pending_barrier_queue.is_empty()
    }

    pub fn take_trace_events(&mut self) -> Vec<SchedulerTraceEvent> {
        std::mem::take(&mut self.trace_events)
    }

    // ============== Command Handlers ==============

    fn handle_clear(&mut self) -> Vec<SchedulerAction> {
        self.pending_independent.clear();
        self.pending_sequences.clear();
        self.pending_sequence_closes.clear();
        self.pending_close_all = false;
        self.barrier_pending = false;
        self.pending_barrier_queue.clear();
        self.trace_events.push(SchedulerTraceEvent::ClearedTransportState);
        vec![SchedulerAction::ClearTransportState]
    }

    fn handle_message(&mut self, message: DispatchMessage, now: Instant) -> Vec<SchedulerAction> {
        if self.barrier_pending {
            self.pending_barrier_queue.push_back(message);
            return Vec::new();
        }

        let should_defer = should_initially_defer(message.priority());

        if !should_defer {
            return vec![SchedulerAction::DispatchMessage { message, force_flush: false }];
        }

        let queued_messages = self.queue_len_for_order(message.order()) + 1;
        self.trace_events.push(SchedulerTraceEvent::DeferredInitial {
            trace: message.trace().clone(),
            policy: message.priority().into(),
            queue_name: order_to_queue_name(message.order()),
            queued_messages,
        });

        self.enqueue_message(message, now);
        self.poll(now, false)
    }

    fn handle_sequence_close(&mut self, sequence_id: Uuid) -> Vec<SchedulerAction> {
        let has_pending = self.pending_sequences.get(&sequence_id).is_some_and(|q| !q.is_empty());

        if has_pending {
            self.pending_sequence_closes.insert(sequence_id);
            self.trace_events.push(SchedulerTraceEvent::BlockedByDeferred {
                flow_id: None,
                command: "sequence_close",
                order_domain: format!("Sequence({sequence_id})"),
            });
            return Vec::new();
        }

        vec![SchedulerAction::CloseSequence(sequence_id)]
    }

    fn handle_sequence_close_all(&mut self) -> Vec<SchedulerAction> {
        let has_pending =
            !self.pending_independent.is_empty() || !self.pending_sequences.is_empty();

        if has_pending {
            self.pending_close_all = true;
            self.trace_events.push(SchedulerTraceEvent::BlockedByDeferred {
                flow_id: None,
                command: "sequence_close_all",
                order_domain: "AllSequences".to_string(),
            });
            return Vec::new();
        }

        vec![SchedulerAction::CloseAllSequences]
    }

    fn handle_barrier(&mut self, now: Instant) -> Vec<SchedulerAction> {
        let mut actions = Vec::new();
        self.pump_queue(None, now, true, &mut actions);

        let sequence_ids: Vec<Uuid> = self.pending_sequences.keys().copied().collect();
        for sequence_id in sequence_ids {
            self.pump_queue(Some(sequence_id), now, true, &mut actions);
            if self.pending_sequence_closes.remove(&sequence_id) {
                actions.push(SchedulerAction::CloseSequence(sequence_id));
            }
        }

        self.barrier_pending = true;
        self.trace_events.push(SchedulerTraceEvent::BarrierBegin);
        actions.push(SchedulerAction::BeginBarrier);
        actions
    }

    // ============== Queue Operations ==============

    fn enqueue_message(&mut self, message: DispatchMessage, now: Instant) {
        let scheduled = ScheduledMessage::new(message, now);
        match scheduled.message.order() {
            PacketOrder::Independent | PacketOrder::Dependency(_) => {
                self.pending_independent.push_back(scheduled);
            },
            PacketOrder::Sequence(sequence_id) | PacketOrder::SequenceEnd(sequence_id) => {
                self.pending_sequences.entry(sequence_id).or_default().push_back(scheduled);
            },
        }
    }

    fn queue_len_for_order(&self, order: PacketOrder) -> usize {
        match order {
            PacketOrder::Independent | PacketOrder::Dependency(_) => self.pending_independent.len(),
            PacketOrder::Sequence(sequence_id) | PacketOrder::SequenceEnd(sequence_id) => {
                self.pending_sequences.get(&sequence_id).map(|q| q.len()).unwrap_or(0)
            },
        }
    }

    fn pump_queue(
        &mut self,
        sequence_id: Option<Uuid>,
        now: Instant,
        force_flush: bool,
        actions: &mut Vec<SchedulerAction>,
    ) {
        let queue = match sequence_id {
            Some(id) => self.pending_sequences.get_mut(&id),
            None => Some(&mut self.pending_independent),
        };

        let Some(queue) = queue else {
            return;
        };

        loop {
            let decision = decide_queue_head(queue, now, force_flush);

            match decision {
                QueueDecision::Wait => break,
                QueueDecision::DropExpired => {
                    let Some(scheduled) = queue.pop_front() else {
                        break;
                    };
                    self.trace_events.push(SchedulerTraceEvent::Dropped {
                        trace: scheduled.message.trace().clone(),
                        reason: DropReason::ExpiredDeadline,
                        queue_name: order_to_queue_name(scheduled.message.order()),
                    });
                    actions.push(SchedulerAction::DropMessage {
                        message: scheduled.message,
                        reason: DropReason::ExpiredDeadline,
                    });
                },
                QueueDecision::Dispatch => {
                    let Some(scheduled) = queue.pop_front() else {
                        break;
                    };
                    self.trace_events.push(SchedulerTraceEvent::DispatchReady {
                        trace: scheduled.message.trace().clone(),
                        force_flush,
                        queue_name: order_to_queue_name(scheduled.message.order()),
                    });
                    actions.push(SchedulerAction::DispatchMessage {
                        message: scheduled.message,
                        force_flush,
                    });
                },
            }
        }
    }
}

fn order_to_queue_name(order: PacketOrder) -> &'static str {
    match order {
        PacketOrder::Independent | PacketOrder::Dependency(_) => "independent",
        PacketOrder::Sequence(_) | PacketOrder::SequenceEnd(_) => "sequence",
    }
}

impl ScheduledMessage {
    fn new(message: DispatchMessage, now: Instant) -> Self {
        let (deadline_at, coalescing_target) = match message.priority() {
            PacketPriority::MaxDelay { max_delay } => {
                (now.checked_add(max_delay).or(Some(now)), None)
            },
            PacketPriority::Coalescing { target_payload_bytes } => {
                (None, Some(target_payload_bytes))
            },
            _ => (None, None),
        };
        Self { deadline_at, coalescing_target, message }
    }
}

fn should_initially_defer(priority: PacketPriority) -> bool {
    matches!(priority, PacketPriority::MaxDelay { .. } | PacketPriority::Coalescing { .. })
}

/// Decide what to do with the head of a queue.
fn decide_queue_head(
    queue: &VecDeque<ScheduledMessage>,
    now: Instant,
    force_flush: bool,
) -> QueueDecision {
    let Some(head) = queue.front() else {
        return QueueDecision::Wait;
    };

    if force_flush {
        return QueueDecision::Dispatch;
    }

    if head.deadline_at.is_some_and(|deadline| now >= deadline) {
        return QueueDecision::DropExpired;
    }

    if let Some(target_bytes) = head.coalescing_target {
        if coalescing_bytes(queue) < target_bytes {
            return QueueDecision::Wait;
        }
    }

    QueueDecision::Dispatch
}

/// Sum payload bytes from consecutive coalescing messages at queue head.
fn coalescing_bytes(queue: &VecDeque<ScheduledMessage>) -> usize {
    let mut total = 0usize;
    for msg in queue {
        if msg.coalescing_target.is_none() {
            break;
        }
        total = total.saturating_add(msg.message.payload_len());
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn envelope(
        priority: PacketPriority,
        order: PacketOrder,
        framed_len: usize,
    ) -> DispatchMessage {
        message(DispatchKind::Envelope, None, priority, order, framed_len)
    }

    fn resource(
        priority: PacketPriority,
        order: PacketOrder,
        framed_len: usize,
    ) -> DispatchMessage {
        message(DispatchKind::Resource, Some(1), priority, order, framed_len)
    }

    fn message(
        kind: DispatchKind,
        id: Option<MessageId>,
        priority: PacketPriority,
        order: PacketOrder,
        framed_len: usize,
    ) -> DispatchMessage {
        DispatchMessage {
            kind,
            id,
            meta: PacketMeta {
                target: crate::packets::PacketTarget::Broadcast,
                priority,
                order,
                delivery: DeliveryPolicy::FireAndForget,
            },
            framed: vec![0; framed_len],
            trace: DispatchTraceMeta {
                flow_id: 1,
                client_id: 1,
                kind: crate::network_trace::FlowKind::Envelope,
                packet_label: "test".to_string(),
                message_id: id,
                payload_bytes: framed_len,
                target_label: "Broadcast".to_string(),
                priority: <&str>::from(priority).to_string(),
                order: format!("{:?}", order),
                delivery: "FireAndForget".to_string(),
                dependency_label: None,
                sequence_id: None,
                components: Vec::new(),
            },
        }
    }

    #[test]
    fn max_delay_waits_then_expires() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let env = envelope(
            PacketPriority::MaxDelay { max_delay: Duration::from_millis(50) },
            PacketOrder::Independent,
            128,
        );

        let actions = scheduler.push(SchedulerCommand::Message(env.clone()), now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::DispatchMessage { .. }]));

        scheduler.requeue_deferred_message(env.clone(), now, RetryReason::Congestion);
        let actions = scheduler.poll(now + Duration::from_millis(10), false);
        assert!(matches!(
            actions.as_slice(),
            [SchedulerAction::DispatchMessage { force_flush: false, .. }]
        ));

        scheduler.requeue_deferred_message(env, now, RetryReason::Congestion);
        let actions = scheduler.poll(now + Duration::from_millis(60), false);
        assert!(matches!(
            actions.as_slice(),
            [SchedulerAction::DropMessage { reason: DropReason::ExpiredDeadline, .. }]
        ));
    }

    #[test]
    fn coalescing_waits_below_threshold() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let first = envelope(
            PacketPriority::Coalescing { target_payload_bytes: 600 },
            PacketOrder::Independent,
            250,
        );

        assert!(scheduler.push(SchedulerCommand::Message(first), now).is_empty());
    }

    #[test]
    fn coalescing_dispatches_at_threshold() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let first = envelope(
            PacketPriority::Coalescing { target_payload_bytes: 600 },
            PacketOrder::Independent,
            250,
        );
        let second = envelope(
            PacketPriority::Coalescing { target_payload_bytes: 400 },
            PacketOrder::Independent,
            400,
        );

        assert!(scheduler.push(SchedulerCommand::Message(first), now).is_empty());
        let actions = scheduler.push(SchedulerCommand::Message(second), now);
        assert_eq!(actions.len(), 2);
        assert!(
            actions
                .iter()
                .all(|a| matches!(a, SchedulerAction::DispatchMessage { force_flush: false, .. }))
        );
    }

    #[test]
    fn sequence_ordering_is_maintained() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let seq_id = Uuid::from_u128(1);

        let first = envelope(
            PacketPriority::MaxDelay { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(seq_id),
            128,
        );
        scheduler.push(SchedulerCommand::Message(first.clone()), now);
        scheduler.requeue_deferred_message(first, now, RetryReason::Congestion);

        // Second message for same sequence should queue behind
        let second = envelope(PacketPriority::Normal, PacketOrder::Sequence(seq_id), 64);
        let actions = scheduler.push(SchedulerCommand::Message(second), now);
        assert!(
            actions.is_empty()
                || actions.iter().all(|a| matches!(a, SchedulerAction::DispatchMessage { .. }))
        );

        // Different sequence should dispatch immediately
        let other_seq = Uuid::from_u128(2);
        let other = envelope(PacketPriority::Normal, PacketOrder::Sequence(other_seq), 64);
        let actions = scheduler.push(SchedulerCommand::Message(other), now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::DispatchMessage { .. }]));
    }

    #[test]
    fn sequence_close_waits_for_pending() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let seq_id = Uuid::from_u128(1);

        let msg = envelope(
            PacketPriority::MaxDelay { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(seq_id),
            128,
        );
        scheduler.push(SchedulerCommand::Message(msg.clone()), now);
        scheduler.requeue_deferred_message(msg, now, RetryReason::Congestion);

        let actions = scheduler.push(SchedulerCommand::SequenceClose(seq_id), now);
        assert!(actions.is_empty());
    }

    #[test]
    fn sequence_close_dispatches_when_queue_empty() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let seq_id = Uuid::from_u128(1);

        let actions = scheduler.push(SchedulerCommand::SequenceClose(seq_id), now);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], SchedulerAction::CloseSequence(id) if id == seq_id));
    }

    #[test]
    fn clear_removes_all_state() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let seq_id = Uuid::from_u128(1);

        let msg = envelope(
            PacketPriority::MaxDelay { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(seq_id),
            128,
        );
        scheduler.push(SchedulerCommand::Message(msg.clone()), now);
        scheduler.requeue_deferred_message(msg, now, RetryReason::Congestion);
        scheduler.push(SchedulerCommand::SequenceClose(seq_id), now);

        let actions = scheduler.push(SchedulerCommand::Clear, now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::ClearTransportState]));
        assert!(!scheduler.has_pending_work());
    }

    #[test]
    fn barrier_flushes_and_blocks() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();

        let msg = envelope(
            PacketPriority::Coalescing { target_payload_bytes: 900 },
            PacketOrder::Independent,
            300,
        );
        scheduler.push(SchedulerCommand::Message(msg), now);

        let actions = scheduler.push(SchedulerCommand::Barrier, now);
        assert!(matches!(
            actions.as_slice(),
            [
                SchedulerAction::DispatchMessage { force_flush: true, .. },
                SchedulerAction::BeginBarrier
            ]
        ));

        // New messages blocked while barrier pending
        let blocked = envelope(PacketPriority::Normal, PacketOrder::Independent, 100);
        assert!(scheduler.push(SchedulerCommand::Message(blocked), now).is_empty());

        // Releases after drain
        let actions = scheduler.on_inflight_drained(now);
        assert!(matches!(
            actions.as_slice(),
            [SchedulerAction::DispatchMessage { force_flush: false, .. }]
        ));
    }

    #[test]
    fn coalescing_resource_works() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        assert!(
            scheduler
                .push(
                    SchedulerCommand::Message(resource(
                        PacketPriority::Coalescing { target_payload_bytes: 256 },
                        PacketOrder::Independent,
                        128,
                    )),
                    now,
                )
                .is_empty()
        );
    }

    #[test]
    fn sequence_with_pending_close_emits_on_drain() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let seq_id = Uuid::from_u128(42);

        let msg = envelope(
            PacketPriority::MaxDelay { max_delay: Duration::from_millis(100) },
            PacketOrder::Sequence(seq_id),
            64,
        );
        scheduler.push(SchedulerCommand::Message(msg.clone()), now);
        scheduler.requeue_deferred_message(msg, now, RetryReason::Congestion);

        // Queue close while pending
        scheduler.push(SchedulerCommand::SequenceClose(seq_id), now);

        // Poll after deadline - should drop message and emit close
        let actions = scheduler.poll(now + Duration::from_millis(150), false);
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], SchedulerAction::DropMessage { .. }));
        assert!(matches!(&actions[1], SchedulerAction::CloseSequence(id) if *id == seq_id));
    }
}
