use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use uuid::Uuid;

use crate::network_trace::{DispatchTraceMeta, SchedulerTraceEvent};
use crate::packets::{
    DeliveryPolicy, DropReason, MessageId, PacketMeta, PacketOrder, PacketPriority,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderDomain {
    Independent,
    Sequence(Uuid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueHeadAction {
    Dispatch,
    Wait,
    Drop(DropReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchKind {
    Envelope,
    Resource,
}

#[derive(Clone)]
pub struct DispatchMessage {
    pub kind: DispatchKind,
    pub id: Option<MessageId>,
    pub meta: PacketMeta,
    pub framed: Vec<u8>,
    pub trace: DispatchTraceMeta,
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

    pub fn domain(&self) -> OrderDomain {
        domain_for_order(self.meta.order)
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
                PacketPriority::Droppable | PacketPriority::Deadline { .. }
            )
            && matches!(self.meta.order, PacketOrder::Independent | PacketOrder::Dependency(_))
            && self.id.is_none()
    }

    pub fn is_droppable(&self) -> bool {
        matches!(self.priority(), PacketPriority::Droppable)
    }

    pub fn is_deadline(&self) -> bool {
        matches!(self.priority(), PacketPriority::Deadline { .. })
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

#[derive(Clone)]
pub enum SchedulerCommand {
    Message(DispatchMessage),
    SequenceClose(Uuid),
    SequenceCloseAll,
    Clear,
    Barrier,
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

pub struct PacketScheduler {
    deferred_independent: VecDeque<ScheduledMessage>,
    deferred_sequences: HashMap<Uuid, VecDeque<ScheduledMessage>>,
    blocked_commands: VecDeque<SchedulerCommand>,
    barrier_pending: bool,
    trace_events: Vec<SchedulerTraceEvent>,
}

impl PacketScheduler {
    pub fn new() -> Self {
        Self {
            deferred_independent: VecDeque::new(),
            deferred_sequences: HashMap::new(),
            blocked_commands: VecDeque::new(),
            barrier_pending: false,
            trace_events: Vec::new(),
        }
    }

    pub fn push(&mut self, command: SchedulerCommand, now: Instant) -> Vec<SchedulerAction> {
        if matches!(command, SchedulerCommand::Clear) {
            self.deferred_independent.clear();
            self.deferred_sequences.clear();
            self.blocked_commands.clear();
            self.barrier_pending = false;
            self.trace_events.push(SchedulerTraceEvent::ClearedTransportState);
            return vec![SchedulerAction::ClearTransportState];
        }

        if self.barrier_pending {
            self.trace_events.push(SchedulerTraceEvent::BlockedByBarrier {
                flow_id: command_flow_id(&command),
                command: command_name(&command),
            });
            self.blocked_commands.push_back(command);
            return Vec::new();
        }

        if self.conflicts_with_deferred(&command) {
            self.trace_events.push(SchedulerTraceEvent::BlockedByDeferred {
                flow_id: command_flow_id(&command),
                command: command_name(&command),
                order_domain: command_order_domain(&command),
            });
            self.blocked_commands.push_back(command);
            return Vec::new();
        }

        self.process_command_now(command, now)
    }

    pub fn poll(&mut self, now: Instant, force_barrier_flush: bool) -> Vec<SchedulerAction> {
        let mut actions = self.pump_deferred(now, force_barrier_flush);
        if !self.barrier_pending {
            actions.extend(self.drain_blocked_commands(now));
        }
        actions
    }

    pub fn on_inflight_drained(&mut self, now: Instant) -> Vec<SchedulerAction> {
        if self.barrier_pending {
            self.barrier_pending = false;
            self.trace_events.push(SchedulerTraceEvent::BarrierReleased);
        }
        self.poll(now, false)
    }

    pub fn requeue_deferred_message(&mut self, message: DispatchMessage, now: Instant) {
        let scheduled = ScheduledMessage::new(message, now);
        self.trace_events.push(SchedulerTraceEvent::RequeuedCongestion {
            trace: scheduled.message.trace().clone(),
            queued_messages: self.queue_len_for_domain(scheduled.message.domain()) + 1,
        });
        match scheduled.message.domain() {
            OrderDomain::Independent => insert_requeued(&mut self.deferred_independent, scheduled),
            OrderDomain::Sequence(sequence_id) => {
                insert_requeued(self.deferred_sequences.entry(sequence_id).or_default(), scheduled);
            },
        }
    }

    #[cfg(test)]
    pub fn has_pending_work(&self) -> bool {
        self.barrier_pending
            || !self.deferred_independent.is_empty()
            || !self.deferred_sequences.is_empty()
            || !self.blocked_commands.is_empty()
    }

    pub fn take_trace_events(&mut self) -> Vec<SchedulerTraceEvent> {
        std::mem::take(&mut self.trace_events)
    }

    fn process_command_now(
        &mut self,
        command: SchedulerCommand,
        now: Instant,
    ) -> Vec<SchedulerAction> {
        match command {
            SchedulerCommand::Message(message) => {
                if should_initially_defer(message.priority()) {
                    let queue_name = queue_name(message.domain());
                    let queued_messages = self.queue_len_for_domain(message.domain()) + 1;
                    self.trace_events.push(SchedulerTraceEvent::DeferredInitial {
                        trace: message.trace().clone(),
                        policy: priority_name(message.priority()),
                        queue_name,
                        queued_messages,
                    });
                    self.enqueue_deferred_message(message, now);
                    self.poll(now, false)
                } else {
                    vec![SchedulerAction::DispatchMessage { message, force_flush: false }]
                }
            },
            SchedulerCommand::SequenceClose(sequence_id) => {
                vec![SchedulerAction::CloseSequence(sequence_id)]
            },
            SchedulerCommand::SequenceCloseAll => vec![SchedulerAction::CloseAllSequences],
            SchedulerCommand::Barrier => {
                let mut actions = self.pump_deferred(now, true);
                self.barrier_pending = true;
                self.trace_events.push(SchedulerTraceEvent::BarrierBegin);
                actions.push(SchedulerAction::BeginBarrier);
                actions
            },
            SchedulerCommand::Clear => unreachable!("clear handled before process_command_now"),
        }
    }

    fn enqueue_deferred_message(&mut self, message: DispatchMessage, now: Instant) {
        let scheduled = ScheduledMessage::new(message, now);
        match scheduled.message.domain() {
            OrderDomain::Independent => insert_scheduled(&mut self.deferred_independent, scheduled),
            OrderDomain::Sequence(sequence_id) => {
                insert_scheduled(
                    self.deferred_sequences.entry(sequence_id).or_default(),
                    scheduled,
                );
            },
        }
    }

    fn conflicts_with_deferred(&self, command: &SchedulerCommand) -> bool {
        match command {
            SchedulerCommand::Message(message) => match message.domain() {
                OrderDomain::Independent => false,
                OrderDomain::Sequence(sequence_id) => {
                    self.deferred_sequences.contains_key(&sequence_id)
                },
            },
            SchedulerCommand::SequenceClose(sequence_id) => {
                self.deferred_sequences.contains_key(sequence_id)
            },
            SchedulerCommand::SequenceCloseAll => {
                !self.deferred_independent.is_empty() || !self.deferred_sequences.is_empty()
            },
            SchedulerCommand::Barrier | SchedulerCommand::Clear => false,
        }
    }

    fn queue_len_for_domain(&self, domain: OrderDomain) -> usize {
        match domain {
            OrderDomain::Independent => self.deferred_independent.len(),
            OrderDomain::Sequence(sequence_id) => self
                .deferred_sequences
                .get(&sequence_id)
                .map(|queue| queue.len())
                .unwrap_or(0),
        }
    }

    fn pump_deferred(&mut self, now: Instant, force_flush: bool) -> Vec<SchedulerAction> {
        let mut actions = self.pump_deferred_queue(None, now, force_flush);
        let sequence_ids: Vec<Uuid> = self.deferred_sequences.keys().copied().collect();
        for sequence_id in sequence_ids {
            actions.extend(self.pump_deferred_queue(Some(sequence_id), now, force_flush));
        }
        actions
    }

    fn pump_deferred_queue(
        &mut self,
        sequence_id: Option<Uuid>,
        now: Instant,
        force_flush: bool,
    ) -> Vec<SchedulerAction> {
        let mut queue = match sequence_id {
            Some(sequence_id) => self.deferred_sequences.remove(&sequence_id).unwrap_or_default(),
            None => std::mem::take(&mut self.deferred_independent),
        };
        let mut actions = Vec::new();

        loop {
            match queue_head_action(&queue, now, force_flush) {
                QueueHeadAction::Wait => break,
                QueueHeadAction::Drop(reason) => {
                    let Some(scheduled) = queue.pop_front() else {
                        break;
                    };
                    self.trace_events.push(SchedulerTraceEvent::Dropped {
                        trace: scheduled.message.trace().clone(),
                        reason,
                        queue_name: queue_name(scheduled.message.domain()),
                    });
                    actions
                        .push(SchedulerAction::DropMessage { message: scheduled.message, reason });
                },
                QueueHeadAction::Dispatch => {
                    let Some(scheduled) = queue.pop_front() else {
                        break;
                    };
                    self.trace_events.push(SchedulerTraceEvent::DispatchReady {
                        trace: scheduled.message.trace().clone(),
                        force_flush,
                        queue_name: queue_name(scheduled.message.domain()),
                    });
                    actions.push(SchedulerAction::DispatchMessage {
                        message: scheduled.message,
                        force_flush,
                    });
                },
            }
        }

        match sequence_id {
            Some(sequence_id) => {
                if !queue.is_empty() {
                    self.deferred_sequences.insert(sequence_id, queue);
                }
            },
            None => self.deferred_independent = queue,
        }

        actions
    }

    fn drain_blocked_commands(&mut self, now: Instant) -> Vec<SchedulerAction> {
        let mut actions = Vec::new();
        let mut remaining = self.blocked_commands.len();

        while !self.barrier_pending && remaining > 0 {
            let Some(command) = self.blocked_commands.pop_front() else {
                break;
            };

            if self.conflicts_with_deferred(&command) {
                self.blocked_commands.push_back(command);
            } else {
                actions.extend(self.process_command_now(command, now));
            }

            remaining -= 1;
        }

        actions
    }
}

fn command_name(command: &SchedulerCommand) -> &'static str {
    match command {
        SchedulerCommand::Message(_) => "message",
        SchedulerCommand::SequenceClose(_) => "sequence_close",
        SchedulerCommand::SequenceCloseAll => "sequence_close_all",
        SchedulerCommand::Clear => "clear",
        SchedulerCommand::Barrier => "barrier",
    }
}

fn command_flow_id(command: &SchedulerCommand) -> Option<u64> {
    match command {
        SchedulerCommand::Message(message) => Some(message.trace().flow_id),
        _ => None,
    }
}

fn command_order_domain(command: &SchedulerCommand) -> String {
    match command {
        SchedulerCommand::Message(message) => format!("{:?}", message.domain()),
        SchedulerCommand::SequenceClose(sequence_id) => format!("Sequence({sequence_id})"),
        SchedulerCommand::SequenceCloseAll => "AllSequences".to_string(),
        SchedulerCommand::Clear => "All".to_string(),
        SchedulerCommand::Barrier => "Independent".to_string(),
    }
}

fn queue_name(domain: OrderDomain) -> &'static str {
    match domain {
        OrderDomain::Independent => "independent",
        OrderDomain::Sequence(_) => "sequence",
    }
}

fn priority_name(priority: PacketPriority) -> &'static str {
    match priority {
        PacketPriority::Normal => "normal",
        PacketPriority::Droppable => "droppable",
        PacketPriority::Deadline { .. } => "deadline",
        PacketPriority::Coalescing { .. } => "coalescing",
    }
}

impl ScheduledMessage {
    fn new(message: DispatchMessage, now: Instant) -> Self {
        let (deadline_at, coalescing_target) = match message.priority() {
            PacketPriority::Deadline { max_delay } => {
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

fn insert_scheduled(queue: &mut VecDeque<ScheduledMessage>, scheduled: ScheduledMessage) {
    queue.push_back(scheduled);
}

fn insert_requeued(queue: &mut VecDeque<ScheduledMessage>, scheduled: ScheduledMessage) {
    queue.push_front(scheduled);
}

fn should_initially_defer(priority: PacketPriority) -> bool {
    matches!(priority, PacketPriority::Deadline { .. } | PacketPriority::Coalescing { .. })
}

pub fn domain_for_order(order: PacketOrder) -> OrderDomain {
    match order {
        PacketOrder::Independent | PacketOrder::Dependency(_) => OrderDomain::Independent,
        PacketOrder::Sequence(sequence_id) | PacketOrder::SequenceEnd(sequence_id) => {
            OrderDomain::Sequence(sequence_id)
        },
    }
}

fn queue_head_action(
    scheduled_messages: &VecDeque<ScheduledMessage>,
    now: Instant,
    force_flush: bool,
) -> QueueHeadAction {
    let Some(head) = scheduled_messages.front() else {
        return QueueHeadAction::Wait;
    };

    if force_flush {
        return QueueHeadAction::Dispatch;
    }

    if head.deadline_at.is_some_and(|deadline| now >= deadline) {
        return QueueHeadAction::Drop(DropReason::ExpiredDeadline);
    }

    if let Some(target_payload_bytes) = head.coalescing_target {
        if coalescing_run_bytes(scheduled_messages) < target_payload_bytes {
            return QueueHeadAction::Wait;
        }
    }

    QueueHeadAction::Dispatch
}

fn coalescing_run_bytes(scheduled_messages: &VecDeque<ScheduledMessage>) -> usize {
    let mut total = 0usize;
    for envelope in scheduled_messages {
        if envelope.coalescing_target.is_none() {
            break;
        }
        total = total.saturating_add(envelope.message.payload_len());
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
                delivery: DeliveryPolicy::None,
            },
            framed: vec![0; framed_len],
            trace: DispatchTraceMeta {
                flow_id: 1,
                packet_label: "test".to_string(),
                message_id: id,
                payload_bytes: framed_len,
                target_label: "Broadcast".to_string(),
            },
        }
    }

    #[test]
    fn deadline_waits_then_expires() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let env = envelope(
            PacketPriority::Deadline { max_delay: Duration::from_millis(50) },
            PacketOrder::Independent,
            128,
        );

        let actions = scheduler.push(SchedulerCommand::Message(env.clone()), now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::DispatchMessage { .. }]));

        scheduler.requeue_deferred_message(env.clone(), now);
        assert!(matches!(
            scheduler.poll(now + Duration::from_millis(10), false).as_slice(),
            [SchedulerAction::DispatchMessage { force_flush: false, .. }]
        ));
        scheduler.requeue_deferred_message(env, now);
        assert!(matches!(
            scheduler.poll(now + Duration::from_millis(60), false).as_slice(),
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
    fn coalescing_dispatches_once_threshold_is_reached() {
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
        assert!(actions.iter().all(|action| {
            matches!(action, SchedulerAction::DispatchMessage { force_flush: false, .. })
        }));
    }

    #[test]
    fn deferred_sequence_blocks_only_same_sequence() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let blocked_sequence = Uuid::from_u128(1);
        let other_sequence = Uuid::from_u128(2);
        let first = envelope(
            PacketPriority::Deadline { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(blocked_sequence),
            128,
        );
        scheduler.push(SchedulerCommand::Message(first.clone()), now);
        scheduler.requeue_deferred_message(first, now);

        assert!(scheduler
            .push(
                SchedulerCommand::Message(envelope(
                    PacketPriority::Normal,
                    PacketOrder::Sequence(blocked_sequence),
                    64,
                )),
                now,
            )
            .is_empty());

        let other_actions = scheduler.push(
            SchedulerCommand::Message(envelope(
                PacketPriority::Normal,
                PacketOrder::Sequence(other_sequence),
                64,
            )),
            now,
        );
        assert!(matches!(other_actions.as_slice(), [SchedulerAction::DispatchMessage { .. }]));
    }

    #[test]
    fn sequence_close_waits_for_that_sequence() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let sequence_id = Uuid::from_u128(1);
        let first = envelope(
            PacketPriority::Deadline { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(sequence_id),
            128,
        );
        scheduler.push(SchedulerCommand::Message(first.clone()), now);
        scheduler.requeue_deferred_message(first, now);

        assert!(scheduler.push(SchedulerCommand::SequenceClose(sequence_id), now).is_empty());
    }

    #[test]
    fn sequence_close_all_waits_for_all_deferred_work() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        scheduler.push(
            SchedulerCommand::Message(envelope(
                PacketPriority::Coalescing { target_payload_bytes: 600 },
                PacketOrder::Independent,
                250,
            )),
            now,
        );

        assert!(scheduler.push(SchedulerCommand::SequenceCloseAll, now).is_empty());
    }

    #[test]
    fn clear_removes_deferred_and_blocked_work() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let sequence_id = Uuid::from_u128(1);
        let first = envelope(
            PacketPriority::Deadline { max_delay: Duration::from_secs(1) },
            PacketOrder::Sequence(sequence_id),
            128,
        );
        scheduler.push(SchedulerCommand::Message(first.clone()), now);
        scheduler.requeue_deferred_message(first, now);
        scheduler.push(SchedulerCommand::SequenceClose(sequence_id), now);

        let actions = scheduler.push(SchedulerCommand::Clear, now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::ClearTransportState]));
        assert!(!scheduler.has_pending_work());
    }

    #[test]
    fn barrier_force_flushes_and_blocks_later_messages_until_drained() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let first = envelope(
            PacketPriority::Coalescing { target_payload_bytes: 900 },
            PacketOrder::Independent,
            300,
        );
        scheduler.push(SchedulerCommand::Message(first), now);

        let actions = scheduler.push(SchedulerCommand::Barrier, now);
        assert!(matches!(
            actions.as_slice(),
            [
                SchedulerAction::DispatchMessage { force_flush: true, .. },
                SchedulerAction::BeginBarrier
            ]
        ));

        assert!(scheduler
            .push(
                SchedulerCommand::Message(envelope(
                    PacketPriority::Normal,
                    PacketOrder::Independent,
                    100,
                )),
                now,
            )
            .is_empty());

        let released = scheduler.on_inflight_drained(now);
        assert!(matches!(
            released.as_slice(),
            [SchedulerAction::DispatchMessage { force_flush: false, .. }]
        ));
    }

    #[test]
    fn coalescing_resource_waits_below_threshold() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        assert!(scheduler
            .push(
                SchedulerCommand::Message(resource(
                    PacketPriority::Coalescing { target_payload_bytes: 256 },
                    PacketOrder::Independent,
                    128,
                )),
                now,
            )
            .is_empty());
    }

    #[test]
    fn resource_sequence_conflict_blocks_same_sequence() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        let sequence_id = Uuid::from_u128(9);
        scheduler.requeue_deferred_message(
            resource(PacketPriority::Normal, PacketOrder::Sequence(sequence_id), 32),
            now,
        );

        assert!(scheduler
            .push(
                SchedulerCommand::Message(resource(
                    PacketPriority::Normal,
                    PacketOrder::Sequence(sequence_id),
                    64,
                )),
                now,
            )
            .is_empty());
    }
}
