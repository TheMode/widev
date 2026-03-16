use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use uuid::Uuid;

use crate::packets::{DeliveryPolicy, DropReason, EnvelopeId, PacketOrder, PacketPriority};

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

#[derive(Clone)]
pub struct DispatchEnvelope {
    pub id: Option<EnvelopeId>,
    pub priority: PacketPriority,
    pub order: PacketOrder,
    pub delivery: DeliveryPolicy,
    pub framed: Vec<u8>,
}

impl DispatchEnvelope {
    pub fn domain(&self) -> OrderDomain {
        domain_for_order(self.order)
    }

    pub fn is_datagram_eligible(&self) -> bool {
        matches!(self.priority, PacketPriority::Droppable | PacketPriority::Deadline { .. })
            && matches!(self.order, PacketOrder::Independent | PacketOrder::Dependency(_))
            && self.id.is_none()
    }

    pub fn is_droppable(&self) -> bool {
        matches!(self.priority, PacketPriority::Droppable)
    }

    pub fn is_deadline(&self) -> bool {
        matches!(self.priority, PacketPriority::Deadline { .. })
    }

    pub fn payload_len(&self) -> usize {
        self.framed.len()
    }
}

#[derive(Clone)]
struct ScheduledEnvelope {
    deadline_at: Option<Instant>,
    coalescing_target: Option<usize>,
    envelope: DispatchEnvelope,
}

#[derive(Clone)]
pub enum SchedulerCommand {
    Envelope(DispatchEnvelope),
    SequenceClose(Uuid),
    SequenceCloseAll,
    Clear,
    Barrier,
}

#[derive(Clone)]
pub enum SchedulerAction {
    DispatchEnvelope {
        envelope: DispatchEnvelope,
        force_flush: bool,
    },
    CloseSequence(Uuid),
    CloseAllSequences,
    ClearTransportState,
    BeginBarrier,
    DropEnvelope {
        envelope: DispatchEnvelope,
        reason: DropReason,
    },
}

pub struct PacketScheduler {
    deferred_independent: VecDeque<ScheduledEnvelope>,
    deferred_sequences: HashMap<Uuid, VecDeque<ScheduledEnvelope>>,
    blocked_commands: VecDeque<SchedulerCommand>,
    barrier_pending: bool,
}

impl PacketScheduler {
    pub fn new() -> Self {
        Self {
            deferred_independent: VecDeque::new(),
            deferred_sequences: HashMap::new(),
            blocked_commands: VecDeque::new(),
            barrier_pending: false,
        }
    }

    pub fn push(&mut self, command: SchedulerCommand, now: Instant) -> Vec<SchedulerAction> {
        if matches!(command, SchedulerCommand::Clear) {
            self.deferred_independent.clear();
            self.deferred_sequences.clear();
            self.blocked_commands.clear();
            self.barrier_pending = false;
            return vec![SchedulerAction::ClearTransportState];
        }

        if self.barrier_pending {
            self.blocked_commands.push_back(command);
            return Vec::new();
        }

        if self.conflicts_with_deferred(&command) {
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
        }
        self.poll(now, false)
    }

    pub fn requeue_deferred(&mut self, envelope: DispatchEnvelope, now: Instant) {
        let scheduled = ScheduledEnvelope::new(envelope, now);
        match scheduled.envelope.domain() {
            OrderDomain::Independent => self.deferred_independent.push_front(scheduled),
            OrderDomain::Sequence(sequence_id) => {
                self.deferred_sequences.entry(sequence_id).or_default().push_front(scheduled);
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

    fn process_command_now(
        &mut self,
        command: SchedulerCommand,
        now: Instant,
    ) -> Vec<SchedulerAction> {
        match command {
            SchedulerCommand::Envelope(envelope) => {
                if should_initially_defer(&envelope) {
                    self.enqueue_deferred(envelope, now);
                    self.poll(now, false)
                } else {
                    vec![SchedulerAction::DispatchEnvelope { envelope, force_flush: false }]
                }
            },
            SchedulerCommand::SequenceClose(sequence_id) => {
                vec![SchedulerAction::CloseSequence(sequence_id)]
            },
            SchedulerCommand::SequenceCloseAll => vec![SchedulerAction::CloseAllSequences],
            SchedulerCommand::Barrier => {
                let mut actions = self.pump_deferred(now, true);
                self.barrier_pending = true;
                actions.push(SchedulerAction::BeginBarrier);
                actions
            },
            SchedulerCommand::Clear => unreachable!("clear handled before process_command_now"),
        }
    }

    fn enqueue_deferred(&mut self, envelope: DispatchEnvelope, now: Instant) {
        let scheduled = ScheduledEnvelope::new(envelope, now);
        match scheduled.envelope.domain() {
            OrderDomain::Independent => self.deferred_independent.push_back(scheduled),
            OrderDomain::Sequence(sequence_id) => {
                self.deferred_sequences.entry(sequence_id).or_default().push_back(scheduled);
            },
        }
    }

    fn conflicts_with_deferred(&self, command: &SchedulerCommand) -> bool {
        match command {
            SchedulerCommand::Envelope(envelope) => match envelope.domain() {
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
                    actions.push(SchedulerAction::DropEnvelope {
                        envelope: scheduled.envelope,
                        reason,
                    });
                },
                QueueHeadAction::Dispatch => {
                    let Some(scheduled) = queue.pop_front() else {
                        break;
                    };
                    actions.push(SchedulerAction::DispatchEnvelope {
                        envelope: scheduled.envelope,
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

impl ScheduledEnvelope {
    fn new(envelope: DispatchEnvelope, now: Instant) -> Self {
        let (deadline_at, coalescing_target) = match envelope.priority {
            PacketPriority::Deadline { max_delay } => {
                (now.checked_add(max_delay).or(Some(now)), None)
            },
            PacketPriority::Coalescing { target_payload_bytes } => {
                (None, Some(target_payload_bytes))
            },
            _ => (None, None),
        };
        Self { deadline_at, coalescing_target, envelope }
    }
}

fn should_initially_defer(envelope: &DispatchEnvelope) -> bool {
    matches!(envelope.priority, PacketPriority::Deadline { .. } | PacketPriority::Coalescing { .. })
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
    scheduled_messages: &VecDeque<ScheduledEnvelope>,
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

fn coalescing_run_bytes(scheduled_messages: &VecDeque<ScheduledEnvelope>) -> usize {
    let mut total = 0usize;
    for envelope in scheduled_messages {
        if envelope.coalescing_target.is_none() {
            break;
        }
        total = total.saturating_add(envelope.envelope.framed.len());
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
    ) -> DispatchEnvelope {
        DispatchEnvelope {
            id: None,
            priority,
            order,
            delivery: DeliveryPolicy::None,
            framed: vec![0; framed_len],
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

        let actions = scheduler.push(SchedulerCommand::Envelope(env.clone()), now);
        assert!(matches!(actions.as_slice(), [SchedulerAction::DispatchEnvelope { .. }]));

        scheduler.requeue_deferred(env.clone(), now);
        assert!(matches!(
            scheduler.poll(now + Duration::from_millis(10), false).as_slice(),
            [SchedulerAction::DispatchEnvelope { force_flush: false, .. }]
        ));
        scheduler.requeue_deferred(env, now);
        assert!(matches!(
            scheduler.poll(now + Duration::from_millis(60), false).as_slice(),
            [SchedulerAction::DropEnvelope { reason: DropReason::ExpiredDeadline, .. }]
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

        assert!(scheduler.push(SchedulerCommand::Envelope(first), now).is_empty());
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

        assert!(scheduler.push(SchedulerCommand::Envelope(first), now).is_empty());
        let actions = scheduler.push(SchedulerCommand::Envelope(second), now);
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|action| {
            matches!(action, SchedulerAction::DispatchEnvelope { force_flush: false, .. })
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
        scheduler.push(SchedulerCommand::Envelope(first.clone()), now);
        scheduler.requeue_deferred(first, now);

        assert!(scheduler
            .push(
                SchedulerCommand::Envelope(envelope(
                    PacketPriority::Normal,
                    PacketOrder::Sequence(blocked_sequence),
                    64,
                )),
                now,
            )
            .is_empty());

        let other_actions = scheduler.push(
            SchedulerCommand::Envelope(envelope(
                PacketPriority::Normal,
                PacketOrder::Sequence(other_sequence),
                64,
            )),
            now,
        );
        assert!(matches!(other_actions.as_slice(), [SchedulerAction::DispatchEnvelope { .. }]));
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
        scheduler.push(SchedulerCommand::Envelope(first.clone()), now);
        scheduler.requeue_deferred(first, now);

        assert!(scheduler.push(SchedulerCommand::SequenceClose(sequence_id), now).is_empty());
    }

    #[test]
    fn sequence_close_all_waits_for_all_deferred_work() {
        let now = Instant::now();
        let mut scheduler = PacketScheduler::new();
        scheduler.push(
            SchedulerCommand::Envelope(envelope(
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
        scheduler.push(SchedulerCommand::Envelope(first.clone()), now);
        scheduler.requeue_deferred(first, now);
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
        scheduler.push(SchedulerCommand::Envelope(first), now);

        let actions = scheduler.push(SchedulerCommand::Barrier, now);
        assert!(matches!(
            actions.as_slice(),
            [
                SchedulerAction::DispatchEnvelope { force_flush: true, .. },
                SchedulerAction::BeginBarrier
            ]
        ));

        assert!(scheduler
            .push(
                SchedulerCommand::Envelope(envelope(
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
            [SchedulerAction::DispatchEnvelope { force_flush: false, .. }]
        ));
    }
}
