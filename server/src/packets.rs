include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use std::time::Duration;

use thiserror::Error;

use crate::game::ClientId;

#[derive(Debug, Clone, Copy, Default)]
pub enum PacketPriority {
    /// Send as soon as practical using the normal transport path.
    ///
    /// This does not opt into unreliable delivery or intentional scheduling
    /// delay beyond ordinary transport backpressure.
    #[default]
    Normal,
    /// Prefer freshness over reliability when the session is over budget.
    ///
    /// If the envelope is `Independent`, has no identifier, and its encoded
    /// payload fits a single writable QUIC datagram, the transport may send it
    /// as a datagram instead of opening a stream. Otherwise, stream-backed
    /// sends may be dropped instead of being queued when congestion budget is
    /// exhausted.
    Droppable,
    /// Retry opportunistically until the delay budget expires.
    ///
    /// This uses the same congestion-sensitive send path as `Droppable`, but
    /// instead of dropping immediately when the session is over budget or a
    /// QUIC datagram is temporarily not writable, the transport may keep the
    /// packet queued until `max_delay` elapses.
    MaxDelay {
        max_delay: Duration,
    },
    /// Keep the packet queued until enough serialized payload has accumulated.
    ///
    /// This is intended to reduce header overhead by waiting for a larger
    /// packet-sized batch before releasing queued work.
    Coalescing {
        target_payload_bytes: usize,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub enum PacketOrder {
    /// No ordering relationship with any other packet.
    #[default]
    Independent,
    /// Declare a client-visible dependency on another envelope's id.
    Dependency(MessageId),
    /// Append this packet to the reused stream for this sequence.
    Sequence(uuid::Uuid),
    /// Append this packet to the reused stream for this sequence, then send FIN.
    SequenceEnd(uuid::Uuid),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeliveryPolicy {
    #[default]
    FireAndForget,
    /// Emit a server-side transport outcome when the envelope is delivered or dropped.
    ObserveTransport,
    /// Emit transport outcomes and also require a client receipt after the full envelope is applied.
    RequireClientReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    TransportDelivered,
    TransportDropped {
        reason: DropReason,
    },
    ClientProcessed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    ExpiredDeadline,
    CongestionBudgetExceeded,
    DatagramRejected,
}

pub type PacketBundle = Vec<S2CPacket>;

#[derive(Debug, Clone, Copy)]
pub enum PacketTarget {
    Client(ClientId),
    Broadcast,
    BroadcastExcept(ClientId),
}

#[derive(Debug, Clone, Copy)]
pub struct PacketMeta {
    pub target: PacketTarget,
    pub priority: PacketPriority,
    pub order: PacketOrder,
    pub delivery: DeliveryPolicy,
}

impl PacketMeta {
    fn new(target: PacketTarget) -> Self {
        Self {
            target,
            priority: PacketPriority::default(),
            order: PacketOrder::default(),
            delivery: DeliveryPolicy::default(),
        }
    }

    fn set_droppable(&mut self) {
        self.priority = PacketPriority::Droppable;
    }

    fn set_max_delay(&mut self, max_delay: Duration) {
        self.priority = PacketPriority::MaxDelay { max_delay };
    }

    fn set_coalescing(&mut self, target_payload_bytes: usize) {
        self.priority = PacketPriority::Coalescing { target_payload_bytes };
    }

    fn set_delivery(&mut self, delivery: DeliveryPolicy) {
        self.delivery = delivery;
    }

    fn set_independent(&mut self) {
        self.order = PacketOrder::Independent;
    }

    fn set_dependency(&mut self, message_id: MessageId) {
        self.order = PacketOrder::Dependency(message_id);
    }

    fn set_sequence(&mut self, sequence_id: uuid::Uuid) {
        self.order = PacketOrder::Sequence(sequence_id);
    }

    fn set_sequence_end(&mut self, sequence_id: uuid::Uuid) {
        self.order = PacketOrder::SequenceEnd(sequence_id);
    }
}

#[derive(Clone)]
pub enum PacketPayload {
    Single(S2CPacket),
    Bundle(PacketBundle),
}

#[derive(Clone)]
pub struct PacketResource {
    pub id: MessageId,
    pub resource_type: String,
    pub blob: Vec<u8>,
    pub usage_count: Option<i32>,
    pub meta: PacketMeta,
}

#[derive(Debug, Clone, Copy)]
pub enum PacketControl {
    SequenceClose {
        sequence_id: uuid::Uuid,
    },
    SequenceCloseAll {
        target: PacketTarget,
    },
    /// Drop queued work for the target and terminate any active sequence.
    Clear {
        target: PacketTarget,
    },
    /// Block later messages for the target until all currently inflight
    /// transport writes for that target have completed locally.
    Barrier {
        target: PacketTarget,
    },
}

#[derive(Clone)]
pub struct PacketEnvelope {
    pub id: Option<MessageId>,
    pub payload: PacketPayload,
    pub meta: PacketMeta,
}

#[derive(Clone)]
pub enum PacketMessage {
    Envelope(PacketEnvelope),
    Resource(PacketResource),
    Control(PacketControl),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageValidationError {
    #[error("resource usage count must be non-negative or null for permanency")]
    InvalidUsageCount,
    #[error("droppable messages cannot have an id")]
    DroppableWithId,
    #[error("delivery-tracked messages require an id")]
    DeliveryRequiresId,
    #[error("droppable messages cannot require a client receipt")]
    DroppableWithClientReceipt,
    #[error("coalescing messages require a non-zero payload target")]
    CoalescingWithZeroTarget,
}

impl PacketEnvelope {
    pub fn new(target: PacketTarget, payload: PacketPayload) -> Self {
        Self { id: None, payload, meta: PacketMeta::new(target) }
    }

    pub fn single(target: PacketTarget, packet: S2CPacket) -> Self {
        Self::new(target, PacketPayload::Single(packet))
    }

    pub fn bundle(target: PacketTarget, bundle: PacketBundle) -> Self {
        Self::new(target, PacketPayload::Bundle(bundle))
    }

    pub fn droppable(mut self) -> Self {
        self.meta.set_droppable();
        self
    }

    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.meta.set_max_delay(max_delay);
        self
    }

    pub fn coalescing(mut self, target_payload_bytes: usize) -> Self {
        self.meta.set_coalescing(target_payload_bytes);
        self
    }

    pub fn id(mut self, id: MessageId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn delivery(mut self, delivery: DeliveryPolicy) -> Self {
        self.meta.set_delivery(delivery);
        self
    }

    pub fn independent(mut self) -> Self {
        self.meta.set_independent();
        self
    }

    pub fn dependency(mut self, message_id: MessageId) -> Self {
        self.meta.set_dependency(message_id);
        self
    }

    pub fn sequence(mut self, sequence_id: uuid::Uuid) -> Self {
        self.meta.set_sequence(sequence_id);
        self
    }

    pub fn sequence_end(mut self, sequence_id: uuid::Uuid) -> Self {
        self.meta.set_sequence_end(sequence_id);
        self
    }

    pub fn validate(&self) -> Result<(), MessageValidationError> {
        validate_message_policy(self.id, self.meta.priority, self.meta.delivery)?;
        Ok(())
    }
}

impl PacketResource {
    pub fn new(
        target: PacketTarget,
        id: MessageId,
        resource_type: impl Into<String>,
        blob: Vec<u8>,
        usage_count: Option<i32>,
    ) -> Self {
        Self {
            id,
            resource_type: resource_type.into(),
            blob,
            usage_count,
            meta: PacketMeta::new(target),
        }
    }

    pub fn droppable(mut self) -> Self {
        self.meta.set_droppable();
        self
    }

    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.meta.set_max_delay(max_delay);
        self
    }

    pub fn coalescing(mut self, target_payload_bytes: usize) -> Self {
        self.meta.set_coalescing(target_payload_bytes);
        self
    }

    pub fn delivery(mut self, delivery: DeliveryPolicy) -> Self {
        self.meta.set_delivery(delivery);
        self
    }

    pub fn independent(mut self) -> Self {
        self.meta.set_independent();
        self
    }

    pub fn dependency(mut self, message_id: MessageId) -> Self {
        self.meta.set_dependency(message_id);
        self
    }

    pub fn sequence(mut self, sequence_id: uuid::Uuid) -> Self {
        self.meta.set_sequence(sequence_id);
        self
    }

    pub fn sequence_end(mut self, sequence_id: uuid::Uuid) -> Self {
        self.meta.set_sequence_end(sequence_id);
        self
    }

    pub fn validate(&self) -> Result<(), MessageValidationError> {
        if let Some(count) = self.usage_count {
            if count < 0 {
                return Err(MessageValidationError::InvalidUsageCount);
            }
        }
        validate_message_policy(Some(self.id), self.meta.priority, self.meta.delivery)?;
        Ok(())
    }
}

fn validate_message_policy(
    id: Option<MessageId>,
    priority: PacketPriority,
    delivery: DeliveryPolicy,
) -> Result<(), MessageValidationError> {
    let tracks_delivery =
        matches!(delivery, DeliveryPolicy::ObserveTransport | DeliveryPolicy::RequireClientReceipt);

    if matches!(priority, PacketPriority::Droppable)
        && delivery == DeliveryPolicy::RequireClientReceipt
    {
        return Err(MessageValidationError::DroppableWithClientReceipt);
    }

    if matches!(priority, PacketPriority::Droppable) && id.is_some() {
        return Err(MessageValidationError::DroppableWithId);
    }

    if tracks_delivery && id.is_none() {
        return Err(MessageValidationError::DeliveryRequiresId);
    }

    if matches!(priority, PacketPriority::Coalescing { target_payload_bytes: 0 }) {
        return Err(MessageValidationError::CoalescingWithZeroTarget);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn droppable_cannot_have_identifier() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .droppable()
                .id(1);

        assert_eq!(envelope.validate(), Err(MessageValidationError::DroppableWithId));
    }

    #[test]
    fn client_receipt_requires_id() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .delivery(DeliveryPolicy::RequireClientReceipt);

        assert_eq!(envelope.validate(), Err(MessageValidationError::DeliveryRequiresId));
    }

    #[test]
    fn transport_observation_requires_id() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .delivery(DeliveryPolicy::ObserveTransport);

        assert_eq!(envelope.validate(), Err(MessageValidationError::DeliveryRequiresId));
    }

    #[test]
    fn droppable_cannot_require_client_receipt() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .droppable()
                .id(1)
                .delivery(DeliveryPolicy::RequireClientReceipt);

        assert_eq!(envelope.validate(), Err(MessageValidationError::DroppableWithClientReceipt));
    }

    #[test]
    fn coalescing_requires_non_zero_target() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .coalescing(0);

        assert_eq!(envelope.validate(), Err(MessageValidationError::CoalescingWithZeroTarget));
    }

    #[test]
    fn resource_usage_count_must_be_non_negative() {
        let resource =
            PacketResource::new(PacketTarget::Broadcast, 1, "texture", vec![1, 2, 3], Some(-1));

        assert_eq!(resource.validate(), Err(MessageValidationError::InvalidUsageCount));
    }

    #[test]
    fn resource_allows_permanent_usage_count() {
        let resource =
            PacketResource::new(PacketTarget::Broadcast, 1, "texture", vec![1, 2, 3], None);

        assert_eq!(resource.validate(), Ok(()));
    }
}
