include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use std::time::Duration;

use thiserror::Error;

use crate::game::ClientId;

pub type ActionId = u64;

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
    Deadline {
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
    /// Append this packet to the reused stream for this sequence.
    Sequence(uuid::Uuid),
    /// Append this packet to the reused stream for this sequence, then send FIN.
    SequenceEnd(uuid::Uuid),
}

pub type PacketBundle = Vec<S2CPacket>;

#[derive(Debug, Clone, Copy)]
pub enum PacketTarget {
    Client(ClientId),
    Broadcast,
    BroadcastExcept(ClientId),
}

#[derive(Clone)]
pub enum PacketPayload {
    Single(S2CPacket),
    Bundle(PacketBundle),
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
    pub identifier: Option<uuid::Uuid>,
    pub target: PacketTarget,
    pub payload: PacketPayload,
    pub priority: PacketPriority,
    pub order: PacketOrder,
}

#[derive(Clone)]
pub enum PacketMessage {
    Envelope(PacketEnvelope),
    Control(PacketControl),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PacketEnvelopeValidationError {
    #[error("droppable envelopes cannot have an identifier")]
    DroppableWithIdentifier,
    #[error("coalescing envelopes require a non-zero payload target")]
    CoalescingWithZeroTarget,
}

impl PacketEnvelope {
    pub fn new(target: PacketTarget, payload: PacketPayload) -> Self {
        Self {
            identifier: None,
            target,
            payload,
            priority: PacketPriority::default(),
            order: PacketOrder::default(),
        }
    }

    pub fn single(target: PacketTarget, packet: S2CPacket) -> Self {
        Self::new(target, PacketPayload::Single(packet))
    }

    pub fn bundle(target: PacketTarget, bundle: PacketBundle) -> Self {
        Self::new(target, PacketPayload::Bundle(bundle))
    }

    pub fn droppable(mut self) -> Self {
        self.priority = PacketPriority::Droppable;
        self
    }

    pub fn deadline(mut self, max_delay: Duration) -> Self {
        self.priority = PacketPriority::Deadline { max_delay };
        self
    }

    pub fn coalescing(mut self, target_payload_bytes: usize) -> Self {
        self.priority = PacketPriority::Coalescing { target_payload_bytes };
        self
    }

    pub fn id(mut self, identifier: uuid::Uuid) -> Self {
        self.identifier = Some(identifier);
        self
    }

    pub fn independent(mut self) -> Self {
        self.order = PacketOrder::Independent;
        self
    }

    pub fn sequence(mut self, sequence_id: uuid::Uuid) -> Self {
        self.order = PacketOrder::Sequence(sequence_id);
        self
    }

    pub fn sequence_end(mut self, sequence_id: uuid::Uuid) -> Self {
        self.order = PacketOrder::SequenceEnd(sequence_id);
        self
    }

    pub fn validate(&self) -> Result<(), PacketEnvelopeValidationError> {
        if matches!(self.priority, PacketPriority::Droppable) && self.identifier.is_some() {
            return Err(PacketEnvelopeValidationError::DroppableWithIdentifier);
        }

        if matches!(self.priority, PacketPriority::Coalescing { target_payload_bytes: 0 }) {
            return Err(PacketEnvelopeValidationError::CoalescingWithZeroTarget);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn droppable_cannot_have_identifier() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .droppable()
                .id(uuid::Uuid::nil());

        assert_eq!(
            envelope.validate(),
            Err(PacketEnvelopeValidationError::DroppableWithIdentifier)
        );
    }

    #[test]
    fn coalescing_requires_non_zero_target() {
        let envelope =
            PacketEnvelope::single(PacketTarget::Broadcast, S2CPacket::Ping { nonce: 1 })
                .coalescing(0);

        assert_eq!(
            envelope.validate(),
            Err(PacketEnvelopeValidationError::CoalescingWithZeroTarget)
        );
    }
}
