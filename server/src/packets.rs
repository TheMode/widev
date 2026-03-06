include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use std::time::Duration;
use thiserror::Error;

use crate::game::ClientId;

pub type ServerTick = u64;
pub type ActionId = u64;

#[derive(Debug, Clone, Copy, Default)]
pub enum PacketPriority {
    /// Default reliable delivery on the session's stream.
    #[default]
    Normal,
    /// Drop instead of queueing when the session is over budget.
    ///
    /// If the envelope is `Independent`, has no identifier, and its encoded
    /// payload fits a single writable QUIC datagram, the transport may send it
    /// as a datagram instead of opening a stream.
    Droppable,
    /// Hint that the packet is only useful if sent within this delay budget.
    MaxDelay(Duration),
    /// Hint that the packet expires once the server reaches this tick.
    MaxTick(ServerTick),
    /// Hint that nearby packets can be batched within this coalescing window.
    Coalesce(Duration),
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

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.priority = PacketPriority::MaxDelay(delay);
        self
    }

    pub fn max_tick(mut self, tick: ServerTick) -> Self {
        self.priority = PacketPriority::MaxTick(tick);
        self
    }

    pub fn coalesce(mut self, window: Duration) -> Self {
        self.priority = PacketPriority::Coalesce(window);
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

        Ok(())
    }
}
