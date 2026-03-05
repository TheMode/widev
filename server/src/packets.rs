include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use std::time::Duration;

use crate::game::ClientId;

pub type StreamID = u64;
pub type ServerTick = u64;
pub type ActionId = u64;

#[derive(Debug, Clone, Copy, Default)]
pub enum PacketPriority {
    #[default]
    Normal,
    Droppable,
    MaxDelay(Duration),
    MaxTick(ServerTick),
    Coalesce(Duration),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ClientPacketMeta {
    /// Client tick at which the payload should be interpreted.
    pub execute_at_tick: Option<ServerTick>,
    /// Client tick deadline after which the payload should be discarded.
    pub read_deadline_tick: Option<ServerTick>,
    /// Client-side action bundle identifier for deferred execution.
    pub action_id: Option<ActionId>,
}

pub type PacketBundle = Vec<S2CPacket>;

#[derive(Clone, Copy)]
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

#[derive(Debug, Clone)]
pub struct StreamSync {
    pub sequential_stream_id: StreamID,
    pub wait_for_fin_stream_ids: Vec<StreamID>,
}

#[derive(Clone)]
pub struct PacketEnvelope {
    pub target: PacketTarget,
    pub payload: PacketPayload,
    pub priority: PacketPriority,
    pub client_meta: ClientPacketMeta,
    pub sync: Option<StreamSync>,
}

impl PacketEnvelope {
    pub fn new(target: PacketTarget, payload: PacketPayload) -> Self {
        Self {
            target,
            payload,
            priority: PacketPriority::default(),
            client_meta: ClientPacketMeta::default(),
            sync: None,
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

    pub fn reliable(mut self) -> Self {
        self.priority = PacketPriority::Normal;
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

    pub fn with_stream_sync(mut self, dependencies: StreamSync) -> Self {
        self.sync = Some(dependencies);
        self
    }

    pub fn with_stream(mut self, stream_id: StreamID) -> Self {
        self.sync = Some(StreamSync {
            sequential_stream_id: stream_id,
            wait_for_fin_stream_ids: Vec::new(),
        });
        self
    }
}
