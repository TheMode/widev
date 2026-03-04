include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use crate::game::ClientId;

pub type StreamID = u64;

#[derive(Debug, Clone, Copy, Default)]
pub struct PacketMeta {
    pub optional: bool,
    pub stream_id: Option<StreamID>,
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

#[derive(Clone)]
pub struct PacketEnvelope {
    pub target: PacketTarget,
    pub payload: PacketPayload,
    pub meta: Option<PacketMeta>,
}
