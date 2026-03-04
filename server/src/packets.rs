include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use crate::game::ClientId;

pub type StreamID = u64;

#[derive(Debug, Clone, Copy, Default)]
pub struct PacketMeta {
    pub optional: bool,
    pub stream_id: Option<StreamID>,
}

#[derive(Debug, Clone, Default)]
pub struct PacketBundle {
    pub meta: Option<PacketMeta>,
    pub packets: Vec<S2CPacket>,
}

#[derive(Clone, Copy)]
pub enum PacketTarget {
    Client(ClientId),
    Broadcast,
    BroadcastExcept(ClientId),
}

#[derive(Clone)]
pub enum PacketMessage {
    Packet(S2CPacket),
    Bundle(PacketBundle),
}

impl PacketBundle {
    pub fn new(meta: PacketMeta) -> Self {
        Self { meta: Some(meta), packets: Vec::new() }
    }

    pub fn single(packet: S2CPacket) -> Self {
        Self { meta: None, packets: vec![packet] }
    }

    pub fn with_meta(meta: PacketMeta, packets: Vec<S2CPacket>) -> Self {
        Self { meta: Some(meta), packets }
    }

    pub fn push(&mut self, packet: S2CPacket) {
        self.packets.push(packet);
    }

    pub fn extend<I>(&mut self, packets: I)
    where
        I: IntoIterator<Item = S2CPacket>,
    {
        self.packets.extend(packets);
    }
}
