include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));

use crate::game::ClientId;

pub type StreamID = u64;

#[derive(Debug, Clone, Copy, Default)]
pub struct PacketMeta {
    pub optional: bool,
    pub stream_id: Option<StreamID>,
}

#[derive(Debug, Clone)]
pub struct PacketWithMeta {
    pub packet: S2CPacket,
    pub meta: Option<PacketMeta>,
}

#[derive(Debug, Clone, Default)]
pub struct PacketBundle {
    pub meta: Option<PacketMeta>,
    pub packets: Vec<PacketWithMeta>,
}

#[derive(Clone, Copy)]
pub enum PacketTarget {
    Client(ClientId),
    Broadcast,
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
        Self { meta: None, packets: vec![PacketWithMeta { packet, meta: None }] }
    }

    pub fn with_meta(meta: PacketMeta, packets: Vec<S2CPacket>) -> Self {
        Self {
            meta: Some(meta),
            packets: packets
                .into_iter()
                .map(|packet| PacketWithMeta { packet, meta: None })
                .collect(),
        }
    }

    pub fn push(&mut self, packet: S2CPacket) {
        self.packets.push(PacketWithMeta { packet, meta: None });
    }

    pub fn push_with_meta(&mut self, packet: S2CPacket, meta: PacketMeta) {
        self.packets.push(PacketWithMeta { packet, meta: Some(meta) });
    }
}
