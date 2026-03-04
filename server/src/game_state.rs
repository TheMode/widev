use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::game::ClientId;
use crate::packets::{PacketBundle, PacketMessage, PacketTarget, S2CPacket, StreamID};

#[derive(Clone)]
pub struct StreamPacket {
    pub stream_id: StreamID,
    pub packet: S2CPacket,
}

struct ClientState {
    datagram_outbox: VecDeque<S2CPacket>,
    stream_outbox: VecDeque<StreamPacket>,
    next_server_uni_stream_id: StreamID,
}

pub struct GameState {
    clients: HashMap<ClientId, ClientState>,
    ticks_per_second: u16,
}

impl GameState {
    const DEFAULT_RELIABLE_STREAM_ID: StreamID = 3;

    pub fn new(ticks_per_second: u16) -> Self {
        Self { clients: HashMap::new(), ticks_per_second: ticks_per_second.max(1) }
    }

    pub fn ticks_per_second(&self) -> u16 {
        self.ticks_per_second
    }

    pub fn tick_interval(&self) -> Duration {
        Duration::from_secs_f64(1.0 / self.ticks_per_second as f64)
    }

    pub fn connect_client(&mut self, client_id: ClientId) {
        self.clients.insert(
            client_id,
            ClientState {
                datagram_outbox: VecDeque::new(),
                stream_outbox: VecDeque::new(),
                next_server_uni_stream_id: 3,
            },
        );
    }

    pub fn disconnect_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }

    pub fn connected_clients(&self) -> Vec<ClientId> {
        self.clients.keys().copied().collect()
    }

    pub fn send(&mut self, target: PacketTarget, message: PacketMessage) {
        let bundle = match message {
            PacketMessage::Packet(packet) => PacketBundle::single(packet),
            PacketMessage::Bundle(bundle) => bundle,
        };
        match target {
            PacketTarget::Client(client_id) => {
                let Some(client) = self.clients.get_mut(&client_id) else {
                    return;
                };
                let meta = bundle.meta.unwrap_or_default();
                for packet in bundle.packets {
                    if meta.optional {
                        client.datagram_outbox.push_back(packet);
                        continue;
                    }
                    let stream_id = meta.stream_id.unwrap_or(Self::DEFAULT_RELIABLE_STREAM_ID);
                    client.stream_outbox.push_back(StreamPacket { stream_id, packet });
                }
            },
            PacketTarget::Broadcast => {
                let meta = bundle.meta.unwrap_or_default();
                for client_id in self.connected_clients() {
                    let Some(client) = self.clients.get_mut(&client_id) else {
                        continue;
                    };
                    for packet in &bundle.packets {
                        if meta.optional {
                            client.datagram_outbox.push_back(packet.clone());
                            continue;
                        }
                        let stream_id = meta.stream_id.unwrap_or(Self::DEFAULT_RELIABLE_STREAM_ID);
                        client.stream_outbox.push_back(StreamPacket {
                            stream_id,
                            packet: packet.clone(),
                        });
                    }
                }
            },
            PacketTarget::BroadcastExcept(excluded_client_id) => {
                let meta = bundle.meta.unwrap_or_default();
                for client_id in self.connected_clients() {
                    if client_id == excluded_client_id {
                        continue;
                    }
                    let Some(client) = self.clients.get_mut(&client_id) else {
                        continue;
                    };
                    for packet in &bundle.packets {
                        if meta.optional {
                            client.datagram_outbox.push_back(packet.clone());
                            continue;
                        }
                        let stream_id = meta.stream_id.unwrap_or(Self::DEFAULT_RELIABLE_STREAM_ID);
                        client.stream_outbox.push_back(StreamPacket {
                            stream_id,
                            packet: packet.clone(),
                        });
                    }
                }
            },
        }
    }

    pub fn create_stream(&mut self, client_id: ClientId) -> Option<StreamID> {
        let client = self.clients.get_mut(&client_id)?;
        let stream_id = client.next_server_uni_stream_id;
        client.next_server_uni_stream_id = client.next_server_uni_stream_id.wrapping_add(4);
        Some(stream_id)
    }

    pub fn drain_datagrams_for(&mut self, client_id: ClientId) -> Vec<S2CPacket> {
        let mut out = Vec::new();
        if let Some(client) = self.clients.get_mut(&client_id) {
            while let Some(packet) = client.datagram_outbox.pop_front() {
                out.push(packet);
            }
        }
        out
    }

    pub fn drain_stream_packets_for(&mut self, client_id: ClientId) -> Vec<StreamPacket> {
        let mut out = Vec::new();
        if let Some(client) = self.clients.get_mut(&client_id) {
            while let Some(packet) = client.stream_outbox.pop_front() {
                out.push(packet);
            }
        }
        out
    }
}
