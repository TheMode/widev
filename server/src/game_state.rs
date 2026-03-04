use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::game::ClientId;
use crate::packets::{PacketEnvelope, PacketMeta, PacketPayload, PacketTarget, S2CPacket, StreamID};

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

    pub fn send(&mut self, envelope: PacketEnvelope) {
        let packets = match envelope.payload {
            PacketPayload::Single(packet) => vec![packet],
            PacketPayload::Bundle(bundle) => bundle,
        };
        let meta = envelope.meta.unwrap_or_default();
        match envelope.target {
            PacketTarget::Client(client_id) => {
                let Some(client) = self.clients.get_mut(&client_id) else {
                    return;
                };
                Self::enqueue_packets(client, packets, meta);
            },
            PacketTarget::Broadcast => {
                for client in self.clients.values_mut() {
                    Self::enqueue_packets(client, packets.iter().cloned(), meta);
                }
            },
            PacketTarget::BroadcastExcept(excluded_client_id) => {
                for (&client_id, client) in self.clients.iter_mut() {
                    if client_id == excluded_client_id {
                        continue;
                    }
                    Self::enqueue_packets(client, packets.iter().cloned(), meta);
                }
            },
        }
    }

    fn enqueue_packets<I>(client: &mut ClientState, packets: I, meta: PacketMeta)
    where
        I: IntoIterator<Item = S2CPacket>,
    {
        if meta.optional {
            client.datagram_outbox.extend(packets);
            return;
        }

        let stream_id = meta.stream_id.unwrap_or(Self::DEFAULT_RELIABLE_STREAM_ID);
        client.stream_outbox.extend(packets.into_iter().map(|packet| StreamPacket {
            stream_id,
            packet,
        }));
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
