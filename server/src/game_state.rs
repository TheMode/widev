use std::collections::{HashMap, VecDeque};

use crate::game::ClientId;
use crate::packets::S2CPacket;

#[derive(Clone)]
pub struct StreamPacket {
    pub stream_id: u64,
    pub packet: S2CPacket,
}

struct ClientState {
    datagram_outbox: VecDeque<S2CPacket>,
    stream_outbox: VecDeque<StreamPacket>,
    next_server_uni_stream_id: u64,
}

pub struct GameState {
    clients: HashMap<ClientId, ClientState>,
}

impl GameState {
    pub fn new() -> Self {
        Self { clients: HashMap::new() }
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

    pub fn enqueue_for(&mut self, client_id: ClientId, packet: S2CPacket) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.datagram_outbox.push_back(packet);
        }
    }

    pub fn broadcast(&mut self, packet: S2CPacket) {
        for client_id in self.connected_clients() {
            self.enqueue_for(client_id, packet.clone());
        }
    }

    pub fn create_stream(&mut self, client_id: ClientId) -> Option<u64> {
        let client = self.clients.get_mut(&client_id)?;
        let stream_id = client.next_server_uni_stream_id;
        client.next_server_uni_stream_id = client.next_server_uni_stream_id.wrapping_add(4);
        Some(stream_id)
    }

    pub fn send_packet_on_stream(
        &mut self,
        client_id: ClientId,
        stream_id: u64,
        packet: S2CPacket,
    ) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.stream_outbox.push_back(StreamPacket { stream_id, packet });
        }
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
