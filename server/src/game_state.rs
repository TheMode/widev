use std::collections::{HashMap, HashSet, VecDeque};

use crate::game::ClientId;
use crate::packets::S2CPacket;

pub struct GameState {
    clients: HashSet<ClientId>,
    outbox: HashMap<ClientId, VecDeque<S2CPacket>>,
}

impl GameState {
    pub fn new() -> Self {
        Self {
            clients: HashSet::new(),
            outbox: HashMap::new(),
        }
    }

    pub fn connect_client(&mut self, client_id: ClientId) {
        self.clients.insert(client_id);
        self.outbox.entry(client_id).or_default();
    }

    pub fn disconnect_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
        self.outbox.remove(&client_id);
    }

    pub fn connected_clients(&self) -> Vec<ClientId> {
        self.clients.iter().copied().collect()
    }

    pub fn enqueue_for(&mut self, client_id: ClientId, packet: S2CPacket) {
        self.outbox.entry(client_id).or_default().push_back(packet);
    }

    pub fn broadcast(&mut self, packet: S2CPacket) {
        for client_id in self.connected_clients() {
            self.enqueue_for(client_id, packet.clone());
        }
    }

    pub fn drain_packets_for(&mut self, client_id: ClientId) -> Vec<S2CPacket> {
        let mut out = Vec::new();
        if let Some(queue) = self.outbox.get_mut(&client_id) {
            while let Some(packet) = queue.pop_front() {
                out.push(packet);
            }
        }
        out
    }
}
