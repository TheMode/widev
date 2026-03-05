use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::game::ClientId;
use crate::packets::{PacketEnvelope, StreamID};

struct ClientState {
    next_server_uni_stream_id: StreamID,
}

pub struct GameState {
    clients: HashMap<ClientId, ClientState>,
    outbox: VecDeque<PacketEnvelope>,
    ticks_per_second: u16,
}

impl GameState {
    pub fn new(ticks_per_second: u16) -> Self {
        Self {
            clients: HashMap::new(),
            outbox: VecDeque::new(),
            ticks_per_second: ticks_per_second.max(1),
        }
    }

    pub fn ticks_per_second(&self) -> u16 {
        self.ticks_per_second
    }

    pub fn tick_interval(&self) -> Duration {
        Duration::from_secs_f64(1.0 / self.ticks_per_second as f64)
    }

    pub fn connect_client(&mut self, client_id: ClientId) {
        self.clients.insert(client_id, ClientState { next_server_uni_stream_id: 3 });
    }

    pub fn disconnect_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }

    pub fn send(&mut self, envelope: PacketEnvelope) {
        self.outbox.push_back(envelope);
    }

    fn alloc_stream_id(client: &mut ClientState) -> StreamID {
        let stream_id = client.next_server_uni_stream_id;
        client.next_server_uni_stream_id = client.next_server_uni_stream_id.wrapping_add(4);
        stream_id
    }

    pub fn create_stream(&mut self, client_id: ClientId) -> Option<StreamID> {
        let client = self.clients.get_mut(&client_id)?;
        Some(Self::alloc_stream_id(client))
    }

    pub fn drain_outbox(&mut self) -> Vec<PacketEnvelope> {
        let mut out = Vec::new();
        while let Some(envelope) = self.outbox.pop_front() {
            out.push(envelope);
        }
        out
    }
}
