use std::collections::VecDeque;
use std::time::Duration;

use crate::packets::{EnvelopeId, PacketControl, PacketEnvelope, PacketMessage};

pub struct GameState {
    outbox: VecDeque<PacketMessage>,
    next_envelope_id: EnvelopeId,
    ticks_per_second: u16,
}

impl GameState {
    pub fn new(ticks_per_second: u16) -> Self {
        Self {
            outbox: VecDeque::new(),
            next_envelope_id: 1,
            ticks_per_second: ticks_per_second.max(1),
        }
    }

    pub fn ticks_per_second(&self) -> u16 {
        self.ticks_per_second
    }

    pub fn tick_interval(&self) -> Duration {
        Duration::from_secs_f64(1.0 / self.ticks_per_second as f64)
    }

    pub fn send(&mut self, envelope: PacketEnvelope) {
        self.outbox.push_back(PacketMessage::Envelope(envelope));
    }

    pub fn alloc_envelope_id(&mut self) -> EnvelopeId {
        let id = self.next_envelope_id;
        self.next_envelope_id = self.next_envelope_id.wrapping_add(1).max(1);
        id
    }

    pub fn control(&mut self, control: PacketControl) {
        self.outbox.push_back(PacketMessage::Control(control));
    }

    pub fn drain_outbox(&mut self) -> Vec<PacketMessage> {
        let mut out = Vec::new();
        while let Some(message) = self.outbox.pop_front() {
            out.push(message);
        }
        out
    }
}
