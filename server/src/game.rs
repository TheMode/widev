use std::time::{Duration, Instant};

use crate::game_state::GameState;
use crate::packets::{C2SPacket, DeliveryOutcome, EnvelopeId};

pub type ClientId = u32;

#[derive(Debug)]
pub enum NetworkEvent {
    ClientConnected(ClientId),
    ClientDisconnected(ClientId),
    DeliveryUpdate {
        client_id: ClientId,
        envelope_id: EnvelopeId,
        outcome: DeliveryOutcome,
    },
    ClientPacket {
        client_id: ClientId,
        packet: C2SPacket,
    },
}

pub trait Game {
    fn on_event(&mut self, state: &mut GameState, event: NetworkEvent);
    fn on_tick(&mut self, state: &mut GameState, now: Instant, dt: Duration);
}
