use std::time::{Duration, Instant};

use crate::game_state::GameState;
use crate::packets::C2SPacket;

pub type ClientId = u32;

pub trait Game {
    fn on_client_connected(&mut self, state: &mut GameState, client_id: ClientId);
    fn on_client_disconnected(&mut self, state: &mut GameState, client_id: ClientId);
    fn on_client_packet(&mut self, state: &mut GameState, client_id: ClientId, packet: C2SPacket);
    fn on_tick(&mut self, state: &mut GameState, now: Instant, dt: Duration);
}
