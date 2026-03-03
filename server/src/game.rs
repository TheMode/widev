use std::time::{Duration, Instant};

use crate::packets::{C2SPacket, S2CPacket};

pub trait Game {
    fn on_client_packet(&mut self, packet: C2SPacket);
    fn on_tick(&mut self, now: Instant, dt: Duration);
    fn collect_bootstrap_packets(&mut self) -> Vec<S2CPacket>;
    fn collect_tick_packets(&mut self, now: Instant) -> Vec<S2CPacket>;
}
