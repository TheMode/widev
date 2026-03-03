use std::time::{Duration, Instant};

use crate::game::Game;
use crate::packets::{C2SPacket, InputType, S2CPacket};

const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PLAYER_SPEED: f32 = 220.0;

pub struct RedSquareGame {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
    last_input_seq: u32,
    pos_x: f32,
    pos_y: f32,
    started_at: Instant,
    last_world_send: Instant,
    world_send_interval: Duration,
}

impl RedSquareGame {
    pub fn new(started_at: Instant) -> Self {
        Self {
            up: false,
            down: false,
            left: false,
            right: false,
            last_input_seq: 0,
            pos_x: GAME_WIDTH * 0.5,
            pos_y: GAME_HEIGHT * 0.5,
            started_at,
            last_world_send: started_at,
            world_send_interval: Duration::from_millis(33),
        }
    }
}

impl Game for RedSquareGame {
    fn on_client_packet(&mut self, packet: C2SPacket) {
        match packet {
            C2SPacket::ClientHello {
                client_name,
                capabilities,
            } => {
                println!("client hello: {client_name} / {capabilities:?}");
            }
            C2SPacket::BindingAssigned { binding_id } => {
                println!("binding {binding_id} acknowledged by client");
            }
            C2SPacket::InputValue { binding_id, value } => {
                let pressed = value >= 0.5;
                self.last_input_seq = self.last_input_seq.wrapping_add(1);
                match binding_id {
                    1 => self.up = pressed,
                    2 => self.down = pressed,
                    3 => self.left = pressed,
                    4 => self.right = pressed,
                    _ => {}
                }
            }
        }
    }

    fn on_tick(&mut self, _now: Instant, dt: Duration) {
        let mut dx = 0.0;
        let mut dy = 0.0;

        if self.left {
            dx -= 1.0;
        }
        if self.right {
            dx += 1.0;
        }
        if self.up {
            dy -= 1.0;
        }
        if self.down {
            dy += 1.0;
        }

        let dt_seconds = dt.as_secs_f32();
        self.pos_x = (self.pos_x + dx * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_WIDTH);
        self.pos_y = (self.pos_y + dy * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_HEIGHT);
    }

    fn collect_bootstrap_packets(&mut self) -> Vec<S2CPacket> {
        vec![
            S2CPacket::ServerHello { tick_rate_hz: 60 },
            S2CPacket::AssetManifest {
                player_color_rgba: [255, 0, 0, 255],
                player_size: 32,
            },
            S2CPacket::BindingDeclare {
                binding_id: 1,
                identifier: "move_up".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::BindingDeclare {
                binding_id: 2,
                identifier: "move_down".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::BindingDeclare {
                binding_id: 3,
                identifier: "move_left".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::BindingDeclare {
                binding_id: 4,
                identifier: "move_right".to_string(),
                input_type: InputType::Toggle,
            },
        ]
    }

    fn collect_tick_packets(&mut self, now: Instant) -> Vec<S2CPacket> {
        if now.duration_since(self.last_world_send) < self.world_send_interval {
            return Vec::new();
        }

        self.last_world_send = now;

        vec![S2CPacket::WorldState {
            server_time_ms: self.started_at.elapsed().as_millis() as u64,
            x: self.pos_x,
            y: self.pos_y,
        }]
    }
}
