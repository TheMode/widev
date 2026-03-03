use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::game::{ClientId, Game};
use crate::game_state::GameState;
use crate::packets::{C2SPacket, InputType, S2CPacket};

const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PLAYER_SPEED: f32 = 220.0;

#[derive(Default, Clone, Copy)]
struct PlayerInput {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
}

#[derive(Clone, Copy)]
struct ElementState {
    x: f32,
    y: f32,
}

pub struct RedSquareGame {
    inputs: HashMap<ClientId, PlayerInput>,
    elements: HashMap<ClientId, ElementState>,
    last_world_send: Instant,
    world_send_interval: Duration,
}

impl RedSquareGame {
    pub fn new(started_at: Instant, _state: &mut GameState) -> Self {
        Self {
            inputs: HashMap::new(),
            elements: HashMap::new(),
            last_world_send: started_at,
            world_send_interval: Duration::from_millis(33),
        }
    }

    fn spawn_element(client_id: ClientId) -> ElementState {
        let row = (client_id as f32 % 8.0).floor();
        let col = ((client_id / 8) as f32 % 8.0).floor();
        ElementState {
            x: 120.0 + col * 60.0,
            y: 120.0 + row * 60.0,
        }
    }

    fn send_bootstrap(&mut self, state: &mut GameState, client_id: ClientId) {
        state.enqueue_for(client_id, S2CPacket::ServerHello { tick_rate_hz: 60 });
        state.enqueue_for(
            client_id,
            S2CPacket::SetGameName {
                name: "Red Square Multiplayer".to_string(),
            },
        );
        state.enqueue_for(
            client_id,
            S2CPacket::AssetManifest {
                player_color_rgba: [255, 0, 0, 255],
                player_size: 32,
            },
        );

        for (binding_id, identifier) in [
            (1, "move_up"),
            (2, "move_down"),
            (3, "move_left"),
            (4, "move_right"),
        ] {
            state.enqueue_for(
                client_id,
                S2CPacket::BindingDeclare {
                    binding_id,
                    identifier: identifier.to_string(),
                    input_type: InputType::Toggle,
                },
            );
        }
    }
}

impl Game for RedSquareGame {
    fn on_client_connected(&mut self, state: &mut GameState, client_id: ClientId) {
        state.connect_client(client_id);
        self.inputs.insert(client_id, PlayerInput::default());

        let element = Self::spawn_element(client_id);
        self.elements.insert(client_id, element);

        self.send_bootstrap(state, client_id);

        let snapshots: Vec<(ClientId, ElementState)> =
            self.elements.iter().map(|(id, e)| (*id, *e)).collect();
        for (element_id, e) in snapshots {
            state.enqueue_for(
                client_id,
                S2CPacket::ElementMoved {
                    element_id,
                    x: e.x,
                    y: e.y,
                },
            );
        }

        state.broadcast(S2CPacket::ElementMoved {
            element_id: client_id,
            x: element.x,
            y: element.y,
        });

        println!("client {client_id} connected");
    }

    fn on_client_disconnected(&mut self, state: &mut GameState, client_id: ClientId) {
        state.disconnect_client(client_id);
        self.inputs.remove(&client_id);
        self.elements.remove(&client_id);

        state.broadcast(S2CPacket::ElementRemoved {
            element_id: client_id,
        });

        println!("client {client_id} disconnected");
    }

    fn on_client_packet(&mut self, _state: &mut GameState, client_id: ClientId, packet: C2SPacket) {
        match packet {
            C2SPacket::ClientHello {
                client_name,
                capabilities,
            } => {
                println!("client {client_id} hello: {client_name} / {capabilities:?}");
            }
            C2SPacket::BindingAssigned { binding_id } => {
                println!("client {client_id} binding {binding_id} acknowledged");
            }
            C2SPacket::InputValue { binding_id, value } => {
                let pressed = value >= 0.5;
                let input = self.inputs.entry(client_id).or_default();
                match binding_id {
                    1 => input.up = pressed,
                    2 => input.down = pressed,
                    3 => input.left = pressed,
                    4 => input.right = pressed,
                    _ => {}
                }
            }
        }
    }

    fn on_tick(&mut self, state: &mut GameState, now: Instant, dt: Duration) {
        let dt_seconds = dt.as_secs_f32();
        for (client_id, element) in &mut self.elements {
            let input = self.inputs.get(client_id).copied().unwrap_or_default();

            let mut dx = 0.0;
            let mut dy = 0.0;
            if input.left {
                dx -= 1.0;
            }
            if input.right {
                dx += 1.0;
            }
            if input.up {
                dy -= 1.0;
            }
            if input.down {
                dy += 1.0;
            }

            element.x = (element.x + dx * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_WIDTH);
            element.y = (element.y + dy * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_HEIGHT);
        }

        if now.duration_since(self.last_world_send) < self.world_send_interval {
            return;
        }
        self.last_world_send = now;

        let snapshots: Vec<(ClientId, ElementState)> =
            self.elements.iter().map(|(id, e)| (*id, *e)).collect();
        for (element_id, e) in snapshots {
            state.broadcast(S2CPacket::ElementMoved {
                element_id,
                x: e.x,
                y: e.y,
            });
        }
    }
}
