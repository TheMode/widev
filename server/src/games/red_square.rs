use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::game::{ClientId, Game, NetworkEvent};
use crate::game_state::GameState;
use crate::packets::{
    InputType, PacketBundle, PacketEnvelope, PacketTarget, PredictionKind, S2CPacket, StreamID,
    TransformPredictionMask,
};

const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PLAYER_SPEED: f32 = 220.0;
const OWN_PLAYER_COLOR: [f32; 4] = [0.74, 0.17, 245.0, 1.0];
const OTHER_PLAYER_COLOR: [f32; 4] = [0.65, 0.24, 29.0, 1.0];

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

struct PlayerState {
    input: PlayerInput,
    element: ElementState,
    control_stream_id: StreamID,
}

pub struct RedSquareGame {
    players: HashMap<ClientId, PlayerState>,
}

impl RedSquareGame {
    pub fn new(_started_at: Instant, _state: &mut GameState) -> Self {
        Self { players: HashMap::new() }
    }

    fn spawn_element(client_id: ClientId) -> ElementState {
        let row = (client_id as f32 % 8.0).floor();
        let col = ((client_id / 8) as f32 % 8.0).floor();
        ElementState { x: 120.0 + col * 60.0, y: 120.0 + row * 60.0 }
    }

    fn send_bootstrap(&mut self, state: &mut GameState, client_id: ClientId) {
        let Some(player) = self.players.get(&client_id) else {
            return;
        };
        let stream_id = player.control_stream_id;

        let mut bundle = vec![
            S2CPacket::ServerHello { tick_rate_hz: state.ticks_per_second() },
            S2CPacket::SetGameName { name: "Red Square Multiplayer".to_string() },
            S2CPacket::SurfaceLockAspectRatio { surface_id: 1, numerator: 4, denominator: 3 },
            S2CPacket::SurfaceClearBackground { surface_id: 1, color: [0.18, 0.02, 250.0, 1.0] },
        ];

        let binding_packets =
            [(1, "move_up"), (2, "move_down"), (3, "move_left"), (4, "move_right")]
                .into_iter()
                .map(|(binding_id, identifier)| S2CPacket::BindingDeclare {
                    binding_id,
                    identifier: identifier.to_string(),
                    input_type: InputType::Toggle,
                });
        bundle.extend(binding_packets);

        state.send(
            PacketEnvelope::bundle(PacketTarget::Client(client_id), bundle)
                .reliable()
                .with_stream(stream_id),
        );
    }
}

impl Game for RedSquareGame {
    fn on_event(&mut self, state: &mut GameState, event: NetworkEvent) {
        match event {
            NetworkEvent::ClientConnected(client_id) => {
                let stream_id = state.create_stream(client_id).unwrap_or(3);
                let element = Self::spawn_element(client_id);
                self.players.insert(
                    client_id,
                    PlayerState {
                        input: PlayerInput::default(),
                        element,
                        control_stream_id: stream_id,
                    },
                );

                self.send_bootstrap(state, client_id);

                let snapshots: Vec<ClientId> = self.players.keys().copied().collect();
                let mut snapshot_bundle: PacketBundle = Vec::new();
                for element_id in snapshots {
                    snapshot_bundle.push(S2CPacket::ElementAdd { element_id });
                    snapshot_bundle.push(S2CPacket::ElementSetColor {
                        element_id,
                        color: if element_id == client_id {
                            OWN_PLAYER_COLOR
                        } else {
                            OTHER_PLAYER_COLOR
                        },
                    });
                    snapshot_bundle.push(S2CPacket::ElementSetTransformPrediction {
                        element_id,
                        enabled: true,
                        affected_mask: TransformPredictionMask::TRANSLATION,
                        kind: PredictionKind::Interpolation,
                    });
                }
                if !snapshot_bundle.is_empty() {
                    state.send(PacketEnvelope::bundle(
                        PacketTarget::Client(client_id),
                        snapshot_bundle,
                    ));
                }

                let mut bundle: PacketBundle = Vec::new();
                bundle.extend([
                    S2CPacket::ElementAdd { element_id: client_id },
                    S2CPacket::ElementSetColor { element_id: client_id, color: OTHER_PLAYER_COLOR },
                    S2CPacket::ElementSetTransformPrediction {
                        element_id: client_id,
                        enabled: true,
                        affected_mask: TransformPredictionMask::TRANSLATION,
                        kind: PredictionKind::Interpolation,
                    },
                ]);
                state
                    .send(PacketEnvelope::bundle(PacketTarget::BroadcastExcept(client_id), bundle));

                log::info!("client {client_id} connected");
            },
            NetworkEvent::ClientDisconnected(client_id) => {
                self.players.remove(&client_id);

                state.send(PacketEnvelope::single(
                    PacketTarget::Broadcast,
                    S2CPacket::ElementRemove { element_id: client_id },
                ));

                log::info!("client {client_id} disconnected");
            },
            NetworkEvent::ClientPacket { client_id, packet } => match packet {
                crate::packets::C2SPacket::ClientHello { client_name, capabilities } => {
                    log::info!("client {client_id} hello: {client_name} / {capabilities:?}");
                },
                crate::packets::C2SPacket::BindingAssigned { binding_id } => {
                    log::info!("client {client_id} binding {binding_id} acknowledged");
                },
                crate::packets::C2SPacket::InputValue { binding_id, value } => {
                    let pressed = value >= 0.5;
                    let player = self.players.entry(client_id).or_insert(PlayerState {
                        input: PlayerInput::default(),
                        element: Self::spawn_element(client_id),
                        control_stream_id: 3,
                    });
                    let input = &mut player.input;
                    match binding_id {
                        1 => input.up = pressed,
                        2 => input.down = pressed,
                        3 => input.left = pressed,
                        4 => input.right = pressed,
                        _ => {},
                    }
                },
                crate::packets::C2SPacket::SurfaceList { surfaces } => {
                    log::info!("client {client_id} surfaces: {surfaces:?}");
                },
                crate::packets::C2SPacket::SurfaceResized { surface_id, width, height } => {
                    log::info!(
                        "client {client_id} surface {surface_id} resized to {}x{}",
                        width,
                        height
                    );
                },
                crate::packets::C2SPacket::Ping { .. } | crate::packets::C2SPacket::Pong { .. } => {
                },
            },
        }
    }

    fn on_tick(&mut self, state: &mut GameState, _now: Instant, dt: Duration) {
        let dt_seconds = dt.as_secs_f32();
        for player in self.players.values_mut() {
            let input = player.input;
            let dx = (input.right as i8 - input.left as i8) as f32;
            let dy = (input.down as i8 - input.up as i8) as f32;

            player.element.x =
                (player.element.x + dx * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_WIDTH);
            player.element.y =
                (player.element.y + dy * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_HEIGHT);
        }

        if !self.players.is_empty() {
            let bundle: PacketBundle = self
                .players
                .iter()
                .map(|(&element_id, player)| S2CPacket::ElementMove {
                    element_id,
                    x: player.element.x,
                    y: player.element.y,
                })
                .collect();
            state.send(PacketEnvelope::bundle(PacketTarget::Broadcast, bundle).droppable());
        }
    }
}
