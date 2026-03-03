use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::game::{ClientId, Game};
use crate::game_state::GameState;
use crate::packets::{
    C2SPacket, InputType, PacketBundle, PacketMessage, PacketMeta, PacketTarget, PredictionKind,
    S2CPacket, StreamID, TransformPredictionMask,
};

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

        let mut bundle = PacketBundle::with_meta(
            PacketMeta { optional: false, stream_id: Some(stream_id) },
            vec![
                S2CPacket::ServerHello { tick_rate_hz: state.ticks_per_second() },
                S2CPacket::SetGameName { name: "Red Square Multiplayer".to_string() },
                S2CPacket::SetTransformPrediction {
                    element_id: client_id,
                    enabled: true,
                    affected_mask: TransformPredictionMask::TRANSLATION,
                    kind: PredictionKind::Interpolation,
                },
                S2CPacket::AssetManifest { player_color_rgba: [255, 0, 0, 255], player_size: 32 },
            ],
        );

        let binding_packets =
            [(1, "move_up"), (2, "move_down"), (3, "move_left"), (4, "move_right")]
                .into_iter()
                .map(|(binding_id, identifier)| S2CPacket::BindingDeclare {
                    binding_id,
                    identifier: identifier.to_string(),
                    input_type: InputType::Toggle,
                });
        bundle.extend(binding_packets);

        state.send(PacketTarget::Client(client_id), PacketMessage::Bundle(bundle));
    }
}

impl Game for RedSquareGame {
    fn on_client_connected(&mut self, state: &mut GameState, client_id: ClientId) {
        let stream_id = state.create_stream(client_id).unwrap_or(3);
        let element = Self::spawn_element(client_id);
        self.players.insert(
            client_id,
            PlayerState { input: PlayerInput::default(), element, control_stream_id: stream_id },
        );

        self.send_bootstrap(state, client_id);

        let snapshots: Vec<(ClientId, ElementState)> =
            self.players.iter().map(|(id, p)| (*id, p.element)).collect();
        let mut snapshot_bundle = PacketBundle::default();
        for (element_id, e) in snapshots {
            snapshot_bundle.push(S2CPacket::ElementMoved { element_id, x: e.x, y: e.y });
            snapshot_bundle.push(S2CPacket::SetTransformPrediction {
                element_id,
                enabled: true,
                affected_mask: TransformPredictionMask::TRANSLATION,
                kind: PredictionKind::Interpolation,
            });
        }
        if !snapshot_bundle.packets.is_empty() {
            state.send(PacketTarget::Client(client_id), PacketMessage::Bundle(snapshot_bundle));
        }

        let element = self.players.get(&client_id).map(|p| p.element).unwrap_or(element);
        let mut bundle = PacketBundle::default();
        bundle.extend([
            S2CPacket::ElementMoved { element_id: client_id, x: element.x, y: element.y },
            S2CPacket::SetTransformPrediction {
                element_id: client_id,
                enabled: true,
                affected_mask: TransformPredictionMask::TRANSLATION,
                kind: PredictionKind::Interpolation,
            },
        ]);
        state.send(PacketTarget::Broadcast, PacketMessage::Bundle(bundle));

        log::info!("client {client_id} connected");
    }

    fn on_client_disconnected(&mut self, state: &mut GameState, client_id: ClientId) {
        self.players.remove(&client_id);

        state.send(
            PacketTarget::Broadcast,
            PacketMessage::Packet(S2CPacket::ElementRemoved { element_id: client_id }),
        );

        log::info!("client {client_id} disconnected");
    }

    fn on_client_packet(&mut self, _state: &mut GameState, client_id: ClientId, packet: C2SPacket) {
        match packet {
            C2SPacket::ClientHello { client_name, capabilities } => {
                log::info!("client {client_id} hello: {client_name} / {capabilities:?}");
            },
            C2SPacket::BindingAssigned { binding_id } => {
                log::info!("client {client_id} binding {binding_id} acknowledged");
            },
            C2SPacket::InputValue { binding_id, value } => {
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
        }
    }

    fn on_tick(&mut self, state: &mut GameState, _now: Instant, dt: Duration) {
        let dt_seconds = dt.as_secs_f32();
        for player in self.players.values_mut() {
            let input = player.input;

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

            player.element.x =
                (player.element.x + dx * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_WIDTH);
            player.element.y =
                (player.element.y + dy * PLAYER_SPEED * dt_seconds).clamp(0.0, GAME_HEIGHT);
        }

        let snapshots: Vec<(ClientId, ElementState)> =
            self.players.iter().map(|(id, p)| (*id, p.element)).collect();
        let mut bundle = PacketBundle::new(PacketMeta { optional: true, stream_id: None });
        for (element_id, e) in snapshots {
            bundle.push(S2CPacket::ElementMoved { element_id, x: e.x, y: e.y });
        }
        if !bundle.packets.is_empty() {
            state.send(PacketTarget::Broadcast, PacketMessage::Bundle(bundle));
        }
    }
}
