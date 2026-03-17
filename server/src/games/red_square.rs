use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::game::{ClientId, Game, NetworkEvent};
use crate::game_state::GameState;
use crate::packets::{
    DeliveryOutcome, DeliveryPolicy, InputType, MessageId, PacketBundle, PacketEnvelope,
    PacketResource, PacketTarget, PredictionKind, S2CPacket, TransformPredictionMask,
};

const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PLAYER_SPEED: f32 = 220.0;
const PLAYER_TEXTURE_SIZE: u32 = 32;

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
    bootstrap_sequence_id: Uuid,
}

pub struct RedSquareGame {
    players: HashMap<ClientId, PlayerState>,
    player_texture_id: MessageId,
    player_texture_png: Vec<u8>,
}

impl RedSquareGame {
    pub fn new(_started_at: Instant, state: &mut GameState) -> Self {
        Self {
            players: HashMap::new(),
            player_texture_id: state.alloc_message_id(),
            player_texture_png: encode_red_square_png(PLAYER_TEXTURE_SIZE),
        }
    }

    fn spawn_element(client_id: ClientId) -> ElementState {
        let row = (client_id as f32 % 8.0).floor();
        let col = ((client_id / 8) as f32 % 8.0).floor();
        ElementState { x: 120.0 + col * 60.0, y: 120.0 + row * 60.0 }
    }

    fn new_player(client_id: ClientId) -> PlayerState {
        PlayerState {
            input: PlayerInput::default(),
            element: Self::spawn_element(client_id),
            bootstrap_sequence_id: Uuid::now_v7(),
        }
    }

    fn send_bootstrap(&mut self, state: &mut GameState, client_id: ClientId) {
        let Some(player) = self.players.get(&client_id) else {
            return;
        };
        let bootstrap_sequence_id = player.bootstrap_sequence_id;
        let message_id = state.alloc_message_id();

        let mut bundle = vec![
            S2CPacket::ServerHello { tick_rate_hz: state.ticks_per_second() },
            S2CPacket::SetGameName { name: "Red Square Multiplayer".to_string() },
            S2CPacket::SurfaceLockDimensions {
                surface_id: 1,
                width: GAME_WIDTH as u32,
                height: GAME_HEIGHT as u32,
            },
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

        let snapshots: Vec<ClientId> = self.players.keys().copied().collect();
        for element_id in snapshots {
            let Some(other_player) = self.players.get(&element_id) else {
                continue;
            };
            bundle.push(S2CPacket::ElementAdd { element_id });
            bundle.push(S2CPacket::ElementSetTexture {
                element_id,
                resource_id: self.player_texture_id,
            });
            bundle.push(S2CPacket::ElementSetTransformPrediction {
                element_id,
                enabled: true,
                affected_mask: TransformPredictionMask::TRANSLATION,
                kind: PredictionKind::Interpolation,
            });
            bundle.push(S2CPacket::ElementMove {
                element_id,
                x: other_player.element.x,
                y: other_player.element.y,
            });
        }
        bundle.push(S2CPacket::Join {});

        state.send_resource(
            PacketResource::new(
                PacketTarget::Client(client_id),
                self.player_texture_id,
                "image/png",
                self.player_texture_png.clone(),
                None,
            )
            .sequence(bootstrap_sequence_id),
        );
        state.send(
            PacketEnvelope::bundle(PacketTarget::Client(client_id), bundle)
                .id(message_id)
                .delivery(DeliveryPolicy::RequireClientReceipt)
                .sequence(bootstrap_sequence_id),
        );
    }
}

impl Game for RedSquareGame {
    fn on_event(&mut self, state: &mut GameState, event: NetworkEvent) {
        match event {
            NetworkEvent::ClientConnected(client_id) => {
                self.players.insert(client_id, Self::new_player(client_id));

                self.send_bootstrap(state, client_id);

                let mut bundle: PacketBundle = Vec::new();
                bundle.extend([
                    S2CPacket::ElementAdd { element_id: client_id },
                    S2CPacket::ElementSetTexture {
                        element_id: client_id,
                        resource_id: self.player_texture_id,
                    },
                    S2CPacket::ElementSetTransformPrediction {
                        element_id: client_id,
                        enabled: true,
                        affected_mask: TransformPredictionMask::TRANSLATION,
                        kind: PredictionKind::Interpolation,
                    },
                    S2CPacket::ElementMove {
                        element_id: client_id,
                        x: self.players[&client_id].element.x,
                        y: self.players[&client_id].element.y,
                    },
                ]);
                state
                    .send(PacketEnvelope::bundle(PacketTarget::BroadcastExcept(client_id), bundle));

                log::info!("client {client_id} connected");
            },
            NetworkEvent::DeliveryUpdate { client_id, message_id, outcome } => match outcome {
                DeliveryOutcome::TransportDelivered => {
                    log::info!(
                        "transport delivered message {message_id:032x} to client {client_id}"
                    );
                },
                DeliveryOutcome::TransportDropped { reason } => {
                    log::info!(
                            "transport dropped message {message_id:032x} for client {client_id}: {reason:?}"
                        );
                },
                DeliveryOutcome::ClientProcessed => {
                    log::info!("client {client_id} processed message {message_id:032x}");
                },
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
                    let player =
                        self.players.entry(client_id).or_insert(Self::new_player(client_id));
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
                _ => {},
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

fn encode_red_square_png(size: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(Cursor::new(&mut bytes), size, size);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().expect("encoding generated texture header");
        let mut rgba = Vec::with_capacity((size * size * 4) as usize);
        for _ in 0..(size * size) {
            rgba.extend_from_slice(&[0xFF, 0x24, 0x24, 0xFF]);
        }
        writer.write_image_data(&rgba).expect("encoding generated texture payload");
    }
    bytes
}
