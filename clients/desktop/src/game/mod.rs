use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Result;
use sha2::{Digest, Sha256};
use winit::keyboard::KeyCode;

mod app;
mod input_path_winit;
mod network;
mod persistence;
mod protocol;
mod renderer;

const INPUT_RESEND_EVERY_FRAMES: u16 = 8;
const LERP_ALPHA: f32 = 0.35;
const PREDICTION_CORRECTION_ALPHA: f32 = 0.12;
const FIXED_FRAME_DT_SECONDS: f32 = 1.0 / 60.0;

pub struct GameConfig {
    pub server_addr: SocketAddr,
}

#[derive(Clone, Copy)]
pub(super) struct RenderState {
    pub(super) x: f32,
    pub(super) y: f32,
    pub(super) size: u16,
    pub(super) color: u32,
}

pub(super) struct BindingPromptState {
    pub(super) identifier: String,
    pub(super) input_type: protocol::InputType,
    pub(super) suggestion: Option<KeyCode>,
}

struct BindingDefinition {
    id: u16,
    identifier: String,
    input_type: protocol::InputType,
}

struct BindingAssignment {
    id: u16,
    key: KeyCode,
    last_value: f32,
    frames_since_send: u16,
}

#[derive(Clone, Copy)]
struct ElementState {
    last_authoritative_x: f32,
    last_authoritative_y: f32,
    last_authoritative_at: Instant,
    target_x: f32,
    target_y: f32,
    draw_x: f32,
    draw_y: f32,
    velocity_x: f32,
    velocity_y: f32,
    prediction: PredictionConfig,
}

#[derive(Clone, Copy)]
struct PredictionConfig {
    enabled: bool,
    affected_mask: protocol::TransformPredictionMask,
    kind: protocol::PredictionKind,
}

impl Default for PredictionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            affected_mask: protocol::TransformPredictionMask::TRANSLATION,
            kind: protocol::PredictionKind::Interpolation,
        }
    }
}

impl PredictionConfig {
    fn affects_translation(self) -> bool {
        self.enabled && self.affected_mask.contains(protocol::TransformPredictionMask::TRANSLATION)
    }
}

pub(super) struct ClientGame {
    net: network::QuicClient,
    sent_hello: bool,
    server_cert_fingerprint: Option<String>,
    draw_size: u16,
    draw_color_rgba: [u8; 4],
    elements: HashMap<u32, ElementState>,
    game_name: String,
    pending_bindings: VecDeque<BindingDefinition>,
    binding_suggestion: Option<KeyCode>,
    active_bindings: Vec<BindingAssignment>,
    binding_store: persistence::BindingStore,
    default_prediction: PredictionConfig,
    pending_prediction: HashMap<u32, PredictionConfig>,
}

impl ClientGame {
    pub(super) fn new(server_addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            net: network::QuicClient::connect(server_addr)?,
            sent_hello: false,
            server_cert_fingerprint: None,
            draw_size: 32,
            draw_color_rgba: [255, 0, 0, 255],
            elements: HashMap::new(),
            game_name: "widev desktop POC".to_string(),
            pending_bindings: VecDeque::new(),
            binding_suggestion: None,
            active_bindings: Vec::new(),
            binding_store: persistence::BindingStore::load_default()?,
            default_prediction: PredictionConfig::default(),
            pending_prediction: HashMap::new(),
        })
    }

    pub(super) fn tick_network(&mut self) -> Result<()> {
        let incoming = self.net.poll()?;

        if self.server_cert_fingerprint.is_none() {
            if let Some(cert_der) = self.net.peer_cert_der() {
                let fp = fingerprint_hex(&cert_der);
                self.server_cert_fingerprint = Some(fp.clone());
                log::info!("connected cert fingerprint: {fp}");
                let cached = self.binding_store.binding_count(&fp);
                if cached == 0 {
                    log::info!("no cached bindings for this server cert");
                } else {
                    log::info!("found {cached} cached binding(s) for this server cert");
                }
            }
        }

        for bytes in incoming.datagrams {
            if let Ok(packet) = protocol::decode_s2c(&bytes) {
                self.handle_server_packet(packet)?;
            }
        }
        for bytes in incoming.streams {
            if let Ok(packet) = protocol::decode_s2c(&bytes) {
                self.handle_server_packet(packet)?;
            }
        }

        if self.net.is_established() && !self.sent_hello {
            let hello = protocol::C2SPacket::ClientHello {
                client_name: "desktop-client".to_string(),
                capabilities: vec![
                    "render.draw_square".to_string(),
                    "prediction.lerp".to_string(),
                    "input.dynamic_bindings".to_string(),
                    "input.persist_by_cert".to_string(),
                ],
            };
            self.send_c2s(hello)?;
            self.sent_hello = true;
            log::info!("connected to server {}", self.net.server_addr());
        }

        for element in self.elements.values_mut() {
            let prediction = element.prediction;
            if !prediction.affects_translation() {
                element.draw_x = element.target_x;
                element.draw_y = element.target_y;
                continue;
            }

            match prediction.kind {
                protocol::PredictionKind::Interpolation => {
                    element.draw_x += (element.target_x - element.draw_x) * LERP_ALPHA;
                    element.draw_y += (element.target_y - element.draw_y) * LERP_ALPHA;
                },
                protocol::PredictionKind::Extrapolation => {
                    let predicted_x = element.draw_x + element.velocity_x * FIXED_FRAME_DT_SECONDS;
                    let predicted_y = element.draw_y + element.velocity_y * FIXED_FRAME_DT_SECONDS;
                    element.draw_x = predicted_x
                        + (element.target_x - predicted_x) * PREDICTION_CORRECTION_ALPHA;
                    element.draw_y = predicted_y
                        + (element.target_y - predicted_y) * PREDICTION_CORRECTION_ALPHA;
                },
            }
        }

        Ok(())
    }

    pub(super) fn binding_prompt(&self) -> Option<BindingPromptState> {
        let current = self.pending_bindings.front()?;
        Some(BindingPromptState {
            identifier: current.identifier.clone(),
            input_type: current.input_type,
            suggestion: self.binding_suggestion,
        })
    }

    pub(super) fn suggest_binding_key(&mut self, key: KeyCode) {
        self.binding_suggestion = Some(key);
    }

    pub(super) fn confirm_binding(&mut self) -> Result<()> {
        let Some(definition) = self.pending_bindings.pop_front() else {
            return Ok(());
        };

        let Some(key) = self.binding_suggestion.take() else {
            return Ok(());
        };
        let input_path = input_path_winit::input_path_from_key(key);

        self.send_binding_ack(definition.id)?;

        if let Some(cert_fp) = &self.server_cert_fingerprint {
            self.binding_store.set_binding_path(
                cert_fp,
                &definition.identifier,
                input_path.clone(),
            );
            self.binding_store.save()?;
        }

        self.activate_binding(definition.id, key);

        log::info!("assigned '{}' -> {:?}", definition.identifier, key);
        Ok(())
    }

    pub(super) fn skip_binding(&mut self) {
        if let Some(definition) = self.pending_bindings.pop_front() {
            log::info!("skipped binding '{}'", definition.identifier);
        }
        self.binding_suggestion = None;
    }

    pub(super) fn send_bound_inputs<F>(&mut self, mut is_down: F) -> Result<()>
    where
        F: FnMut(KeyCode) -> bool,
    {
        if !self.net.is_established() {
            return Ok(());
        }

        let mut outgoing = Vec::new();
        for binding in &mut self.active_bindings {
            binding.frames_since_send = binding.frames_since_send.saturating_add(1);
            let value = if is_down(binding.key) { 1.0 } else { 0.0 };
            let changed = (value - binding.last_value).abs() >= f32::EPSILON;
            let should_resend = binding.frames_since_send >= INPUT_RESEND_EVERY_FRAMES;
            if !changed && !should_resend {
                continue;
            }

            binding.last_value = value;
            binding.frames_since_send = 0;

            outgoing.push(protocol::C2SPacket::InputValue { binding_id: binding.id, value });
        }

        for packet in outgoing {
            self.send_c2s(packet)?;
        }

        Ok(())
    }

    pub(super) fn render_states(&self) -> Vec<RenderState> {
        self.elements
            .values()
            .map(|e| RenderState {
                x: e.draw_x,
                y: e.draw_y,
                size: self.draw_size,
                color: rgba_to_u32(self.draw_color_rgba),
            })
            .collect()
    }

    pub(super) fn game_name(&self) -> &str {
        &self.game_name
    }

    fn handle_server_packet(&mut self, packet: protocol::S2CPacket) -> Result<()> {
        match packet {
            protocol::S2CPacket::ServerHello { tick_rate_hz } => {
                log::info!("server tick rate: {tick_rate_hz}Hz");
            },
            protocol::S2CPacket::AssetManifest { player_color_rgba, player_size } => {
                self.draw_color_rgba = player_color_rgba;
                self.draw_size = player_size;
            },
            protocol::S2CPacket::SetGameName { name } => {
                self.game_name = name;
            },
            protocol::S2CPacket::SetTransformPrediction {
                element_id,
                enabled,
                affected_mask,
                kind,
            } => {
                let config = PredictionConfig { enabled, affected_mask, kind };
                if let Some(element) = self.elements.get_mut(&element_id) {
                    element.prediction = config;
                } else {
                    self.pending_prediction.insert(element_id, config);
                }
                log::info!(
                    "transform prediction for element {}: enabled={}, affected_mask={:#010b}, kind={:?}",
                    element_id,
                    enabled,
                    affected_mask.bits(),
                    kind
                );
            },
            protocol::S2CPacket::BindingDeclare { binding_id, identifier, input_type } => {
                log::info!("binding request: {identifier} ({input_type:?})");

                if let Some(cert_fp) = &self.server_cert_fingerprint {
                    if let Some(saved_path) =
                        self.binding_store.get_binding_path(cert_fp, &identifier)
                    {
                        if let Some(saved_key) = input_path_winit::key_from_input_path(&saved_path)
                        {
                            self.send_binding_ack(binding_id)?;
                            self.activate_binding(binding_id, saved_key);
                            log::info!(
                                "restored '{}' -> {:?} ({saved_path})",
                                identifier,
                                saved_key
                            );
                            return Ok(());
                        }

                        log::info!(
                            "cached binding for '{}' exists but is not compatible with this backend: {}",
                            identifier, saved_path
                        );
                    }
                }

                self.pending_bindings.push_back(BindingDefinition {
                    id: binding_id,
                    identifier,
                    input_type,
                });
            },
            protocol::S2CPacket::ElementMoved { element_id, x, y } => {
                let now = Instant::now();
                let element = self.elements.entry(element_id).or_insert(ElementState {
                    last_authoritative_x: x,
                    last_authoritative_y: y,
                    last_authoritative_at: now,
                    target_x: x,
                    target_y: y,
                    draw_x: x,
                    draw_y: y,
                    velocity_x: 0.0,
                    velocity_y: 0.0,
                    prediction: self
                        .pending_prediction
                        .remove(&element_id)
                        .unwrap_or(self.default_prediction),
                });
                let dt_seconds = now
                    .duration_since(element.last_authoritative_at)
                    .as_secs_f32()
                    .max(f32::EPSILON);
                element.velocity_x = (x - element.last_authoritative_x) / dt_seconds;
                element.velocity_y = (y - element.last_authoritative_y) / dt_seconds;
                element.last_authoritative_x = x;
                element.last_authoritative_y = y;
                element.last_authoritative_at = now;
                element.target_x = x;
                element.target_y = y;
            },
            protocol::S2CPacket::ElementRemoved { element_id } => {
                self.elements.remove(&element_id);
                self.pending_prediction.remove(&element_id);
            },
        }

        Ok(())
    }

    fn send_binding_ack(&mut self, binding_id: u16) -> Result<()> {
        self.send_c2s(protocol::C2SPacket::BindingAssigned { binding_id })
    }

    fn send_c2s(&mut self, packet: protocol::C2SPacket) -> Result<()> {
        if let Ok(bytes) = protocol::encode_c2s(&packet) {
            self.net.send_datagram(&bytes)?;
        }
        Ok(())
    }

    fn activate_binding(&mut self, id: u16, key: KeyCode) {
        self.active_bindings.push(BindingAssignment {
            id,
            key,
            last_value: 0.0,
            frames_since_send: 0,
        });
    }
}

pub fn run(config: GameConfig) -> Result<()> {
    let game = ClientGame::new(config.server_addr)?;
    app::run(game)
}

fn rgba_to_u32([r, g, b, _a]: [u8; 4]) -> u32 {
    ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
}

fn fingerprint_hex(der: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(der);
    let digest = hasher.finalize();

    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}
