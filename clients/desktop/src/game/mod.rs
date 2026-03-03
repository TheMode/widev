use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Result;
use bindings::{BindingPromptState, DeclareBindingOutcome};
use sha2::{Digest, Sha256};
mod app;
mod bindings;
mod network;
mod persistence;
mod protocol;
mod renderer;

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
    pub(super) width: f32,
    pub(super) height: f32,
    pub(super) color: u32,
}

#[derive(Clone, Copy, Default)]
struct Vec2f {
    x: f32,
    y: f32,
}

#[derive(Clone, Copy, Default)]
pub(super) struct SurfaceState {
    pub(super) dimension_lock: Option<(u32, u32)>,
    pub(super) aspect_ratio_lock: Option<(u32, u32)>,
    pub(super) clear_background: Option<protocol::Color>,
}

#[derive(Clone, Copy)]
struct ElementState {
    visible: bool,
    last_authoritative: Vec2f,
    last_authoritative_at: Instant,
    target: Vec2f,
    draw: Vec2f,
    velocity: Vec2f,
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
    bindings: bindings::BindingState,
    default_prediction: PredictionConfig,
    pending_prediction: HashMap<u32, PredictionConfig>,
    surfaces: HashMap<protocol::SurfaceId, SurfaceState>,
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
            bindings: bindings::BindingState::new(persistence::BindingStore::load_default()?),
            default_prediction: PredictionConfig::default(),
            pending_prediction: HashMap::new(),
            surfaces: HashMap::new(),
        })
    }

    pub(super) fn tick_network(&mut self) -> Result<()> {
        let incoming = self.net.poll()?;

        if self.server_cert_fingerprint.is_none() {
            if let Some(cert_der) = self.net.peer_cert_der() {
                let fp = fingerprint_hex(&cert_der);
                self.server_cert_fingerprint = Some(fp.clone());
                log::info!("connected cert fingerprint: {fp}");
                let cached = self.bindings.binding_count(&fp);
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
                    "render.surfaces".to_string(),
                ],
            };
            self.send_c2s(hello)?;
            self.sent_hello = true;
            log::info!("connected to server {}", self.net.server_addr());
        }

        for element in self.elements.values_mut() {
            let prediction = element.prediction;
            if !prediction.affects_translation() {
                element.draw = element.target;
                continue;
            }

            match prediction.kind {
                protocol::PredictionKind::Interpolation => {
                    element.draw.x += (element.target.x - element.draw.x) * LERP_ALPHA;
                    element.draw.y += (element.target.y - element.draw.y) * LERP_ALPHA;
                },
                protocol::PredictionKind::Extrapolation => {
                    let predicted_x = element.draw.x + element.velocity.x * FIXED_FRAME_DT_SECONDS;
                    let predicted_y = element.draw.y + element.velocity.y * FIXED_FRAME_DT_SECONDS;
                    element.draw.x = predicted_x
                        + (element.target.x - predicted_x) * PREDICTION_CORRECTION_ALPHA;
                    element.draw.y = predicted_y
                        + (element.target.y - predicted_y) * PREDICTION_CORRECTION_ALPHA;
                },
            }
        }

        Ok(())
    }

    pub(in crate::game) fn binding_prompt(&self) -> Option<BindingPromptState> {
        self.bindings.binding_prompt()
    }

    pub(in crate::game) fn apply_binding_ui_action(
        &mut self,
        action: bindings::UiAction,
    ) -> Result<()> {
        match action {
            bindings::UiAction::Confirm => {
                if let Some(confirmed) =
                    self.bindings.confirm_binding(self.server_cert_fingerprint.as_deref())?
                {
                    self.send_binding_ack(confirmed.binding_id)?;
                    log::info!("assigned '{}' -> {}", confirmed.identifier, confirmed.input);
                }
            },
            other => self.bindings.apply_ui_action(other),
        }
        Ok(())
    }

    pub(in crate::game) fn send_bound_inputs<F>(&mut self, read_value: F) -> Result<()>
    where
        F: FnMut(&bindings::InputPath) -> f32,
    {
        if !self.net.is_established() {
            return Ok(());
        }

        for (binding_id, value) in self.bindings.sample_values(read_value) {
            self.send_c2s(protocol::C2SPacket::InputValue { binding_id, value })?;
        }

        Ok(())
    }

    pub(super) fn render_states(&self) -> Vec<RenderState> {
        self.elements
            .values()
            .filter(|e| e.visible)
            .map(|e| RenderState {
                x: e.draw.x,
                y: e.draw.y,
                width: self.draw_size as f32,
                height: self.draw_size as f32,
                color: rgba_to_u32(self.draw_color_rgba),
            })
            .collect()
    }

    pub(super) fn game_name(&self) -> &str {
        &self.game_name
    }

    pub(super) fn is_connected(&self) -> bool {
        self.net.is_established()
    }

    pub(super) fn send_surface_list(
        &mut self,
        surfaces: Vec<(protocol::SurfaceId, String, u32, u32)>,
    ) -> Result<()> {
        if !self.net.is_established() {
            return Ok(());
        }
        self.send_c2s(protocol::C2SPacket::SurfaceList { surfaces })
    }

    pub(super) fn send_surface_resized(
        &mut self,
        surface_id: protocol::SurfaceId,
        width: u32,
        height: u32,
    ) -> Result<()> {
        if !self.net.is_established() {
            return Ok(());
        }
        self.send_c2s(protocol::C2SPacket::SurfaceResized { surface_id, width, height })
    }

    pub(super) fn surface_state(&self, surface_id: protocol::SurfaceId) -> SurfaceState {
        self.surfaces.get(&surface_id).copied().unwrap_or_default()
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
            protocol::S2CPacket::SurfaceLockDimensions { surface_id, width, height } => {
                let state = self.surfaces.entry(surface_id).or_default();
                if width == 0 || height == 0 {
                    state.dimension_lock = None;
                    log::info!("surface {surface_id} dimension lock removed");
                } else {
                    state.dimension_lock = Some((width, height));
                    log::info!("surface {surface_id} dimension lock: {}x{}", width, height);
                }
            },
            protocol::S2CPacket::SurfaceLockAspectRatio { surface_id, numerator, denominator } => {
                let state = self.surfaces.entry(surface_id).or_default();
                if numerator == 0 || denominator == 0 {
                    state.aspect_ratio_lock = None;
                    log::info!("surface {surface_id} aspect-ratio lock removed");
                } else {
                    state.aspect_ratio_lock = Some((numerator, denominator));
                    log::info!(
                        "surface {surface_id} aspect-ratio lock: {}/{}",
                        numerator,
                        denominator
                    );
                }
            },
            protocol::S2CPacket::SurfaceClearBackground { surface_id, color } => {
                let state = self.surfaces.entry(surface_id).or_default();
                state.clear_background = Some(color);
                log::info!(
                    "surface {surface_id} background clear color (oklch): [{:.3}, {:.3}, {:.2}, {:.3}]",
                    color[0],
                    color[1],
                    color[2],
                    color[3]
                );
            },
            protocol::S2CPacket::ElementSetTransformPrediction {
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
                match self.bindings.declare_binding(
                    self.server_cert_fingerprint.as_deref(),
                    binding_id,
                    identifier,
                    input_type,
                ) {
                    DeclareBindingOutcome::Restored {
                        binding_id,
                        input,
                        saved_path,
                        identifier,
                    } => {
                        self.send_binding_ack(binding_id)?;
                        log::info!("restored '{}' -> {} ({saved_path})", identifier, input);
                    },
                    DeclareBindingOutcome::Pending => {},
                }
            },
            protocol::S2CPacket::ElementAdd { element_id } => {
                let now = Instant::now();
                self.elements.entry(element_id).or_insert(ElementState {
                    visible: false,
                    last_authoritative: Vec2f::default(),
                    last_authoritative_at: now,
                    target: Vec2f::default(),
                    draw: Vec2f::default(),
                    velocity: Vec2f::default(),
                    prediction: self
                        .pending_prediction
                        .remove(&element_id)
                        .unwrap_or(self.default_prediction),
                });
            },
            protocol::S2CPacket::ElementMove { element_id, x, y } => {
                let Some(element) = self.elements.get_mut(&element_id) else {
                    log::debug!("ignored ElementMove for unknown element_id={element_id}");
                    return Ok(());
                };
                let position = Vec2f { x, y };
                if !element.visible {
                    element.last_authoritative = position;
                    element.target = position;
                    element.draw = position;
                    element.velocity = Vec2f::default();
                    element.last_authoritative_at = Instant::now();
                    element.visible = true;
                    return Ok(());
                }
                let now = Instant::now();
                let dt_seconds = now
                    .duration_since(element.last_authoritative_at)
                    .as_secs_f32()
                    .max(f32::EPSILON);
                element.velocity.x = (position.x - element.last_authoritative.x) / dt_seconds;
                element.velocity.y = (position.y - element.last_authoritative.y) / dt_seconds;
                element.last_authoritative = position;
                element.last_authoritative_at = now;
                element.target = position;
                element.visible = true;
            },
            protocol::S2CPacket::ElementRemove { element_id } => {
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
