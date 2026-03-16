use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

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
const PING_INTERVAL: Duration = Duration::from_secs(2);
const LATENCY_SMOOTHING_ALPHA: f64 = 0.06;
const PLAYER_SIZE: f32 = 32.0;
const DEFAULT_PLAYER_COLOR: protocol::Color = [0.65, 0.24, 29.0, 1.0];
const CLIENT_CAPABILITIES: &[&str] = &[
    "render.draw_square",
    "prediction.lerp",
    "input.dynamic_bindings",
    "input.persist_by_cert",
    "render.surfaces",
];

pub struct GameConfig {
    pub server_addr: SocketAddr,
}

#[derive(Clone, Copy, PartialEq)]
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

#[derive(Clone, Copy, Default, PartialEq)]
pub(super) struct SurfaceState {
    pub(super) dimension_lock: Option<(u32, u32)>,
    pub(super) aspect_ratio_lock: Option<(u32, u32)>,
    pub(super) clear_background: Option<protocol::Color>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClientPhase {
    Connecting,
    Handshaking,
    JoinedPendingWindow,
    Running,
}

struct SessionBootstrap {
    game_name: String,
    surfaces: HashMap<protocol::SurfaceId, SurfaceState>,
}

impl SessionBootstrap {
    fn new() -> Self {
        Self { game_name: "widev desktop POC".to_string(), surfaces: HashMap::new() }
    }

    fn game_name(&self) -> &str {
        &self.game_name
    }

    fn surface_state(&self, surface_id: protocol::SurfaceId) -> SurfaceState {
        self.surfaces.get(&surface_id).copied().unwrap_or_default()
    }

    fn set_game_name(&mut self, name: String) {
        self.game_name = name;
    }

    fn surface_state_mut(&mut self, surface_id: protocol::SurfaceId) -> &mut SurfaceState {
        self.surfaces.entry(surface_id).or_default()
    }

    fn apply_surface_dimension_lock(
        &mut self,
        surface_id: protocol::SurfaceId,
        width: u32,
        height: u32,
    ) {
        let state = self.surface_state_mut(surface_id);
        if width == 0 || height == 0 {
            state.dimension_lock = None;
            log::info!("surface {surface_id} dimension lock removed");
            return;
        }
        state.dimension_lock = Some((width, height));
        log::info!("surface {surface_id} dimension lock: {}x{}", width, height);
    }

    fn apply_surface_aspect_ratio_lock(
        &mut self,
        surface_id: protocol::SurfaceId,
        numerator: u32,
        denominator: u32,
    ) {
        let state = self.surface_state_mut(surface_id);
        if numerator == 0 || denominator == 0 {
            state.aspect_ratio_lock = None;
            log::info!("surface {surface_id} aspect-ratio lock removed");
            return;
        }
        state.aspect_ratio_lock = Some((numerator, denominator));
        log::info!("surface {surface_id} aspect-ratio lock: {}/{}", numerator, denominator);
    }

    fn apply_surface_clear_background(
        &mut self,
        surface_id: protocol::SurfaceId,
        color: protocol::Color,
    ) {
        self.surface_state_mut(surface_id).clear_background = Some(color);
        log::info!(
            "surface {surface_id} background clear color (oklch): [{:.3}, {:.3}, {:.2}, {:.3}]",
            color[0],
            color[1],
            color[2],
            color[3]
        );
    }
}

#[derive(Clone, Copy)]
struct ElementState {
    visible: bool,
    color: protocol::Color,
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

impl ElementState {
    fn hidden(now: Instant, prediction: PredictionConfig, color: protocol::Color) -> Self {
        Self {
            visible: false,
            color,
            last_authoritative: Vec2f::default(),
            last_authoritative_at: now,
            target: Vec2f::default(),
            draw: Vec2f::default(),
            velocity: Vec2f::default(),
            prediction,
        }
    }

    fn set_position_immediate(&mut self, position: Vec2f, now: Instant) {
        self.visible = true;
        self.last_authoritative = position;
        self.last_authoritative_at = now;
        self.target = position;
        self.draw = position;
        self.velocity = Vec2f::default();
    }

    fn apply_authoritative_move(&mut self, position: Vec2f, now: Instant) {
        let dt_seconds =
            now.duration_since(self.last_authoritative_at).as_secs_f32().max(f32::EPSILON);
        self.velocity.x = (position.x - self.last_authoritative.x) / dt_seconds;
        self.velocity.y = (position.y - self.last_authoritative.y) / dt_seconds;
        self.last_authoritative = position;
        self.last_authoritative_at = now;
        self.target = position;
        self.visible = true;
    }

    fn tick_prediction(&mut self) {
        let prediction = self.prediction;
        if !prediction.affects_translation() {
            self.draw = self.target;
            return;
        }

        match prediction.kind {
            protocol::PredictionKind::Interpolation => {
                self.draw.x += (self.target.x - self.draw.x) * LERP_ALPHA;
                self.draw.y += (self.target.y - self.draw.y) * LERP_ALPHA;
            },
            protocol::PredictionKind::Extrapolation => {
                let predicted_x = self.draw.x + self.velocity.x * FIXED_FRAME_DT_SECONDS;
                let predicted_y = self.draw.y + self.velocity.y * FIXED_FRAME_DT_SECONDS;
                self.draw.x =
                    predicted_x + (self.target.x - predicted_x) * PREDICTION_CORRECTION_ALPHA;
                self.draw.y =
                    predicted_y + (self.target.y - predicted_y) * PREDICTION_CORRECTION_ALPHA;
            },
        }
    }
}

pub(super) struct ClientGame {
    net: network::QuicClient,
    phase: ClientPhase,
    server_cert_fingerprint: Option<String>,
    elements: HashMap<u32, ElementState>,
    pending_envelopes: VecDeque<protocol::DecodedEnvelope>,
    processed_envelope_ids: HashSet<protocol::EnvelopeId>,
    bootstrap: SessionBootstrap,
    bindings: bindings::BindingState,
    default_prediction: PredictionConfig,
    pending_prediction: HashMap<u32, PredictionConfig>,
    pending_ping_nonces: HashMap<u64, Instant>,
    next_ping_nonce: u64,
    last_ping_sent_at: Instant,
    smoothed_path_rtt: Option<Duration>,
}

#[derive(Clone, Copy)]
pub(super) struct LatencySnapshot {
    pub(super) connected: bool,
    pub(super) quiche_rtt: Option<Duration>,
}

impl ClientGame {
    pub(super) fn new(server_addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            net: network::QuicClient::connect(server_addr)?,
            phase: ClientPhase::Connecting,
            server_cert_fingerprint: None,
            elements: HashMap::new(),
            pending_envelopes: VecDeque::new(),
            processed_envelope_ids: HashSet::new(),
            bootstrap: SessionBootstrap::new(),
            bindings: bindings::BindingState::new(persistence::BindingStore::load_default()?),
            default_prediction: PredictionConfig::default(),
            pending_prediction: HashMap::new(),
            pending_ping_nonces: HashMap::new(),
            next_ping_nonce: 1,
            last_ping_sent_at: Instant::now(),
            smoothed_path_rtt: None,
        })
    }

    pub(super) fn tick_network(&mut self) -> Result<()> {
        let incoming = self.net.poll()?;

        self.ensure_server_identity_logged();

        for bytes in incoming.datagrams.into_iter().chain(incoming.streams) {
            let Some(envelope) = protocol::decode_envelope(&bytes) else {
                continue;
            };
            self.pending_envelopes.push_back(envelope);
        }
        self.process_ready_envelopes()?;

        if self.net.is_established() && self.phase == ClientPhase::Connecting {
            let hello = protocol::C2SPacket::ClientHello {
                client_name: "desktop-client".to_string(),
                capabilities: CLIENT_CAPABILITIES
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
            };
            self.send_c2s(hello)?;
            self.phase = ClientPhase::Handshaking;
            log::info!("connected to server {}", self.net.server_addr());
        }
        self.maybe_send_ping()?;
        self.update_smoothed_latency();

        for element in self.elements.values_mut() {
            element.tick_prediction();
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
                width: PLAYER_SIZE,
                height: PLAYER_SIZE,
                color: oklch_to_u32(e.color),
            })
            .collect()
    }

    pub(super) fn game_name(&self) -> &str {
        self.bootstrap.game_name()
    }

    pub(super) fn phase(&self) -> ClientPhase {
        self.phase
    }

    pub(super) fn is_connected(&self) -> bool {
        self.net.is_established()
    }

    pub(super) fn send_surface_list(
        &mut self,
        surfaces: Vec<(protocol::SurfaceId, u32, u32)>,
    ) -> Result<()> {
        self.send_when_connected(protocol::C2SPacket::SurfaceList { surfaces })
    }

    pub(super) fn send_surface_resized(
        &mut self,
        surface_id: protocol::SurfaceId,
        width: u32,
        height: u32,
    ) -> Result<()> {
        self.send_when_connected(protocol::C2SPacket::SurfaceResized { surface_id, width, height })
    }

    pub(super) fn surface_state(&self, surface_id: protocol::SurfaceId) -> SurfaceState {
        self.bootstrap.surface_state(surface_id)
    }

    pub(super) fn mark_window_running(&mut self) {
        if self.phase == ClientPhase::JoinedPendingWindow {
            self.phase = ClientPhase::Running;
        }
    }

    pub(super) fn latency_snapshot(&self) -> LatencySnapshot {
        LatencySnapshot { connected: self.net.is_established(), quiche_rtt: self.smoothed_path_rtt }
    }

    fn update_smoothed_latency(&mut self) {
        let Some(current_rtt) = self.net.path_rtt() else {
            self.smoothed_path_rtt = None;
            return;
        };

        let smoothed = match self.smoothed_path_rtt {
            Some(previous_rtt) => {
                let previous_secs = previous_rtt.as_secs_f64();
                let current_secs = current_rtt.as_secs_f64();
                Duration::from_secs_f64(
                    previous_secs + (current_secs - previous_secs) * LATENCY_SMOOTHING_ALPHA,
                )
            },
            None => current_rtt,
        };
        self.smoothed_path_rtt = Some(smoothed);
    }

    fn ensure_server_identity_logged(&mut self) {
        if self.server_cert_fingerprint.is_some() {
            return;
        }
        let Some(cert_der) = self.net.peer_cert_der() else {
            return;
        };

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

    fn apply_transform_prediction(
        &mut self,
        element_id: u32,
        enabled: bool,
        affected_mask: protocol::TransformPredictionMask,
        kind: protocol::PredictionKind,
    ) {
        let config = PredictionConfig { enabled, affected_mask, kind };
        let changed = if let Some(element) = self.elements.get_mut(&element_id) {
            let changed = element.prediction.enabled != config.enabled
                || element.prediction.kind != config.kind
                || element.prediction.affected_mask.bits() != config.affected_mask.bits();
            element.prediction = config;
            changed
        } else {
            let changed = self
                .pending_prediction
                .get(&element_id)
                .map(|prev| {
                    prev.enabled != config.enabled
                        || prev.kind != config.kind
                        || prev.affected_mask.bits() != config.affected_mask.bits()
                })
                .unwrap_or(true);
            self.pending_prediction.insert(element_id, config);
            changed
        };

        if changed {
            log::info!(
                "transform prediction for element {}: enabled={}, affected_mask={:#010b}, kind={:?}",
                element_id,
                enabled,
                affected_mask.bits(),
                kind
            );
        } else {
            log::debug!("ignored duplicate transform prediction for element {}", element_id);
        }
    }

    fn handle_binding_declare(
        &mut self,
        binding_id: u16,
        identifier: String,
        input_type: protocol::InputType,
    ) -> Result<()> {
        log::debug!("binding request: {identifier} ({input_type:?})");
        match self.bindings.declare_binding(
            self.server_cert_fingerprint.as_deref(),
            binding_id,
            identifier,
            input_type,
        ) {
            DeclareBindingOutcome::Restored { binding_id, input, identifier } => {
                self.send_binding_ack(binding_id)?;
                log::debug!("restored '{}' -> {}", identifier, input);
            },
            DeclareBindingOutcome::Pending => {},
        }
        Ok(())
    }

    fn handle_element_add(&mut self, element_id: u32) {
        let now = Instant::now();
        let prediction =
            self.pending_prediction.remove(&element_id).unwrap_or(self.default_prediction);
        self.elements
            .entry(element_id)
            .or_insert_with(|| ElementState::hidden(now, prediction, DEFAULT_PLAYER_COLOR));
    }

    fn handle_element_move(&mut self, element_id: u32, x: f32, y: f32) {
        let Some(element) = self.elements.get_mut(&element_id) else {
            log::debug!("ignored ElementMove for unknown element_id={element_id}");
            return;
        };
        let now = Instant::now();
        let position = Vec2f { x, y };
        if !element.visible {
            element.set_position_immediate(position, now);
            return;
        }
        element.apply_authoritative_move(position, now);
    }

    fn apply_element_color(&mut self, element_id: u32, color: protocol::Color) {
        if let Some(element) = self.elements.get_mut(&element_id) {
            element.color = color;
        } else {
            log::debug!("ignored ElementSetColor for unknown element_id={element_id}");
        }
    }

    fn handle_server_packet(&mut self, packet: protocol::S2CPacket) -> Result<()> {
        match packet {
            protocol::S2CPacket::ServerHello { tick_rate_hz } => {
                log::info!("server tick rate: {tick_rate_hz}Hz");
            },
            protocol::S2CPacket::Ping { nonce } => {
                self.send_c2s(protocol::C2SPacket::Pong { nonce })?;
            },
            protocol::S2CPacket::Pong { nonce } => {
                if let Some(sent_at) = self.pending_ping_nonces.remove(&nonce) {
                    let rtt = sent_at.elapsed();
                    let rtt_ms = rtt.as_secs_f64() * 1000.0;
                    let quiche_rtt_ms = self
                        .net
                        .path_rtt()
                        .map(|rtt| rtt.as_secs_f64() * 1000.0)
                        .unwrap_or_default();
                    log::debug!(
                        "client latency: {:.2}ms (quiche_rtt={:.2}ms)",
                        rtt_ms,
                        quiche_rtt_ms
                    );
                }
            },
            protocol::S2CPacket::SetGameName { name } => {
                self.bootstrap.set_game_name(name);
            },
            protocol::S2CPacket::Join {} => {
                if !matches!(self.phase, ClientPhase::JoinedPendingWindow | ClientPhase::Running) {
                    self.phase = ClientPhase::JoinedPendingWindow;
                    log::info!("join received; client can initialize surfaces and render");
                }
            },
            protocol::S2CPacket::SurfaceLockDimensions { surface_id, width, height } => {
                self.bootstrap.apply_surface_dimension_lock(surface_id, width, height);
            },
            protocol::S2CPacket::SurfaceLockAspectRatio { surface_id, numerator, denominator } => {
                self.bootstrap.apply_surface_aspect_ratio_lock(surface_id, numerator, denominator);
            },
            protocol::S2CPacket::SurfaceClearBackground { surface_id, color } => {
                self.bootstrap.apply_surface_clear_background(surface_id, color);
            },
            protocol::S2CPacket::ElementSetTransformPrediction {
                element_id,
                enabled,
                affected_mask,
                kind,
            } => {
                self.apply_transform_prediction(element_id, enabled, affected_mask, kind);
            },
            protocol::S2CPacket::ElementSetColor { element_id, color } => {
                self.apply_element_color(element_id, color);
            },
            protocol::S2CPacket::BindingDeclare { binding_id, identifier, input_type } => {
                self.handle_binding_declare(binding_id, identifier, input_type)?;
            },
            protocol::S2CPacket::ElementAdd { element_id } => {
                self.handle_element_add(element_id);
            },
            protocol::S2CPacket::ElementMove { element_id, x, y } => {
                self.handle_element_move(element_id, x, y);
            },
            protocol::S2CPacket::ElementRemove { element_id } => {
                self.elements.remove(&element_id);
                self.pending_prediction.remove(&element_id);
            },
        }

        Ok(())
    }

    fn process_ready_envelopes(&mut self) -> Result<()> {
        loop {
            let mut progressed = false;
            let mut remaining = VecDeque::new();

            while let Some(envelope) = self.pending_envelopes.pop_front() {
                if !self.dependency_satisfied(envelope.dependency_id) {
                    remaining.push_back(envelope);
                    continue;
                }

                self.apply_decoded_envelope(envelope)?;
                progressed = true;
            }

            self.pending_envelopes = remaining;
            if !progressed {
                break;
            }
        }

        Ok(())
    }

    fn dependency_satisfied(&self, dependency_id: Option<protocol::EnvelopeId>) -> bool {
        dependency_id.is_none_or(|id| self.processed_envelope_ids.contains(&id))
    }

    fn apply_decoded_envelope(&mut self, envelope: protocol::DecodedEnvelope) -> Result<()> {
        for packet in envelope.packets {
            self.handle_server_packet(packet)?;
        }

        if let Some(id) = envelope.id {
            self.processed_envelope_ids.insert(id);
        }
        if let Some(envelope_id) = envelope.receipt_id {
            self.send_when_connected(protocol::C2SPacket::Receipt { envelope_id })?;
        }

        Ok(())
    }

    fn send_binding_ack(&mut self, binding_id: u16) -> Result<()> {
        self.send_c2s(protocol::C2SPacket::BindingAssigned { binding_id })
    }

    fn send_when_connected(&mut self, packet: protocol::C2SPacket) -> Result<()> {
        if self.net.is_established() {
            self.send_c2s(packet)?;
        }
        Ok(())
    }

    fn send_c2s(&mut self, packet: protocol::C2SPacket) -> Result<()> {
        if let Ok(bytes) = protocol::encode_c2s(&packet) {
            self.net.send_datagram(&bytes)?;
        }
        Ok(())
    }

    fn maybe_send_ping(&mut self) -> Result<()> {
        if !self.net.is_established() {
            return Ok(());
        }
        if self.last_ping_sent_at.elapsed() < PING_INTERVAL {
            return Ok(());
        }

        let nonce = self.next_ping_nonce;
        self.next_ping_nonce = self.next_ping_nonce.wrapping_add(1).max(1);
        self.pending_ping_nonces.insert(nonce, Instant::now());
        self.last_ping_sent_at = Instant::now();
        self.send_c2s(protocol::C2SPacket::Ping { nonce })
    }
}

pub fn run(config: GameConfig) -> Result<()> {
    let game = ClientGame::new(config.server_addr)?;
    app::run(game)
}

fn oklch_to_u32([l, c, h_deg, _alpha]: protocol::Color) -> u32 {
    let l = l.clamp(0.0, 1.0) as f64;
    let c = c.max(0.0) as f64;
    let hue = (h_deg as f64).to_radians();
    let a = c * hue.cos();
    let b = c * hue.sin();

    let l_ = l + 0.396_337_777_4 * a + 0.215_803_757_3 * b;
    let m_ = l - 0.105_561_345_8 * a - 0.063_854_172_8 * b;
    let s_ = l - 0.089_484_177_5 * a - 1.291_485_548 * b;

    let l3 = l_ * l_ * l_;
    let m3 = m_ * m_ * m_;
    let s3 = s_ * s_ * s_;

    let r = (4.076_741_662_1 * l3 - 3.307_711_591_3 * m3 + 0.230_969_929_2 * s3).clamp(0.0, 1.0);
    let g = (-1.268_438_004_6 * l3 + 2.609_757_401_1 * m3 - 0.341_319_396_5 * s3).clamp(0.0, 1.0);
    let b = (-0.004_196_086_3 * l3 - 0.703_418_614_7 * m3 + 1.707_614_701 * s3).clamp(0.0, 1.0);

    (((r * 255.0).round() as u32) << 16)
        | (((g * 255.0).round() as u32) << 8)
        | ((b * 255.0).round() as u32)
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
