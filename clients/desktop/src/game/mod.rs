use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bindings::{BindingPromptState, DeclareBindingOutcome};
use sha2::{Digest, Sha256};
mod app;
mod bindings;
mod network;
mod network_migration;
mod packet_chain;
#[allow(dead_code)]
mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}
mod persistence;
use self::packet_chain::PacketChain;
use self::packets as protocol;
use self::renderer::TextCommand;
mod renderer;

const LERP_ALPHA: f32 = 0.35;
const PREDICTION_CORRECTION_ALPHA: f32 = 0.12;
const FIXED_FRAME_DT_SECONDS: f32 = 1.0 / 60.0;
const LATENCY_SMOOTHING_ALPHA: f64 = 0.06;
const PLAYER_SIZE: f32 = 32.0;
const DEFAULT_ELEMENT_TINT: protocol::Color = [1.0, 0.0, 0.0, 1.0];
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

#[derive(Clone, PartialEq, Eq)]
pub(super) struct TextureResource {
    pub(super) width: u32,
    pub(super) height: u32,
    pub(super) rgba: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum ClientResourcePayload {
    Texture(TextureResource),
    Unsupported,
}

#[derive(Clone)]
pub(super) struct ClientResource {
    resource_type: String,
    remaining_uses: Option<u32>,
    active_elements: HashSet<u32>,
    payload: ClientResourcePayload,
}

impl ClientResource {
    fn new(resource_type: String, usage_count: Option<i32>, blob: &[u8]) -> Result<Self> {
        let remaining_uses = match usage_count {
            None => None,
            Some(-1) => return Err(anyhow::anyhow!("invalid resource usage_count=-1")),
            Some(count) if count >= 0 => Some(count as u32),
            Some(negative) => {
                return Err(anyhow::anyhow!("invalid resource usage_count={negative}"));
            },
        };
        let payload = decode_resource_payload(&resource_type, blob);
        Ok(Self { resource_type, remaining_uses, active_elements: HashSet::new(), payload })
    }

    pub(super) fn texture(&self) -> Option<&TextureResource> {
        match &self.payload {
            ClientResourcePayload::Texture(texture) => Some(texture),
            ClientResourcePayload::Unsupported => None,
        }
    }

    fn usage_count_display(&self) -> i32 {
        self.remaining_uses.map(|remaining| remaining as i32).unwrap_or(-1)
    }

    fn consume_for_element(&mut self, element_id: u32) -> bool {
        if matches!(self.remaining_uses, Some(0)) {
            return false;
        }
        if let Some(remaining_uses) = &mut self.remaining_uses {
            *remaining_uses -= 1;
        }
        self.active_elements.insert(element_id);
        true
    }

    fn release_element(&mut self, element_id: u32) {
        self.active_elements.remove(&element_id);
    }

    fn should_free(&self) -> bool {
        matches!(self.remaining_uses, Some(0)) && self.active_elements.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(super) struct RenderState {
    pub(super) x: f32,
    pub(super) y: f32,
    pub(super) width: f32,
    pub(super) height: f32,
    pub(super) depth: i32,
    pub(super) color: u32,
    pub(super) texture_id: Option<protocol::MessageId>,
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

#[derive(Clone)]
struct ElementState {
    visible: bool,
    depth: i32,
    color: protocol::Color,
    visual: ElementVisual,
    last_authoritative: Vec2f,
    last_authoritative_at: Instant,
    target: Vec2f,
    draw: Vec2f,
    velocity: Vec2f,
    prediction: PredictionConfig,
}

#[derive(Clone)]
enum ElementVisual {
    SolidRect {
        width: f32,
        height: f32,
    },
    Texture {
        width: f32,
        height: f32,
        texture_id: Option<protocol::MessageId>,
    },
    Text {
        text: String,
        max_width: f32,
        font_size: f32,
        line_height: f32,
    },
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
            depth: 0,
            color,
            visual: ElementVisual::new(protocol::ElementKind::SolidRect),
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

impl ElementVisual {
    fn new(kind: protocol::ElementKind) -> Self {
        match kind {
            protocol::ElementKind::SolidRect => {
                Self::SolidRect { width: PLAYER_SIZE, height: PLAYER_SIZE }
            },
            protocol::ElementKind::Texture => {
                Self::Texture { width: PLAYER_SIZE, height: PLAYER_SIZE, texture_id: None }
            },
            protocol::ElementKind::Text => Self::Text {
                text: String::new(),
                max_width: PLAYER_SIZE,
                font_size: 24.0,
                line_height: 28.0,
            },
        }
    }

    fn texture_id(&self) -> Option<protocol::MessageId> {
        match self {
            Self::Texture { texture_id, .. } => *texture_id,
            Self::SolidRect { .. } | Self::Text { .. } => None,
        }
    }
}

pub(super) struct ClientGame {
    net: network::QuicClient,
    phase: ClientPhase,
    server_cert_fingerprint: Option<String>,
    elements: HashMap<u32, ElementState>,
    resources: HashMap<protocol::MessageId, ClientResource>,
    packet_chain: PacketChain,
    bootstrap: SessionBootstrap,
    bindings: bindings::BindingState,
    default_prediction: PredictionConfig,
    pending_prediction: HashMap<u32, PredictionConfig>,
    smoothed_path_rtt: Option<Duration>,
    render_revision: u64,
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
            resources: HashMap::new(),
            packet_chain: PacketChain::new(),
            bootstrap: SessionBootstrap::new(),
            bindings: bindings::BindingState::new(persistence::BindingStore::load_default()?),
            default_prediction: PredictionConfig::default(),
            pending_prediction: HashMap::new(),
            smoothed_path_rtt: None,
            render_revision: 0,
        })
    }

    pub(super) fn tick_network(&mut self) -> Result<()> {
        let incoming = self.net.poll()?;

        self.ensure_server_identity_logged();

        for message in incoming.messages {
            self.packet_chain.push(message);
        }
        self.process_ready_messages()?;

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
        self.update_smoothed_latency();

        for element in self.elements.values_mut() {
            element.tick_prediction();
        }

        Ok(())
    }

    pub(super) fn handle_network_change(&mut self) -> Result<()> {
        self.net.handle_network_change()?;
        self.smoothed_path_rtt = None;
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
                    log::info!("assigned '{}' -> {}", confirmed.identifier, confirmed.binding);
                }
            },
            other => self.bindings.apply_ui_action(other),
        }
        Ok(())
    }

    pub(in crate::game) fn send_bound_inputs<F>(&mut self, read_value: F) -> Result<()>
    where
        F: FnMut(&bindings::RawSource) -> protocol::InputPayload,
    {
        if !self.net.is_established() {
            return Ok(());
        }

        for (binding_id, value) in self.bindings.sample_values(read_value) {
            self.send_c2s(protocol::C2SPacket::InputValue { binding_id, value })?;
        }

        Ok(())
    }

    fn build_scene(
        &self,
        render_states: &mut Vec<RenderState>,
        text_commands: &mut Vec<TextCommand>,
    ) {
        let mut sorted_text_commands = Vec::new();
        for (&element_id, element) in &self.elements {
            if !element.visible {
                continue;
            }

            match &element.visual {
                ElementVisual::SolidRect { width, height } => render_states.push(RenderState {
                    x: element.draw.x,
                    y: element.draw.y,
                    width: *width,
                    height: *height,
                    depth: element.depth,
                    color: oklch_to_u32(element.color),
                    texture_id: None,
                }),
                ElementVisual::Texture { width, height, texture_id } => {
                    if texture_id.and_then(|id| self.texture_resource(id)).is_none() {
                        continue;
                    }
                    render_states.push(RenderState {
                        x: element.draw.x,
                        y: element.draw.y,
                        width: *width,
                        height: *height,
                        depth: element.depth,
                        color: oklch_to_u32(element.color),
                        texture_id: *texture_id,
                    });
                },
                ElementVisual::Text { text, max_width, font_size, line_height } => {
                    if text.is_empty() {
                        continue;
                    }
                    sorted_text_commands.push((
                        element.depth,
                        element_id,
                        TextCommand {
                            text: text.clone(),
                            x: element.draw.x,
                            y: element.draw.y,
                            max_width: (*max_width).max(1.0),
                            font_size: (*font_size).max(1.0),
                            line_height: (*line_height).max(*font_size + 1.0),
                            depth: element.depth,
                            color: oklch_to_u32(element.color),
                        },
                    ));
                },
            }
        }

        sorted_text_commands.sort_by_key(|(depth, element_id, _)| (*depth, *element_id));
        text_commands.extend(sorted_text_commands.into_iter().map(|(_, _, command)| command));
    }

    pub(super) fn resources(&self) -> &HashMap<protocol::MessageId, ClientResource> {
        &self.resources
    }

    pub(super) fn render_revision(&self) -> u64 {
        self.render_revision
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

    pub(super) fn set_network_waker(&mut self, wake_notifier: Arc<dyn Fn() + Send + Sync>) {
        self.net.set_wake_notifier(wake_notifier);
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
            DeclareBindingOutcome::Restored { binding_id, binding, identifier } => {
                self.send_binding_ack(binding_id)?;
                log::debug!("restored '{}' -> {}", identifier, binding);
            },
            DeclareBindingOutcome::Pending => {},
        }
        Ok(())
    }

    fn handle_element_add(&mut self, element_id: u32, kind: protocol::ElementKind) {
        let now = Instant::now();
        let prediction =
            self.pending_prediction.remove(&element_id).unwrap_or(self.default_prediction);
        self.elements.entry(element_id).or_insert_with(|| {
            let mut element = ElementState::hidden(now, prediction, DEFAULT_ELEMENT_TINT);
            element.visual = ElementVisual::new(kind);
            element
        });
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
            if element.color != color {
                element.color = color;
                self.bump_render_revision();
            }
        } else {
            log::debug!("ignored ElementSetColor for unknown element_id={element_id}");
        }
    }

    fn apply_element_texture(&mut self, element_id: u32, resource_id: protocol::MessageId) {
        let Some(previous_resource_id) = self.element_texture_id(element_id) else {
            log::debug!("ignored ElementSetTexture for unknown element_id={element_id}");
            return;
        };

        if let Some(previous_resource_id) = previous_resource_id.filter(|id| *id != resource_id) {
            self.release_resource_binding(previous_resource_id, element_id);
        }

        let mut accepted = true;
        if self.resources.contains_key(&resource_id) {
            accepted = self.bind_resource_to_element(resource_id, element_id);
        }

        let next_texture_id = if accepted {
            Some(resource_id)
        } else if previous_resource_id == Some(resource_id) {
            previous_resource_id
        } else {
            None
        };
        let changed = self.set_element_texture_id(element_id, next_texture_id);

        if !accepted {
            log::warn!("resource {resource_id} has no remaining uses for element {element_id}");
        }

        if changed && next_texture_id != previous_resource_id {
            self.bump_render_revision();
        }
    }

    fn apply_element_size(&mut self, element_id: u32, width: f32, height: f32) {
        if let Some(element) = self.elements.get_mut(&element_id) {
            let width = width.max(1.0);
            let height = height.max(1.0);
            let changed = match &mut element.visual {
                ElementVisual::SolidRect { width: current_width, height: current_height }
                | ElementVisual::Texture { width: current_width, height: current_height, .. } => {
                    if *current_width == width && *current_height == height {
                        false
                    } else {
                        *current_width = width;
                        *current_height = height;
                        true
                    }
                },
                ElementVisual::Text { .. } => false,
            };
            if changed {
                self.bump_render_revision();
            }
        } else {
            log::debug!("ignored ElementSetSize for unknown element_id={element_id}");
        }
    }

    fn apply_element_depth(&mut self, element_id: u32, depth: i32) {
        if let Some(element) = self.elements.get_mut(&element_id) {
            if element.depth != depth {
                element.depth = depth;
                self.bump_render_revision();
            }
        } else {
            log::debug!("ignored ElementSetDepth for unknown element_id={element_id}");
        }
    }

    fn apply_element_text_content(&mut self, element_id: u32, text: String) {
        if let Some(element) = self.elements.get_mut(&element_id) {
            if let ElementVisual::Text { text: current_text, .. } = &mut element.visual {
                if *current_text != text {
                    *current_text = text;
                    self.bump_render_revision();
                }
            }
        } else {
            log::debug!("ignored ElementSetTextContent for unknown element_id={element_id}");
        }
    }

    fn apply_element_text_layout(
        &mut self,
        element_id: u32,
        max_width: f32,
        font_size: f32,
        line_height: f32,
    ) {
        if let Some(element) = self.elements.get_mut(&element_id) {
            let max_width = max_width.max(1.0);
            let font_size = font_size.max(1.0);
            let line_height = line_height.max(font_size + 1.0);
            if let ElementVisual::Text {
                max_width: current_max_width,
                font_size: current_font_size,
                line_height: current_line_height,
                ..
            } = &mut element.visual
            {
                if *current_max_width != max_width
                    || *current_font_size != font_size
                    || *current_line_height != line_height
                {
                    *current_max_width = max_width;
                    *current_font_size = font_size;
                    *current_line_height = line_height;
                    self.bump_render_revision();
                }
            }
        } else {
            log::debug!("ignored ElementSetTextLayout for unknown element_id={element_id}");
        }
    }

    fn reset_scene(&mut self) {
        let removed_elements: Vec<(u32, Option<protocol::MessageId>)> = self
            .elements
            .drain()
            .map(|(element_id, element)| (element_id, element.visual.texture_id()))
            .collect();
        for (element_id, texture_id) in removed_elements {
            if let Some(resource_id) = texture_id {
                self.release_resource_binding(resource_id, element_id);
            }
        }
        self.pending_prediction.clear();
        self.bump_render_revision();
        log::info!("scene reset");
    }

    fn handle_server_packet(&mut self, packet: protocol::S2CPacket) -> Result<()> {
        match packet {
            protocol::S2CPacket::ServerHello { tick_rate_hz } => {
                log::info!("server tick rate: {tick_rate_hz}Hz");
            },
            protocol::S2CPacket::Ping { .. } | protocol::S2CPacket::Pong { .. } => {},
            protocol::S2CPacket::SetGameName { name } => {
                self.bootstrap.set_game_name(name);
            },
            protocol::S2CPacket::Join {} => {
                if !matches!(self.phase, ClientPhase::JoinedPendingWindow | ClientPhase::Running) {
                    self.phase = ClientPhase::JoinedPendingWindow;
                    log::info!("join received; client can initialize surfaces and render");
                }
            },
            protocol::S2CPacket::ResetScene {} => {
                self.reset_scene();
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
            protocol::S2CPacket::ElementSetSize { element_id, width, height } => {
                self.apply_element_size(element_id, width, height);
            },
            protocol::S2CPacket::ElementSetDepth { element_id, depth } => {
                self.apply_element_depth(element_id, depth);
            },
            protocol::S2CPacket::ElementSetTextContent { element_id, text } => {
                self.apply_element_text_content(element_id, text);
            },
            protocol::S2CPacket::ElementSetTextLayout {
                element_id,
                max_width,
                font_size,
                line_height,
            } => {
                self.apply_element_text_layout(element_id, max_width, font_size, line_height);
            },
            protocol::S2CPacket::ElementSetTexture { element_id, resource_id } => {
                self.apply_element_texture(element_id, resource_id);
            },
            protocol::S2CPacket::ResourceFree { resource_id } => {
                self.free_resource(resource_id);
            },
            protocol::S2CPacket::BindingDeclare { binding_id, identifier, input_type } => {
                self.handle_binding_declare(binding_id, identifier, input_type)?;
            },
            protocol::S2CPacket::ElementAdd { element_id, kind } => {
                self.handle_element_add(element_id, kind);
            },
            protocol::S2CPacket::ElementMove { element_id, x, y } => {
                self.handle_element_move(element_id, x, y);
            },
            protocol::S2CPacket::ElementRemove { element_id } => {
                if let Some(element) = self.elements.remove(&element_id) {
                    if let Some(resource_id) = element.visual.texture_id() {
                        self.release_resource_binding(resource_id, element_id);
                    }
                }
                self.pending_prediction.remove(&element_id);
            },
        }

        Ok(())
    }

    fn process_ready_messages(&mut self) -> Result<()> {
        let ready_messages =
            self.packet_chain.drain_ready(|resource_id| self.resources.contains_key(&resource_id));
        for message in ready_messages {
            self.apply_decoded_message(message)?;
        }
        Ok(())
    }

    fn apply_decoded_message(
        &mut self,
        message: protocol::decode::DecodedServerMessage,
    ) -> Result<()> {
        let receipt_id = match &message {
            protocol::decode::DecodedServerMessage::Envelope(envelope) => envelope.receipt_id,
            protocol::decode::DecodedServerMessage::Resource(resource) => resource.receipt_id,
        };

        match message {
            protocol::decode::DecodedServerMessage::Envelope(envelope) => {
                for packet in envelope.packets {
                    self.handle_server_packet(packet)?;
                }
            },
            protocol::decode::DecodedServerMessage::Resource(resource) => {
                self.store_resource(
                    resource.id,
                    resource.resource_type,
                    resource.usage_count,
                    resource.blob,
                )?;
            },
        }

        if let Some(message_id) = receipt_id {
            self.send_c2s(protocol::C2SPacket::Receipt { message_id })?;
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
        self.net.send_packet(packet)?;
        Ok(())
    }

    fn store_resource(
        &mut self,
        resource_id: protocol::MessageId,
        resource_type: String,
        usage_count: Option<i32>,
        blob: Vec<u8>,
    ) -> Result<()> {
        let resource = match ClientResource::new(resource_type, usage_count, &blob) {
            Ok(resource) => resource,
            Err(_) => {
                log::warn!("ignored invalid resource {resource_id} usage_count={usage_count:?}");
                return Ok(());
            },
        };
        let had_texture = self.texture_resource(resource_id).is_some();
        self.resources.insert(resource_id, resource);
        self.reconcile_resource_bindings(resource_id);
        let has_texture = self.texture_resource(resource_id).is_some();
        if had_texture != has_texture {
            self.bump_render_revision();
        }
        if let Some(resource) = self.resources.get(&resource_id) {
            log::debug!(
                "received resource {} type={} bytes={} remaining_uses={}",
                resource_id,
                resource.resource_type,
                blob.len(),
                resource.usage_count_display()
            );
        }
        Ok(())
    }

    fn texture_resource(&self, resource_id: protocol::MessageId) -> Option<&TextureResource> {
        self.resources.get(&resource_id).and_then(ClientResource::texture)
    }

    fn bind_resource_to_element(
        &mut self,
        resource_id: protocol::MessageId,
        element_id: u32,
    ) -> bool {
        let Some(resource) = self.resources.get_mut(&resource_id) else {
            return false;
        };
        resource.consume_for_element(element_id)
    }

    fn release_resource_binding(&mut self, resource_id: protocol::MessageId, element_id: u32) {
        let Some(resource) = self.resources.get_mut(&resource_id) else {
            return;
        };
        resource.release_element(element_id);
        if resource.should_free() {
            self.resources.remove(&resource_id);
            self.bump_render_revision();
        }
    }

    fn free_resource(&mut self, resource_id: protocol::MessageId) {
        let Some(resource) = self.resources.remove(&resource_id) else {
            return;
        };

        for element_id in resource.active_elements {
            let changed = self.set_element_texture_id(element_id, None);
            if changed {
                log::debug!("cleared freed resource {resource_id} from element {element_id}");
            }
        }

        self.bump_render_revision();
        log::debug!("freed resource {resource_id} on server request");
    }

    fn reconcile_resource_bindings(&mut self, resource_id: protocol::MessageId) {
        let element_ids: Vec<u32> = self
            .elements
            .iter()
            .filter_map(|(&element_id, element)| {
                (element.visual.texture_id() == Some(resource_id)).then_some(element_id)
            })
            .collect();

        for element_id in element_ids {
            if self.bind_resource_to_element(resource_id, element_id) {
                continue;
            }
            if self.set_element_texture_id(element_id, None) {
                self.bump_render_revision();
            }
            log::warn!("resource {resource_id} exhausted before binding to element {element_id}");
        }
    }

    fn element_texture_id(&self, element_id: u32) -> Option<Option<protocol::MessageId>> {
        self.elements.get(&element_id).map(|element| element.visual.texture_id())
    }

    fn set_element_texture_id(
        &mut self,
        element_id: u32,
        texture_id: Option<protocol::MessageId>,
    ) -> bool {
        let Some(element) = self.elements.get_mut(&element_id) else {
            return false;
        };
        match &mut element.visual {
            ElementVisual::Texture { texture_id: current_texture_id, .. } => {
                if *current_texture_id == texture_id {
                    return false;
                }
                *current_texture_id = texture_id;
                true
            },
            ElementVisual::SolidRect { .. } | ElementVisual::Text { .. } => false,
        }
    }

    fn bump_render_revision(&mut self) {
        self.render_revision = self.render_revision.wrapping_add(1);
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

fn decode_png_texture(bytes: &[u8]) -> Result<TextureResource> {
    let mut decoder = png::Decoder::new(Cursor::new(bytes));
    decoder.set_transformations(png::Transformations::EXPAND | png::Transformations::STRIP_16);
    let mut reader = decoder.read_info()?;
    let mut rgba = vec![0; reader.output_buffer_size()];
    let info = reader.next_frame(&mut rgba)?;
    if info.color_type != png::ColorType::Rgba {
        return Err(anyhow::anyhow!("expected RGBA png output, got {:?}", info.color_type));
    }
    rgba.truncate(info.buffer_size());
    Ok(TextureResource { width: info.width, height: info.height, rgba })
}

fn decode_resource_payload(resource_type: &str, blob: &[u8]) -> ClientResourcePayload {
    match resource_type {
        "image/png" => match decode_png_texture(blob) {
            Ok(texture) => ClientResourcePayload::Texture(texture),
            Err(err) => {
                log::warn!("failed decoding png resource: {err:#}");
                ClientResourcePayload::Unsupported
            },
        },
        _ => ClientResourcePayload::Unsupported,
    }
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
