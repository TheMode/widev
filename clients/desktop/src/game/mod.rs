use std::collections::VecDeque;
use std::net::SocketAddr;

use anyhow::Result;
use minifb::Key;
use sha2::{Digest, Sha256};

mod app;
mod network;
mod persistence;
mod protocol;

const INPUT_RESEND_EVERY_FRAMES: u16 = 8;
const LERP_ALPHA: f32 = 0.35;

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
    pub(super) suggestion: Option<Key>,
}

struct BindingDefinition {
    id: u16,
    identifier: String,
    input_type: protocol::InputType,
}

struct BindingAssignment {
    id: u16,
    key: Key,
    last_value: f32,
    frames_since_send: u16,
}

pub(super) struct ClientGame {
    net: network::QuicClient,
    sent_hello: bool,
    server_cert_fingerprint: Option<String>,
    target_x: f32,
    target_y: f32,
    draw_x: f32,
    draw_y: f32,
    draw_size: u16,
    draw_color_rgba: [u8; 4],
    pending_bindings: VecDeque<BindingDefinition>,
    binding_suggestion: Option<Key>,
    active_bindings: Vec<BindingAssignment>,
    binding_store: persistence::BindingStore,
}

impl ClientGame {
    pub(super) fn new(server_addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            net: network::QuicClient::connect(server_addr)?,
            sent_hello: false,
            server_cert_fingerprint: None,
            target_x: 400.0,
            target_y: 300.0,
            draw_x: 400.0,
            draw_y: 300.0,
            draw_size: 32,
            draw_color_rgba: [255, 0, 0, 255],
            pending_bindings: VecDeque::new(),
            binding_suggestion: None,
            active_bindings: Vec::new(),
            binding_store: persistence::BindingStore::load_default()?,
        })
    }

    pub(super) fn tick_network(&mut self) -> Result<()> {
        let incoming = self.net.poll()?;

        if self.server_cert_fingerprint.is_none() {
            if let Some(cert_der) = self.net.peer_cert_der() {
                let fp = fingerprint_hex(&cert_der);
                self.server_cert_fingerprint = Some(fp.clone());
                println!("connected cert fingerprint: {fp}");
                let cached = self.binding_store.binding_count(&fp);
                if cached == 0 {
                    println!("no cached bindings for this server cert");
                } else {
                    println!("found {cached} cached binding(s) for this server cert");
                }
            }
        }

        for bytes in incoming {
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
            println!("connected to server {}", self.net.server_addr());
        }

        self.draw_x += (self.target_x - self.draw_x) * LERP_ALPHA;
        self.draw_y += (self.target_y - self.draw_y) * LERP_ALPHA;

        Ok(())
    }

    pub(super) fn binding_prompt(&self) -> Option<BindingPromptState> {
        let current = self.pending_bindings.front()?;
        Some(BindingPromptState {
            identifier: current.identifier.clone(),
            input_type: current.input_type.clone(),
            suggestion: self.binding_suggestion,
        })
    }

    pub(super) fn suggest_binding_key(&mut self, key: Key) {
        self.binding_suggestion = Some(key);
    }

    pub(super) fn confirm_binding(&mut self) -> Result<()> {
        let Some(definition) = self.pending_bindings.pop_front() else {
            return Ok(());
        };

        let Some(key) = self.binding_suggestion.take() else {
            return Ok(());
        };

        self.send_binding_ack(definition.id)?;

        if let Some(cert_fp) = &self.server_cert_fingerprint {
            self.binding_store
                .set_key(cert_fp, &definition.identifier, key);
            self.binding_store.save()?;
        }

        self.activate_binding(definition.id, key);

        println!("assigned '{}' -> {:?}", definition.identifier, key);
        Ok(())
    }

    pub(super) fn skip_binding(&mut self) {
        if let Some(definition) = self.pending_bindings.pop_front() {
            println!("skipped binding '{}'", definition.identifier);
        }
        self.binding_suggestion = None;
    }

    pub(super) fn send_bound_inputs<F>(&mut self, mut is_down: F) -> Result<()>
    where
        F: FnMut(Key) -> bool,
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

            outgoing.push(protocol::C2SPacket::InputValue {
                binding_id: binding.id,
                value,
            });
        }

        for packet in outgoing {
            self.send_c2s(packet)?;
        }

        Ok(())
    }

    pub(super) fn render_state(&self) -> RenderState {
        RenderState {
            x: self.draw_x,
            y: self.draw_y,
            size: self.draw_size,
            color: rgba_to_u32(self.draw_color_rgba),
        }
    }

    fn handle_server_packet(&mut self, packet: protocol::S2CPacket) -> Result<()> {
        match packet {
            protocol::S2CPacket::ServerHello { tick_rate_hz } => {
                println!("server tick rate: {tick_rate_hz}Hz");
            }
            protocol::S2CPacket::AssetManifest {
                player_color_rgba,
                player_size,
            } => {
                self.draw_color_rgba = player_color_rgba;
                self.draw_size = player_size;
            }
            protocol::S2CPacket::BindingDeclare {
                binding_id,
                identifier,
                input_type,
            } => {
                println!("binding request: {identifier} ({input_type:?})");

                if let Some(cert_fp) = &self.server_cert_fingerprint {
                    if let Some(saved_key) = self.binding_store.get_key(cert_fp, &identifier) {
                        self.send_binding_ack(binding_id)?;
                        self.activate_binding(binding_id, saved_key);
                        println!("restored '{}' -> {:?}", identifier, saved_key);
                        return Ok(());
                    }
                }

                self.pending_bindings.push_back(BindingDefinition {
                    id: binding_id,
                    identifier,
                    input_type,
                });
            }
            protocol::S2CPacket::WorldState { x, y, .. } => {
                self.target_x = x;
                self.target_y = y;
            }
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

    fn activate_binding(&mut self, id: u16, key: Key) {
        self.active_bindings.push(BindingAssignment {
            id,
            key,
            last_value: 0.0,
            frames_since_send: 0,
        });
    }
}

pub fn run(config: GameConfig) -> Result<()> {
    let mut game = ClientGame::new(config.server_addr)?;
    app::run(&mut game)
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
