use std::net::SocketAddr;

use anyhow::Result;

mod app;
mod network;
mod protocol;

pub struct GameConfig {
    pub server_addr: SocketAddr,
}

#[derive(Clone, Copy, Default)]
pub(super) struct InputState {
    pub(super) up: bool,
    pub(super) down: bool,
    pub(super) left: bool,
    pub(super) right: bool,
}

#[derive(Clone, Copy)]
pub(super) struct RenderState {
    pub(super) x: f32,
    pub(super) y: f32,
    pub(super) size: u16,
    pub(super) color: u32,
}

pub(super) struct ClientGame {
    net: network::QuicClient,
    input_seq: u32,
    sent_hello: bool,
    target_x: f32,
    target_y: f32,
    draw_x: f32,
    draw_y: f32,
    draw_size: u16,
    draw_color_rgba: [u8; 4],
}

impl ClientGame {
    pub(super) fn new(server_addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            net: network::QuicClient::connect(server_addr)?,
            input_seq: 0,
            sent_hello: false,
            target_x: 400.0,
            target_y: 300.0,
            draw_x: 400.0,
            draw_y: 300.0,
            draw_size: 32,
            draw_color_rgba: [255, 0, 0, 255],
        })
    }

    pub(super) fn tick(&mut self, input: InputState) -> Result<()> {
        let incoming = self.net.poll()?;
        for bytes in incoming {
            if let Ok(packet) = protocol::decode_s2c(&bytes) {
                self.handle_server_packet(packet);
            }
        }

        if self.net.is_established() {
            if !self.sent_hello {
                let hello = protocol::C2SPacket::ClientHello {
                    client_name: "desktop-client".to_string(),
                    capabilities: vec![
                        "render.draw_square".to_string(),
                        "prediction.lerp".to_string(),
                    ],
                };
                if let Ok(bytes) = protocol::encode_c2s(&hello) {
                    self.net.send_datagram(&bytes)?;
                }
                self.sent_hello = true;
                println!("connected to server {}", self.net.server_addr());
            }

            self.input_seq = self.input_seq.wrapping_add(1);
            let input_packet = protocol::C2SPacket::InputState {
                seq: self.input_seq,
                up: input.up,
                down: input.down,
                left: input.left,
                right: input.right,
            };
            if let Ok(bytes) = protocol::encode_c2s(&input_packet) {
                self.net.send_datagram(&bytes)?;
            }
        }

        self.draw_x += (self.target_x - self.draw_x) * 0.35;
        self.draw_y += (self.target_y - self.draw_y) * 0.35;

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

    fn handle_server_packet(&mut self, packet: protocol::S2CPacket) {
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
            protocol::S2CPacket::WorldState { x, y, .. } => {
                self.target_x = x;
                self.target_y = y;
            }
        }
    }
}

pub fn run(config: GameConfig) -> Result<()> {
    let mut game = ClientGame::new(config.server_addr)?;
    app::run(&mut game)
}

fn rgba_to_u32([r, g, b, _a]: [u8; 4]) -> u32 {
    ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
}
