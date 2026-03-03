use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use anyhow::{Context, Result};
use minifb::{Key, Window, WindowOptions};
use quiche::RecvInfo;
use rand::RngCore;

mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

use packets::{decode_s2c, encode_c2s, C2SPacket, S2CPacket};

const WIDTH: usize = 800;
const HEIGHT: usize = 600;
const MAX_DATAGRAM_SIZE: usize = 1350;

pub struct GameConfig {
    pub server_addr: SocketAddr,
}

pub fn run(config: GameConfig) -> Result<()> {
    let mut window = Window::new("widev desktop POC", WIDTH, HEIGHT, WindowOptions::default())
        .context("failed to create desktop window")?;

    window.set_target_fps(60);

    let mut net = QuicClient::connect(config.server_addr)?;
    let mut buffer = vec![0x101010u32; WIDTH * HEIGHT];

    while window.is_open() && !window.is_key_down(Key::Escape) {
        let input = InputState {
            up: window.is_key_down(Key::W),
            down: window.is_key_down(Key::S),
            left: window.is_key_down(Key::A),
            right: window.is_key_down(Key::D),
        };

        net.poll()?;
        net.send_input(input);

        clear(&mut buffer, 0x101010);
        let (x, y, size, color) = net.render_state();
        draw_square(&mut buffer, x as i32, y as i32, size as i32, color);

        window
            .update_with_buffer(&buffer, WIDTH, HEIGHT)
            .context("failed to update frame buffer")?;
    }

    Ok(())
}

#[derive(Clone, Copy, Default)]
struct InputState {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
}

struct QuicClient {
    socket: UdpSocket,
    conn: quiche::Connection,
    server_addr: SocketAddr,
    local_addr: SocketAddr,
    send_buf: [u8; MAX_DATAGRAM_SIZE],
    recv_buf: [u8; 65_535],
    app_buf: [u8; 4096],
    input_seq: u32,
    sent_hello: bool,
    target_x: f32,
    target_y: f32,
    draw_x: f32,
    draw_y: f32,
    draw_size: u16,
    draw_color_rgba: [u8; 4],
}

impl QuicClient {
    fn connect(server_addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to bind UDP socket")?;
        socket
            .set_nonblocking(true)
            .context("failed to set UDP socket non-blocking")?;

        let local_addr = socket.local_addr().context("failed to get local addr")?;

        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        config.verify_peer(false);
        config
            .set_application_protos(&[b"widev-poc-quic"])
            .context("failed setting ALPN")?;
        config.set_max_idle_timeout(10_000);
        config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
        config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
        config.set_initial_max_data(10_000_000);
        config.set_initial_max_stream_data_bidi_local(1_000_000);
        config.set_initial_max_stream_data_bidi_remote(1_000_000);
        config.set_initial_max_streams_bidi(16);
        config.set_initial_max_streams_uni(16);
        config.enable_dgram(true, 64, 64);

        let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
        rand::thread_rng().fill_bytes(&mut scid);
        let scid = quiche::ConnectionId::from_ref(&scid);

        let conn = quiche::connect(
            Some("widev.local"),
            &scid,
            local_addr,
            server_addr,
            &mut config,
        )
        .context("failed to create QUIC connection")?;

        let mut client = Self {
            socket,
            conn,
            server_addr,
            local_addr,
            send_buf: [0; MAX_DATAGRAM_SIZE],
            recv_buf: [0; 65_535],
            app_buf: [0; 4096],
            input_seq: 0,
            sent_hello: false,
            target_x: (WIDTH / 2) as f32,
            target_y: (HEIGHT / 2) as f32,
            draw_x: (WIDTH / 2) as f32,
            draw_y: (HEIGHT / 2) as f32,
            draw_size: 32,
            draw_color_rgba: [255, 0, 0, 255],
        };

        client.flush_outgoing()?;
        Ok(client)
    }

    fn poll(&mut self) -> Result<()> {
        loop {
            let recv = self.socket.recv_from(&mut self.recv_buf);
            let (len, from) = match recv {
                Ok(v) => v,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err).context("socket recv_from failed"),
            };

            if from != self.server_addr {
                continue;
            }

            let recv_info = RecvInfo {
                from,
                to: self.local_addr,
            };

            if let Err(err) = self.conn.recv(&mut self.recv_buf[..len], recv_info) {
                if err != quiche::Error::Done {
                    eprintln!("client conn.recv error: {err:?}");
                }
            }
        }

        if self.conn.is_established() && !self.sent_hello {
            let hello = C2SPacket::ClientHello {
                client_name: "desktop-client".to_string(),
                capabilities: vec![
                    "render.draw_square".to_string(),
                    "prediction.lerp".to_string(),
                ],
            };
            if let Ok(bytes) = encode_c2s(&hello) {
                let _ = self.conn.dgram_send(&bytes);
            }
            self.sent_hello = true;
            println!("connected to server {}", self.server_addr);
        }

        loop {
            match self.conn.dgram_recv(&mut self.app_buf) {
                Ok(len) => {
                    if let Ok(packet) = decode_s2c(&self.app_buf[..len]) {
                        self.handle_server_packet(packet);
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }

        if let Some(timeout) = self.conn.timeout() {
            if timeout.is_zero() {
                self.conn.on_timeout();
            }
        }

        self.draw_x += (self.target_x - self.draw_x) * 0.35;
        self.draw_y += (self.target_y - self.draw_y) * 0.35;

        self.flush_outgoing()?;
        Ok(())
    }

    fn handle_server_packet(&mut self, packet: S2CPacket) {
        match packet {
            S2CPacket::ServerHello { tick_rate_hz } => {
                println!("server tick rate: {tick_rate_hz}Hz");
            }
            S2CPacket::AssetManifest {
                player_color_rgba,
                player_size,
            } => {
                self.draw_color_rgba = player_color_rgba;
                self.draw_size = player_size;
            }
            S2CPacket::WorldState { x, y, .. } => {
                self.target_x = x;
                self.target_y = y;
            }
        }
    }

    fn send_input(&mut self, input: InputState) {
        if !self.conn.is_established() {
            return;
        }

        self.input_seq = self.input_seq.wrapping_add(1);

        let packet = C2SPacket::InputState {
            seq: self.input_seq,
            up: input.up,
            down: input.down,
            left: input.left,
            right: input.right,
        };

        if let Ok(bytes) = encode_c2s(&packet) {
            let _ = self.conn.dgram_send(&bytes);
        }
    }

    fn flush_outgoing(&mut self) -> Result<()> {
        loop {
            match self.conn.send(&mut self.send_buf) {
                Ok((len, send_info)) => {
                    self.socket
                        .send_to(&self.send_buf[..len], send_info.to)
                        .context("socket send_to failed")?;
                }
                Err(quiche::Error::Done) => break,
                Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
            }
        }

        Ok(())
    }

    fn render_state(&self) -> (f32, f32, u16, u32) {
        (
            self.draw_x,
            self.draw_y,
            self.draw_size,
            rgba_to_u32(self.draw_color_rgba),
        )
    }
}

fn rgba_to_u32([r, g, b, _a]: [u8; 4]) -> u32 {
    ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
}

fn clear(buf: &mut [u32], color: u32) {
    buf.fill(color);
}

fn draw_square(buf: &mut [u32], x: i32, y: i32, size: i32, color: u32) {
    let half = size / 2;
    let x_min = (x - half).max(0);
    let y_min = (y - half).max(0);
    let x_max = (x + half).min(WIDTH as i32 - 1);
    let y_max = (y + half).min(HEIGHT as i32 - 1);

    for yy in y_min..=y_max {
        for xx in x_min..=x_max {
            let idx = yy as usize * WIDTH + xx as usize;
            buf[idx] = color;
        }
    }
}
