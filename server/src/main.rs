use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use quiche::{Connection, RecvInfo};
use rand::RngCore;

mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

use packets::{decode_c2s, encode_s2c, C2SPacket, S2CPacket};

const MAX_DATAGRAM_SIZE: usize = 1350;
const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PLAYER_SPEED: f32 = 220.0;

#[derive(Default, Debug, Clone, Copy)]
struct InputState {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
}

struct Session {
    conn: Connection,
    client_addr: SocketAddr,
    input: InputState,
    pos_x: f32,
    pos_y: f32,
    last_world_send: Instant,
    sent_bootstrap: bool,
}

#[derive(Debug, Parser)]
#[command(name = "widev-server")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    bind: SocketAddr,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bind_addr = args.bind;

    let socket = UdpSocket::bind(bind_addr)
        .with_context(|| format!("failed to bind UDP socket at {bind_addr}"))?;
    socket
        .set_nonblocking(true)
        .context("failed to set UDP socket non-blocking")?;

    let local_addr = socket.local_addr().context("failed to read local addr")?;
    println!("server listening on {local_addr}");

    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs");
    ensure_dev_certs(&cert_dir)?;
    let cert_path = cert_dir.join("cert.crt");
    let key_path = cert_dir.join("cert.key");
    let cert_path_str = cert_path
        .to_str()
        .context("certificate path is not valid UTF-8")?;
    let key_path_str = key_path
        .to_str()
        .context("private key path is not valid UTF-8")?;

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config
        .load_cert_chain_from_pem_file(cert_path_str)
        .with_context(|| format!("failed to load {}", cert_path.display()))?;
    config
        .load_priv_key_from_pem_file(key_path_str)
        .with_context(|| format!("failed to load {}", key_path.display()))?;
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
    config.verify_peer(false);

    let server_start = Instant::now();
    let mut recv_buf = [0u8; 65_535];
    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut app_buf = [0u8; 4096];

    let mut session: Option<Session> = None;
    let mut last_tick = Instant::now();
    let tick = Duration::from_millis(16);

    loop {
        loop {
            let recv = socket.recv_from(&mut recv_buf);
            let (len, from) = match recv {
                Ok(v) => v,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err).context("socket recv_from failed"),
            };

            if session.is_none() {
                let mut pkt_buf = recv_buf[..len].to_vec();
                let hdr = match quiche::Header::from_slice(&mut pkt_buf, quiche::MAX_CONN_ID_LEN) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                if hdr.ty != quiche::Type::Initial {
                    continue;
                }

                let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
                rand::thread_rng().fill_bytes(&mut scid);
                let scid = quiche::ConnectionId::from_ref(&scid);

                let conn = quiche::accept(&scid, None, local_addr, from, &mut config)
                    .context("failed to accept incoming QUIC connection")?;

                println!("accepted connection from {from}");

                session = Some(Session {
                    conn,
                    client_addr: from,
                    input: InputState::default(),
                    pos_x: GAME_WIDTH * 0.5,
                    pos_y: GAME_HEIGHT * 0.5,
                    last_world_send: Instant::now(),
                    sent_bootstrap: false,
                });
            }

            if let Some(active) = session.as_mut() {
                if from != active.client_addr {
                    continue;
                }

                let recv_info = RecvInfo {
                    from,
                    to: local_addr,
                };

                if let Err(err) = active.conn.recv(&mut recv_buf[..len], recv_info) {
                    if err != quiche::Error::Done {
                        eprintln!("conn.recv failed: {err:?}");
                    }
                }
            }
        }

        let now = Instant::now();
        let dt = now.duration_since(last_tick);
        if dt >= tick {
            if let Some(active) = session.as_mut() {
                if active.conn.is_established() {
                    pump_app_packets(active, &mut app_buf);

                    if !active.sent_bootstrap {
                        send_bootstrap(active);
                        active.sent_bootstrap = true;
                    }

                    simulate(active, dt.as_secs_f32());

                    if now.duration_since(active.last_world_send) >= Duration::from_millis(33) {
                        let packet = S2CPacket::WorldState {
                            server_time_ms: server_start.elapsed().as_millis() as u64,
                            x: active.pos_x,
                            y: active.pos_y,
                        };
                        if let Ok(bytes) = encode_s2c(&packet) {
                            let _ = active.conn.dgram_send(&bytes);
                        }
                        active.last_world_send = now;
                    }
                }

                flush_quic(&socket, active, &mut send_buf)?;

                if active.conn.is_closed() {
                    println!("client disconnected");
                    session = None;
                }
            }

            last_tick = now;
        }

        if let Some(active) = session.as_mut() {
            if let Some(timeout) = active.conn.timeout() {
                if timeout.is_zero() {
                    active.conn.on_timeout();
                }
            }
        }

        std::thread::sleep(Duration::from_millis(1));
    }
}

fn pump_app_packets(active: &mut Session, app_buf: &mut [u8]) {
    loop {
        match active.conn.dgram_recv(app_buf) {
            Ok(len) => {
                if let Ok(packet) = decode_c2s(&app_buf[..len]) {
                    match packet {
                        C2SPacket::ClientHello {
                            client_name,
                            capabilities,
                        } => {
                            println!("client hello: {client_name} / {capabilities:?}");
                        }
                        C2SPacket::InputState {
                            up,
                            down,
                            left,
                            right,
                            ..
                        } => {
                            active.input = InputState {
                                up,
                                down,
                                left,
                                right,
                            };
                        }
                    }
                }
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
}

fn send_bootstrap(active: &mut Session) {
    let hello = S2CPacket::ServerHello { tick_rate_hz: 60 };
    let manifest = S2CPacket::AssetManifest {
        player_color_rgba: [255, 0, 0, 255],
        player_size: 32,
    };

    if let Ok(bytes) = encode_s2c(&hello) {
        let _ = active.conn.dgram_send(&bytes);
    }

    if let Ok(bytes) = encode_s2c(&manifest) {
        let _ = active.conn.dgram_send(&bytes);
    }
}

fn simulate(active: &mut Session, dt: f32) {
    let mut dx = 0.0;
    let mut dy = 0.0;

    if active.input.left {
        dx -= 1.0;
    }
    if active.input.right {
        dx += 1.0;
    }
    if active.input.up {
        dy -= 1.0;
    }
    if active.input.down {
        dy += 1.0;
    }

    active.pos_x = (active.pos_x + dx * PLAYER_SPEED * dt).clamp(0.0, GAME_WIDTH);
    active.pos_y = (active.pos_y + dy * PLAYER_SPEED * dt).clamp(0.0, GAME_HEIGHT);
}

fn flush_quic(socket: &UdpSocket, active: &mut Session, send_buf: &mut [u8]) -> Result<()> {
    loop {
        match active.conn.send(send_buf) {
            Ok((len, send_info)) => {
                let _ = socket.send_to(&send_buf[..len], send_info.to)?;
            }
            Err(quiche::Error::Done) => break,
            Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
        }
    }

    Ok(())
}

fn ensure_dev_certs(cert_dir: &PathBuf) -> Result<()> {
    let cert_path = cert_dir.join("cert.crt");
    let key_path = cert_dir.join("cert.key");
    if cert_path.exists() && key_path.exists() {
        return Ok(());
    }

    fs::create_dir_all(cert_dir)
        .with_context(|| format!("failed to create cert directory {}", cert_dir.display()))?;

    let certified_key = rcgen::generate_simple_self_signed(vec!["widev.local".to_string()])
        .context("failed to generate self-signed cert")?;
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.key_pair.serialize_pem();

    fs::write(&cert_path, cert_pem)
        .with_context(|| format!("failed to write {}", cert_path.display()))?;
    fs::write(&key_path, key_pem)
        .with_context(|| format!("failed to write {}", key_path.display()))?;

    println!("generated local dev certs in {}", cert_dir.display());
    Ok(())
}
