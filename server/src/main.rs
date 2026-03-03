use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use quiche::{Connection, RecvInfo};
use rand::RngCore;

mod game;
mod game_state;
mod games;
#[allow(dead_code)]
mod packets;

use game::{ClientId, Game};
use game_state::GameState;
use packets::{decode_c2s, encode_s2c, S2CPacket};

const MAX_DATAGRAM_SIZE: usize = 1350;
const TICK_INTERVAL: Duration = Duration::from_millis(16);
const IDLE_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug, Parser)]
#[command(name = "widev-server")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    bind: SocketAddr,
}

struct Session {
    client_id: ClientId,
    conn: Connection,
    client_addr: SocketAddr,
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

    let mut config = build_server_quic_config(cert_path_str, key_path_str, &cert_path, &key_path)?;

    let mut recv_buf = [0u8; 65_535];
    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut app_buf = [0u8; 4096];

    let mut sessions: Vec<Session> = Vec::new();
    let mut next_client_id: ClientId = 1;
    let mut game_state = GameState::new();
    let mut game = games::default_game(Instant::now(), &mut game_state);
    let mut last_tick = Instant::now();

    loop {
        loop {
            let recv = socket.recv_from(&mut recv_buf);
            let (len, from) = match recv {
                Ok(v) => v,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err).context("socket recv_from failed"),
            };

            if let Some(session) = sessions.iter_mut().find(|s| s.client_addr == from) {
                let recv_info = RecvInfo {
                    from,
                    to: local_addr,
                };
                if let Err(err) = session.conn.recv(&mut recv_buf[..len], recv_info) {
                    if err != quiche::Error::Done {
                        eprintln!("conn.recv failed: {err:?}");
                    }
                }
                continue;
            }

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

            let client_id = next_client_id;
            next_client_id = next_client_id.wrapping_add(1).max(1);

            sessions.push(Session {
                client_id,
                conn,
                client_addr: from,
            });
            game_state.connect_client(client_id);
            game.on_client_connected(&mut game_state, client_id);
            println!("accepted connection from {from} as client {client_id}");
        }

        let now = Instant::now();
        let dt = now.duration_since(last_tick);
        if dt >= TICK_INTERVAL {
            for session in &mut sessions {
                if session.conn.is_established() {
                    pump_app_packets(session, &mut app_buf, game.as_mut(), &mut game_state);
                }
            }

            game.on_tick(&mut game_state, now, dt);

            let mut disconnected_ids: Vec<ClientId> = Vec::new();
            sessions.retain_mut(|session| {
                if session.conn.is_established() {
                    let packets = game_state.drain_packets_for(session.client_id);
                    send_game_packets(session, packets);
                }

                if flush_quic(&socket, session, &mut send_buf).is_err() || session.conn.is_closed()
                {
                    disconnected_ids.push(session.client_id);
                    return false;
                }

                true
            });

            for client_id in disconnected_ids {
                game.on_client_disconnected(&mut game_state, client_id);
                game_state.disconnect_client(client_id);
            }

            last_tick = now;
        }

        for session in &mut sessions {
            if let Some(timeout) = session.conn.timeout() {
                if timeout.is_zero() {
                    session.conn.on_timeout();
                }
            }
        }

        std::thread::sleep(IDLE_SLEEP);
    }
}

fn pump_app_packets(
    session: &mut Session,
    app_buf: &mut [u8],
    game: &mut dyn Game,
    state: &mut GameState,
) {
    loop {
        match session.conn.dgram_recv(app_buf) {
            Ok(len) => {
                if let Ok(packet) = decode_c2s(&app_buf[..len]) {
                    game.on_client_packet(state, session.client_id, packet);
                }
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
}

fn send_game_packets(session: &mut Session, packets: Vec<S2CPacket>) {
    for packet in packets {
        if let Ok(bytes) = encode_s2c(&packet) {
            let _ = session.conn.dgram_send(&bytes);
        }
    }
}

fn flush_quic(socket: &UdpSocket, session: &mut Session, send_buf: &mut [u8]) -> Result<()> {
    loop {
        match session.conn.send(send_buf) {
            Ok((len, send_info)) => {
                socket.send_to(&send_buf[..len], send_info.to)?;
            }
            Err(quiche::Error::Done) => break,
            Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
        }
    }

    Ok(())
}

fn build_server_quic_config(
    cert_path_str: &str,
    key_path_str: &str,
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> Result<quiche::Config> {
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
    Ok(config)
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
