use std::collections::{hash_map::Entry, HashMap};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use quiche::RecvInfo;
use rand::Rng;

const MAX_DATAGRAM_SIZE: usize = 1350;
const WORKER_IDLE_SLEEP: Duration = Duration::from_millis(5);
const ACTIVE_CONNECTION_ID_LIMIT: u64 = 4;
const TARGET_ACTIVE_SCIDS: usize = ACTIVE_CONNECTION_ID_LIMIT as usize;

pub(super) struct IncomingPackets {
    pub(super) datagrams: Vec<Vec<u8>>,
    pub(super) streams: Vec<Vec<u8>>,
}

enum WorkerCommand {
    SendDatagram(Vec<u8>),
    RebindSocket(UdpSocket),
    Shutdown,
}

struct WorkerIncoming {
    datagrams: Vec<Vec<u8>>,
    streams: Vec<Vec<u8>>,
}

pub(super) struct QuicClient {
    server_addr: SocketAddr,
    command_tx: mpsc::Sender<WorkerCommand>,
    incoming_rx: mpsc::Receiver<WorkerIncoming>,
    worker_handle: Option<thread::JoinHandle<()>>,
    is_established: Arc<AtomicBool>,
    peer_cert_der: Arc<Mutex<Option<Vec<u8>>>>,
    path_rtt_us: Arc<AtomicU64>,
}

#[derive(Default)]
struct QuicStreamState {
    recv_buffer: Vec<u8>,
    recv_finished: bool,
}

impl QuicClient {
    pub(super) fn connect(server_addr: SocketAddr) -> Result<Self> {
        log::info!("connecting to server {server_addr}...");
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to bind UDP socket")?;
        socket.set_nonblocking(true).context("failed to set UDP socket non-blocking")?;

        let local_addr = socket.local_addr().context("failed to get local addr")?;
        log::info!("local UDP endpoint: {local_addr}");

        let mut config = build_client_quic_config()?;

        let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
        rand::rng().fill_bytes(&mut scid);
        let scid = quiche::ConnectionId::from_ref(&scid);

        let conn =
            quiche::connect(Some("widev.local"), &scid, local_addr, server_addr, &mut config)
                .context("failed to create QUIC connection")?;
        log::info!("QUIC connection object created, starting handshake...");

        let (command_tx, command_rx) = mpsc::channel::<WorkerCommand>();
        let (incoming_tx, incoming_rx) = mpsc::channel::<WorkerIncoming>();

        let is_established = Arc::new(AtomicBool::new(false));
        let peer_cert_der = Arc::new(Mutex::new(None));
        let path_rtt_us = Arc::new(AtomicU64::new(0));

        let is_established_worker = Arc::clone(&is_established);
        let peer_cert_der_worker = Arc::clone(&peer_cert_der);
        let path_rtt_us_worker = Arc::clone(&path_rtt_us);

        let worker_handle = thread::spawn(move || {
            if let Err(err) = run_worker(
                socket,
                conn,
                server_addr,
                local_addr,
                command_rx,
                incoming_tx,
                is_established_worker,
                peer_cert_der_worker,
                path_rtt_us_worker,
            ) {
                log::error!("client network worker failed: {err:#}");
            }
        });

        Ok(Self {
            server_addr,
            command_tx,
            incoming_rx,
            worker_handle: Some(worker_handle),
            is_established,
            peer_cert_der,
            path_rtt_us,
        })
    }

    pub(super) fn is_established(&self) -> bool {
        self.is_established.load(Ordering::Relaxed)
    }

    pub(super) fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub(super) fn peer_cert_der(&self) -> Option<Vec<u8>> {
        self.peer_cert_der.lock().ok().and_then(|cert| cert.clone())
    }

    pub(super) fn path_rtt(&self) -> Option<Duration> {
        let micros = self.path_rtt_us.load(Ordering::Relaxed);
        if micros == 0 {
            None
        } else {
            Some(Duration::from_micros(micros))
        }
    }

    pub(super) fn poll(&mut self) -> Result<IncomingPackets> {
        let mut datagrams = Vec::new();
        let mut streams = Vec::new();
        while let Ok(incoming) = self.incoming_rx.try_recv() {
            datagrams.extend(incoming.datagrams);
            streams.extend(incoming.streams);
        }
        Ok(IncomingPackets { datagrams, streams })
    }

    pub(super) fn send_datagram(&mut self, payload: &[u8]) -> Result<()> {
        self.command_tx
            .send(WorkerCommand::SendDatagram(payload.to_vec()))
            .context("client network worker is unavailable")
    }

    pub(super) fn handle_network_change(&mut self) -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to rebind UDP socket")?;
        socket.set_nonblocking(true).context("failed to set rebound UDP socket non-blocking")?;
        self.command_tx
            .send(WorkerCommand::RebindSocket(socket))
            .context("client network worker is unavailable")
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        let _ = self.command_tx.send(WorkerCommand::Shutdown);
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    mut socket: UdpSocket,
    mut conn: quiche::Connection,
    server_addr: SocketAddr,
    mut local_addr: SocketAddr,
    command_rx: mpsc::Receiver<WorkerCommand>,
    incoming_tx: mpsc::Sender<WorkerIncoming>,
    is_established: Arc<AtomicBool>,
    peer_cert_der: Arc<Mutex<Option<Vec<u8>>>>,
    path_rtt_us: Arc<AtomicU64>,
) -> Result<()> {
    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut recv_buf = [0u8; 65_535];
    let mut app_buf = [0u8; 4096];
    let mut stream_states: HashMap<u64, QuicStreamState> = HashMap::new();

    advertise_spare_scids(&mut conn);
    flush_outgoing(&socket, &mut conn, &mut send_buf)?;

    loop {
        let mut had_activity = false;

        if process_worker_commands(
            &command_rx,
            &mut socket,
            &mut conn,
            &mut local_addr,
            &mut had_activity,
        )? {
            return Ok(());
        }
        recv_udp(&socket, &mut conn, &mut recv_buf, server_addr, local_addr, &mut had_activity)?;
        let datagrams = drain_datagrams(&mut conn, &mut app_buf, &mut had_activity);
        let streams = drain_streams(&mut conn, &mut app_buf, &mut stream_states, &mut had_activity);

        if let Some(timeout) = conn.timeout() {
            if timeout.is_zero() {
                conn.on_timeout();
                had_activity = true;
            }
        }

        drain_path_events(&mut conn);
        advertise_spare_scids(&mut conn);

        if flush_outgoing(&socket, &mut conn, &mut send_buf)? {
            had_activity = true;
        }

        if !datagrams.is_empty() || !streams.is_empty() {
            let _ = incoming_tx.send(WorkerIncoming { datagrams, streams });
        }

        refresh_shared_connection_state(&conn, &is_established, &peer_cert_der, &path_rtt_us);

        if conn.is_closed() {
            return Ok(());
        }

        if !had_activity {
            // Keep this tiny to reduce jitter while still avoiding a busy loop.
            thread::sleep(WORKER_IDLE_SLEEP);
        }
    }
}

fn process_worker_commands(
    command_rx: &mpsc::Receiver<WorkerCommand>,
    socket: &mut UdpSocket,
    conn: &mut quiche::Connection,
    local_addr: &mut SocketAddr,
    had_activity: &mut bool,
) -> Result<bool> {
    while let Ok(cmd) = command_rx.try_recv() {
        match cmd {
            WorkerCommand::SendDatagram(payload) => {
                let _ = conn.dgram_send(&payload);
                *had_activity = true;
            },
            WorkerCommand::RebindSocket(new_socket) => {
                let new_local_addr =
                    new_socket.local_addr().context("failed to get rebound local addr")?;
                let previous_local_addr = *local_addr;
                let previous_socket = std::mem::replace(socket, new_socket);
                *local_addr = new_local_addr;
                if conn.is_established() {
                    if let Err(err) = conn.migrate_source(*local_addr) {
                        *socket = previous_socket;
                        *local_addr = previous_local_addr;
                        log::warn!("failed to migrate QUIC connection to rebound socket: {err:?}");
                        continue;
                    }
                    log::info!("client migrated QUIC socket to {}", *local_addr);
                } else {
                    log::info!("client rebound UDP socket to {}", *local_addr);
                }
                *had_activity = true;
            },
            WorkerCommand::Shutdown => return Ok(true),
        }
    }
    Ok(false)
}

fn advertise_spare_scids(conn: &mut quiche::Connection) {
    while conn.active_scids() < TARGET_ACTIVE_SCIDS && conn.scids_left() > 0 {
        let mut cid = [0u8; quiche::MAX_CONN_ID_LEN];
        rand::rng().fill_bytes(&mut cid);
        let cid = quiche::ConnectionId::from_ref(&cid);
        if let Err(err) = conn.new_scid(&cid, rand::random::<u128>(), false) {
            log::debug!("failed to advertise spare client CID: {err:?}");
            break;
        }
    }
}

fn drain_path_events(conn: &mut quiche::Connection) {
    while let Some(event) = conn.path_event_next() {
        match event {
            quiche::PathEvent::New(local, peer) => {
                log::debug!("client observed path {} -> {}", local, peer);
            },
            quiche::PathEvent::Validated(local, peer) => {
                log::info!("client validated path {} -> {}", local, peer);
            },
            quiche::PathEvent::FailedValidation(local, peer) => {
                log::warn!("client path validation failed {} -> {}", local, peer);
            },
            quiche::PathEvent::Closed(local, peer) => {
                log::debug!("client closed path {} -> {}", local, peer);
            },
            quiche::PathEvent::ReusedSourceConnectionId(seq, old, new) => {
                log::warn!(
                    "client reused CID seq {} from {} -> {} to {} -> {}",
                    seq,
                    old.0,
                    old.1,
                    new.0,
                    new.1
                );
            },
            quiche::PathEvent::PeerMigrated(_, peer) => {
                log::warn!("server unexpectedly migrated to {}", peer);
            },
        }
    }
}

fn recv_udp(
    socket: &UdpSocket,
    conn: &mut quiche::Connection,
    recv_buf: &mut [u8],
    server_addr: SocketAddr,
    local_addr: SocketAddr,
    had_activity: &mut bool,
) -> Result<()> {
    loop {
        let recv = socket.recv_from(recv_buf);
        let (len, from) = match recv {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err).context("socket recv_from failed"),
        };

        if from != server_addr {
            continue;
        }

        let recv_info = RecvInfo { from, to: local_addr };
        if let Err(err) = conn.recv(&mut recv_buf[..len], recv_info) {
            if err != quiche::Error::Done {
                log::warn!("client conn.recv error: {err:?}");
            }
        }
        *had_activity = true;
    }
    Ok(())
}

fn drain_datagrams(
    conn: &mut quiche::Connection,
    app_buf: &mut [u8],
    had_activity: &mut bool,
) -> Vec<Vec<u8>> {
    let mut datagrams = Vec::new();
    loop {
        match conn.dgram_recv(app_buf) {
            Ok(len) => {
                let mut framed = app_buf[..len].to_vec();
                datagrams.extend(drain_framed_packets(&mut framed));
                *had_activity = true;
            },
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
    datagrams
}

fn drain_streams(
    conn: &mut quiche::Connection,
    app_buf: &mut [u8],
    stream_states: &mut HashMap<u64, QuicStreamState>,
    had_activity: &mut bool,
) -> Vec<Vec<u8>> {
    let mut streams = Vec::new();
    for stream_id in conn.readable() {
        loop {
            match conn.stream_recv(stream_id, app_buf) {
                Ok((len, fin)) => {
                    let chunk = app_buf[..len].to_vec();
                    streams.extend(ingest_stream_data(stream_states, conn, stream_id, &chunk, fin));
                    *had_activity = true;
                    if fin {
                        break;
                    }
                },
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }
    }
    streams
}

fn refresh_shared_connection_state(
    conn: &quiche::Connection,
    is_established: &AtomicBool,
    peer_cert_der: &Mutex<Option<Vec<u8>>>,
    path_rtt_us: &AtomicU64,
) {
    is_established.store(conn.is_established(), Ordering::Relaxed);
    if !conn.is_established() {
        return;
    }

    if let Ok(mut cert_slot) = peer_cert_der.lock() {
        if cert_slot.is_none() {
            *cert_slot = conn.peer_cert().map(|bytes| bytes.to_vec());
        }
    }
    if let Some(path) = conn.path_stats().next() {
        path_rtt_us.store(path.rtt.as_micros() as u64, Ordering::Relaxed);
    }
}

fn ingest_stream_data(
    stream_states: &mut HashMap<u64, QuicStreamState>,
    conn: &quiche::Connection,
    stream_id: u64,
    bytes: &[u8],
    fin: bool,
) -> Vec<Vec<u8>> {
    let state = match stream_states.entry(stream_id) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            log::debug!("client stream {} created (rx)", stream_id);
            entry.insert(QuicStreamState::default())
        },
    };
    state.recv_buffer.extend_from_slice(bytes);
    if fin && !state.recv_finished {
        log::debug!("client stream {} received FIN", stream_id);
    }
    state.recv_finished |= fin;

    let frames = drain_framed_packets(&mut state.recv_buffer);

    cleanup_stream_if_closed(stream_states, conn, stream_id);
    frames
}

fn drain_framed_packets(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while let Some(frame) = pop_frame(buffer) {
        frames.push(frame);
    }
    frames
}

fn cleanup_stream_if_closed(
    stream_states: &mut HashMap<u64, QuicStreamState>,
    conn: &quiche::Connection,
    stream_id: u64,
) {
    let should_remove = if let Some(state) = stream_states.get(&stream_id) {
        state.recv_buffer.is_empty() && (state.recv_finished || conn.stream_finished(stream_id))
    } else {
        false
    };
    if should_remove {
        stream_states.remove(&stream_id);
        log::debug!("client stream {} cleaned up", stream_id);
    }
}

fn flush_outgoing(
    socket: &UdpSocket,
    conn: &mut quiche::Connection,
    send_buf: &mut [u8],
) -> Result<bool> {
    let mut sent_any = false;
    loop {
        match conn.send(send_buf) {
            Ok((len, send_info)) => {
                socket.send_to(&send_buf[..len], send_info.to).context("socket send_to failed")?;
                sent_any = true;
            },
            Err(quiche::Error::Done) => break,
            Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
        }
    }

    Ok(sent_any)
}

fn build_client_quic_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.verify_peer(false);
    config.set_application_protos(&[b"widev-poc-quic"]).context("failed setting ALPN")?;
    config.set_max_idle_timeout(10_000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(16);
    config.set_initial_max_streams_uni(16);
    config.set_active_connection_id_limit(ACTIVE_CONNECTION_ID_LIMIT);
    config.set_disable_active_migration(false);
    config.enable_dgram(true, 64, 64);
    Ok(config)
}

fn pop_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buffer.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    if buffer.len() < 4 + len {
        return None;
    }
    let payload = buffer[4..4 + len].to_vec();
    buffer.drain(..4 + len);
    Some(payload)
}
