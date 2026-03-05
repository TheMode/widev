use std::collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque};
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mio::{Events, Interest, Poll, Token};
use quiche::{Connection, RecvInfo};
use rand::RngCore;

use crate::game::{ClientId, NetworkEvent};
use crate::packets::{
    decode_c2s, encode_s2c, C2SPacket, PacketEnvelope, PacketPayload, PacketPriority, PacketTarget,
    S2CPacket, StreamID,
};

const MAX_DATAGRAM_SIZE: usize = 1350;
const POLL_SLEEP: Duration = Duration::from_millis(10);
const PING_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_RELIABLE_STREAM_ID: StreamID = 3;
const SERVER_SOCKET: Token = Token(0);
const IO_MAX_WAIT: Duration = Duration::from_millis(10);

enum IoCommand {
    AddConnection {
        client_id: ClientId,
        client_addr: SocketAddr,
        initial_packet: Vec<u8>,
    },
    IncomingDatagram {
        from: SocketAddr,
        data: Vec<u8>,
    },
    DispatchEnvelopes(Vec<PacketEnvelope>),
    Shutdown,
}

struct PacedDatagram {
    at: Instant,
    to: SocketAddr,
    bytes: Vec<u8>,
    seq: u64,
}

impl PartialEq for PacedDatagram {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}

impl Eq for PacedDatagram {}

impl PartialOrd for PacedDatagram {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PacedDatagram {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap pops the earliest datagram first.
        other.at.cmp(&self.at).then_with(|| other.seq.cmp(&self.seq))
    }
}

pub struct NetworkRuntime {
    io_senders: Vec<mpsc::Sender<IoCommand>>,
    event_rx: mpsc::Receiver<NetworkEvent>,
    threads: Vec<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl NetworkRuntime {
    pub fn start(bind_addr: SocketAddr) -> Result<Self> {
        let thread_count =
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).max(1);

        let recv_socket = UdpSocket::bind(bind_addr)
            .with_context(|| format!("failed to bind UDP socket at {bind_addr}"))?;
        recv_socket.set_nonblocking(true).context("failed to set UDP socket non-blocking")?;
        let local_addr = recv_socket.local_addr().context("failed to read local addr")?;
        log::info!("server listening on {local_addr} with {thread_count} I/O threads");

        let owner_by_addr: Arc<Mutex<HashMap<SocketAddr, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let next_client_id = Arc::new(AtomicU32::new(1));
        let running = Arc::new(AtomicBool::new(true));

        let (event_tx, event_rx) = mpsc::channel::<NetworkEvent>();

        let mut threads = Vec::new();
        let mut io_senders = Vec::with_capacity(thread_count);

        for shard_id in 0..thread_count {
            let (io_tx, io_rx) = mpsc::channel::<IoCommand>();
            io_senders.push(io_tx);

            let socket =
                recv_socket.try_clone().context("failed to clone UDP socket for I/O thread")?;
            let event_tx = event_tx.clone();
            let owner_by_addr = Arc::clone(&owner_by_addr);
            let running = Arc::clone(&running);

            let handle = thread::spawn(move || {
                if let Err(err) = run_io_thread(
                    shard_id,
                    thread_count,
                    socket,
                    local_addr,
                    io_rx,
                    event_tx,
                    owner_by_addr,
                    running,
                ) {
                    log::error!("I/O thread {shard_id} crashed: {err:#}");
                }
            });
            threads.push(handle);
        }

        let recv_io_senders = io_senders.clone();
        let owner_by_addr = Arc::clone(&owner_by_addr);
        let next_client_id = Arc::clone(&next_client_id);
        let running_recv = Arc::clone(&running);

        let recv_handle = thread::spawn(move || {
            if let Err(err) = run_recv_thread(
                recv_socket,
                recv_io_senders,
                owner_by_addr,
                next_client_id,
                thread_count,
                running_recv,
            ) {
                log::error!("receiver thread crashed: {err:#}");
            }
        });
        threads.push(recv_handle);

        Ok(Self { io_senders, event_rx, threads, running })
    }

    pub fn drain_events(&self) -> Vec<NetworkEvent> {
        let mut out = Vec::new();
        while let Ok(event) = self.event_rx.try_recv() {
            out.push(event);
        }
        out
    }

    pub fn dispatch_envelopes(&self, envelopes: Vec<PacketEnvelope>) {
        if envelopes.is_empty() {
            return;
        }
        for sender in &self.io_senders {
            let _ = sender.send(IoCommand::DispatchEnvelopes(envelopes.clone()));
        }
    }
}

impl Drop for NetworkRuntime {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        for sender in &self.io_senders {
            let _ = sender.send(IoCommand::Shutdown);
        }
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }
}

fn run_recv_thread(
    recv_socket: UdpSocket,
    io_senders: Vec<mpsc::Sender<IoCommand>>,
    owner_by_addr: Arc<Mutex<HashMap<SocketAddr, usize>>>,
    next_client_id: Arc<AtomicU32>,
    shard_count: usize,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut socket = mio::net::UdpSocket::from_std(recv_socket);
    let mut poll = Poll::new().context("failed to create mio::Poll")?;
    poll.registry()
        .register(&mut socket, SERVER_SOCKET, Interest::READABLE)
        .context("failed to register UDP socket with mio")?;
    let mut events = Events::with_capacity(1024);

    let mut recv_buf = [0u8; 65_535];

    while running.load(Ordering::Relaxed) {
        poll.poll(&mut events, Some(POLL_SLEEP)).context("mio poll failed")?;

        for event in events.iter() {
            if event.token() != SERVER_SOCKET || !event.is_readable() {
                continue;
            }

            loop {
                let (len, from) = match socket.recv_from(&mut recv_buf) {
                    Ok(v) => v,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) => return Err(err).context("UDP recv_from failed"),
                };

                let data = recv_buf[..len].to_vec();
                if let Some(owner_shard) = lookup_owner(&owner_by_addr, from) {
                    let _ =
                        io_senders[owner_shard].send(IoCommand::IncomingDatagram { from, data });
                    continue;
                }

                // Only initial packets create new sessions.
                let mut hdr_buf = data.clone();
                let hdr = match quiche::Header::from_slice(&mut hdr_buf, quiche::MAX_CONN_ID_LEN) {
                    Ok(h) => h,
                    Err(_) => continue,
                };
                if hdr.ty != quiche::Type::Initial {
                    continue;
                }

                let client_id = next_client_id.fetch_add(1, Ordering::Relaxed).max(1);
                let shard_id = shard_for_client(client_id, shard_count);
                set_owner(&owner_by_addr, from, shard_id);

                if io_senders[shard_id]
                    .send(IoCommand::AddConnection {
                        client_id,
                        client_addr: from,
                        initial_packet: data,
                    })
                    .is_err()
                {
                    remove_owner(&owner_by_addr, from);
                }
            }
        }
    }

    Ok(())
}

fn run_io_thread(
    shard_id: usize,
    shard_count: usize,
    socket: UdpSocket,
    local_addr: SocketAddr,
    io_rx: mpsc::Receiver<IoCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    owner_by_addr: Arc<Mutex<HashMap<SocketAddr, usize>>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut config = build_server_quic_config()?;

    let mut sessions: HashMap<ClientId, Session> = HashMap::new();
    let mut client_id_by_addr: HashMap<SocketAddr, ClientId> = HashMap::new();
    let mut paced_datagrams: BinaryHeap<PacedDatagram> = BinaryHeap::new();
    let mut next_paced_seq: u64 = 1;

    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut app_buf = [0u8; 4096];

    while running.load(Ordering::Relaxed) {
        flush_due_paced_datagrams(&socket, &mut paced_datagrams)?;

        for session in sessions.values_mut() {
            if let Some(timeout) = session.conn.timeout() {
                if timeout.is_zero() {
                    session.conn.on_timeout();
                }
            }
        }

        let wait_for = compute_io_wait(&sessions, &paced_datagrams);
        match io_rx.recv_timeout(wait_for) {
            Ok(cmd) => handle_io_command(
                cmd,
                shard_id,
                shard_count,
                local_addr,
                &mut config,
                &mut sessions,
                &mut client_id_by_addr,
                &event_tx,
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {},
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        while let Ok(cmd) = io_rx.try_recv() {
            if matches!(cmd, IoCommand::Shutdown) {
                return Ok(());
            }
            handle_io_command(
                cmd,
                shard_id,
                shard_count,
                local_addr,
                &mut config,
                &mut sessions,
                &mut client_id_by_addr,
                &event_tx,
            );
        }

        let mut disconnected = Vec::new();

        for (&client_id, session) in sessions.iter_mut() {
            pump_app_packets(session, &mut app_buf, &event_tx);
            session.maybe_send_ping();

            if session.flush_stream_writes().is_err()
                || flush_quic(
                    session,
                    &mut send_buf,
                    &mut paced_datagrams,
                    &mut next_paced_seq,
                )
                .is_err()
                || session.conn.is_closed()
            {
                disconnected.push(client_id);
            }
        }

        flush_due_paced_datagrams(&socket, &mut paced_datagrams)?;

        for client_id in disconnected {
            if let Some(session) = sessions.remove(&client_id) {
                client_id_by_addr.remove(&session.client_addr);
                remove_owner(&owner_by_addr, session.client_addr);
                let _ = event_tx.send(NetworkEvent::ClientDisconnected(client_id));
            }
        }
    }

    Ok(())
}

fn compute_io_wait(
    sessions: &HashMap<ClientId, Session>,
    paced_datagrams: &BinaryHeap<PacedDatagram>,
) -> Duration {
    let mut wait = IO_MAX_WAIT;

    for session in sessions.values() {
        if let Some(timeout) = session.conn.timeout() {
            wait = wait.min(timeout);
        }
    }

    if let Some(next) = paced_datagrams.peek() {
        let now = Instant::now();
        if next.at <= now {
            return Duration::ZERO;
        }
        wait = wait.min(next.at.duration_since(now));
    }

    wait
}

#[allow(clippy::too_many_arguments)]
fn handle_io_command(
    cmd: IoCommand,
    shard_id: usize,
    shard_count: usize,
    local_addr: SocketAddr,
    config: &mut quiche::Config,
    sessions: &mut HashMap<ClientId, Session>,
    client_id_by_addr: &mut HashMap<SocketAddr, ClientId>,
    event_tx: &mpsc::Sender<NetworkEvent>,
) {
    match cmd {
        IoCommand::AddConnection { client_id, client_addr, initial_packet } => {
            handle_add_connection(
                shard_id,
                shard_count,
                local_addr,
                config,
                sessions,
                client_id_by_addr,
                event_tx,
                client_id,
                client_addr,
                initial_packet,
            )
        },
        IoCommand::IncomingDatagram { from, data } => {
            if let Some(client_id) = client_id_by_addr.get(&from).copied() {
                if let Some(session) = sessions.get_mut(&client_id) {
                    let mut data = data;
                    recv_on_session(session, &mut data, from, local_addr, "conn.recv");
                }
            }
        },
        IoCommand::DispatchEnvelopes(envelopes) => {
            for envelope in envelopes {
                dispatch_envelope_for_thread(shard_id, shard_count, sessions, &envelope);
            }
        },
        IoCommand::Shutdown => {},
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_add_connection(
    shard_id: usize,
    shard_count: usize,
    local_addr: SocketAddr,
    config: &mut quiche::Config,
    sessions: &mut HashMap<ClientId, Session>,
    client_id_by_addr: &mut HashMap<SocketAddr, ClientId>,
    event_tx: &mpsc::Sender<NetworkEvent>,
    client_id: ClientId,
    client_addr: SocketAddr,
    initial_packet: Vec<u8>,
) {
    if shard_for_client(client_id, shard_count) != shard_id {
        return;
    }

    let mut pkt_buf = initial_packet;
    let hdr = match quiche::Header::from_slice(&mut pkt_buf, quiche::MAX_CONN_ID_LEN) {
        Ok(h) => h,
        Err(_) => return,
    };
    if hdr.ty != quiche::Type::Initial {
        return;
    }

    let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid);
    let scid = quiche::ConnectionId::from_ref(&scid);

    let conn = match quiche::accept(&scid, None, local_addr, client_addr, config) {
        Ok(conn) => conn,
        Err(err) => {
            log::warn!("failed to accept incoming QUIC connection: {err:?}");
            return;
        },
    };

    let mut session = Session::new(client_id, client_addr, conn);
    recv_on_session(&mut session, &mut pkt_buf, client_addr, local_addr, "conn.recv after accept");

    sessions.insert(client_id, session);
    client_id_by_addr.insert(client_addr, client_id);
    let _ = event_tx.send(NetworkEvent::ClientConnected(client_id));
    log::info!("accepted connection from {client_addr} as client {client_id}");
}

fn dispatch_envelope_for_thread(
    shard_id: usize,
    shard_count: usize,
    sessions: &mut HashMap<ClientId, Session>,
    envelope: &PacketEnvelope,
) {
    match envelope.target {
        PacketTarget::Client(client_id) => {
            if shard_for_client(client_id, shard_count) != shard_id {
                return;
            }
            if let Some(session) = sessions.get_mut(&client_id) {
                send_envelope_to_session(session, envelope);
            }
        },
        PacketTarget::Broadcast => {
            for session in sessions.values_mut() {
                send_envelope_to_session(session, envelope);
            }
        },
        PacketTarget::BroadcastExcept(excluded_client_id) => {
            if shard_for_client(excluded_client_id, shard_count) != shard_id {
                // Fast path: no local client is excluded for this shard.
                for session in sessions.values_mut() {
                    send_envelope_to_session(session, envelope);
                }
            } else {
                for (&client_id, session) in sessions.iter_mut() {
                    if client_id == excluded_client_id {
                        continue;
                    }
                    send_envelope_to_session(session, envelope);
                }
            }
        },
    }
}

fn send_envelope_to_session(session: &mut Session, envelope: &PacketEnvelope) {
    let stream_id = envelope
        .sync
        .as_ref()
        .map(|sync| sync.sequential_stream_id)
        .unwrap_or(DEFAULT_RELIABLE_STREAM_ID);
    match &envelope.payload {
        PacketPayload::Single(packet) => {
            queue_stream_packet_with_priority(session, stream_id, packet, envelope.priority)
        },
        PacketPayload::Bundle(bundle) => {
            queue_stream_bundle_with_priority(session, stream_id, bundle, envelope.priority)
        },
    }
}

fn queue_stream_packet_with_priority(
    session: &mut Session,
    stream_id: StreamID,
    packet: &S2CPacket,
    priority: PacketPriority,
) {
    let Ok(payload) = encode_s2c(packet) else {
        return;
    };
    let framed_len = 4 + payload.len();

    if matches!(priority, PacketPriority::Droppable) && !session.has_stream_budget(framed_len) {
        log::debug!(
            "dropping packet for client {} due to stream congestion budget",
            session.client_id
        );
        return;
    }

    session.queue_stream_packet(stream_id, payload);
}

fn queue_stream_bundle_with_priority(
    session: &mut Session,
    stream_id: StreamID,
    bundle: &[S2CPacket],
    priority: PacketPriority,
) {
    if bundle.is_empty() {
        return;
    }

    let mut framed_payloads = Vec::with_capacity(bundle.len());
    let mut total_framed_len = 0usize;

    for packet in bundle {
        let Ok(payload) = encode_s2c(packet) else {
            continue;
        };
        let framed_len = 4 + payload.len();
        total_framed_len = total_framed_len.saturating_add(framed_len);
        framed_payloads.push(payload);
    }

    if framed_payloads.is_empty() {
        return;
    }

    if matches!(priority, PacketPriority::Droppable) && !session.has_stream_budget(total_framed_len)
    {
        log::debug!(
            "dropping bundle for client {} due to stream congestion budget",
            session.client_id
        );
        return;
    }

    session.queue_stream_bundle(stream_id, framed_payloads, total_framed_len);
}

fn pump_app_packets(
    session: &mut Session,
    app_buf: &mut [u8],
    event_tx: &mpsc::Sender<NetworkEvent>,
) {
    loop {
        match session.conn.dgram_recv(app_buf) {
            Ok(len) => {
                decode_and_forward_c2s(session, &app_buf[..len], event_tx);
            },
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }

    for stream_id in session.conn.readable() {
        loop {
            match session.conn.stream_recv(stream_id, app_buf) {
                Ok((len, fin)) => {
                    let frames = session.ingest_stream_data(stream_id, &app_buf[..len], fin);
                    for frame in frames {
                        decode_and_forward_c2s(session, &frame, event_tx);
                    }
                    if fin {
                        break;
                    }
                },
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }
    }
}

fn decode_and_forward_c2s(
    session: &mut Session,
    bytes: &[u8],
    event_tx: &mpsc::Sender<NetworkEvent>,
) {
    let Ok(packet) = decode_c2s(bytes) else {
        return;
    };
    if handle_c2s_control_packet(session, &packet) {
        return;
    }
    let _ = event_tx.send(NetworkEvent::ClientPacket { client_id: session.client_id, packet });
}

fn handle_c2s_control_packet(session: &mut Session, packet: &C2SPacket) -> bool {
    match packet {
        C2SPacket::Ping { nonce } => {
            if let Ok(bytes) = encode_s2c(&S2CPacket::Pong { nonce: *nonce }) {
                let _ = session.conn.dgram_send(&bytes);
            }
            true
        },
        C2SPacket::Pong { nonce } => {
            if let Some(sent_at) = session.pending_ping_nonces.remove(nonce) {
                let rtt_ms = sent_at.elapsed().as_secs_f64() * 1000.0;
                let quiche_rtt_ms = session
                    .conn
                    .path_stats()
                    .next()
                    .map(|stats| stats.rtt.as_secs_f64() * 1000.0)
                    .unwrap_or_default();
                log::debug!(
                    "server latency client {}: {:.2}ms (quiche_rtt={:.2}ms)",
                    session.client_id,
                    rtt_ms,
                    quiche_rtt_ms
                );
            }
            true
        },
        _ => false,
    }
}

fn recv_on_session(
    session: &mut Session,
    buf: &mut [u8],
    from: SocketAddr,
    local_addr: SocketAddr,
    context: &str,
) {
    let recv_info = RecvInfo { from, to: local_addr };
    if let Err(err) = session.conn.recv(buf, recv_info) {
        if err != quiche::Error::Done {
            log::warn!("{context} failed: {err:?}");
        }
    }
}

fn flush_quic(
    session: &mut Session,
    send_buf: &mut [u8],
    paced_datagrams: &mut BinaryHeap<PacedDatagram>,
    next_paced_seq: &mut u64,
) -> Result<()> {
    loop {
        match session.conn.send(send_buf) {
            Ok((len, send_info)) => {
                push_paced_datagram(
                    paced_datagrams,
                    PacedDatagram {
                        at: send_info.at,
                        to: send_info.to,
                        bytes: send_buf[..len].to_vec(),
                        seq: *next_paced_seq,
                    },
                );
                *next_paced_seq = next_paced_seq.wrapping_add(1).max(1);
            },
            Err(quiche::Error::Done) => break,
            Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
        }
    }

    Ok(())
}

fn flush_due_paced_datagrams(
    socket: &UdpSocket,
    paced_datagrams: &mut BinaryHeap<PacedDatagram>,
) -> Result<()> {
    let now = Instant::now();
    while let Some(pending) = paced_datagrams.peek() {
        if pending.at > now {
            break;
        }
        let pending = paced_datagrams.pop().expect("heap top exists");
        socket.send_to(&pending.bytes, pending.to)?;
    }
    Ok(())
}

fn push_paced_datagram(paced_datagrams: &mut BinaryHeap<PacedDatagram>, datagram: PacedDatagram) {
    paced_datagrams.push(datagram);
}

fn build_server_quic_config() -> Result<quiche::Config> {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs");
    ensure_dev_certs(&cert_dir)?;
    let cert_path = cert_dir.join("cert.crt");
    let key_path = cert_dir.join("cert.key");
    let cert_path_str = cert_path.to_str().context("certificate path is not valid UTF-8")?;
    let key_path_str = key_path.to_str().context("private key path is not valid UTF-8")?;

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config
        .load_cert_chain_from_pem_file(cert_path_str)
        .with_context(|| format!("failed to load {}", cert_path.display()))?;
    config
        .load_priv_key_from_pem_file(key_path_str)
        .with_context(|| format!("failed to load {}", key_path.display()))?;
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
    config.enable_dgram(true, 64, 64);
    config.verify_peer(false);
    Ok(config)
}

fn ensure_dev_certs(cert_dir: &Path) -> Result<()> {
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

    log::info!("generated local dev certs in {}", cert_dir.display());
    Ok(())
}

struct Session {
    client_id: ClientId,
    conn: Connection,
    client_addr: SocketAddr,
    streams: HashMap<u64, QuicStreamState>,
    pending_ping_nonces: HashMap<u64, Instant>,
    next_ping_nonce: u64,
    last_ping_sent_at: Instant,
    queued_stream_bytes: usize,
}

struct PendingStreamWrite {
    data: Vec<u8>,
    offset: usize,
}

#[derive(Default)]
struct QuicStreamState {
    recv_buffer: Vec<u8>,
    pending_writes: VecDeque<PendingStreamWrite>,
    recv_finished: bool,
}

impl Session {
    fn new(client_id: ClientId, client_addr: SocketAddr, conn: Connection) -> Self {
        Self {
            client_id,
            conn,
            client_addr,
            streams: HashMap::new(),
            pending_ping_nonces: HashMap::new(),
            next_ping_nonce: 1,
            last_ping_sent_at: Instant::now(),
            queued_stream_bytes: 0,
        }
    }

    fn maybe_send_ping(&mut self) {
        if !self.conn.is_established() {
            return;
        }
        if self.last_ping_sent_at.elapsed() < PING_INTERVAL {
            return;
        }

        let nonce = self.next_ping_nonce;
        self.next_ping_nonce = self.next_ping_nonce.wrapping_add(1).max(1);
        self.pending_ping_nonces.insert(nonce, Instant::now());
        self.last_ping_sent_at = Instant::now();

        if let Ok(bytes) = encode_s2c(&S2CPacket::Ping { nonce }) {
            let _ = self.conn.dgram_send(&bytes);
        }
    }

    fn stream_state_mut(&mut self, stream_id: u64, direction: &str) -> &mut QuicStreamState {
        match self.streams.entry(stream_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                log::debug!(
                    "server stream {} created for client {} ({direction})",
                    stream_id,
                    self.client_id
                );
                entry.insert(QuicStreamState::default())
            },
        }
    }

    fn requeue_pending_write(&mut self, stream_id: u64, chunk: PendingStreamWrite) {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.pending_writes.push_front(chunk);
        }
    }

    fn ingest_stream_data(&mut self, stream_id: u64, bytes: &[u8], fin: bool) -> Vec<Vec<u8>> {
        let client_id = self.client_id;
        let state = self.stream_state_mut(stream_id, "rx");
        state.recv_buffer.extend_from_slice(bytes);
        if fin && !state.recv_finished {
            log::debug!("server stream {} received FIN from client {}", stream_id, client_id);
        }
        state.recv_finished |= fin;

        let mut frames = Vec::new();
        while let Some(frame) = pop_frame(&mut state.recv_buffer) {
            frames.push(frame);
        }
        if state.recv_finished && !state.recv_buffer.is_empty() {
            log::warn!(
                "dropping {} trailing bytes on stream {} after FIN (incomplete frame)",
                state.recv_buffer.len(),
                stream_id
            );
            state.recv_buffer.clear();
        }

        self.cleanup_stream_if_closed(stream_id);
        frames
    }

    fn queue_stream_packet(&mut self, stream_id: u64, payload: Vec<u8>) {
        let mut framed = Vec::with_capacity(4 + payload.len());
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);
        self.queued_stream_bytes = self.queued_stream_bytes.saturating_add(framed.len());

        let state = self.stream_state_mut(stream_id, "tx");
        state.pending_writes.push_back(PendingStreamWrite { data: framed, offset: 0 });
    }

    fn queue_stream_bundle(
        &mut self,
        stream_id: u64,
        payloads: Vec<Vec<u8>>,
        total_framed_len: usize,
    ) {
        if payloads.is_empty() {
            return;
        }

        let mut framed = Vec::with_capacity(total_framed_len);
        for payload in payloads {
            framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            framed.extend_from_slice(&payload);
        }

        self.queued_stream_bytes = self.queued_stream_bytes.saturating_add(framed.len());
        let state = self.stream_state_mut(stream_id, "tx");
        state.pending_writes.push_back(PendingStreamWrite { data: framed, offset: 0 });
    }

    fn queued_stream_bytes(&self) -> usize {
        self.queued_stream_bytes
    }

    fn has_stream_budget(&self, next_framed_len: usize) -> bool {
        let quantum = self.conn.send_quantum();
        self.queued_stream_bytes().saturating_add(next_framed_len) <= quantum
    }

    fn flush_stream_writes(&mut self) -> Result<()> {
        let stream_ids: Vec<u64> = self.streams.keys().copied().collect();
        for stream_id in stream_ids {
            loop {
                let Some(mut chunk) = self
                    .streams
                    .get_mut(&stream_id)
                    .and_then(|state| state.pending_writes.pop_front())
                else {
                    break;
                };

                match self.conn.stream_send(stream_id, &chunk.data[chunk.offset..], false) {
                    Ok(written) => {
                        chunk.offset += written;
                        self.queued_stream_bytes =
                            self.queued_stream_bytes.saturating_sub(written);
                        if chunk.offset < chunk.data.len() {
                            self.requeue_pending_write(stream_id, chunk);
                            break;
                        }
                    },
                    Err(quiche::Error::Done) => {
                        self.requeue_pending_write(stream_id, chunk);
                        break;
                    },
                    Err(err) => return Err(anyhow::anyhow!("stream_send failed: {err:?}")),
                }
            }

            self.cleanup_stream_if_closed(stream_id);
        }

        Ok(())
    }

    fn cleanup_stream_if_closed(&mut self, stream_id: u64) {
        let should_remove = if let Some(state) = self.streams.get(&stream_id) {
            state.pending_writes.is_empty()
                && state.recv_buffer.is_empty()
                && (state.recv_finished || self.conn.stream_finished(stream_id))
        } else {
            false
        };
        if should_remove {
            self.streams.remove(&stream_id);
            log::debug!("server stream {} cleaned up for client {}", stream_id, self.client_id);
        }
    }
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

fn shard_for_client(client_id: ClientId, shard_count: usize) -> usize {
    client_id as usize % shard_count
}

fn lookup_owner(
    owner_by_addr: &Arc<Mutex<HashMap<SocketAddr, usize>>>,
    addr: SocketAddr,
) -> Option<usize> {
    owner_by_addr.lock().ok().and_then(|owners| owners.get(&addr).copied())
}

fn set_owner(
    owner_by_addr: &Arc<Mutex<HashMap<SocketAddr, usize>>>,
    addr: SocketAddr,
    shard: usize,
) {
    if let Ok(mut owners) = owner_by_addr.lock() {
        owners.insert(addr, shard);
    }
}

fn remove_owner(owner_by_addr: &Arc<Mutex<HashMap<SocketAddr, usize>>>, addr: SocketAddr) {
    if let Ok(mut owners) = owner_by_addr.lock() {
        owners.remove(&addr);
    }
}
