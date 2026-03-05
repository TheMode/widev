use std::collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque};
use std::fs;
use std::io::{self, IoSliceMut};
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use quiche::{Connection, RecvInfo};
use rand::RngCore;
use socket2::{Domain, Protocol, Socket, Type};

use crate::game::{ClientId, NetworkEvent};
use crate::packets::{
    decode_c2s, encode_s2c, C2SPacket, PacketEnvelope, PacketPayload, PacketPriority, PacketTarget,
    S2CPacket, StreamID,
};

const MAX_DATAGRAM_SIZE: usize = 1350;
const MAX_RECV_DATAGRAM_SIZE: usize = 65_535;
const RECV_BATCH_SIZE: usize = quinn_udp::BATCH_SIZE;
const PING_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_RELIABLE_STREAM_ID: StreamID = 3;
const IO_MAX_WAIT: Duration = Duration::from_millis(10);
const IO_BACKPRESSURE_WAIT: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
struct RecvMeta {
    slot: usize,
    offset: usize,
    from: SocketAddr,
    len: usize,
}

struct RecvBatch {
    storage: Vec<u8>,
    metas: Vec<RecvMeta>,
}

impl RecvBatch {
    fn new() -> Self {
        Self {
            storage: vec![0u8; RECV_BATCH_SIZE * MAX_RECV_DATAGRAM_SIZE],
            metas: Vec::with_capacity(RECV_BATCH_SIZE),
        }
    }

    fn clear(&mut self) {
        self.metas.clear();
    }

    fn push(&mut self, slot: usize, offset: usize, from: SocketAddr, len: usize) {
        self.metas.push(RecvMeta { slot, offset, from, len });
    }

    fn len(&self) -> usize {
        self.metas.len()
    }

    fn from(&self, index: usize) -> SocketAddr {
        self.metas[index].from
    }

    fn packet(&self, index: usize) -> &[u8] {
        let meta = self.metas[index];
        let slot_start = meta.slot * MAX_RECV_DATAGRAM_SIZE;
        let start = slot_start + meta.offset;
        &self.storage[start..start + meta.len]
    }
}

struct RecvBatcher {
    socket: UdpSocket,
    udp_state: quinn_udp::UdpSocketState,
    rx_meta: Vec<quinn_udp::RecvMeta>,
}

impl RecvBatcher {
    fn new(socket: UdpSocket) -> io::Result<Self> {
        let udp_state = quinn_udp::UdpSocketState::new((&socket).into())?;
        let rx_meta = vec![quinn_udp::RecvMeta::default(); RECV_BATCH_SIZE];
        Ok(Self { socket, udp_state, rx_meta })
    }

    fn recv_next_batch(&mut self, batch: &mut RecvBatch) -> io::Result<usize> {
        batch.clear();

        let received = {
            let mut bufs: Vec<IoSliceMut<'_>> = batch
                .storage
                .chunks_mut(MAX_RECV_DATAGRAM_SIZE)
                .take(RECV_BATCH_SIZE)
                .map(IoSliceMut::new)
                .collect();

            match self.udp_state.recv((&self.socket).into(), &mut bufs, &mut self.rx_meta) {
                Ok(n) => n,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(0),
                Err(err) => return Err(err),
            }
        };

        for slot in 0..received {
            let meta = &self.rx_meta[slot];
            let stride = meta.stride.max(1);
            let mut offset = 0usize;

            while offset < meta.len {
                let len = (meta.len - offset).min(stride);
                batch.push(slot, offset, meta.addr, len);
                offset += stride;
            }
        }

        Ok(batch.len())
    }
}

struct UdpCapabilities {
    batch_size: usize,
    batch_recv: bool,
    batch_send: bool,
    gso_enabled: bool,
    gro_enabled: bool,
    max_gso_segments: usize,
    gro_segments: usize,
}

fn detect_udp_capabilities(socket: &UdpSocket) -> io::Result<UdpCapabilities> {
    let state = quinn_udp::UdpSocketState::new(socket.into())?;
    let max_gso_segments = state.max_gso_segments().max(1);
    let gro_segments = state.gro_segments().max(1);
    Ok(UdpCapabilities {
        batch_size: quinn_udp::BATCH_SIZE,
        batch_recv: quinn_udp::BATCH_SIZE > 1,
        batch_send: quinn_udp::BATCH_SIZE > 1,
        gso_enabled: max_gso_segments > 1,
        gro_enabled: gro_segments > 1,
        max_gso_segments,
        gro_segments,
    })
}

enum IoCommand {
    DispatchEnvelopes(Arc<[PacketEnvelope]>),
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

#[derive(Clone, Copy, PartialEq, Eq)]
struct QuicTimeout {
    at: Instant,
    client_id: ClientId,
    generation: u64,
}

impl PartialOrd for QuicTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QuicTimeout {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap pops the earliest timeout first.
        other.at.cmp(&self.at).then_with(|| other.generation.cmp(&self.generation))
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

        let first_socket = bind_reuseport_udp_socket(bind_addr)
            .with_context(|| format!("failed to bind UDP socket at {bind_addr}"))?;
        let local_addr = first_socket.local_addr().context("failed to read local addr")?;
        log::info!("server listening on {local_addr} with {thread_count} I/O threads");
        let caps = detect_udp_capabilities(&first_socket)
            .context("failed to detect UDP networking capabilities")?;
        log::info!(
            "network capabilities:\n  batch_read={} (batch_size={})\n  batch_write={}\n  gso={} (max_segments={})\n  gro={} (segments={})",
            if caps.batch_recv { "enabled" } else { "disabled" },
            caps.batch_size,
            if caps.batch_send { "enabled" } else { "disabled" },
            if caps.gso_enabled { "enabled" } else { "disabled" },
            caps.max_gso_segments,
            if caps.gro_enabled { "enabled" } else { "disabled" },
            caps.gro_segments
        );

        let running = Arc::new(AtomicBool::new(true));
        let next_client_id = Arc::new(AtomicU32::new(1));

        let (event_tx, event_rx) = mpsc::channel::<NetworkEvent>();

        let mut threads = Vec::new();
        let mut io_senders = Vec::with_capacity(thread_count);
        let mut first_socket = Some(first_socket);

        for shard_id in 0..thread_count {
            let (io_tx, io_rx) = mpsc::channel::<IoCommand>();
            io_senders.push(io_tx);

            let socket = if shard_id == 0 {
                first_socket.take().expect("first socket available")
            } else {
                bind_reuseport_udp_socket(local_addr)
                    .context("failed to bind UDP socket for I/O shard")?
            };

            let event_tx = event_tx.clone();
            let io_running = Arc::clone(&running);
            let next_client_id = Arc::clone(&next_client_id);
            let handle = thread::spawn(move || {
                if let Err(err) = run_io_thread(
                    shard_id,
                    socket,
                    local_addr,
                    io_rx,
                    event_tx,
                    io_running,
                    next_client_id,
                ) {
                    log::error!("I/O thread {shard_id} crashed: {err:#}");
                }
            });
            threads.push(handle);
        }

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
        let shared: Arc<[PacketEnvelope]> = envelopes.into();
        for sender in &self.io_senders {
            let _ = sender.send(IoCommand::DispatchEnvelopes(Arc::clone(&shared)));
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

fn run_io_thread(
    shard_id: usize,
    socket: UdpSocket,
    local_addr: SocketAddr,
    io_rx: mpsc::Receiver<IoCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    running: Arc<AtomicBool>,
    next_client_id: Arc<AtomicU32>,
) -> Result<()> {
    let recv_socket = socket.try_clone().context("failed to clone UDP socket for I/O receiver")?;
    let send_udp_state = quinn_udp::UdpSocketState::new((&socket).into())
        .context("failed to initialize UDP sender state")?;
    let mut recv_batcher =
        RecvBatcher::new(recv_socket).context("failed to initialize quinn-udp receiver state")?;
    let mut recv_batch = RecvBatch::new();
    let mut shard = ShardState::new(shard_id, local_addr, build_server_quic_config()?, event_tx);
    let mut delayed_datagrams: BinaryHeap<PacedDatagram> = BinaryHeap::new();
    let mut ready_datagrams: VecDeque<PacedDatagram> = VecDeque::new();
    let mut next_paced_seq: u64 = 1;

    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut app_buf = [0u8; 4096];

    while running.load(Ordering::Relaxed) {
        drain_received_datagrams(
            &mut recv_batcher,
            &mut recv_batch,
            &mut shard,
            next_client_id.as_ref(),
        );

        // Flush any previously queued datagrams whose pacing deadline has arrived.
        flush_due_paced_datagrams(
            &socket,
            &send_udp_state,
            &mut delayed_datagrams,
            &mut ready_datagrams,
        )?;

        // Drive due QUIC timeouts (tracked in a min-heap).
        shard.process_due_quic_timeouts();

        // Wait for cross-thread work, bounded by next QUIC/pacing deadline.
        let wait_for = compute_io_wait(&mut shard, &delayed_datagrams, &ready_datagrams);
        if !recv_and_drain_io_commands(&io_rx, wait_for, &mut shard) {
            break;
        }
        drain_received_datagrams(
            &mut recv_batcher,
            &mut recv_batch,
            &mut shard,
            next_client_id.as_ref(),
        );

        let disconnected = process_sessions_tick(
            &mut shard,
            &mut app_buf,
            &mut send_buf,
            &mut delayed_datagrams,
            &mut ready_datagrams,
            &mut next_paced_seq,
        );

        // Send datagrams generated by this iteration that are already due.
        flush_due_paced_datagrams(
            &socket,
            &send_udp_state,
            &mut delayed_datagrams,
            &mut ready_datagrams,
        )?;

        cleanup_disconnected_sessions(&mut shard, disconnected);
    }

    Ok(())
}

fn recv_and_drain_io_commands(
    io_rx: &mpsc::Receiver<IoCommand>,
    wait_for: Duration,
    shard: &mut ShardState,
) -> bool {
    match io_rx.recv_timeout(wait_for) {
        Ok(cmd) => {
            if matches!(cmd, IoCommand::Shutdown) {
                return false;
            }
            handle_io_command(cmd, shard);
        },
        Err(mpsc::RecvTimeoutError::Timeout) => return true,
        Err(mpsc::RecvTimeoutError::Disconnected) => return false,
    }

    while let Ok(cmd) = io_rx.try_recv() {
        if matches!(cmd, IoCommand::Shutdown) {
            return false;
        }
        handle_io_command(cmd, shard);
    }

    true
}

fn process_sessions_tick(
    shard: &mut ShardState,
    app_buf: &mut [u8],
    send_buf: &mut [u8],
    delayed_datagrams: &mut BinaryHeap<PacedDatagram>,
    ready_datagrams: &mut VecDeque<PacedDatagram>,
    next_paced_seq: &mut u64,
) -> Vec<ClientId> {
    let mut disconnected = Vec::new();
    let mut touched_sessions = Vec::new();

    // Pump application I/O and flush QUIC output for each session.
    for (&client_id, session) in shard.sessions.iter_mut() {
        touched_sessions.push(client_id);
        pump_app_packets(session, app_buf, &shard.event_tx);
        session.maybe_send_ping();

        if session.flush_stream_writes().is_err()
            || flush_quic(session, send_buf, delayed_datagrams, ready_datagrams, next_paced_seq)
                .is_err()
            || session.conn.is_closed()
        {
            disconnected.push(client_id);
        }
    }

    for client_id in touched_sessions {
        shard.refresh_quic_timeout(client_id);
    }

    disconnected
}

fn cleanup_disconnected_sessions(shard: &mut ShardState, disconnected: Vec<ClientId>) {
    for client_id in disconnected {
        if let Some(session) = shard.sessions.remove(&client_id) {
            shard.client_id_by_addr.remove(&session.client_addr);
            let _ = shard.event_tx.send(NetworkEvent::ClientDisconnected(client_id));
        }
    }
}

struct ShardState {
    shard_id: usize,
    local_addr: SocketAddr,
    config: quiche::Config,
    sessions: HashMap<ClientId, Session>,
    client_id_by_addr: HashMap<SocketAddr, ClientId>,
    quic_timeouts: BinaryHeap<QuicTimeout>,
    event_tx: mpsc::Sender<NetworkEvent>,
}

impl ShardState {
    fn new(
        shard_id: usize,
        local_addr: SocketAddr,
        config: quiche::Config,
        event_tx: mpsc::Sender<NetworkEvent>,
    ) -> Self {
        Self {
            shard_id,
            local_addr,
            config,
            sessions: HashMap::new(),
            client_id_by_addr: HashMap::new(),
            quic_timeouts: BinaryHeap::new(),
            event_tx,
        }
    }

    fn refresh_quic_timeout(&mut self, client_id: ClientId) {
        let Some(session) = self.sessions.get_mut(&client_id) else {
            return;
        };

        session.timeout_generation = session.timeout_generation.wrapping_add(1).max(1);
        if let Some(timeout) = session.conn.timeout() {
            let now = Instant::now();
            let at = now.checked_add(timeout).unwrap_or(now);
            self.quic_timeouts.push(QuicTimeout {
                at,
                client_id,
                generation: session.timeout_generation,
            });
        }
    }

    fn with_session_and_refresh(&mut self, client_id: ClientId, f: impl FnOnce(&mut Session)) {
        let mut had_session = false;
        if let Some(session) = self.sessions.get_mut(&client_id) {
            f(session);
            had_session = true;
        }
        if had_session {
            self.refresh_quic_timeout(client_id);
        }
    }

    fn process_due_quic_timeouts(&mut self) {
        loop {
            let Some(next) = self.peek_next_quic_timeout().copied() else {
                break;
            };
            if next.at > Instant::now() {
                break;
            }
            self.quic_timeouts.pop();

            let mut should_refresh = false;
            if let Some(session) = self.sessions.get_mut(&next.client_id) {
                if session.timeout_generation == next.generation {
                    session.conn.on_timeout();
                    should_refresh = true;
                }
            }
            if should_refresh {
                self.refresh_quic_timeout(next.client_id);
            }
        }
    }

    fn peek_next_quic_timeout(&mut self) -> Option<&QuicTimeout> {
        while let Some(top) = self.quic_timeouts.peek() {
            let is_stale = match self.sessions.get(&top.client_id) {
                Some(session) => session.timeout_generation != top.generation,
                None => true,
            };
            if !is_stale {
                break;
            }
            self.quic_timeouts.pop();
        }
        self.quic_timeouts.peek()
    }
}

fn compute_io_wait(
    shard: &mut ShardState,
    delayed_datagrams: &BinaryHeap<PacedDatagram>,
    ready_datagrams: &VecDeque<PacedDatagram>,
) -> Duration {
    if !ready_datagrams.is_empty() {
        return IO_BACKPRESSURE_WAIT;
    }

    let mut wait = IO_MAX_WAIT;

    if let Some(next_timeout) = shard.peek_next_quic_timeout().map(|entry| entry.at) {
        let now = Instant::now();
        if next_timeout <= now {
            return Duration::ZERO;
        }
        wait = wait.min(next_timeout.duration_since(now));
    }

    if let Some(next) = delayed_datagrams.peek() {
        let now = Instant::now();
        if next.at <= now {
            return Duration::ZERO;
        }
        wait = wait.min(next.at.duration_since(now));
    }

    wait
}

fn handle_io_command(cmd: IoCommand, shard: &mut ShardState) {
    match cmd {
        IoCommand::DispatchEnvelopes(envelopes) => {
            for envelope in envelopes.iter() {
                dispatch_envelope_for_thread(shard, envelope);
            }
        },
        IoCommand::Shutdown => {},
    }
}

fn drain_received_datagrams(
    recv_batcher: &mut RecvBatcher,
    recv_batch: &mut RecvBatch,
    shard: &mut ShardState,
    next_client_id: &AtomicU32,
) {
    loop {
        let batch_count = match recv_batcher.recv_next_batch(recv_batch) {
            Ok(count) => count,
            Err(err) => {
                log::warn!("UDP receive failed on shard {}: {err}", shard.shard_id);
                break;
            },
        };
        if batch_count == 0 {
            break;
        }

        for index in 0..batch_count {
            let from = recv_batch.from(index);
            let data = recv_batch.packet(index).to_vec();
            handle_received_datagram(shard, from, data, next_client_id);
        }
    }
}

fn handle_received_datagram(
    shard: &mut ShardState,
    from: SocketAddr,
    data: Vec<u8>,
    next_client_id: &AtomicU32,
) {
    if let Some(client_id) = shard.client_id_by_addr.get(&from).copied() {
        let local_addr = shard.local_addr;
        shard.with_session_and_refresh(client_id, |session| {
            let mut data = data;
            recv_on_session(session, &mut data, from, local_addr, "conn.recv");
        });
        return;
    }

    // Only initial packets create new sessions.
    let mut hdr_buf = data.clone();
    let hdr = match quiche::Header::from_slice(&mut hdr_buf, quiche::MAX_CONN_ID_LEN) {
        Ok(h) => h,
        Err(_) => return,
    };
    if hdr.ty != quiche::Type::Initial {
        return;
    }

    let Ok(client_id) = next_client_id.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
        if id == u32::MAX {
            None
        } else {
            Some(id + 1)
        }
    }) else {
        log::error!("exhausted global ClientId space; rejecting new connection from {from}");
        return;
    };
    handle_add_connection(shard, client_id, from, data);
}

fn handle_add_connection(
    shard: &mut ShardState,
    client_id: ClientId,
    client_addr: SocketAddr,
    initial_packet: Vec<u8>,
) {
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

    let conn = match quiche::accept(&scid, None, shard.local_addr, client_addr, &mut shard.config) {
        Ok(conn) => conn,
        Err(err) => {
            log::warn!("failed to accept incoming QUIC connection: {err:?}");
            return;
        },
    };

    let mut session = Session::new(client_id, client_addr, conn);
    recv_on_session(
        &mut session,
        &mut pkt_buf,
        client_addr,
        shard.local_addr,
        "conn.recv after accept",
    );

    shard.sessions.insert(client_id, session);
    shard.client_id_by_addr.insert(client_addr, client_id);
    shard.refresh_quic_timeout(client_id);
    let _ = shard.event_tx.send(NetworkEvent::ClientConnected(client_id));
    log::info!("accepted connection from {client_addr} as client {client_id}");
}

fn dispatch_envelope_for_thread(shard: &mut ShardState, envelope: &PacketEnvelope) {
    if !envelope_has_local_targets(shard, envelope.target) {
        return;
    }

    let stream_id = envelope
        .sync
        .as_ref()
        .map(|sync| sync.sequential_stream_id)
        .unwrap_or(DEFAULT_RELIABLE_STREAM_ID);
    let Some(serialized) = serialize_envelope_payload(&envelope.payload) else {
        return;
    };

    for_each_target_session_mut(shard, envelope.target, |session| {
        send_serialized_envelope_to_session(session, stream_id, envelope.priority, &serialized);
    });
}

fn envelope_has_local_targets(shard: &ShardState, target: PacketTarget) -> bool {
    match target {
        PacketTarget::Client(client_id) => shard.sessions.contains_key(&client_id),
        PacketTarget::Broadcast => !shard.sessions.is_empty(),
        PacketTarget::BroadcastExcept(excluded_client_id) => {
            if shard.sessions.len() > 1 {
                return true;
            }
            shard
                .sessions
                .keys()
                .next()
                .map(|client_id| *client_id != excluded_client_id)
                .unwrap_or(false)
        },
    }
}

fn for_each_target_session_mut(
    shard: &mut ShardState,
    target: PacketTarget,
    mut f: impl FnMut(&mut Session),
) {
    match target {
        PacketTarget::Client(client_id) => {
            if let Some(session) = shard.sessions.get_mut(&client_id) {
                f(session);
            }
        },
        PacketTarget::Broadcast => {
            for session in shard.sessions.values_mut() {
                f(session);
            }
        },
        PacketTarget::BroadcastExcept(excluded_client_id) => {
            for (&client_id, session) in shard.sessions.iter_mut() {
                if client_id != excluded_client_id {
                    f(session);
                }
            }
        },
    }
}

fn send_serialized_envelope_to_session(
    session: &mut Session,
    stream_id: StreamID,
    priority: PacketPriority,
    serialized: &SerializedEnvelopePayload,
) {
    if matches!(priority, PacketPriority::Droppable)
        && !session.has_stream_budget(serialized.framed.len())
    {
        log::debug!(
            "dropping envelope for client {} due to stream congestion budget",
            session.client_id
        );
        return;
    }

    session.queue_stream_payloads(stream_id, serialized);
}

struct SerializedEnvelopePayload {
    framed: Vec<u8>,
}

fn serialize_envelope_payload(payload: &PacketPayload) -> Option<SerializedEnvelopePayload> {
    let mut framed = Vec::new();

    match payload {
        PacketPayload::Single(packet) => {
            let Ok(encoded) = encode_s2c(packet) else {
                return None;
            };
            framed.reserve(4 + encoded.len());
            framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            framed.extend_from_slice(&encoded);
        },
        PacketPayload::Bundle(bundle) => {
            if bundle.is_empty() {
                return None;
            }

            for packet in bundle {
                let Ok(encoded) = encode_s2c(packet) else {
                    continue;
                };
                framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                framed.extend_from_slice(&encoded);
            }
        },
    }

    if framed.is_empty() {
        return None;
    }

    Some(SerializedEnvelopePayload { framed })
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
    delayed_datagrams: &mut BinaryHeap<PacedDatagram>,
    ready_datagrams: &mut VecDeque<PacedDatagram>,
    next_paced_seq: &mut u64,
) -> Result<()> {
    let now = Instant::now();
    loop {
        match session.conn.send(send_buf) {
            Ok((len, send_info)) => {
                push_paced_datagram(
                    delayed_datagrams,
                    ready_datagrams,
                    now,
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
    send_socket: &UdpSocket,
    send_udp_state: &quinn_udp::UdpSocketState,
    delayed_datagrams: &mut BinaryHeap<PacedDatagram>,
    ready_datagrams: &mut VecDeque<PacedDatagram>,
) -> Result<()> {
    let now = Instant::now();
    while let Some(next) = delayed_datagrams.peek() {
        if next.at > now {
            break;
        }
        ready_datagrams.push_back(delayed_datagrams.pop().expect("heap top exists"));
    }

    if ready_datagrams.is_empty() {
        return Ok(());
    }

    let max_gso_segments = send_udp_state.max_gso_segments().max(1);

    while let Some(first) = ready_datagrams.front() {
        let destination = first.to;
        let segment_len = first.bytes.len();
        let mut segment_count = 1usize;

        for datagram in ready_datagrams.iter().skip(1) {
            if segment_count >= max_gso_segments {
                break;
            }
            if datagram.to != destination || datagram.bytes.len() != segment_len {
                break;
            }
            segment_count += 1;
        }

        let mut gso_payload = Vec::new();
        let (contents, segment_size) = if segment_count > 1 {
            gso_payload.reserve(segment_len * segment_count);
            for datagram in ready_datagrams.iter().take(segment_count) {
                gso_payload.extend_from_slice(&datagram.bytes);
            }
            (&gso_payload[..], Some(segment_len))
        } else {
            (&first.bytes[..], None)
        };

        let transmit =
            quinn_udp::Transmit { destination, ecn: None, contents, segment_size, src_ip: None };

        match send_udp_state.send(send_socket.into(), &transmit) {
            Ok(()) => {
                for _ in 0..segment_count {
                    let _ = ready_datagrams.pop_front();
                }
            },
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err.into()),
        };
    }

    Ok(())
}

fn push_paced_datagram(
    delayed_datagrams: &mut BinaryHeap<PacedDatagram>,
    ready_datagrams: &mut VecDeque<PacedDatagram>,
    now: Instant,
    datagram: PacedDatagram,
) {
    if datagram.at <= now {
        ready_datagrams.push_back(datagram);
    } else {
        delayed_datagrams.push(datagram);
    }
}

fn bind_reuseport_udp_socket(bind_addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = if bind_addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos"
    ))]
    socket.set_reuse_port(true)?;
    socket.bind(&bind_addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
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
    timeout_generation: u64,
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
            timeout_generation: 0,
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

    fn queue_stream_payloads(&mut self, stream_id: u64, payload: &SerializedEnvelopePayload) {
        if payload.framed.is_empty() {
            return;
        }

        self.queued_stream_bytes = self.queued_stream_bytes.saturating_add(payload.framed.len());
        let state = self.stream_state_mut(stream_id, "tx");
        state
            .pending_writes
            .push_back(PendingStreamWrite { data: payload.framed.clone(), offset: 0 });
    }

    fn has_stream_budget(&self, next_framed_len: usize) -> bool {
        let quantum = self.conn.send_quantum();
        // Allow a single oversized payload while still preventing unbounded queue growth.
        let budget = quantum.max(next_framed_len);
        self.queued_stream_bytes.saturating_add(next_framed_len) <= budget
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
                        self.queued_stream_bytes = self.queued_stream_bytes.saturating_sub(written);
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
