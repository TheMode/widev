use std::collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet, VecDeque};
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
use rand::Rng;
use uuid::Uuid;

use crate::game::{ClientId, NetworkEvent};
use crate::network_trace::{DispatchTraceMeta, NetworkTracer, SessionSnapshot, SessionTracer};
use crate::packet_codec::{
    decode_c2s_packet, serialize_packet_message, serialize_s2c_packet_message,
};
use crate::packet_scheduler::{
    DispatchKind, DispatchMessage, PacketScheduler, SchedulerAction, SchedulerCommand,
};
use crate::packets::{
    DropReason, PacketControl, PacketEnvelope, PacketMessage, PacketOrder, PacketPriority,
    PacketResource, PacketTarget,
};

const MAX_DATAGRAM_SIZE: usize = 1350;
const MAX_RECV_DATAGRAM_SIZE: usize = 65_535;
const RECV_BATCH_SIZE: usize = quinn_udp::BATCH_SIZE;
const SERVER_CONN_ID_LEN: usize = quiche::MAX_CONN_ID_LEN;
const FIRST_SERVER_UNI_STREAM_ID: u64 = 3;
const PING_INTERVAL: Duration = Duration::from_secs(2);
const IO_MAX_WAIT: Duration = Duration::from_millis(10);
const IO_BACKPRESSURE_WAIT: Duration = Duration::from_millis(1);
const STREAM_RESET_ERROR_CODE: u64 = 0;
const ACTIVE_CONNECTION_ID_LIMIT: u64 = 4;
const TARGET_ACTIVE_SCIDS: usize = ACTIVE_CONNECTION_ID_LIMIT as usize;

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
    DispatchMessages(Arc<[PacketMessage]>),
    ReceivedDatagram {
        from: SocketAddr,
        dcid: Vec<u8>,
        data: Vec<u8>,
    },
    Shutdown,
}

struct PacedDatagram {
    at: Instant,
    to: SocketAddr,
    bytes: Vec<u8>,
}

impl PartialEq for PacedDatagram {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
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
        other.at.cmp(&self.at)
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
    io_workers: Vec<IoWorkerHandle>,
    recv_thread: Option<thread::JoinHandle<()>>,
    event_rx: mpsc::Receiver<NetworkEvent>,
    running: Arc<AtomicBool>,
}

struct IoWorkerHandle {
    sender: mpsc::Sender<IoCommand>,
    thread: thread::JoinHandle<()>,
}

impl NetworkRuntime {
    pub fn start(bind_addr: SocketAddr) -> Result<Self> {
        let worker_count =
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).max(1);
        let tracer = NetworkTracer::from_env();

        let socket = bind_udp_socket(bind_addr)
            .with_context(|| format!("failed to bind UDP socket at {bind_addr}"))?;
        let local_addr = socket.local_addr().context("failed to read local addr")?;
        log::info!(
            "server listening on {local_addr} with 1 receive thread and {worker_count} I/O workers"
        );
        let caps = detect_udp_capabilities(&socket)
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

        let mut io_workers = Vec::with_capacity(worker_count);
        let mut worker_senders = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let (io_tx, io_rx) = mpsc::channel::<IoCommand>();
            let worker_socket =
                socket.try_clone().context("failed to clone UDP socket for I/O worker")?;

            let event_tx = event_tx.clone();
            let io_running = Arc::clone(&running);
            let next_client_id = Arc::clone(&next_client_id);
            let worker_tracer = Arc::clone(&tracer);
            let handle = thread::spawn(move || {
                if let Err(err) = run_io_thread(
                    worker_id,
                    worker_count,
                    worker_socket,
                    local_addr,
                    io_rx,
                    event_tx,
                    io_running,
                    next_client_id,
                    worker_tracer,
                ) {
                    log::error!("I/O worker {worker_id} crashed: {err:#}");
                }
            });
            worker_senders.push(io_tx.clone());
            io_workers.push(IoWorkerHandle { sender: io_tx, thread: handle });
        }

        let recv_running = Arc::clone(&running);
        let recv_thread = thread::spawn(move || {
            if let Err(err) = run_recv_thread(socket, worker_senders, recv_running) {
                log::error!("receive thread crashed: {err:#}");
            }
        });

        Ok(Self { io_workers, recv_thread: Some(recv_thread), event_rx, running })
    }

    pub fn drain_events(&self) -> Vec<NetworkEvent> {
        let mut out = Vec::new();
        while let Ok(event) = self.event_rx.try_recv() {
            out.push(event);
        }
        out
    }

    pub fn dispatch_messages(&self, mut messages: Vec<PacketMessage>) {
        messages.retain(validate_message_for_dispatch);
        if messages.is_empty() {
            return;
        }
        let shared: Arc<[PacketMessage]> = messages.into();
        for worker in &self.io_workers {
            let _ = worker.sender.send(IoCommand::DispatchMessages(Arc::clone(&shared)));
        }
    }
}

fn validate_message_for_dispatch(message: &PacketMessage) -> bool {
    match message {
        PacketMessage::Envelope(envelope) => {
            if let Err(err) = envelope.validate() {
                log::warn!("dropping invalid envelope: {err}");
                return false;
            }
            true
        },
        PacketMessage::Resource(resource) => {
            if let Err(err) = resource.validate() {
                log::warn!("dropping invalid resource: {err}");
                return false;
            }
            true
        },
        PacketMessage::Control(_) => true,
    }
}

impl Drop for NetworkRuntime {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        let io_workers = std::mem::take(&mut self.io_workers);

        for worker in &io_workers {
            let _ = worker.sender.send(IoCommand::Shutdown);
        }

        if let Some(recv_thread) = self.recv_thread.take() {
            let _ = recv_thread.join();
        }

        for worker in io_workers {
            let _ = worker.thread.join();
        }
    }
}

fn run_recv_thread(
    socket: UdpSocket,
    worker_senders: Vec<mpsc::Sender<IoCommand>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let udp_state = quinn_udp::UdpSocketState::new((&socket).into())
        .context("failed to initialize receive thread UDP state")?;
    let mut storage = vec![0u8; RECV_BATCH_SIZE * MAX_RECV_DATAGRAM_SIZE];
    let mut rx_meta = vec![quinn_udp::RecvMeta::default(); RECV_BATCH_SIZE];
    let mut batch = Vec::with_capacity(RECV_BATCH_SIZE);

    while running.load(Ordering::Relaxed) {
        let batch_count =
            match recv_next_batch(&socket, &udp_state, &mut storage, &mut rx_meta, &mut batch) {
                Ok(count) => count,
                Err(err) => {
                    log::warn!("UDP receive failed on receive thread: {err}");
                    thread::sleep(IO_BACKPRESSURE_WAIT);
                    continue;
                },
            };

        if batch_count == 0 {
            thread::sleep(IO_BACKPRESSURE_WAIT);
            continue;
        }

        for index in 0..batch_count {
            let (from, data) = &batch[index];
            let from = *from;
            let data = data.clone();
            let dcid = packet_destination_cid(&data).unwrap_or_default();
            let worker_index = if dcid.is_empty() {
                worker_index_for_addr(from, worker_senders.len())
            } else {
                worker_index_for_cid(&dcid, worker_senders.len())
            };
            let _ =
                worker_senders[worker_index].send(IoCommand::ReceivedDatagram { from, dcid, data });
        }
    }

    Ok(())
}

fn recv_next_batch(
    socket: &UdpSocket,
    udp_state: &quinn_udp::UdpSocketState,
    storage: &mut [u8],
    rx_meta: &mut [quinn_udp::RecvMeta],
    batch: &mut Vec<(SocketAddr, Vec<u8>)>,
) -> io::Result<usize> {
    batch.clear();

    let received = {
        let mut chunks = storage.chunks_mut(MAX_RECV_DATAGRAM_SIZE);
        let mut bufs: [IoSliceMut<'_>; RECV_BATCH_SIZE] = std::array::from_fn(|_| {
            IoSliceMut::new(chunks.next().expect("fixed receive chunk count"))
        });

        match udp_state.recv(socket.into(), &mut bufs[..], rx_meta) {
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(0),
            Err(err) => return Err(err),
        }
    };

    for (slot, meta) in rx_meta.iter().take(received).enumerate() {
        let slot_start = slot * MAX_RECV_DATAGRAM_SIZE;
        let stride = meta.stride.max(1);
        let mut offset = 0usize;

        while offset < meta.len {
            let len = (meta.len - offset).min(stride);
            let start = slot_start + offset;
            batch.push((meta.addr, storage[start..start + len].to_vec()));
            offset += stride;
        }
    }

    Ok(batch.len())
}

fn packet_destination_cid(packet: &[u8]) -> Option<Vec<u8>> {
    let mut hdr_buf = packet.to_vec();
    let hdr = quiche::Header::from_slice(&mut hdr_buf, SERVER_CONN_ID_LEN).ok()?;
    Some(hdr.dcid.as_ref().to_vec())
}

fn worker_index_for_cid(cid: &[u8], worker_count: usize) -> usize {
    debug_assert!(worker_count > 0);
    cid.iter().fold(0usize, |acc, byte| acc.wrapping_mul(131) ^ usize::from(*byte)) % worker_count
}

fn worker_index_for_addr(addr: SocketAddr, worker_count: usize) -> usize {
    debug_assert!(worker_count > 0);
    match addr {
        SocketAddr::V4(addr) => {
            let ip = u32::from_be_bytes(addr.ip().octets()) as usize;
            let port = addr.port() as usize;
            (ip ^ port) % worker_count
        },
        SocketAddr::V6(addr) => {
            let folded = addr.ip().octets().chunks_exact(4).fold(0usize, |acc, chunk| {
                acc ^ u32::from_be_bytes(chunk.try_into().unwrap()) as usize
            });
            (folded ^ addr.port() as usize) % worker_count
        },
    }
}

fn run_io_thread(
    worker_id: usize,
    worker_count: usize,
    socket: UdpSocket,
    local_addr: SocketAddr,
    io_rx: mpsc::Receiver<IoCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    running: Arc<AtomicBool>,
    next_client_id: Arc<AtomicU32>,
    tracer: Arc<NetworkTracer>,
) -> Result<()> {
    let send_udp_state = quinn_udp::UdpSocketState::new((&socket).into())
        .context("failed to initialize UDP sender state")?;
    let mut shard = ShardState::new(
        worker_id,
        worker_count,
        local_addr,
        build_server_quic_config()?,
        event_tx,
        next_client_id,
        tracer,
    );
    let mut delayed_datagrams: BinaryHeap<PacedDatagram> = BinaryHeap::new();
    let mut ready_datagrams: VecDeque<PacedDatagram> = VecDeque::new();

    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut app_buf = [0u8; 4096];

    while running.load(Ordering::Relaxed) {
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

        let disconnected = process_sessions_tick(
            &mut shard,
            &mut app_buf,
            &mut send_buf,
            &mut delayed_datagrams,
            &mut ready_datagrams,
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
) -> Vec<ClientId> {
    let mut disconnected = Vec::new();
    let mut touched_sessions = Vec::new();

    // Pump application I/O and flush QUIC output for each session.
    for (&client_id, session) in shard.sessions.iter_mut() {
        touched_sessions.push(client_id);
        pump_app_packets(session, app_buf, &shard.event_tx);
        session.poll_scheduler();
        session.maybe_send_ping();
        session.maybe_log_network_snapshot();

        let stream_flush_failed = session.flush_stream_writes().is_err();
        session.poll_scheduler();
        let quic_flush_failed =
            flush_quic(session, send_buf, delayed_datagrams, ready_datagrams).is_err();
        if stream_flush_failed
            || quic_flush_failed
            || session.conn.is_closed()
            || session.disconnect_requested
        {
            disconnected.push(client_id);
        }
    }

    for client_id in touched_sessions {
        shard.reconcile_session(client_id);
        shard.refresh_quic_timeout(client_id);
    }

    disconnected
}

fn cleanup_disconnected_sessions(shard: &mut ShardState, disconnected: Vec<ClientId>) {
    for client_id in disconnected {
        if let Some(session) = shard.sessions.remove(&client_id) {
            for cid in session.tracked_scids {
                shard.client_id_by_cid.remove(&cid);
            }
            let _ = shard.event_tx.send(NetworkEvent::ClientDisconnected(client_id));
        }
    }
}

struct ShardState {
    worker_id: usize,
    worker_count: usize,
    local_addr: SocketAddr,
    config: quiche::Config,
    tracer: Arc<NetworkTracer>,
    sessions: HashMap<ClientId, Session>,
    client_id_by_cid: HashMap<Vec<u8>, ClientId>,
    quic_timeouts: BinaryHeap<QuicTimeout>,
    event_tx: mpsc::Sender<NetworkEvent>,
    next_client_id: Arc<AtomicU32>,
}

impl ShardState {
    fn new(
        worker_id: usize,
        worker_count: usize,
        local_addr: SocketAddr,
        config: quiche::Config,
        event_tx: mpsc::Sender<NetworkEvent>,
        next_client_id: Arc<AtomicU32>,
        tracer: Arc<NetworkTracer>,
    ) -> Self {
        Self {
            worker_id,
            worker_count,
            local_addr,
            config,
            tracer,
            sessions: HashMap::new(),
            client_id_by_cid: HashMap::new(),
            quic_timeouts: BinaryHeap::new(),
            event_tx,
            next_client_id,
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

    fn register_session(&mut self, client_id: ClientId, session: Session) {
        self.sessions.insert(client_id, session);
        self.reconcile_session(client_id);
        self.refresh_quic_timeout(client_id);
        let _ = self.event_tx.send(NetworkEvent::ClientConnected(client_id));
    }

    fn reconcile_session(&mut self, client_id: ClientId) {
        let Some(session) = self.sessions.get_mut(&client_id) else {
            return;
        };

        drain_path_events(session);
        advertise_spare_scids(&mut session.conn, self.worker_id, self.worker_count);
        while session.conn.retired_scid_next().is_some() {}

        let old_scids = std::mem::take(&mut session.tracked_scids);
        let new_scids: HashSet<Vec<u8>> =
            session.conn.source_ids().map(|cid| cid.as_ref().to_vec()).collect();
        session.tracked_scids = new_scids.clone();

        for cid in old_scids.difference(&new_scids) {
            self.client_id_by_cid.remove(cid);
        }
        for cid in &new_scids {
            self.client_id_by_cid.insert(cid.clone(), client_id);
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
        IoCommand::DispatchMessages(messages) => {
            for message in messages.iter() {
                match message {
                    PacketMessage::Envelope(envelope) => {
                        dispatch_envelope_for_thread(shard, envelope)
                    },
                    PacketMessage::Resource(resource) => {
                        dispatch_resource_for_thread(shard, resource)
                    },
                    PacketMessage::Control(control) => dispatch_control_for_thread(shard, *control),
                }
            }
        },
        IoCommand::ReceivedDatagram { from, dcid, data } => {
            handle_received_datagram(shard, from, &dcid, data);
        },
        IoCommand::Shutdown => {},
    }
}

fn handle_received_datagram(shard: &mut ShardState, from: SocketAddr, dcid: &[u8], data: Vec<u8>) {
    if let Some(client_id) = shard.client_id_by_cid.get(dcid).copied() {
        let local_addr = shard.local_addr;
        shard.with_session_and_refresh(client_id, |session| {
            let mut data = data;
            session.client_addr = from;
            recv_on_session(session, &mut data, from, local_addr, "conn.recv");
        });
        shard.reconcile_session(client_id);
    } else {
        handle_add_connection(shard, from, data);
    }
}

fn handle_add_connection(shard: &mut ShardState, client_addr: SocketAddr, initial_packet: Vec<u8>) {
    let mut pkt_buf = initial_packet;
    let hdr = match quiche::Header::from_slice(&mut pkt_buf, quiche::MAX_CONN_ID_LEN) {
        Ok(h) => h,
        Err(_) => return,
    };

    // Only Initial packets are allowed to create a fresh QUIC session.
    if hdr.ty != quiche::Type::Initial {
        return;
    }

    let Ok(client_id) =
        shard.next_client_id.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
            if id == u32::MAX {
                None
            } else {
                Some(id + 1)
            }
        })
    else {
        log::error!("exhausted global ClientId space; rejecting new connection from {client_addr}");
        return;
    };

    let server_cid = generate_server_cid_for_worker(shard.worker_id, shard.worker_count);
    let scid = quiche::ConnectionId::from_ref(&server_cid);

    let conn = match quiche::accept(&scid, None, shard.local_addr, client_addr, &mut shard.config) {
        Ok(conn) => conn,
        Err(err) => {
            log::warn!("failed to accept incoming QUIC connection: {err:?}");
            return;
        },
    };

    let mut session = Session::new(
        client_id,
        client_addr,
        shard.event_tx.clone(),
        Arc::clone(&shard.tracer),
        conn,
    );
    recv_on_session(
        &mut session,
        &mut pkt_buf,
        client_addr,
        shard.local_addr,
        "conn.recv after accept",
    );

    shard.register_session(client_id, session);
    log::info!("accepted connection from {client_addr} as client {client_id}");
}

fn generate_server_cid_for_worker(worker_id: usize, worker_count: usize) -> Vec<u8> {
    let mut cid = vec![0u8; SERVER_CONN_ID_LEN];
    loop {
        rand::rng().fill_bytes(&mut cid);
        if worker_index_for_cid(&cid, worker_count) == worker_id {
            return cid;
        }
    }
}

fn advertise_spare_scids(conn: &mut Connection, worker_id: usize, worker_count: usize) {
    while conn.active_scids() < TARGET_ACTIVE_SCIDS && conn.scids_left() > 0 {
        let cid = generate_server_cid_for_worker(worker_id, worker_count);
        let cid = quiche::ConnectionId::from_ref(&cid);
        if let Err(err) = conn.new_scid(&cid, rand::random::<u128>(), false) {
            log::debug!("failed to advertise spare server CID: {err:?}");
            break;
        }
    }
}

fn drain_path_events(session: &mut Session) {
    while let Some(event) = session.conn.path_event_next() {
        match event {
            quiche::PathEvent::New(local, peer) => {
                log::debug!("client {} observed new path {} -> {}", session.client_id, local, peer);
            },
            quiche::PathEvent::Validated(local, peer) => {
                log::debug!("client {} validated path {} -> {}", session.client_id, local, peer);
            },
            quiche::PathEvent::FailedValidation(local, peer) => {
                log::debug!(
                    "client {} path validation failed {} -> {}",
                    session.client_id,
                    local,
                    peer
                );
            },
            quiche::PathEvent::Closed(local, peer) => {
                log::debug!("client {} closed path {} -> {}", session.client_id, local, peer);
            },
            quiche::PathEvent::ReusedSourceConnectionId(seq, old, new) => {
                log::warn!(
                    "client {} reused CID seq {} from {} -> {} to {} -> {}",
                    session.client_id,
                    seq,
                    old.0,
                    old.1,
                    new.0,
                    new.1
                );
            },
            quiche::PathEvent::PeerMigrated(_, peer) => {
                session.client_addr = peer;
                log::info!("client {} migrated to {}", session.client_id, peer);
            },
        }
    }
}

fn dispatch_envelope_for_thread(shard: &mut ShardState, envelope: &PacketEnvelope) {
    if !envelope_has_local_targets(shard, envelope.meta.target) {
        return;
    }

    let Some(framed) = serialize_packet_message(&PacketMessage::Envelope(envelope.clone())) else {
        return;
    };

    for_each_target_session_mut(shard, envelope.meta.target, |session| {
        session.send_envelope(envelope, &framed);
    });
}

fn dispatch_resource_for_thread(shard: &mut ShardState, resource: &PacketResource) {
    if !envelope_has_local_targets(shard, resource.meta.target) {
        return;
    }

    let Some(framed) = serialize_packet_message(&PacketMessage::Resource(resource.clone())) else {
        return;
    };

    for_each_target_session_mut(shard, resource.meta.target, |session| {
        session.send_resource(resource, &framed);
    });
}

fn dispatch_control_for_thread(shard: &mut ShardState, control: PacketControl) {
    match control {
        PacketControl::SequenceClose { sequence_id } => {
            for session in shard.sessions.values_mut() {
                session.tracer.on_control(control);
                session.dispatch_scheduler_command(SchedulerCommand::SequenceClose(sequence_id));
            }
        },
        PacketControl::SequenceCloseAll { target } => {
            dispatch_control_to_target(shard, target, SchedulerCommand::SequenceCloseAll);
        },
        PacketControl::Clear { target } => {
            dispatch_control_to_target(shard, target, SchedulerCommand::Clear);
        },
        PacketControl::Barrier { target } => {
            dispatch_control_to_target(shard, target, SchedulerCommand::Barrier);
        },
    }
}

fn dispatch_control_to_target(
    shard: &mut ShardState,
    target: PacketTarget,
    command: SchedulerCommand,
) {
    if !envelope_has_local_targets(shard, target) {
        return;
    }

    for_each_target_session_mut(shard, target, |session| {
        session.tracer.on_control(match &command {
            SchedulerCommand::SequenceClose(sequence_id) => {
                PacketControl::SequenceClose { sequence_id: *sequence_id }
            },
            SchedulerCommand::SequenceCloseAll => PacketControl::SequenceCloseAll { target },
            SchedulerCommand::Clear => PacketControl::Clear { target },
            SchedulerCommand::Barrier => PacketControl::Barrier { target },
            SchedulerCommand::Message(_) => return,
        });
        session.dispatch_scheduler_command(command.clone())
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

enum EnvelopeDispatchResult {
    Sent,
    DeferredByCongestion,
    Dropped(DropReason),
}

struct StreamTarget {
    stream_id: u64,
    fin: bool,
}

fn pump_app_packets(
    session: &mut Session,
    app_buf: &mut [u8],
    event_tx: &mpsc::Sender<NetworkEvent>,
) {
    // Drain datagrams
    loop {
        match session.conn.dgram_recv(app_buf) {
            Ok(len) => {
                decode_and_forward_c2s(session, &app_buf[..len], "datagram", event_tx);
            },
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }

    // Drain streams
    for stream_id in session.conn.readable() {
        loop {
            match session.conn.stream_recv(stream_id, app_buf) {
                Ok((len, fin)) => {
                    let frames = session.ingest_stream_data(stream_id, &app_buf[..len], fin);
                    for frame in frames {
                        decode_and_forward_c2s(session, &frame, "stream", event_tx);
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
    transport: &str,
    event_tx: &mpsc::Sender<NetworkEvent>,
) {
    match decode_c2s_packet(bytes) {
        Some(crate::packets::C2SPacket::Ping { nonce }) => {
            session.tracer.on_rx_packet(
                transport,
                bytes.len(),
                &crate::packets::C2SPacket::Ping { nonce },
                None,
            );
            if let Some(bytes) =
                serialize_s2c_packet_message(&crate::packets::S2CPacket::Pong { nonce })
            {
                let _ = session.conn.dgram_send(&bytes);
            }
        },
        Some(crate::packets::C2SPacket::Pong { nonce }) => {
            let rtt_ms = session
                .pending_ping_nonces
                .remove(&nonce)
                .map(|sent_at| sent_at.elapsed().as_secs_f64() * 1000.0);
            session.tracer.on_rx_packet(
                transport,
                bytes.len(),
                &crate::packets::C2SPacket::Pong { nonce },
                rtt_ms,
            );
        },
        Some(crate::packets::C2SPacket::Receipt { message_id }) => {
            session.tracer.on_rx_packet(
                transport,
                bytes.len(),
                &crate::packets::C2SPacket::Receipt { message_id },
                None,
            );
            session
                .tracer
                .on_transport_outcome(message_id, crate::packets::DeliveryOutcome::ClientProcessed);
            let _ = event_tx.send(NetworkEvent::DeliveryUpdate {
                client_id: session.client_id,
                message_id,
                outcome: crate::packets::DeliveryOutcome::ClientProcessed,
            });
        },
        Some(crate::packets::C2SPacket::Disconnect {}) => {
            session.tracer.on_rx_packet(
                transport,
                bytes.len(),
                &crate::packets::C2SPacket::Disconnect {},
                None,
            );
            session.disconnect_requested = true;
            let _ = session.conn.close(true, 0, b"client_disconnect");
        },
        Some(packet) => {
            session.tracer.on_rx_packet(transport, bytes.len(), &packet, None);
            let _ =
                event_tx.send(NetworkEvent::ClientPacket { client_id: session.client_id, packet });
        },
        None => session.tracer.on_rx_decode_failed(transport, bytes.len()),
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
) -> Result<()> {
    let now = Instant::now();
    loop {
        match session.conn.send(send_buf) {
            Ok((len, send_info)) => {
                let datagram = PacedDatagram {
                    at: send_info.at,
                    to: send_info.to,
                    bytes: send_buf[..len].to_vec(),
                };
                if datagram.at <= now {
                    ready_datagrams.push_back(datagram);
                } else {
                    delayed_datagrams.push(datagram);
                }
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
    // Append now due paced datagrams
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

    // Send ready datagrams
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

fn bind_udp_socket(bind_addr: SocketAddr) -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind(bind_addr)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
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
    config.set_active_connection_id_limit(ACTIVE_CONNECTION_ID_LIMIT);
    config.set_disable_active_migration(false);
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
    let key_pem = certified_key.signing_key.serialize_pem();

    fs::write(&cert_path, cert_pem)
        .with_context(|| format!("failed to write {}", cert_path.display()))?;
    fs::write(&key_path, key_pem)
        .with_context(|| format!("failed to write {}", key_path.display()))?;

    log::info!("generated local dev certs in {}", cert_dir.display());
    Ok(())
}

struct Session {
    client_id: ClientId,
    event_tx: mpsc::Sender<NetworkEvent>,
    tracer: SessionTracer,
    conn: Connection,
    client_addr: SocketAddr,
    tracked_scids: HashSet<Vec<u8>>,
    streams: HashMap<u64, QuicStreamState>,
    next_server_uni_stream_id: u64,
    sequence_streams: HashMap<Uuid, u64>,
    scheduler: PacketScheduler,
    inflight_message_count: usize,
    pending_ping_nonces: HashMap<u64, Instant>,
    next_ping_nonce: u64,
    last_ping_sent_at: Instant,
    queued_stream_bytes: usize,
    disconnect_requested: bool,
    timeout_generation: u64,
}

struct PendingStreamWrite {
    data: Vec<u8>,
    offset: usize,
    fin: bool,
    tracks_inflight: bool,
    transport_receipt_id: Option<crate::packets::MessageId>,
    trace: DispatchTraceMeta,
}

#[derive(Default)]
struct QuicStreamState {
    recv_buffer: Vec<u8>,
    pending_writes: VecDeque<PendingStreamWrite>,
    recv_finished: bool,
}

impl Session {
    fn new(
        client_id: ClientId,
        client_addr: SocketAddr,
        event_tx: mpsc::Sender<NetworkEvent>,
        tracer: Arc<NetworkTracer>,
        conn: Connection,
    ) -> Self {
        let tracked_scids = conn.source_ids().map(|cid| cid.as_ref().to_vec()).collect();
        Self {
            client_id,
            event_tx,
            tracer: SessionTracer::new(tracer, client_id),
            conn,
            client_addr,
            tracked_scids,
            streams: HashMap::new(),
            next_server_uni_stream_id: FIRST_SERVER_UNI_STREAM_ID,
            sequence_streams: HashMap::new(),
            scheduler: PacketScheduler::new(),
            inflight_message_count: 0,
            pending_ping_nonces: HashMap::new(),
            next_ping_nonce: 1,
            last_ping_sent_at: Instant::now(),
            queued_stream_bytes: 0,
            disconnect_requested: false,
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

        if let Some(bytes) =
            serialize_s2c_packet_message(&crate::packets::S2CPacket::Ping { nonce })
        {
            let _ = self.conn.dgram_send(&bytes);
            self.tracer.on_keepalive_ping(bytes.len(), nonce);
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

    fn alloc_server_uni_stream_id(&mut self) -> u64 {
        let stream_id = self.next_server_uni_stream_id;
        self.next_server_uni_stream_id =
            self.next_server_uni_stream_id.checked_add(4).expect("server uni stream id overflow");
        stream_id
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

    fn send_envelope(&mut self, envelope: &PacketEnvelope, framed: &[u8]) {
        let trace = self.tracer.register_envelope(envelope, framed.len());
        self.dispatch_scheduler_command(SchedulerCommand::Message(DispatchMessage::new(
            DispatchKind::Envelope,
            envelope.id,
            envelope.meta,
            framed.to_vec(),
            trace,
        )));
    }

    fn send_resource(&mut self, resource: &PacketResource, framed: &[u8]) {
        let trace = self.tracer.register_resource(resource, framed.len());
        self.dispatch_scheduler_command(SchedulerCommand::Message(DispatchMessage::new(
            DispatchKind::Resource,
            Some(resource.id),
            resource.meta,
            framed.to_vec(),
            trace,
        )));
    }

    fn dispatch_scheduler_command(&mut self, command: SchedulerCommand) {
        let actions = self.scheduler.push(command, Instant::now());
        self.drain_scheduler_trace();
        self.execute_scheduler_actions(actions);
    }

    fn poll_scheduler(&mut self) {
        let actions = self.scheduler.poll(Instant::now(), false);
        self.drain_scheduler_trace();
        self.execute_scheduler_actions(actions);
    }

    fn execute_scheduler_actions(&mut self, mut actions: Vec<SchedulerAction>) {
        let mut index = 0usize;
        while index < actions.len() {
            let action = actions[index].clone();
            if let Some(follow_up) = self.execute_scheduler_action(action) {
                actions.extend(follow_up);
            }
            self.drain_scheduler_trace();
            index += 1;
        }
    }

    fn execute_scheduler_action(
        &mut self,
        action: SchedulerAction,
    ) -> Option<Vec<SchedulerAction>> {
        match action {
            SchedulerAction::DispatchMessage { message, force_flush } => {
                match self.dispatch_message(message.clone(), force_flush) {
                    EnvelopeDispatchResult::Sent => None,
                    EnvelopeDispatchResult::Dropped(reason) => {
                        self.emit_delivery_update(
                            message.delivery(),
                            message.maybe_id(),
                            crate::packets::DeliveryOutcome::TransportDropped { reason },
                        );
                        self.finish_untracked_flow(
                            &message,
                            crate::packets::DeliveryOutcome::TransportDropped { reason },
                        );
                        None
                    },
                    EnvelopeDispatchResult::DeferredByCongestion => {
                        self.scheduler.requeue_deferred_message(message, Instant::now());
                        None
                    },
                }
            },
            SchedulerAction::CloseSequence(sequence_id) => {
                self.close_sequence(sequence_id);
                None
            },
            SchedulerAction::CloseAllSequences => {
                self.close_all_sequences();
                None
            },
            SchedulerAction::ClearTransportState => {
                self.clear_transport_state();
                None
            },
            SchedulerAction::BeginBarrier => {
                self.drain_scheduler_trace();
                if self.inflight_message_count == 0 {
                    let actions = self.scheduler.on_inflight_drained(Instant::now());
                    self.drain_scheduler_trace();
                    Some(actions)
                } else {
                    None
                }
            },
            SchedulerAction::DropMessage { message, reason } => {
                self.emit_delivery_update(
                    message.delivery(),
                    message.maybe_id(),
                    crate::packets::DeliveryOutcome::TransportDropped { reason },
                );
                self.finish_untracked_flow(
                    &message,
                    crate::packets::DeliveryOutcome::TransportDropped { reason },
                );
                log::debug!(
                    "dropping scheduled {} for client {}: id={:?} order={:?} reason={:?}",
                    message.kind_name(),
                    self.client_id,
                    message.maybe_id(),
                    message.order(),
                    reason
                );
                None
            },
        }
    }

    fn can_send_as_datagram(&self, message: &DispatchMessage) -> bool {
        let datagram_fits = self
            .conn
            .dgram_max_writable_len()
            .is_some_and(|max_len| message.payload_len() <= max_len);

        message.is_datagram_eligible() && datagram_fits
    }

    fn send_datagram(&mut self, message: &DispatchMessage) -> EnvelopeDispatchResult {
        self.tracer.on_datagram_attempt(message.trace(), self.conn.dgram_max_writable_len());
        match self.conn.dgram_send(message.framed()) {
            Ok(()) => {
                self.emit_delivery_update(
                    message.delivery(),
                    message.maybe_id(),
                    crate::packets::DeliveryOutcome::TransportDelivered,
                );
                log::debug!(
                    "client {} sent {} {:?} ({:?}) as datagram",
                    self.client_id,
                    message.kind_name(),
                    message.maybe_id(),
                    message.order()
                );
                self.tracer.on_datagram_result(
                    message.trace(),
                    "sent",
                    format!("bytes={}", message.payload_len()),
                    Some(crate::packets::DeliveryOutcome::TransportDelivered),
                );
                EnvelopeDispatchResult::Sent
            },
            Err(
                quiche::Error::Done | quiche::Error::InvalidState | quiche::Error::BufferTooShort,
            ) => {
                let result = match message.priority() {
                    PacketPriority::Deadline { .. } => EnvelopeDispatchResult::DeferredByCongestion,
                    _ => EnvelopeDispatchResult::Dropped(DropReason::DatagramRejected),
                };
                log::debug!(
                    "{} datagram {} for client {}: id={:?} order={:?}",
                    match result {
                        EnvelopeDispatchResult::DeferredByCongestion => "deferring",
                        EnvelopeDispatchResult::Dropped(_) => "dropping",
                        EnvelopeDispatchResult::Sent => "sent",
                    },
                    message.kind_name(),
                    self.client_id,
                    message.maybe_id(),
                    message.order()
                );
                self.tracer.on_datagram_result(
                    message.trace(),
                    match result {
                        EnvelopeDispatchResult::DeferredByCongestion => "deferred",
                        EnvelopeDispatchResult::Dropped(_) => "dropped",
                        EnvelopeDispatchResult::Sent => "sent",
                    },
                    "reason=datagram_not_writable".to_string(),
                    match result {
                        EnvelopeDispatchResult::Dropped(reason) => {
                            Some(crate::packets::DeliveryOutcome::TransportDropped { reason })
                        },
                        _ => None,
                    },
                );
                result
            },
            Err(err) => {
                log::debug!(
                    "dropping datagram {} for client {} after dgram_send error: {err:?}",
                    message.kind_name(),
                    self.client_id,
                );
                self.tracer.on_datagram_result(
                    message.trace(),
                    "dropped",
                    format!("reason=error error={err:?}"),
                    Some(crate::packets::DeliveryOutcome::TransportDropped {
                        reason: DropReason::DatagramRejected,
                    }),
                );
                EnvelopeDispatchResult::Dropped(DropReason::DatagramRejected)
            },
        }
    }

    fn close_sequence(&mut self, sequence_id: Uuid) {
        let Some(stream_id) = self.sequence_streams.remove(&sequence_id) else {
            return;
        };
        log::debug!(
            "client {} resetting sequence {:?} on stream {}",
            self.client_id,
            sequence_id,
            stream_id
        );
        self.reset_stream(stream_id);
    }

    fn close_all_sequences(&mut self) {
        let stream_ids = self.take_all_sequence_stream_ids();
        self.reset_streams(stream_ids, "resetting sequence stream");
    }

    fn clear_transport_state(&mut self) {
        let mut stream_ids = self.take_all_sequence_stream_ids();
        stream_ids.extend(self.pending_write_stream_ids());
        self.reset_streams(stream_ids, "clearing backlog with reset on stream");
    }

    fn take_all_sequence_stream_ids(&mut self) -> Vec<u64> {
        self.sequence_streams.drain().map(|(_, stream_id)| stream_id).collect()
    }

    fn pending_write_stream_ids(&self) -> HashSet<u64> {
        self.streams
            .iter()
            .filter_map(|(&stream_id, state)| {
                (!state.pending_writes.is_empty()).then_some(stream_id)
            })
            .collect()
    }

    fn reset_streams<I>(&mut self, stream_ids: I, action: &str)
    where
        I: IntoIterator<Item = u64>,
    {
        for stream_id in stream_ids {
            log::debug!("client {} {} {}", self.client_id, action, stream_id);
            self.reset_stream(stream_id);
        }
    }

    fn dispatch_message(
        &mut self,
        message: DispatchMessage,
        force_flush: bool,
    ) -> EnvelopeDispatchResult {
        if self.can_send_as_datagram(&message) {
            match self.send_datagram(&message) {
                EnvelopeDispatchResult::Sent => {
                    return EnvelopeDispatchResult::Sent;
                },
                EnvelopeDispatchResult::DeferredByCongestion if !force_flush => {
                    return EnvelopeDispatchResult::DeferredByCongestion;
                },
                EnvelopeDispatchResult::Dropped(reason) if !force_flush => {
                    return EnvelopeDispatchResult::Dropped(reason);
                },
                EnvelopeDispatchResult::DeferredByCongestion
                | EnvelopeDispatchResult::Dropped(_) => {},
            }
        }

        if message.is_droppable() && !self.has_stream_budget(message.framed().len()) && !force_flush
        {
            log::debug!(
                "dropping {} for client {} due to stream congestion budget",
                message.kind_name(),
                self.client_id
            );
            return EnvelopeDispatchResult::Dropped(DropReason::CongestionBudgetExceeded);
        }

        if message.is_deadline() && !self.has_stream_budget(message.framed().len()) && !force_flush
        {
            log::debug!(
                "deferring deadline {} for client {} due to stream congestion budget",
                message.kind_name(),
                self.client_id,
            );
            return EnvelopeDispatchResult::DeferredByCongestion;
        }

        let StreamTarget { stream_id, fin } = self.resolve_stream_target(message.order());
        let transport_reason = self.stream_transport_reason(&message);
        self.tracer.on_stream_transport_selected(
            message.trace(),
            stream_id,
            fin,
            &transport_reason,
        );

        log::debug!(
            "client {} opening server stream {} for {} {:?} ({:?}, fin={})",
            self.client_id,
            stream_id,
            message.kind_name(),
            message.maybe_id(),
            message.order(),
            fin
        );
        self.queue_stream_payloads(
            stream_id,
            message.framed(),
            fin,
            matches!(
                message.delivery(),
                crate::packets::DeliveryPolicy::ObserveTransport
                    | crate::packets::DeliveryPolicy::RequireClientReceipt
            )
            .then_some(message.maybe_id())
            .flatten(),
            message.trace().clone(),
        );
        EnvelopeDispatchResult::Sent
    }

    fn resolve_stream_target(&mut self, order: PacketOrder) -> StreamTarget {
        match order {
            PacketOrder::Independent | PacketOrder::Dependency(_) => {
                StreamTarget { stream_id: self.alloc_server_uni_stream_id(), fin: true }
            },
            PacketOrder::Sequence(sequence_id) => {
                StreamTarget { stream_id: self.sequence_stream_id(sequence_id), fin: false }
            },
            PacketOrder::SequenceEnd(sequence_id) => {
                let stream_id = self.sequence_stream_id(sequence_id);
                self.sequence_streams.remove(&sequence_id);
                StreamTarget { stream_id, fin: true }
            },
        }
    }

    fn sequence_stream_id(&mut self, sequence_id: Uuid) -> u64 {
        if let Some(stream_id) = self.sequence_streams.get(&sequence_id).copied() {
            stream_id
        } else {
            let stream_id = self.alloc_server_uni_stream_id();
            self.sequence_streams.insert(sequence_id, stream_id);
            stream_id
        }
    }

    fn queue_stream_payloads(
        &mut self,
        stream_id: u64,
        payload: &[u8],
        fin: bool,
        transport_receipt_id: Option<crate::packets::MessageId>,
        trace: DispatchTraceMeta,
    ) {
        if payload.is_empty() {
            return;
        }

        let queued_before = self.queued_stream_bytes;
        let inflight_before = self.inflight_message_count;
        self.inflight_message_count = self.inflight_message_count.saturating_add(1);
        self.queued_stream_bytes = self.queued_stream_bytes.saturating_add(payload.len());
        self.tracer.on_stream_queued(
            &trace,
            transport_receipt_id,
            queued_before,
            self.queued_stream_bytes,
            inflight_before,
            self.inflight_message_count,
        );
        let state = self.stream_state_mut(stream_id, "tx");
        state.pending_writes.push_back(PendingStreamWrite {
            data: payload.to_vec(),
            offset: 0,
            fin,
            tracks_inflight: true,
            transport_receipt_id,
            trace,
        });
    }

    fn discard_pending_writes(&mut self, stream_id: u64) {
        let Some(state) = self.streams.get_mut(&stream_id) else {
            return;
        };

        for chunk in state.pending_writes.drain(..) {
            if chunk.tracks_inflight {
                self.inflight_message_count = self.inflight_message_count.saturating_sub(1);
                self.tracer.on_flow_aborted(chunk.trace.flow_id, "stream_reset_or_clear");
            }

            let remaining = chunk.data.len().saturating_sub(chunk.offset);
            self.queued_stream_bytes = self.queued_stream_bytes.saturating_sub(remaining);
        }
    }

    fn reset_stream(&mut self, stream_id: u64) {
        self.discard_pending_writes(stream_id);

        match self.conn.stream_shutdown(stream_id, quiche::Shutdown::Write, STREAM_RESET_ERROR_CODE)
        {
            Ok(()) | Err(quiche::Error::Done) | Err(quiche::Error::InvalidStreamState(_)) => {},
            Err(err) => {
                log::debug!(
                    "client {} failed to reset stream {}: {err:?}",
                    self.client_id,
                    stream_id
                );
            },
        }

        self.cleanup_stream_if_closed(stream_id);
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

                let send_fin = chunk.fin;
                match self.conn.stream_send(stream_id, &chunk.data[chunk.offset..], send_fin) {
                    Ok(written) => {
                        let queued_before = self.queued_stream_bytes;
                        chunk.offset += written;
                        self.queued_stream_bytes = self.queued_stream_bytes.saturating_sub(written);
                        self.tracer.on_stream_write(
                            &chunk.trace,
                            stream_id,
                            written,
                            chunk.data.len(),
                            send_fin,
                            queued_before,
                            self.queued_stream_bytes,
                        );
                        if chunk.offset < chunk.data.len() {
                            self.tracer.on_stream_backpressure(
                                &chunk.trace,
                                stream_id,
                                "partial_write",
                                chunk.data.len().saturating_sub(chunk.offset),
                            );
                            self.requeue_pending_write(stream_id, chunk);
                            break;
                        }
                        if chunk.tracks_inflight {
                            if let Some(message_id) = chunk.transport_receipt_id {
                                self.tracer.on_transport_outcome(
                                    message_id,
                                    crate::packets::DeliveryOutcome::TransportDelivered,
                                );
                                let _ = self.event_tx.send(NetworkEvent::DeliveryUpdate {
                                    client_id: self.client_id,
                                    message_id,
                                    outcome: crate::packets::DeliveryOutcome::TransportDelivered,
                                });
                            } else {
                                self.tracer.on_flow_outcome(
                                    chunk.trace.flow_id,
                                    crate::packets::DeliveryOutcome::TransportDelivered,
                                );
                            }
                            self.inflight_message_count =
                                self.inflight_message_count.saturating_sub(1);
                            if self.inflight_message_count == 0 {
                                let actions = self.scheduler.on_inflight_drained(Instant::now());
                                self.execute_scheduler_actions(actions);
                            }
                        }
                    },
                    Err(quiche::Error::StreamLimit) => {
                        log::debug!(
                            "client {} hit peer stream limit while opening stream {}, retrying later",
                            self.client_id,
                            stream_id
                        );
                        self.tracer.on_stream_backpressure(
                            &chunk.trace,
                            stream_id,
                            "stream_limit",
                            chunk.data.len().saturating_sub(chunk.offset),
                        );
                        self.requeue_pending_write(stream_id, chunk);
                        break;
                    },
                    Err(quiche::Error::Done) => {
                        self.tracer.on_stream_backpressure(
                            &chunk.trace,
                            stream_id,
                            "not_writable",
                            chunk.data.len().saturating_sub(chunk.offset),
                        );
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
            self.sequence_streams.retain(|_, id| *id != stream_id);
            log::debug!("server stream {} cleaned up for client {}", stream_id, self.client_id);
        }
    }

    fn emit_delivery_update(
        &mut self,
        delivery: crate::packets::DeliveryPolicy,
        id: Option<crate::packets::MessageId>,
        outcome: crate::packets::DeliveryOutcome,
    ) {
        if delivery == crate::packets::DeliveryPolicy::None {
            return;
        }
        let Some(message_id) = id else {
            return;
        };
        self.tracer.on_transport_outcome(message_id, outcome);
        let _ = self.event_tx.send(NetworkEvent::DeliveryUpdate {
            client_id: self.client_id,
            message_id,
            outcome,
        });
    }

    fn maybe_log_network_snapshot(&mut self) {
        let rtt_ms = self.conn.path_stats().next().map(|stats| stats.rtt.as_secs_f64() * 1000.0);
        self.tracer.maybe_log_snapshot(SessionSnapshot {
            established: self.conn.is_established(),
            rtt_ms,
            queued_stream_bytes: self.queued_stream_bytes,
            inflight_messages: self.inflight_message_count,
            active_streams: self.streams.len(),
            active_sequences: self.sequence_streams.len(),
            pending_pings: self.pending_ping_nonces.len(),
            send_quantum: self.conn.send_quantum(),
        });
    }

    fn stream_transport_reason(&self, message: &DispatchMessage) -> String {
        if !message.is_datagram_eligible() {
            if message.maybe_id().is_some() {
                return "datagram_ineligible:id_present".to_string();
            }
            return format!("datagram_ineligible:order={:?}", message.order());
        }

        match self.conn.dgram_max_writable_len() {
            Some(max_len) if message.payload_len() > max_len => {
                format!("datagram_ineligible:payload_exceeds_writable_len({max_len})")
            },
            Some(_) => "stream_selected_after_datagram_fallback".to_string(),
            None => "datagram_ineligible:not_writable".to_string(),
        }
    }

    fn drain_scheduler_trace(&mut self) {
        self.tracer.on_scheduler_events(self.scheduler.take_trace_events());
    }

    fn finish_untracked_flow(
        &mut self,
        message: &DispatchMessage,
        outcome: crate::packets::DeliveryOutcome,
    ) {
        if message.maybe_id().is_none() {
            self.tracer.on_flow_outcome(message.trace().flow_id, outcome);
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
