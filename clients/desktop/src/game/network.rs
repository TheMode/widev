use std::collections::VecDeque;
use std::collections::{HashMap, hash_map::Entry};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use mio::net::UdpSocket as MioUdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quiche::RecvInfo;
use rand::Rng;

use super::packets as protocol;

const MAX_DATAGRAM_SIZE: usize = 1350;
const ACTIVE_CONNECTION_ID_LIMIT: u64 = 4;
const TARGET_ACTIVE_SCIDS: usize = ACTIVE_CONNECTION_ID_LIMIT as usize;
const SOCKET_TOKEN: Token = Token(0);
const COMMAND_TOKEN: Token = Token(1);
const CLIENT_STREAM_ID: u64 = 0;

type WakeNotifier = Arc<dyn Fn() + Send + Sync>;

pub(super) struct IncomingPackets {
    pub(super) messages: Vec<protocol::decode::DecodedServerMessage>,
}

enum WorkerCommand {
    SendPacket(protocol::C2SPacket),
    RebindSocket(UdpSocket),
    Shutdown,
}

enum WorkerControl {
    Continue,
    ShutdownRequested,
}

struct WorkerIncoming {
    messages: Vec<protocol::decode::DecodedServerMessage>,
}

pub(super) struct QuicClient {
    server_addr: SocketAddr,
    command_tx: mpsc::Sender<WorkerCommand>,
    incoming_rx: mpsc::Receiver<WorkerIncoming>,
    worker_handle: Option<thread::JoinHandle<()>>,
    is_established: Arc<AtomicBool>,
    peer_cert_der: Arc<Mutex<Option<Vec<u8>>>>,
    path_rtt_us: Arc<AtomicU64>,
    wake_notifier: Arc<Mutex<Option<WakeNotifier>>>,
    worker_waker: Arc<Waker>,
}

#[derive(Default)]
struct QuicStreamState {
    recv_buffer: Vec<u8>,
    recv_finished: bool,
}

struct PendingStreamWrite {
    data: Vec<u8>,
    offset: usize,
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
        let poll = Poll::new().context("failed to create mio poll")?;
        let worker_waker = Arc::new(
            Waker::new(poll.registry(), COMMAND_TOKEN).context("failed to create mio waker")?,
        );

        let is_established = Arc::new(AtomicBool::new(false));
        let peer_cert_der = Arc::new(Mutex::new(None));
        let path_rtt_us = Arc::new(AtomicU64::new(0));
        let wake_notifier = Arc::new(Mutex::new(None));

        let is_established_worker = Arc::clone(&is_established);
        let peer_cert_der_worker = Arc::clone(&peer_cert_der);
        let path_rtt_us_worker = Arc::clone(&path_rtt_us);
        let wake_notifier_worker = Arc::clone(&wake_notifier);
        let worker_waker_handle = Arc::clone(&worker_waker);

        let worker_handle = thread::spawn(move || {
            if let Err(err) = run_worker(
                poll,
                socket,
                conn,
                server_addr,
                local_addr,
                command_rx,
                incoming_tx,
                is_established_worker,
                peer_cert_der_worker,
                path_rtt_us_worker,
                wake_notifier_worker,
                worker_waker_handle,
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
            wake_notifier,
            worker_waker,
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
        if micros == 0 { None } else { Some(Duration::from_micros(micros)) }
    }

    pub(super) fn poll(&mut self) -> Result<IncomingPackets> {
        let mut messages = Vec::new();
        while let Ok(incoming) = self.incoming_rx.try_recv() {
            messages.extend(incoming.messages);
        }
        Ok(IncomingPackets { messages })
    }

    pub(super) fn send_packet(&mut self, packet: protocol::C2SPacket) -> Result<()> {
        self.command_tx
            .send(WorkerCommand::SendPacket(packet))
            .context("client network worker is unavailable")?;
        let _ = self.worker_waker.wake();
        Ok(())
    }

    pub(super) fn handle_network_change(&mut self) -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to rebind UDP socket")?;
        socket.set_nonblocking(true).context("failed to set rebound UDP socket non-blocking")?;
        self.command_tx
            .send(WorkerCommand::RebindSocket(socket))
            .context("client network worker is unavailable")?;
        let _ = self.worker_waker.wake();
        Ok(())
    }

    pub(super) fn set_wake_notifier(&mut self, notifier: WakeNotifier) {
        if let Ok(mut slot) = self.wake_notifier.lock() {
            *slot = Some(notifier);
        }
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        let _ = self.command_tx.send(WorkerCommand::SendPacket(protocol::C2SPacket::Disconnect {}));
        let _ = self.command_tx.send(WorkerCommand::Shutdown);
        let _ = self.worker_waker.wake();
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    mut poll: Poll,
    socket: UdpSocket,
    mut conn: quiche::Connection,
    server_addr: SocketAddr,
    mut local_addr: SocketAddr,
    command_rx: mpsc::Receiver<WorkerCommand>,
    incoming_tx: mpsc::Sender<WorkerIncoming>,
    is_established: Arc<AtomicBool>,
    peer_cert_der: Arc<Mutex<Option<Vec<u8>>>>,
    path_rtt_us: Arc<AtomicU64>,
    wake_notifier: Arc<Mutex<Option<WakeNotifier>>>,
    _worker_waker: Arc<Waker>,
) -> Result<()> {
    let mut socket = MioUdpSocket::from_std(socket);
    poll.registry()
        .register(&mut socket, SOCKET_TOKEN, Interest::READABLE)
        .context("failed to register UDP socket with mio")?;
    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut recv_buf = [0u8; 65_535];
    let mut app_buf = [0u8; 4096];
    let mut stream_states: HashMap<u64, QuicStreamState> = HashMap::new();
    let mut pending_writes: VecDeque<PendingStreamWrite> = VecDeque::new();
    let mut last_established = false;
    let mut shutdown_requested = false;
    let mut events = Events::with_capacity(8);

    advertise_spare_scids(&mut conn);
    flush_outgoing(&socket, &mut conn, &mut send_buf)?;

    loop {
        let timeout = conn.timeout();
        poll.poll(&mut events, timeout).context("client network poll failed")?;

        let poll_timed_out = events.is_empty() && timeout.is_some_and(|value| !value.is_zero());

        if matches!(
            process_worker_commands(
                &command_rx,
                poll.registry(),
                &mut socket,
                &mut conn,
                &mut local_addr,
                &mut pending_writes,
            )?,
            WorkerControl::ShutdownRequested
        ) {
            shutdown_requested = true;
        }
        if events.iter().any(|event| event.token() == SOCKET_TOKEN && event.is_readable()) {
            recv_udp(&socket, &mut conn, &mut recv_buf, server_addr, local_addr)?;
        }
        let datagrams = drain_datagrams(&mut conn, &mut app_buf);
        let streams = drain_streams(&mut conn, &mut app_buf, &mut stream_states);
        let messages = decode_server_messages(datagrams.into_iter().chain(streams.into_iter()));
        queue_immediate_responses(&mut pending_writes, &messages);

        if poll_timed_out || conn.timeout().is_some_and(|value| value.is_zero()) {
            conn.on_timeout();
        }

        drain_path_events(&mut conn);
        advertise_spare_scids(&mut conn);

        flush_stream_writes(&mut conn, &mut pending_writes)?;
        let _ = flush_outgoing(&socket, &mut conn, &mut send_buf)?;

        refresh_shared_connection_state(&conn, &is_established, &peer_cert_der, &path_rtt_us);
        let established = conn.is_established();
        if established != last_established {
            last_established = established;
            notify_waker(&wake_notifier);
        }

        if !messages.is_empty() {
            let _ = incoming_tx.send(WorkerIncoming { messages });
            notify_waker(&wake_notifier);
        }

        if conn.is_closed() || (shutdown_requested && pending_writes.is_empty()) {
            return Ok(());
        }
    }
}

fn process_worker_commands(
    command_rx: &mpsc::Receiver<WorkerCommand>,
    registry: &mio::Registry,
    socket: &mut MioUdpSocket,
    conn: &mut quiche::Connection,
    local_addr: &mut SocketAddr,
    pending_writes: &mut VecDeque<PendingStreamWrite>,
) -> Result<WorkerControl> {
    while let Ok(cmd) = command_rx.try_recv() {
        match cmd {
            WorkerCommand::SendPacket(packet) => {
                queue_c2s_packet(pending_writes, packet);
            },
            WorkerCommand::RebindSocket(new_socket) => {
                let new_local_addr =
                    new_socket.local_addr().context("failed to get rebound local addr")?;
                let previous_local_addr = *local_addr;
                registry.deregister(socket).context("failed to deregister UDP socket")?;
                let mut new_socket = MioUdpSocket::from_std(new_socket);
                registry
                    .register(&mut new_socket, SOCKET_TOKEN, Interest::READABLE)
                    .context("failed to register rebound UDP socket")?;
                let previous_socket = std::mem::replace(socket, new_socket);
                *local_addr = new_local_addr;
                if conn.is_established() {
                    if let Err(err) = conn.migrate_source(*local_addr) {
                        registry.deregister(socket).ok();
                        let mut previous_socket = previous_socket;
                        registry
                            .register(&mut previous_socket, SOCKET_TOKEN, Interest::READABLE)
                            .ok();
                        *socket = previous_socket;
                        *local_addr = previous_local_addr;
                        log::warn!("failed to migrate QUIC connection to rebound socket: {err:?}");
                        continue;
                    }
                    log::info!("client migrated QUIC socket to {}", *local_addr);
                } else {
                    log::info!("client rebound UDP socket to {}", *local_addr);
                }
            },
            WorkerCommand::Shutdown => return Ok(WorkerControl::ShutdownRequested),
        }
    }
    Ok(WorkerControl::Continue)
}

fn notify_waker(wake_notifier: &Arc<Mutex<Option<WakeNotifier>>>) {
    let notifier = wake_notifier.lock().ok().and_then(|slot| slot.clone());
    if let Some(notifier) = notifier {
        notifier();
    }
}

fn queue_immediate_responses<'a>(
    pending_writes: &mut VecDeque<PendingStreamWrite>,
    messages: impl IntoIterator<Item = &'a protocol::decode::DecodedServerMessage>,
) {
    for message in messages {
        match message {
            protocol::decode::DecodedServerMessage::Envelope(envelope) => {
                for packet in &envelope.packets {
                    if let protocol::S2CPacket::Ping { nonce } = packet {
                        queue_c2s_packet(
                            pending_writes,
                            protocol::C2SPacket::Pong { nonce: *nonce },
                        );
                    }
                }
            },
            protocol::decode::DecodedServerMessage::Resource(_) => {},
        };
    }
}

fn decode_server_messages(
    packets: impl IntoIterator<Item = Vec<u8>>,
) -> Vec<protocol::decode::DecodedServerMessage> {
    packets
        .into_iter()
        .filter_map(|packet| {
            let decoded = protocol::decode::server_message(&packet);
            if decoded.is_none() {
                log::debug!("client dropped undecodable server packet ({} bytes)", packet.len());
            }
            decoded
        })
        .collect()
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
    socket: &MioUdpSocket,
    conn: &mut quiche::Connection,
    recv_buf: &mut [u8],
    server_addr: SocketAddr,
    local_addr: SocketAddr,
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
    }
    Ok(())
}

fn drain_datagrams(conn: &mut quiche::Connection, app_buf: &mut [u8]) -> Vec<Vec<u8>> {
    let mut datagrams = Vec::new();
    loop {
        match conn.dgram_recv(app_buf) {
            Ok(len) => {
                let mut framed = app_buf[..len].to_vec();
                datagrams.extend(drain_framed_packets(&mut framed));
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
) -> Vec<Vec<u8>> {
    let mut streams = Vec::new();
    for stream_id in conn.readable() {
        loop {
            match conn.stream_recv(stream_id, app_buf) {
                Ok((len, fin)) => {
                    let chunk = app_buf[..len].to_vec();
                    streams.extend(ingest_stream_data(stream_states, conn, stream_id, &chunk, fin));
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

fn flush_stream_writes(
    conn: &mut quiche::Connection,
    pending_writes: &mut VecDeque<PendingStreamWrite>,
) -> Result<()> {
    loop {
        let Some(mut write) = pending_writes.pop_front() else {
            break;
        };

        match conn.stream_send(CLIENT_STREAM_ID, &write.data[write.offset..], false) {
            Ok(written) => {
                write.offset += written;
                if write.offset < write.data.len() {
                    pending_writes.push_front(write);
                    break;
                }
            },
            Err(quiche::Error::Done | quiche::Error::StreamLimit) => {
                pending_writes.push_front(write);
                break;
            },
            Err(err) => return Err(anyhow::anyhow!("client stream_send failed: {err:?}")),
        }
    }

    Ok(())
}

fn flush_outgoing(
    socket: &MioUdpSocket,
    conn: &mut quiche::Connection,
    send_buf: &mut [u8],
) -> Result<bool> {
    let mut sent_any = false;
    loop {
        match conn.send(send_buf) {
            Ok((len, send_info)) => {
                match socket.send_to(&send_buf[..len], send_info.to) {
                    Ok(_) => {},
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => return Err(err).context("socket send_to failed"),
                }
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

fn frame_outbound_packet(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

fn queue_c2s_packet(
    pending_writes: &mut VecDeque<PendingStreamWrite>,
    packet: protocol::C2SPacket,
) {
    let Ok(bytes) = protocol::encode_c2s(&packet) else {
        return;
    };
    pending_writes.push_back(PendingStreamWrite { data: frame_outbound_packet(&bytes), offset: 0 });
}
