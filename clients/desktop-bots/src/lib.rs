use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use mio::{Events, Interest, Poll, Token};
use quiche::RecvInfo;
use rand::Rng;

#[allow(dead_code)]
pub mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

pub use self::packets as protocol;

const MAX_DATAGRAM_SIZE: usize = 1350;
const MAX_WORKER_POLL_WAIT: Duration = Duration::from_millis(10);
const DEFAULT_CLIENT_NAME: &str = "desktop-bot";
const DEFAULT_CAPABILITIES: &[&str] = &["stress.multiclient", "input.synthetic"];
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_millis(500);

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy)]
pub struct BotRunnerConfig {
    pub server_addr: SocketAddr,
    pub bot_count: usize,
    pub joins_per_second: f64,
    pub bot_tick_hz: u32,
    pub worker_threads: usize,
    pub close_on_exit: bool,
}

pub trait BotFlow: Send + 'static {
    fn on_established(&mut self, _ctx: &mut BotContext<'_>) -> Result<()> {
        Ok(())
    }

    fn on_server_packet(
        &mut self,
        _ctx: &mut BotContext<'_>,
        _packet: &protocol::S2CPacket,
    ) -> Result<()> {
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &mut BotContext<'_>, _now: Instant) -> Result<()> {
        Ok(())
    }
}

pub struct BotContext<'a> {
    pub bot_id: u32,
    outgoing: &'a mut Vec<protocol::C2SPacket>,
}

impl<'a> BotContext<'a> {
    pub fn send(&mut self, packet: protocol::C2SPacket) {
        self.outgoing.push(packet);
    }
}

#[derive(Default)]
struct QuicStreamState {
    recv_buffer: Vec<u8>,
    recv_finished: bool,
}

struct BotSession {
    bot_id: u32,
    token: Token,
    server_addr: SocketAddr,
    local_addr: SocketAddr,
    socket: mio::net::UdpSocket,
    socket_writable: bool,
    pending_send: Option<PendingDatagram>,
    conn: quiche::Connection,
    stream_states: HashMap<u64, QuicStreamState>,
    pending_envelopes: VecDeque<protocol::decode::DecodedEnvelope>,
    processed_envelope_ids: HashSet<protocol::EnvelopeId>,
    flow: Box<dyn BotFlow>,
    outgoing: Vec<protocol::C2SPacket>,
    established_notified: bool,
    tick_interval: Option<Duration>,
    next_tick_at: Instant,
    close_on_exit: bool,
}

enum WorkerCommand {
    AddBot(u32),
}

enum WorkerEvent {
    BotJoined,
}

struct PendingDatagram {
    len: usize,
    to: SocketAddr,
    bytes: [u8; MAX_DATAGRAM_SIZE],
}

pub struct PassiveFlow;

impl BotFlow for PassiveFlow {
    fn on_established(&mut self, ctx: &mut BotContext<'_>) -> Result<()> {
        ctx.send(protocol::C2SPacket::ClientHello {
            client_name: format!("{DEFAULT_CLIENT_NAME}-{}", ctx.bot_id),
            capabilities: DEFAULT_CAPABILITIES.iter().map(|v| (*v).to_string()).collect(),
        });
        Ok(())
    }
}

pub struct AckAndMoveFlow {
    up: Option<u16>,
    down: Option<u16>,
    left: Option<u16>,
    right: Option<u16>,
    ticks: u64,
}

impl AckAndMoveFlow {
    pub fn new() -> Self {
        Self { up: None, down: None, left: None, right: None, ticks: 0 }
    }
}

impl Default for AckAndMoveFlow {
    fn default() -> Self {
        Self::new()
    }
}

impl BotFlow for AckAndMoveFlow {
    fn on_established(&mut self, ctx: &mut BotContext<'_>) -> Result<()> {
        ctx.send(protocol::C2SPacket::ClientHello {
            client_name: format!("{DEFAULT_CLIENT_NAME}-{}", ctx.bot_id),
            capabilities: vec![
                "stress.multiclient".to_string(),
                "input.synthetic".to_string(),
                "flow.ack_and_move".to_string(),
            ],
        });
        Ok(())
    }

    fn on_server_packet(
        &mut self,
        ctx: &mut BotContext<'_>,
        packet: &protocol::S2CPacket,
    ) -> Result<()> {
        if let protocol::S2CPacket::BindingDeclare { binding_id, identifier, .. } = packet {
            match identifier.as_str() {
                "move_up" => self.up = Some(*binding_id),
                "move_down" => self.down = Some(*binding_id),
                "move_left" => self.left = Some(*binding_id),
                "move_right" => self.right = Some(*binding_id),
                _ => {},
            }
            ctx.send(protocol::C2SPacket::BindingAssigned { binding_id: *binding_id });
        }
        Ok(())
    }

    fn on_tick(&mut self, ctx: &mut BotContext<'_>, _now: Instant) -> Result<()> {
        self.ticks = self.ticks.wrapping_add(1);

        let phase = (self.ticks / 30) % 4;
        let (up, down, left, right) = match phase {
            0 => (false, false, false, true),
            1 => (false, true, false, false),
            2 => (false, false, true, false),
            _ => (true, false, false, false),
        };

        if let Some(binding_id) = self.up {
            ctx.send(protocol::C2SPacket::InputValue {
                binding_id,
                value: if up { 1.0 } else { 0.0 },
            });
        }
        if let Some(binding_id) = self.down {
            ctx.send(protocol::C2SPacket::InputValue {
                binding_id,
                value: if down { 1.0 } else { 0.0 },
            });
        }
        if let Some(binding_id) = self.left {
            ctx.send(protocol::C2SPacket::InputValue {
                binding_id,
                value: if left { 1.0 } else { 0.0 },
            });
        }
        if let Some(binding_id) = self.right {
            ctx.send(protocol::C2SPacket::InputValue {
                binding_id,
                value: if right { 1.0 } else { 0.0 },
            });
        }

        Ok(())
    }
}

pub fn run_with_flow<F>(config: BotRunnerConfig, flow_factory: F) -> Result<()>
where
    F: Fn(u32) -> Box<dyn BotFlow> + Send + Sync + 'static,
{
    SHUTDOWN_REQUESTED.store(false, Ordering::Relaxed);
    install_signal_handlers();

    if config.bot_count == 0 {
        log::warn!("bot_count is 0; nothing to run");
        return Ok(());
    }

    let worker_threads = if config.worker_threads == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).max(1)
    } else {
        config.worker_threads.max(1)
    };

    let flow_factory: Arc<dyn Fn(u32) -> Box<dyn BotFlow> + Send + Sync> = Arc::new(flow_factory);
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    let mut worker_txs = Vec::with_capacity(worker_threads);
    let mut worker_handles = Vec::with_capacity(worker_threads);

    for worker_id in 0..worker_threads {
        let (tx, rx) = mpsc::channel::<WorkerCommand>();
        worker_txs.push(tx);

        let flow_factory = Arc::clone(&flow_factory);
        let worker_config = config;
        let event_tx = event_tx.clone();
        worker_handles.push(thread::spawn(move || {
            if let Err(err) = run_worker(worker_id, rx, event_tx, flow_factory, &worker_config) {
                log::error!("worker {worker_id} crashed: {err:#}");
            }
        }));
    }
    drop(event_tx);

    let joined_progress = build_join_progress_bar(config.bot_count as u64);
    joined_progress.enable_steady_tick(Duration::from_millis(100));

    let progress_handle = spawn_join_progress_consumer(event_rx, joined_progress.clone());

    let join_interval = if config.joins_per_second <= 0.0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(1.0 / config.joins_per_second)
    };

    let mut queued_bots = 0usize;
    for idx in 0..config.bot_count {
        if shutdown_requested() {
            log::info!("shutdown requested; stopping bot creation");
            break;
        }

        let bot_id = (idx + 1) as u32;
        let worker_idx = idx % worker_threads;

        worker_txs[worker_idx]
            .send(WorkerCommand::AddBot(bot_id))
            .context("failed to queue bot creation to worker")?;
        queued_bots += 1;

        if !join_interval.is_zero() {
            thread::sleep(join_interval);
        }
    }

    joined_progress.set_length(queued_bots as u64);

    drop(worker_txs);

    joined_progress.suspend(|| {
        log::info!(
            "spawned {} bots across {} workers at {:.2} joins/sec targeting {}",
            queued_bots,
            worker_threads,
            config.joins_per_second,
            config.server_addr
        );
    });

    for handle in worker_handles {
        let _ = handle.join();
    }
    let _ = progress_handle.join();

    Ok(())
}

fn build_join_progress_bar(target: u64) -> ProgressBar {
    let progress = ProgressBar::new(target);
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} joined",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=>-"),
    );
    progress
}

fn spawn_join_progress_consumer(
    event_rx: mpsc::Receiver<WorkerEvent>,
    progress_bar: ProgressBar,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut joined = 0u64;
        while let Ok(event) = event_rx.recv() {
            match event {
                WorkerEvent::BotJoined => {
                    joined = joined.saturating_add(1);
                    progress_bar.set_position(joined);
                    if joined >= progress_bar.length().unwrap_or(0) {
                        break;
                    }
                },
            }
        }
        progress_bar.finish_with_message("join phase complete");
    })
}

fn run_worker(
    worker_id: usize,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
    flow_factory: Arc<dyn Fn(u32) -> Box<dyn BotFlow> + Send + Sync>,
    config: &BotRunnerConfig,
) -> Result<()> {
    let mut poll = Poll::new().context("failed to create mio poll")?;
    let mut events = Events::with_capacity(1024);
    let mut sessions = Vec::<BotSession>::new();
    let mut index_by_token = HashMap::<Token, usize>::new();
    let mut next_token_id: usize = 1;

    let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
    let mut recv_buf = [0u8; 65_535];
    let mut app_buf = [0u8; 4096];

    let mut commands_closed = false;
    let mut shutdown_deadline: Option<Instant> = None;

    loop {
        let mut had_activity = false;

        if shutdown_requested() && shutdown_deadline.is_none() {
            commands_closed = true;
            shutdown_deadline = Some(Instant::now() + SHUTDOWN_GRACE_PERIOD);
            if config.close_on_exit {
                send_connection_close_for_sessions(&mut sessions, &mut send_buf);
            }
        }

        loop {
            match cmd_rx.try_recv() {
                Ok(WorkerCommand::AddBot(bot_id)) => {
                    let flow = flow_factory(bot_id);
                    let token = Token(next_token_id);
                    next_token_id = next_token_id.wrapping_add(1).max(1);
                    let mut session = create_bot_session(bot_id, token, flow, config)
                        .with_context(|| {
                            format!("failed to create bot session {bot_id} on worker {worker_id}")
                        })?;
                    poll.registry()
                        .register(
                            &mut session.socket,
                            session.token,
                            Interest::READABLE | Interest::WRITABLE,
                        )
                        .context("failed to register bot UDP socket with poll")?;
                    index_by_token.insert(session.token, sessions.len());
                    flush_outgoing(&mut session, &mut send_buf)?;
                    sessions.push(session);
                    had_activity = true;
                },
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    commands_closed = true;
                    break;
                },
            }
        }

        let now = Instant::now();
        let mut idx = sessions.len();
        while idx > 0 {
            idx -= 1;
            let was_established = sessions[idx].established_notified;
            match process_session_logic(&mut sessions[idx], now, &mut send_buf, &mut app_buf) {
                Ok(active) => {
                    had_activity |= active;
                    if !was_established && sessions[idx].established_notified {
                        let _ = event_tx.send(WorkerEvent::BotJoined);
                    }
                },
                Err(err) => {
                    log::warn!("worker {worker_id} bot {} failed: {err:#}", sessions[idx].bot_id);
                    sessions[idx].conn.close(false, 0, b"worker processing error").ok();
                },
            }
        }

        if remove_closed_sessions(&mut sessions, &mut index_by_token, poll.registry()) {
            had_activity = true;
        }

        if commands_closed && sessions.is_empty() {
            return Ok(());
        }

        if let Some(deadline) = shutdown_deadline {
            if Instant::now() >= deadline {
                if config.close_on_exit {
                    send_connection_close_for_sessions(&mut sessions, &mut send_buf);
                }
                clear_sessions(&mut sessions, &mut index_by_token, poll.registry());
                return Ok(());
            }
        }

        let wait = if had_activity { Duration::ZERO } else { compute_poll_wait(&sessions) };
        poll.poll(&mut events, Some(wait)).context("poll failed")?;

        let mut recv_failed_tokens = Vec::new();
        for event in events.iter() {
            let Some(&session_idx) = index_by_token.get(&event.token()) else {
                continue;
            };
            let Some(session) = sessions.get_mut(session_idx) else {
                continue;
            };

            if event.is_writable() {
                session.socket_writable = true;
            }
            if event.is_readable() {
                match recv_udp(session, &mut recv_buf) {
                    Ok(_) => {},
                    Err(err) => {
                        log::warn!(
                            "worker {worker_id} bot {} recv failed: {err:#}",
                            session.bot_id
                        );
                        recv_failed_tokens.push(event.token());
                    },
                }
            }
        }

        if !recv_failed_tokens.is_empty() {
            for token in recv_failed_tokens {
                if let Some(&session_idx) = index_by_token.get(&token) {
                    if let Some(session) = sessions.get_mut(session_idx) {
                        session.conn.close(false, 0, b"recv error").ok();
                    }
                }
            }
            remove_closed_sessions(&mut sessions, &mut index_by_token, poll.registry());
        }
    }
}

fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGINT, on_shutdown_signal as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_shutdown_signal as libc::sighandler_t);
    }
}

extern "C" fn on_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
}

fn shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

fn create_bot_session(
    bot_id: u32,
    token: Token,
    flow: Box<dyn BotFlow>,
    config: &BotRunnerConfig,
) -> Result<BotSession> {
    let socket = mio::net::UdpSocket::bind("0.0.0.0:0".parse().expect("valid socket addr"))
        .context("failed to bind UDP socket")?;

    let local_addr = socket.local_addr().context("failed to query local socket address")?;

    let mut quic_config = build_client_quic_config()?;
    let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::rng().fill_bytes(&mut scid);
    let scid = quiche::ConnectionId::from_ref(&scid);

    let conn = quiche::connect(
        Some("widev.local"),
        &scid,
        local_addr,
        config.server_addr,
        &mut quic_config,
    )
    .context("failed to create QUIC connection")?;

    let tick_interval = if config.bot_tick_hz == 0 {
        None
    } else {
        Some(Duration::from_secs_f64(1.0 / config.bot_tick_hz as f64))
    };

    Ok(BotSession {
        bot_id,
        token,
        server_addr: config.server_addr,
        local_addr,
        socket,
        socket_writable: true,
        pending_send: None,
        conn,
        stream_states: HashMap::new(),
        pending_envelopes: VecDeque::new(),
        processed_envelope_ids: HashSet::new(),
        flow,
        outgoing: Vec::with_capacity(16),
        established_notified: false,
        tick_interval,
        next_tick_at: Instant::now(),
        close_on_exit: config.close_on_exit,
    })
}

impl Drop for BotSession {
    fn drop(&mut self) {
        if !self.close_on_exit || self.conn.is_closed() {
            return;
        }

        let _ = self.conn.close(false, 0, b"bot shutdown");
        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];
        for _ in 0..8 {
            match self.conn.send(&mut send_buf) {
                Ok((len, send_info)) => {
                    let _ = self.socket.send_to(&send_buf[..len], send_info.to);
                },
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }
    }
}

fn process_session_logic(
    session: &mut BotSession,
    now: Instant,
    send_buf: &mut [u8],
    app_buf: &mut [u8],
) -> Result<bool> {
    let mut had_activity = false;

    process_datagrams(session, app_buf, &mut had_activity)?;
    process_streams(session, app_buf, &mut had_activity)?;

    if session.conn.is_established() && !session.established_notified {
        session.established_notified = true;
        let mut ctx = BotContext { bot_id: session.bot_id, outgoing: &mut session.outgoing };
        session.flow.on_established(&mut ctx)?;
        had_activity = true;
    }

    if let Some(interval) = session.tick_interval {
        if now >= session.next_tick_at {
            let mut ctx = BotContext { bot_id: session.bot_id, outgoing: &mut session.outgoing };
            session.flow.on_tick(&mut ctx, now)?;
            while session.next_tick_at <= now {
                session.next_tick_at += interval;
            }
            had_activity = true;
        }
    }

    if !session.outgoing.is_empty() {
        for packet in session.outgoing.drain(..) {
            if let Ok(bytes) = protocol::encode_c2s(&packet) {
                let _ = session.conn.dgram_send(&bytes);
                had_activity = true;
            }
        }
    }

    if let Some(timeout) = session.conn.timeout() {
        if timeout.is_zero() {
            session.conn.on_timeout();
            had_activity = true;
        }
    }

    if flush_outgoing(session, send_buf)? {
        had_activity = true;
    }

    Ok(had_activity)
}

fn recv_udp(session: &mut BotSession, recv_buf: &mut [u8]) -> Result<bool> {
    let mut had_activity = false;
    loop {
        let recv = session.socket.recv_from(recv_buf);
        let (len, from) = match recv {
            Ok(v) => v,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err).context("socket recv_from failed"),
        };

        if from != session.server_addr {
            continue;
        }

        let recv_info = RecvInfo { from, to: session.local_addr };
        if let Err(err) = session.conn.recv(&mut recv_buf[..len], recv_info) {
            if err != quiche::Error::Done {
                log::debug!("bot {} conn.recv error: {err:?}", session.bot_id);
            }
        }
        had_activity = true;
    }
    Ok(had_activity)
}

fn process_datagrams(
    session: &mut BotSession,
    app_buf: &mut [u8],
    had_activity: &mut bool,
) -> Result<()> {
    loop {
        match session.conn.dgram_recv(app_buf) {
            Ok(len) => {
                let mut framed = app_buf[..len].to_vec();
                for frame in drain_framed_packets(&mut framed) {
                    let Some(envelope) = protocol::decode::s2c_envelope(&frame) else {
                        continue;
                    };
                    queue_envelope(session, envelope)?;
                }
                *had_activity = true;
            },
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }

    Ok(())
}

fn process_streams(
    session: &mut BotSession,
    app_buf: &mut [u8],
    had_activity: &mut bool,
) -> Result<()> {
    for stream_id in session.conn.readable() {
        loop {
            match session.conn.stream_recv(stream_id, app_buf) {
                Ok((len, fin)) => {
                    let frames = {
                        let state = session.stream_states.entry(stream_id).or_default();
                        state.recv_buffer.extend_from_slice(&app_buf[..len]);
                        state.recv_finished |= fin;
                        drain_framed_packets(&mut state.recv_buffer)
                    };

                    for frame in frames {
                        let Some(envelope) = protocol::decode::s2c_envelope(&frame) else {
                            continue;
                        };
                        queue_envelope(session, envelope)?;
                    }

                    cleanup_stream_if_closed(&mut session.stream_states, &session.conn, stream_id);
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

    Ok(())
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
    }
}

fn compute_poll_wait(sessions: &[BotSession]) -> Duration {
    if sessions.is_empty() {
        return MAX_WORKER_POLL_WAIT;
    }

    let now = Instant::now();
    let mut wait = MAX_WORKER_POLL_WAIT;

    for session in sessions {
        if session.pending_send.is_some() && session.socket_writable {
            return Duration::ZERO;
        }
        if !session.outgoing.is_empty() {
            return Duration::ZERO;
        }
        if let Some(timeout) = session.conn.timeout() {
            wait = wait.min(timeout);
        }

        if session.conn.is_closed() {
            return Duration::ZERO;
        }

        if let Some(_) = session.tick_interval {
            if session.next_tick_at <= now {
                return Duration::ZERO;
            }
            wait = wait.min(session.next_tick_at.saturating_duration_since(now));
        }
    }

    wait
}

fn handle_s2c_packet(session: &mut BotSession, packet: protocol::S2CPacket) -> Result<()> {
    if let protocol::S2CPacket::Ping { nonce } = packet {
        session.outgoing.push(protocol::C2SPacket::Pong { nonce });
    }

    let mut ctx = BotContext { bot_id: session.bot_id, outgoing: &mut session.outgoing };
    session.flow.on_server_packet(&mut ctx, &packet)
}

fn queue_envelope(
    session: &mut BotSession,
    envelope: protocol::decode::DecodedEnvelope,
) -> Result<()> {
    session.pending_envelopes.push_back(envelope);
    process_ready_envelopes(session)
}

fn process_ready_envelopes(session: &mut BotSession) -> Result<()> {
    loop {
        let mut progressed = false;
        let mut remaining = VecDeque::new();

        while let Some(envelope) = session.pending_envelopes.pop_front() {
            if !dependency_satisfied(session, envelope.dependency_id) {
                remaining.push_back(envelope);
                continue;
            }

            apply_decoded_envelope(session, envelope)?;
            progressed = true;
        }

        session.pending_envelopes = remaining;
        if !progressed {
            break;
        }
    }

    Ok(())
}

fn dependency_satisfied(session: &BotSession, dependency_id: Option<protocol::EnvelopeId>) -> bool {
    dependency_id.is_none_or(|id| session.processed_envelope_ids.contains(&id))
}

fn apply_decoded_envelope(
    session: &mut BotSession,
    envelope: protocol::decode::DecodedEnvelope,
) -> Result<()> {
    for packet in envelope.packets {
        handle_s2c_packet(session, packet)?;
    }
    if let Some(id) = envelope.id {
        session.processed_envelope_ids.insert(id);
    }
    if let Some(envelope_id) = envelope.receipt_id {
        session.outgoing.push(protocol::C2SPacket::Receipt { envelope_id });
    }
    Ok(())
}

fn flush_outgoing(session: &mut BotSession, send_buf: &mut [u8]) -> Result<bool> {
    let mut sent_any = false;

    if let Some(pending) = &session.pending_send {
        match session.socket.send_to(&pending.bytes[..pending.len], pending.to) {
            Ok(_) => {
                session.pending_send = None;
                sent_any = true;
            },
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                session.socket_writable = false;
                return Ok(sent_any);
            },
            Err(err) => return Err(err).context("socket send_to failed"),
        }
    }

    if !session.socket_writable {
        return Ok(sent_any);
    }

    loop {
        match session.conn.send(send_buf) {
            Ok((len, send_info)) => match session.socket.send_to(&send_buf[..len], send_info.to) {
                Ok(_) => {
                    sent_any = true;
                },
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    let mut pending =
                        PendingDatagram { len, to: send_info.to, bytes: [0u8; MAX_DATAGRAM_SIZE] };
                    pending.bytes[..len].copy_from_slice(&send_buf[..len]);
                    session.pending_send = Some(pending);
                    session.socket_writable = false;
                    break;
                },
                Err(err) => return Err(err).context("socket send_to failed"),
            },
            Err(quiche::Error::Done) => break,
            Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
        }
    }

    Ok(sent_any)
}

fn send_connection_close_for_sessions(sessions: &mut [BotSession], send_buf: &mut [u8]) {
    for session in sessions.iter_mut() {
        if session.conn.is_closed() {
            continue;
        }
        let _ = session.conn.close(false, 0, b"bot shutdown");
        let _ = flush_outgoing(session, send_buf);
    }
}

fn remove_closed_sessions(
    sessions: &mut Vec<BotSession>,
    index_by_token: &mut HashMap<Token, usize>,
    registry: &mio::Registry,
) -> bool {
    let mut removed_any = false;
    let mut idx = sessions.len();
    while idx > 0 {
        idx -= 1;
        if !sessions[idx].conn.is_closed() {
            continue;
        }
        let mut removed = sessions.swap_remove(idx);
        registry.deregister(&mut removed.socket).ok();
        index_by_token.remove(&removed.token);
        if idx < sessions.len() {
            let moved_token = sessions[idx].token;
            index_by_token.insert(moved_token, idx);
        }
        removed_any = true;
    }
    removed_any
}

fn clear_sessions(
    sessions: &mut Vec<BotSession>,
    index_by_token: &mut HashMap<Token, usize>,
    registry: &mio::Registry,
) {
    while let Some(mut session) = sessions.pop() {
        registry.deregister(&mut session.socket).ok();
    }
    index_by_token.clear();
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
    config.enable_dgram(true, 64, 64);
    Ok(config)
}
