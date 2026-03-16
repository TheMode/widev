use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::game::ClientId;
use crate::packets::{
    C2SPacket, DeliveryOutcome, DeliveryPolicy, DropReason, MessageId, PacketControl,
    PacketEnvelope, PacketOrder, PacketPriority, PacketResource, PacketTarget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    EveryEvent,
    OnFlowComplete,
    Batched {
        interval_ms: u64,
    },
}

#[derive(Debug, Clone)]
pub struct LogWriterConfig {
    pub enabled: bool,
    pub log_dir: PathBuf,
    pub flush_policy: FlushPolicy,
    pub also_log_to_console: bool,
    pub verbose: bool,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_dir: PathBuf::from("logs/network"),
            flush_policy: FlushPolicy::OnFlowComplete,
            also_log_to_console: true,
            verbose: false,
        }
    }
}

impl LogWriterConfig {
    pub fn from_env() -> Self {
        let enabled = std::env::var("WIDEV_NET_LOG")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);

        let log_dir = std::env::var("WIDEV_NET_LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("logs/network"));

        let flush_policy = std::env::var("WIDEV_NET_LOG_FLUSH")
            .ok()
            .and_then(|value| match value.as_str() {
                "every" => Some(FlushPolicy::EveryEvent),
                "batch" | "batched" => Some(FlushPolicy::Batched { interval_ms: 100 }),
                _ => None,
            })
            .unwrap_or(FlushPolicy::OnFlowComplete);

        let also_log_to_console = std::env::var("WIDEV_NET_LOG_CONSOLE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(true);

        let verbose = std::env::var("WIDEV_NET_LOG_VERBOSE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);

        Self { enabled, log_dir, flush_policy, also_log_to_console, verbose }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowKind {
    Envelope,
    Resource,
    Datagram,
}

#[derive(Debug, Clone)]
pub enum LogEvent {
    TransportSummary {
        client_id: ClientId,
        absolute_ms: u64,
        direction: TransportDirection,
        transport: String,
        bytes: usize,
        packet_type: String,
        extra: Option<String>,
    },
    FlowComplete {
        client_id: ClientId,
        flow_id: u64,
        packet_label: String,
        target_label: String,
        payload_bytes: usize,
        priority: String,
        order: String,
        delivery: &'static str,
        message_id: Option<u128>,
        steps: Vec<FlowStepData>,
        packets: Vec<PacketEventData>,
        kind: FlowKind,
        usage_count: Option<i32>,
        outcome: String,
        total_ms: f64,
        deadline_remaining_ms: Option<f64>,
    },
    SessionStart {
        client_id: ClientId,
    },
}

#[derive(Debug, Clone)]
pub struct FlowStepData {
    pub label: String,
    pub elapsed_ms: f64,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct PacketEventData {
    pub _index: usize,
    pub packet_type: String,
    pub _bytes: usize,
    pub _message_id: Option<u128>,
    pub steps: Vec<FlowStepData>,
}

#[derive(Debug, Clone, Copy)]
pub enum TransportDirection {
    Rx,
    Tx,
}

pub struct NetworkLogWriter {
    sender: Sender<LogEvent>,
    handle: Option<thread::JoinHandle<()>>,
}

impl NetworkLogWriter {
    pub fn start(config: LogWriterConfig) -> Self {
        let (sender, receiver) = mpsc::channel::<LogEvent>();

        if !config.enabled {
            return Self { sender, handle: None };
        }

        let handle = thread::spawn(move || {
            run_writer_thread(receiver, config);
        });

        Self { sender, handle: Some(handle) }
    }

    pub fn send(&self, event: LogEvent) {
        let _ = self.sender.send(event);
    }

    pub fn is_enabled(&self) -> bool {
        self.handle.is_some()
    }
}

impl Drop for NetworkLogWriter {
    fn drop(&mut self) {
        let _ = self.sender.send(LogEvent::SessionStart { client_id: 0 });
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone)]
struct TransportSummaryLine {
    absolute_ms: u64,
    direction: TransportDirection,
    transport: String,
    bytes: usize,
    packet_type: String,
    extra: Option<String>,
}

struct CollapsedFlowState {
    packet_label: String,
    target_label: String,
    priority: String,
    kind: FlowKind,
    payload_bytes: usize,
    count: usize,
    min_latency_ms: f64,
    max_latency_ms: f64,
}

struct ClientSession {
    writer: BufWriter<File>,
    _start_time: Instant,
    transport_buffer: Vec<TransportSummaryLine>,
    transport_header_shown: bool,
    collapsed_flow: Option<CollapsedFlowState>,
}

fn run_writer_thread(receiver: Receiver<LogEvent>, config: LogWriterConfig) {
    if let Err(err) = fs::create_dir_all(&config.log_dir) {
        log::error!("failed to create log directory: {err}");
        return;
    }

    let mut sessions: HashMap<ClientId, ClientSession> = HashMap::new();
    let batch_interval = match config.flush_policy {
        FlushPolicy::Batched { interval_ms } => Some(Duration::from_millis(interval_ms)),
        _ => None,
    };

    loop {
        let timeout = batch_interval.map(|d| d.saturating_sub(Duration::from_millis(0)));
        let event = match timeout {
            Some(dur) if dur.is_zero() => {
                flush_all(&mut sessions);
                continue;
            },
            Some(dur) => receiver.recv_timeout(dur),
            None => receiver.recv().map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected),
        };

        match event {
            Ok(LogEvent::SessionStart { client_id: 0 }) => {
                // Flush any pending collapsed flows before shutdown
                for session in sessions.values_mut() {
                    flush_collapsed_flow(session, config.also_log_to_console);
                }
                flush_all(&mut sessions);
                break;
            },
            Ok(LogEvent::SessionStart { client_id }) => {
                let _ = get_or_create_session(&mut sessions, client_id, &config.log_dir);
            },
            Ok(event) => {
                handle_event(
                    event,
                    &mut sessions,
                    &config.log_dir,
                    config.also_log_to_console,
                    config.verbose,
                );
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                flush_all(&mut sessions);
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn get_or_create_session<'a>(
    sessions: &'a mut HashMap<ClientId, ClientSession>,
    client_id: ClientId,
    log_dir: &PathBuf,
) -> &'a mut ClientSession {
    sessions.entry(client_id).or_insert_with(|| {
        let path = log_dir.join(format!("client_{client_id}.log"));
        match OpenOptions::new().create(true).write(true).truncate(true).open(&path) {
            Ok(f) => ClientSession {
                writer: BufWriter::new(f),
                _start_time: Instant::now(),
                transport_buffer: Vec::new(),
                transport_header_shown: false,
                collapsed_flow: None,
            },
            Err(err) => {
                log::error!("failed to open log file {path:?}: {err}");
                panic!("Cannot create log file");
            },
        }
    })
}

fn handle_event(
    event: LogEvent,
    sessions: &mut HashMap<ClientId, ClientSession>,
    log_dir: &PathBuf,
    also_log_to_console: bool,
    verbose: bool,
) {
    match event {
        LogEvent::TransportSummary {
            client_id,
            absolute_ms,
            direction,
            transport,
            bytes,
            packet_type,
            extra,
        } => {
            let session = get_or_create_session(sessions, client_id, log_dir);
            session.transport_buffer.push(TransportSummaryLine {
                absolute_ms,
                direction,
                transport,
                bytes,
                packet_type,
                extra,
            });
        },
        LogEvent::FlowComplete {
            client_id,
            flow_id,
            packet_label,
            target_label,
            payload_bytes,
            priority,
            order,
            delivery,
            message_id,
            steps,
            packets,
            kind,
            usage_count,
            outcome,
            total_ms,
            deadline_remaining_ms,
        } => {
            let session = get_or_create_session(sessions, client_id, log_dir);

            // Flush transport timeline before any flow (to capture late-arriving transport events)
            if !session.transport_buffer.is_empty() {
                flush_transport_timeline(session, also_log_to_console);
                session.transport_header_shown = true;
            }

            // Check if this flow can be collapsed with previous identical flows
            let should_collapse = kind == FlowKind::Datagram;

            if should_collapse {
                if let Some(ref mut collapsed) = session.collapsed_flow {
                    // Check if this flow matches the collapsed pattern
                    if collapsed.packet_label == packet_label
                        && collapsed.target_label == target_label
                        && collapsed.priority == priority
                        && collapsed.kind == kind
                    {
                        // Extend the collapsed range
                        collapsed.count += 1;
                        collapsed.min_latency_ms = collapsed.min_latency_ms.min(total_ms);
                        collapsed.max_latency_ms = collapsed.max_latency_ms.max(total_ms);
                        return; // Don't output individual flow
                    } else {
                        // Different flow - flush the collapsed one first
                        flush_collapsed_flow(session, also_log_to_console);
                    }
                }

                // Start new collapsed flow
                session.collapsed_flow = Some(CollapsedFlowState {
                    packet_label,
                    target_label,
                    priority,
                    kind,
                    payload_bytes,
                    count: 1,
                    min_latency_ms: total_ms,
                    max_latency_ms: total_ms,
                });
                return; // Don't output individual flow (will be flushed later)
            } else {
                // Non-collapsible flow - flush any pending collapsed flow first
                if session.collapsed_flow.is_some() {
                    flush_collapsed_flow(session, also_log_to_console);
                }

                // Output the full flow block
                let flow_block = format_flow_block(
                    flow_id,
                    &packet_label,
                    &target_label,
                    payload_bytes,
                    &priority,
                    &order,
                    delivery,
                    message_id,
                    &steps,
                    &packets,
                    kind,
                    usage_count,
                    &outcome,
                    total_ms,
                    deadline_remaining_ms,
                    verbose,
                );
                for line in flow_block.lines() {
                    write_line(session, line, also_log_to_console);
                }
            }
        },
        LogEvent::SessionStart { .. } => {},
    }
}

fn flush_transport_timeline(session: &mut ClientSession, also_log_to_console: bool) {
    if session.transport_buffer.is_empty() {
        return;
    }

    // Collect all lines first to avoid borrow issues
    let mut lines: Vec<String> = Vec::new();
    lines.push("── transport timeline ──────────────────────────────────────────".to_string());

    for entry in &session.transport_buffer {
        let direction_str = match entry.direction {
            TransportDirection::Rx => "RX",
            TransportDirection::Tx => "TX",
        };

        let extra_str = entry.extra.as_ref().map(|e| format!("  {}", e)).unwrap_or_default();

        lines.push(format!(
            " T+{:>5}ms  {:<2}  {:<9}  {:>4} B  {}{}",
            entry.absolute_ms,
            direction_str,
            entry.transport,
            entry.bytes,
            entry.packet_type,
            extra_str
        ));
    }

    lines.push("────────────────────────────────────────────────────────────────".to_string());

    // Now write all lines
    for line in lines {
        write_line(session, &line, also_log_to_console);
    }

    session.transport_buffer.clear();
}

fn flush_collapsed_flow(session: &mut ClientSession, also_log_to_console: bool) {
    if let Some(collapsed) = session.collapsed_flow.take() {
        if collapsed.count == 1 {
            // Single flow - just show a compact line
            let line = format!(
                "  {:<30}  {:<12}  {:<10}  {}B  [{:.3}ms]",
                collapsed.packet_label,
                collapsed.target_label,
                collapsed.priority,
                collapsed.payload_bytes,
                collapsed.min_latency_ms
            );
            write_line(session, &line, also_log_to_console);
        } else {
            // Multiple collapsed flows
            let line = format!(
                "× {:<3}  {:<30}  {:<12}  {:<10}  {}B  [{:.3}–{:.3}ms]",
                collapsed.count,
                collapsed.packet_label,
                collapsed.target_label,
                collapsed.priority,
                collapsed.payload_bytes,
                collapsed.min_latency_ms,
                collapsed.max_latency_ms
            );
            write_line(session, &line, also_log_to_console);
        }
    }
}

fn format_flow_block(
    flow_id: u64,
    packet_label: &str,
    target_label: &str,
    payload_bytes: usize,
    priority: &str,
    order: &str,
    delivery: &str,
    message_id: Option<u128>,
    steps: &[FlowStepData],
    packets: &[PacketEventData],
    kind: FlowKind,
    usage_count: Option<i32>,
    outcome: &str,
    total_ms: f64,
    deadline_remaining_ms: Option<f64>,
    verbose: bool,
) -> String {
    let mut lines = Vec::new();

    // Parse packet label for bundle info
    let (packet_type, bundle_count) = if packet_label.contains("bundle=") {
        let parts: Vec<&str> = packet_label.split(" bundle=").collect();
        let count = parts[1].parse::<usize>().unwrap_or(1);
        (parts[0].to_string(), count)
    } else {
        (packet_label.to_string(), if packets.len() > 1 { packets.len() } else { 1 })
    };

    // Header line
    let bundle_suffix =
        if bundle_count > 1 { format!("/bundle={}", bundle_count) } else { String::new() };
    lines.push(format!(
        "FLOW {} ── {}{} ── {} ── {}",
        flow_id, packet_type, bundle_suffix, target_label, delivery
    ));

    // Metadata lines
    lines.push(format!("  target   : {}", target_label));

    // Format priority with deadline remaining if applicable
    let priority_display = if let Some(remaining) = deadline_remaining_ms {
        if remaining < 0.0 {
            "Deadline(expired)".to_string()
        } else {
            format!("Deadline({:.0}ms remaining)", remaining)
        }
    } else {
        priority.to_string()
    };

    lines.push(format!("  priority : {:<20} │  order    : {}", priority_display, order));

    // Message ID from envelope or "—"
    let msg_id = message_id.map(|id| id.to_string()).unwrap_or_else(|| "—".to_string());
    lines.push(format!("  delivery : {:<20} │  msg      : {}", delivery, msg_id));

    // Usage count for resources
    if let Some(count) = usage_count {
        let count_str = if count == -1 { "permanent".to_string() } else { count.to_string() };
        lines.push(format!("  usage    : {}", count_str));
    }

    // Main flow box
    let box_label = match kind {
        FlowKind::Datagram => format!("datagram ── {}B", payload_bytes),
        _ => format!("envelope ── {}B", payload_bytes),
    };
    lines.push(format!(
        "  ┌─ {} ─{:─<48}┐",
        box_label,
        "─".repeat(48usize.saturating_sub(box_label.len()))
    ));

    // Envelope-level steps
    let mut flush_elapsed_ms: Option<f64> = None;
    for step in steps {
        if step.label == "T.flush" || step.label == "T.stream_flush" {
            flush_elapsed_ms = Some(step.elapsed_ms);
        }

        // Skip scheduler steps unless verbose
        if !verbose && (step.label.starts_with("S.") || step.label.starts_with("sched.")) {
            continue;
        }

        let formatted = format_envelope_step(step, kind);
        lines.push(format!("  │ {:<63} │", formatted));
    }

    // Packet-level steps (only for non-datagram flows with packets that have steps)
    let packets_with_steps: Vec<_> =
        packets.iter().enumerate().filter(|(_, p)| !p.steps.is_empty()).collect();
    if kind != FlowKind::Datagram && !packets_with_steps.is_empty() {
        lines.push(
            "  │                                                                 │".to_string(),
        );

        for (idx, packet) in packets_with_steps {
            let max_header_len = 48;
            let packet_type_trunc = if packet.packet_type.len() > max_header_len - 15 {
                format!("{}...", &packet.packet_type[..max_header_len - 18])
            } else {
                packet.packet_type.clone()
            };
            let packet_header =
                format!("packet {}/{} · {}", idx + 1, packets.len(), packet_type_trunc);
            let padding_len = 54usize.saturating_sub(packet_header.len());
            lines.push(format!("  │  ┌─ {} ─{:─<54}┐ │", packet_header, "─".repeat(padding_len)));

            for step in &packet.steps {
                let formatted = format_packet_step(step, flush_elapsed_ms);
                lines.push(format!("  │  │ {:<59} │ │", formatted));
            }

            lines.push(
                "  │  └─────────────────────────────────────────────────────────────┘ │"
                    .to_string(),
            );
        }
    }

    lines.push("  └─────────────────────────────────────────────────────────────────┘".to_string());

    // Footer with outcome and total
    lines.push(format!("  outcome  : {:<20} │  total : {:.3}ms", outcome, total_ms));

    // Add note annotation if meaningful
    if let Some(note) = generate_footer_note(delivery, priority, outcome) {
        lines.push(format!("  note     : {}", note));
    }

    lines.join("\n")
}

fn format_envelope_step(step: &FlowStepData, kind: FlowKind) -> String {
    let offset = step.elapsed_ms;
    let label = &step.label;
    let detail = &step.detail;

    match label.as_str() {
        "Q.enqueue" => {
            let framed = extract_value(detail, "framed=");
            let framed_clean = framed.as_ref().map(|s| s.trim_end_matches('B')).unwrap_or("?");
            format!("+{:>6.2}ms  Q.enqueue   framed={}B", offset, framed_clean)
        },
        "S.defer" => {
            if kind == FlowKind::Datagram {
                // For datagram flows, show eligibility instead
                format!("+{:>6.2}ms  S.defer     [checking eligibility]", offset)
            } else {
                let queue = extract_value(detail, "queue=");
                format!(
                    "+{:>6.2}ms  S.defer     queue={}",
                    offset,
                    queue.unwrap_or("?".to_string())
                )
            }
        },
        "S.dispatch" => {
            let queue = extract_value(detail, "queue=");
            format!("+{:>6.2}ms  S.dispatch  queue={}", offset, queue.unwrap_or("?".to_string()))
        },
        "T.select" => {
            if detail.contains("route=datagram") {
                // Check if this is actually a datagram flow or was routed to stream
                if kind == FlowKind::Datagram {
                    format!("+{:>6.2}ms  T.select    → datagram  [Independent·no-id·fits]", offset)
                } else {
                    let reason = extract_ineligible_reason(detail);
                    format!(
                        "+{:>6.2}ms  T.select    → stream    [datagram ineligible: {}]",
                        offset, reason
                    )
                }
            } else if detail.contains("route=stream") {
                let stream_id = extract_value(detail, "stream_id=");
                let reason = extract_after(detail, "reason=").unwrap_or("normal".to_string());
                format!(
                    "+{:>6.2}ms  T.select    → stream:{}  [{}]",
                    offset,
                    stream_id.unwrap_or("?".to_string()),
                    reason
                )
            } else {
                format!("+{:>6.2}ms  T.select    {}", offset, detail)
            }
        },
        "T.flush" | "T.stream_flush" => {
            let written = extract_value(detail, "written=");
            let queued = extract_arrow_value(detail, "queued_bytes:");
            format!(
                "+{:>6.2}ms  T.flush     written={}  queued {}",
                offset,
                written.unwrap_or("?".to_string()),
                queued.unwrap_or("?".to_string())
            )
        },
        "D.delivered" => {
            let transport = extract_value(detail, "transport=");
            format!("+{:>6.2}ms  D.delivered {}", offset, transport.unwrap_or("local".to_string()))
        },
        _ => format!("+{:>6.2}ms  {:<10} {}", offset, label, detail),
    }
}

fn format_packet_step(step: &FlowStepData, flush_elapsed_ms: Option<f64>) -> String {
    let offset = step.elapsed_ms;
    let label = &step.label;
    let detail = &step.detail;

    match label.as_str() {
        "T.stream_queue" | "T.stream_q" => {
            let queued = extract_arrow_value(detail, "queued_bytes:");
            let inflight = extract_arrow_value(detail, "inflight:");
            format!(
                "+{:>6.2}ms  T.stream_q  queued {} · inflight {}",
                offset,
                queued.unwrap_or("?".to_string()),
                inflight.unwrap_or("?".to_string())
            )
        },
        "D.receipt" => {
            let outcome = if detail.contains("ClientProcessed") || detail.contains("success") {
                "✓ ClientProcessed"
            } else {
                "✗ failed"
            };

            // Calculate RTT if we have flush time
            let rtt_str = flush_elapsed_ms
                .map(|flush_ms| {
                    let rtt = offset - flush_ms;
                    format!(" · rtt={:.2}ms", rtt)
                })
                .unwrap_or_default();

            format!("+{:>6.2}ms  D.receipt   {}{}", offset, outcome, rtt_str)
        },
        _ => format!("+{:>6.2}ms  {:<10} {}", offset, label, detail),
    }
}

fn extract_ineligible_reason(detail: &str) -> String {
    if detail.contains("id_present") {
        "id present".to_string()
    } else if detail.contains("not_independent") {
        "not independent".to_string()
    } else if detail.contains("too_large") || detail.contains("exceeds") {
        "too large".to_string()
    } else {
        "not eligible".to_string()
    }
}

fn generate_footer_note(delivery: &str, priority: &str, outcome: &str) -> Option<String> {
    if delivery == "None" {
        Some("no receipt — delivery policy is None".to_string())
    } else if priority.contains("Droppable") && outcome.contains("TransportDelivered") {
        Some("droppable — receipt not possible".to_string())
    } else if outcome.contains("ExpiredDeadline") {
        // Extract time from priority if possible
        let expired_time = if let Some(start) = priority.find('(') {
            if let Some(end) = priority[start..].find(')') {
                priority[start + 1..start + end].to_string()
            } else {
                "N.NN".to_string()
            }
        } else {
            "N.NN".to_string()
        };
        Some(format!("deadline expired after {}", expired_time))
    } else if outcome.contains("CongestionBudgetExceeded") {
        Some("dropped — congestion budget exhausted".to_string())
    } else {
        None
    }
}

fn extract_value(s: &str, key: &str) -> Option<String> {
    s.split(key).nth(1).and_then(|rest| rest.split_whitespace().next().map(|v| v.to_string()))
}

fn extract_after(s: &str, key: &str) -> Option<String> {
    s.split(key).nth(1).map(|rest| rest.trim().to_string())
}

fn extract_arrow_value(s: &str, key: &str) -> Option<String> {
    s.split(key).nth(1).and_then(|rest| {
        let trimmed = rest.trim_start();
        trimmed.split(|c| c == ' ' || c == '|').next().map(|v| v.to_string())
    })
}

fn write_line(session: &mut ClientSession, line: &str, also_log_to_console: bool) {
    if line.is_empty() {
        return;
    }

    if also_log_to_console {
        log::info!("{line}");
    }

    let _ = writeln!(session.writer, "{line}");
}

fn flush_all(sessions: &mut HashMap<ClientId, ClientSession>) {
    for session in sessions.values_mut() {
        let _ = session.writer.flush();
    }
}

#[derive(Debug, Clone)]
pub struct DispatchTraceMeta {
    pub flow_id: u64,
    pub packet_label: String,
    pub message_id: Option<MessageId>,
    pub payload_bytes: usize,
    pub target_label: String,
}

#[derive(Debug, Clone)]
pub enum SchedulerTraceEvent {
    DeferredInitial {
        trace: DispatchTraceMeta,
        policy: &'static str,
        queue_name: &'static str,
        queued_messages: usize,
    },
    RequeuedCongestion {
        trace: DispatchTraceMeta,
        queued_messages: usize,
    },
    DispatchReady {
        trace: DispatchTraceMeta,
        force_flush: bool,
        queue_name: &'static str,
    },
    Dropped {
        trace: DispatchTraceMeta,
        reason: DropReason,
        queue_name: &'static str,
    },
    BlockedByBarrier {
        flow_id: Option<u64>,
        command: &'static str,
    },
    BlockedByDeferred {
        flow_id: Option<u64>,
        command: &'static str,
        order_domain: String,
    },
    BarrierBegin,
    BarrierReleased,
    ClearedTransportState,
}

pub struct NetworkTracer {
    console_enabled: bool,
    next_flow_id: AtomicU64,
    log_writer: NetworkLogWriter,
}

impl NetworkTracer {
    pub fn from_env() -> Arc<Self> {
        let console_enabled = std::env::var("WIDEV_NET_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);

        let log_config = LogWriterConfig::from_env();
        let log_writer = NetworkLogWriter::start(log_config);

        Arc::new(Self { console_enabled, next_flow_id: AtomicU64::new(1), log_writer })
    }

    pub fn next_dispatch_trace(
        &self,
        packet_label: String,
        message_id: Option<MessageId>,
        payload_bytes: usize,
        target_label: String,
    ) -> DispatchTraceMeta {
        DispatchTraceMeta {
            flow_id: self.next_flow_id.fetch_add(1, Ordering::Relaxed),
            packet_label,
            message_id,
            payload_bytes,
            target_label,
        }
    }

    fn file_logging_enabled(&self) -> bool {
        self.log_writer.is_enabled()
    }

    fn tracing_enabled(&self) -> bool {
        self.console_enabled || self.file_logging_enabled()
    }
}

struct PacketTraceEntry {
    index: usize,
    packet_type: String,
    bytes: usize,
    message_id: Option<MessageId>,
    steps: Vec<FlowStep>,
}

struct FlowTrace {
    packet_label: String,
    message_id: Option<MessageId>,
    target_label: String,
    payload_bytes: usize,
    priority: String,
    priority_type: PacketPriority,
    order: String,
    delivery: &'static str,
    await_client_receipt: bool,
    started_at: Instant,
    steps: Vec<FlowStep>,
    packets: Vec<PacketTraceEntry>,
    kind: FlowKind,
    usage_count: Option<i32>,
    flush_elapsed_ms: Option<f64>,
}

struct FlowStep {
    label: String,
    at: Instant,
    detail: String,
}

pub struct SessionTracer {
    tracer: Arc<NetworkTracer>,
    client_id: ClientId,
    active_flows: HashMap<u64, FlowTrace>,
    session_start: Instant,
}

impl SessionTracer {
    pub fn new(tracer: Arc<NetworkTracer>, client_id: ClientId) -> Self {
        let now = Instant::now();
        Self { tracer, client_id, active_flows: HashMap::new(), session_start: now }
    }

    fn now_ms(&self) -> u64 {
        self.session_start.elapsed().as_millis() as u64
    }

    fn determine_flow_kind(priority: PacketPriority, order: PacketOrder, has_id: bool) -> FlowKind {
        // Datagram eligible: Droppable + Independent + no id
        match (priority, order) {
            (PacketPriority::Droppable, PacketOrder::Independent) if !has_id => FlowKind::Datagram,
            _ => FlowKind::Envelope,
        }
    }

    fn create_packet_entries(&self, envelope: &PacketEnvelope) -> Vec<PacketTraceEntry> {
        match &envelope.payload {
            crate::packets::PacketPayload::Single(packet) => {
                vec![PacketTraceEntry {
                    index: 0,
                    packet_type: variant_name(packet),
                    bytes: 0, // Will be set from framed_len
                    message_id: envelope.id,
                    steps: Vec::new(),
                }]
            },
            crate::packets::PacketPayload::Bundle(bundle) => {
                bundle
                    .iter()
                    .enumerate()
                    .map(|(idx, packet)| {
                        PacketTraceEntry {
                            index: idx,
                            packet_type: variant_name(packet),
                            bytes: 0,
                            message_id: None, // Bundle packets don't have individual IDs
                            steps: Vec::new(),
                        }
                    })
                    .collect()
            },
        }
    }

    pub fn register_envelope(
        &mut self,
        envelope: &PacketEnvelope,
        framed_len: usize,
    ) -> DispatchTraceMeta {
        let kind = Self::determine_flow_kind(
            envelope.meta.priority,
            envelope.meta.order,
            envelope.id.is_some(),
        );

        let packets = self.create_packet_entries(envelope);
        let packet_count = packets.len();

        let trace = self.tracer.next_dispatch_trace(
            envelope_label(envelope),
            envelope.id,
            framed_len,
            describe_target(envelope.meta.target),
        );

        self.active_flows.insert(
            trace.flow_id,
            FlowTrace::new(
                &trace,
                envelope.meta.priority.describe_long(),
                envelope.meta.priority,
                envelope.meta.order.describe_long(),
                envelope.meta.delivery.describe_short(),
                envelope.meta.delivery == DeliveryPolicy::RequireClientReceipt,
                kind,
                None,
                packets,
            ),
        );

        // Distribute bytes across packets
        if let Some(flow) = self.active_flows.get_mut(&trace.flow_id) {
            let bytes_per_packet = framed_len / packet_count.max(1);
            for packet in &mut flow.packets {
                packet.bytes = bytes_per_packet;
            }
        }

        self.push_step(
            trace.flow_id,
            "Q.enqueue",
            format!(
                "priority={} order={} delivery={} framed={}B",
                envelope.meta.priority.describe_long(),
                envelope.meta.order.describe_long(),
                envelope.meta.delivery.describe_short(),
                framed_len
            ),
        );
        trace
    }

    pub fn register_resource(
        &mut self,
        resource: &PacketResource,
        framed_len: usize,
    ) -> DispatchTraceMeta {
        let trace = self.tracer.next_dispatch_trace(
            format!("resource/{}", resource.resource_type),
            Some(resource.id),
            framed_len,
            describe_target(resource.meta.target),
        );

        // Resources are never datagrams (they have IDs)
        let packets = vec![PacketTraceEntry {
            index: 0,
            packet_type: format!("resource/{}", resource.resource_type),
            bytes: framed_len,
            message_id: Some(resource.id),
            steps: Vec::new(),
        }];

        self.active_flows.insert(
            trace.flow_id,
            FlowTrace::new(
                &trace,
                resource.meta.priority.describe_long(),
                resource.meta.priority,
                resource.meta.order.describe_long(),
                resource.meta.delivery.describe_short(),
                resource.meta.delivery == DeliveryPolicy::RequireClientReceipt,
                FlowKind::Resource,
                Some(resource.usage_count),
                packets,
            ),
        );

        self.push_step(
            trace.flow_id,
            "Q.enqueue",
            format!(
                "priority={} order={} delivery={} framed={}B usage_count={}",
                resource.meta.priority.describe_long(),
                resource.meta.order.describe_long(),
                resource.meta.delivery.describe_short(),
                framed_len,
                resource.usage_count
            ),
        );
        trace
    }

    pub fn on_scheduler_events(&mut self, events: Vec<SchedulerTraceEvent>) {
        if events.is_empty() {
            return;
        }

        for event in events {
            match event {
                SchedulerTraceEvent::DeferredInitial {
                    trace,
                    policy,
                    queue_name,
                    queued_messages,
                } => {
                    self.push_step(
                        trace.flow_id,
                        "S.defer",
                        format!(
                            "queue={} queued={} policy={}",
                            queue_name, queued_messages, policy
                        ),
                    );
                },
                SchedulerTraceEvent::RequeuedCongestion { trace, queued_messages } => {
                    self.push_step(
                        trace.flow_id,
                        "S.requeue",
                        format!("queue_depth={} reason=congestion", queued_messages),
                    );
                },
                SchedulerTraceEvent::DispatchReady { trace, force_flush, queue_name } => {
                    self.push_step(
                        trace.flow_id,
                        "S.dispatch",
                        format!("queue={} flush={}", queue_name, force_flush),
                    );
                },
                SchedulerTraceEvent::Dropped { trace, reason, queue_name } => {
                    self.push_step(
                        trace.flow_id,
                        "S.drop",
                        format!("queue={} reason={:?}", queue_name, reason),
                    );
                },
                _ => {},
            }
        }
    }

    pub fn on_datagram_attempt(&mut self, trace: &DispatchTraceMeta, writable_len: Option<usize>) {
        self.push_step(
            trace.flow_id,
            "T.select",
            format!(
                "route=datagram writable_len={}",
                writable_len.map(|v| v.to_string()).unwrap_or_else(|| "none".to_string())
            ),
        );
    }

    pub fn on_datagram_result(
        &mut self,
        trace: &DispatchTraceMeta,
        outcome: &str,
        extra: impl Into<String>,
        terminal: Option<DeliveryOutcome>,
    ) {
        let detail = extra.into();
        let label = if matches!(terminal, Some(DeliveryOutcome::TransportDelivered)) {
            "D.delivered"
        } else {
            "T.dgram_send"
        };
        self.push_step(trace.flow_id, label, format!("status={} {}", outcome, detail));
        if let Some(outcome) = terminal {
            self.finish_flow(trace.flow_id, outcome);
        }
    }

    pub fn on_stream_transport_selected(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        fin: bool,
        reason: &str,
    ) {
        self.push_step(
            trace.flow_id,
            "T.select",
            format!("route=stream stream_id={} fin={} reason={}", stream_id, fin, reason),
        );
    }

    pub fn on_stream_queued(
        &mut self,
        trace: &DispatchTraceMeta,
        message_id: Option<MessageId>,
        queued_before: usize,
        queued_after: usize,
        inflight_before: usize,
        inflight_after: usize,
    ) {
        // Find the packet entry with matching message_id, or use the last one
        let step = FlowStep {
            label: "T.stream_queue".to_string(),
            at: Instant::now(),
            detail: format!(
                "queued_bytes: {}→{} | inflight: {}→{}",
                queued_before, queued_after, inflight_before, inflight_after
            ),
        };

        if let Some(flow) = self.active_flows.get_mut(&trace.flow_id) {
            let packet_idx = if let Some(msg_id) = message_id {
                flow.packets.iter().position(|p| p.message_id == Some(msg_id))
            } else {
                None
            }
            .unwrap_or(flow.packets.len().saturating_sub(1));

            if packet_idx < flow.packets.len() {
                flow.packets[packet_idx].steps.push(step);
            }
        }
    }

    pub fn on_stream_write(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        written: usize,
        total: usize,
        fin: bool,
        queued_before: usize,
        queued_after: usize,
    ) {
        let step_label = "T.flush";
        let step_detail = format!(
            "stream_id={} written={}/{} fin={} queued_bytes: {}→{}",
            stream_id, written, total, fin, queued_before, queued_after
        );

        self.push_step(trace.flow_id, step_label, step_detail);

        // Record flush time for RTT calculation
        if let Some(flow) = self.active_flows.get_mut(&trace.flow_id) {
            let elapsed_ms = flow.started_at.elapsed().as_secs_f64() * 1000.0;
            flow.flush_elapsed_ms = Some(elapsed_ms);
        }
    }

    pub fn on_stream_backpressure(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        reason: &str,
        remaining: usize,
    ) {
        self.push_step(
            trace.flow_id,
            "T.backpressure",
            format!("stream_id={} reason={} remaining={}B", stream_id, reason, remaining),
        );
    }

    pub fn on_transport_outcome(&mut self, message_id: MessageId, outcome: DeliveryOutcome) {
        if let Some(flow_id) = self.find_flow_by_message_id(message_id) {
            // Find the packet with this message_id and add receipt step
            if let Some(flow) = self.active_flows.get_mut(&flow_id) {
                if let Some(packet) =
                    flow.packets.iter_mut().find(|p| p.message_id == Some(message_id))
                {
                    packet.steps.push(FlowStep {
                        label: "D.receipt".to_string(),
                        at: Instant::now(),
                        detail: format!("outcome={:?}", outcome),
                    });
                }
            }

            // Handle different outcome types
            match outcome {
                DeliveryOutcome::ClientProcessed => {
                    // Receipt received - flow is complete
                    self.finish_flow(flow_id, outcome);
                },
                DeliveryOutcome::TransportDelivered => {
                    // Add D.delivered for transport delivery
                    self.push_step(flow_id, "D.delivered", "transport=local_quic".to_string());

                    // Check if we're waiting for client receipt
                    let await_receipt = self
                        .active_flows
                        .get(&flow_id)
                        .map(|f| f.await_client_receipt)
                        .unwrap_or(false);

                    if await_receipt {
                        // Don't finish yet, waiting for receipt
                        return;
                    }

                    // No receipt needed, finish now
                    self.finish_flow(flow_id, outcome);
                },
                _ => {
                    // Other outcomes (dropped, etc.) - finish immediately
                    self.finish_flow(flow_id, outcome);
                },
            }
        }
    }

    pub fn on_flow_outcome(&mut self, flow_id: u64, outcome: DeliveryOutcome) {
        if matches!(outcome, DeliveryOutcome::TransportDelivered) {
            self.push_step(flow_id, "D.delivered", "transport=local_quic".to_string());
        }
        self.finish_flow(flow_id, outcome);
    }

    pub fn on_flow_aborted(&mut self, flow_id: u64, reason: &str) {
        self.finish_flow_with_label(flow_id, format!("Aborted({reason})"));
    }

    pub fn on_control(&self, _control: PacketControl) {
        // Suppressed in structured format
    }

    pub fn on_keepalive_ping(&self, bytes: usize, nonce: u64) {
        if self.tracer.file_logging_enabled() {
            self.tracer.log_writer.send(LogEvent::TransportSummary {
                client_id: self.client_id,
                absolute_ms: self.now_ms(),
                direction: TransportDirection::Tx,
                transport: "datagram".to_string(),
                bytes,
                packet_type: "Ping".to_string(),
                extra: Some(format!("nonce={}", nonce)),
            });
        }
    }

    pub fn on_rx_packet(
        &self,
        transport: &str,
        bytes: usize,
        packet: &C2SPacket,
        rtt_ms: Option<f64>,
    ) {
        if self.tracer.file_logging_enabled() {
            let packet_type = variant_name(packet);
            let extra = rtt_ms.map(|rtt| format!("← rtt={:.2}ms", rtt));

            self.tracer.log_writer.send(LogEvent::TransportSummary {
                client_id: self.client_id,
                absolute_ms: self.now_ms(),
                direction: TransportDirection::Rx,
                transport: transport.to_string(),
                bytes,
                packet_type,
                extra,
            });
        }
    }

    pub fn on_rx_decode_failed(&self, _transport: &str, _bytes: usize) {
        // Suppressed in structured format
    }

    pub fn maybe_log_snapshot(&mut self, _snapshot: SessionSnapshot) {
        // Suppressed - data is derivable from flows
    }

    fn finish_flow(&mut self, flow_id: u64, outcome: DeliveryOutcome) {
        self.finish_flow_with_label(flow_id, describe_delivery_outcome(outcome));
    }

    fn finish_flow_with_label(&mut self, flow_id: u64, outcome_label: String) {
        let Some(flow) = self.active_flows.remove(&flow_id) else {
            return;
        };
        if !self.tracer.tracing_enabled() {
            return;
        }

        let total_ms = flow.started_at.elapsed().as_secs_f64() * 1000.0;

        // Calculate deadline remaining if applicable
        let deadline_remaining_ms = match (flow.priority_type, outcome_label.as_str()) {
            (PacketPriority::Deadline { max_delay: _ }, "TransportDropped(ExpiredDeadline)") => {
                Some(-1.0) // Negative signals expired
            },
            (PacketPriority::Deadline { max_delay }, _) => {
                let remaining = max_delay.as_secs_f64() * 1000.0 - total_ms;
                Some(remaining)
            },
            _ => None,
        };

        let steps: Vec<FlowStepData> = flow
            .steps
            .iter()
            .map(|s| FlowStepData {
                label: s.label.clone(),
                elapsed_ms: s.at.duration_since(flow.started_at).as_secs_f64() * 1000.0,
                detail: s.detail.clone(),
            })
            .collect();

        let packets: Vec<PacketEventData> = flow
            .packets
            .iter()
            .map(|p| PacketEventData {
                _index: p.index,
                packet_type: p.packet_type.clone(),
                _bytes: p.bytes,
                _message_id: p.message_id.map(|id| id as u128),
                steps: p
                    .steps
                    .iter()
                    .map(|s| FlowStepData {
                        label: s.label.clone(),
                        elapsed_ms: s.at.duration_since(flow.started_at).as_secs_f64() * 1000.0,
                        detail: s.detail.clone(),
                    })
                    .collect(),
            })
            .collect();

        self.tracer.log_writer.send(LogEvent::FlowComplete {
            client_id: self.client_id,
            flow_id,
            packet_label: flow.packet_label,
            target_label: flow.target_label,
            payload_bytes: flow.payload_bytes,
            priority: flow.priority,
            order: flow.order,
            delivery: flow.delivery,
            message_id: flow.message_id.map(|id| id as u128),
            steps,
            packets,
            kind: flow.kind,
            usage_count: flow.usage_count,
            outcome: outcome_label,
            total_ms,
            deadline_remaining_ms,
        });
    }

    fn find_flow_by_message_id(&self, message_id: MessageId) -> Option<u64> {
        self.active_flows
            .iter()
            .find(|(_, flow)| flow.message_id == Some(message_id))
            .map(|(flow_id, _)| *flow_id)
    }

    fn push_step(&mut self, flow_id: u64, label: &str, detail: String) {
        if let Some(flow) = self.active_flows.get_mut(&flow_id) {
            flow.steps.push(FlowStep { label: label.to_string(), at: Instant::now(), detail });
        }
    }
}

impl FlowTrace {
    fn new(
        trace: &DispatchTraceMeta,
        priority: String,
        priority_type: PacketPriority,
        order: String,
        delivery: &'static str,
        await_client_receipt: bool,
        kind: FlowKind,
        usage_count: Option<i32>,
        packets: Vec<PacketTraceEntry>,
    ) -> Self {
        Self {
            packet_label: trace.packet_label.clone(),
            message_id: trace.message_id,
            target_label: trace.target_label.clone(),
            payload_bytes: trace.payload_bytes,
            priority,
            priority_type,
            order,
            delivery,
            await_client_receipt,
            started_at: Instant::now(),
            steps: Vec::new(),
            packets,
            kind,
            usage_count,
            flush_elapsed_ms: None,
        }
    }
}

pub struct SessionSnapshot {
    pub established: bool,
    pub rtt_ms: Option<f64>,
    pub queued_stream_bytes: usize,
    pub inflight_messages: usize,
    pub active_streams: usize,
    pub active_sequences: usize,
    pub pending_pings: usize,
    pub send_quantum: usize,
}

fn envelope_label(envelope: &PacketEnvelope) -> String {
    match &envelope.payload {
        crate::packets::PacketPayload::Single(packet) => variant_name(packet),
        crate::packets::PacketPayload::Bundle(bundle) => {
            let first =
                bundle.first().map(variant_name).unwrap_or_else(|| "EmptyBundle".to_string());
            format!("{first} bundle={}", bundle.len())
        },
    }
}

fn variant_name<T: fmt::Debug>(value: &T) -> String {
    let rendered = format!("{value:?}");
    rendered.split([' ', '{', '(']).next().unwrap_or("Unknown").to_string()
}

trait DescribeShort {
    fn describe_short(&self) -> &'static str;
}

trait DescribeLong {
    fn describe_long(&self) -> String;
}

impl DescribeShort for PacketPriority {
    fn describe_short(&self) -> &'static str {
        match self {
            Self::Normal => "Normal",
            Self::Droppable => "Droppable",
            Self::Deadline { .. } => "Deadline",
            Self::Coalescing { .. } => "Coalescing",
        }
    }
}

impl DescribeLong for PacketPriority {
    fn describe_long(&self) -> String {
        match self {
            Self::Normal => "Normal".to_string(),
            Self::Droppable => "Droppable".to_string(),
            Self::Deadline { max_delay } => {
                format!("Deadline({:.0}ms)", max_delay.as_secs_f64() * 1000.0)
            },
            Self::Coalescing { target_payload_bytes } => {
                format!("Coalescing({target_payload_bytes}B)")
            },
        }
    }
}

impl DescribeLong for PacketOrder {
    fn describe_long(&self) -> String {
        match self {
            Self::Independent => "Independent".to_string(),
            Self::Dependency(message_id) => format!("Dependency({message_id})"),
            Self::Sequence(sequence_id) => {
                format!("Sequence({})", &format!("{:?}", sequence_id)[..8])
            },
            Self::SequenceEnd(sequence_id) => {
                format!("SequenceEnd({})", &format!("{:?}", sequence_id)[..8])
            },
        }
    }
}

impl DescribeShort for DeliveryPolicy {
    fn describe_short(&self) -> &'static str {
        match self {
            Self::FireAndForget => "FireAndForget",
            Self::ObserveTransport => "ObserveTransport",
            Self::RequireClientReceipt => "RequireClientReceipt",
        }
    }
}

fn describe_target(target: PacketTarget) -> String {
    match target {
        PacketTarget::Client(client_id) => format!("Client({client_id})"),
        PacketTarget::Broadcast => "Broadcast".to_string(),
        PacketTarget::BroadcastExcept(client_id) => format!("BroadcastExcept({client_id})"),
    }
}

fn describe_delivery_outcome(outcome: DeliveryOutcome) -> String {
    match outcome {
        DeliveryOutcome::TransportDelivered => "TransportDelivered".to_string(),
        DeliveryOutcome::TransportDropped { reason } => format!("TransportDropped({reason:?})"),
        DeliveryOutcome::ClientProcessed => "ClientProcessed".to_string(),
    }
}
