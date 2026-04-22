use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use super::*;

#[derive(Clone)]
pub(crate) struct TraceSinkManager {
    inner: Arc<TraceSinkInner>,
}

struct TraceSinkInner {
    sender: Sender<TraceCommand>,
    enabled: bool,
}

enum TraceCommand {
    Event(TraceEvent),
    Shutdown,
}

impl TraceSinkManager {
    pub(crate) fn start(config: NetworkTraceConfig) -> Self {
        let (sender, receiver) = mpsc::channel();
        if !config.enabled {
            return Self { inner: Arc::new(TraceSinkInner { sender, enabled: false }) };
        }

        thread::spawn(move || run_sink_thread(receiver, config));
        Self { inner: Arc::new(TraceSinkInner { sender, enabled: true }) }
    }

    pub(crate) fn send(&self, event: TraceEvent) {
        if self.inner.enabled {
            let _ = self.inner.sender.send(TraceCommand::Event(event));
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.enabled
    }
}

impl Drop for TraceSinkManager {
    fn drop(&mut self) {
        if self.inner.enabled && Arc::strong_count(&self.inner) == 1 {
            let _ = self.inner.sender.send(TraceCommand::Shutdown);
        }
    }
}

trait TraceSink {
    fn write_event(&mut self, event: &TraceEvent, verbose: bool, also_log_to_console: bool);
    fn flush(&mut self);
}

struct SinkWriter {
    events: Option<BufWriter<File>>,
    timeline: Option<BufWriter<File>>,
    timeline_projector: TimelineProjector,
}

impl SinkWriter {
    fn new(events: Option<File>, timeline: Option<File>) -> Self {
        Self {
            events: events.map(BufWriter::new),
            timeline: timeline.map(BufWriter::new),
            timeline_projector: TimelineProjector::new(),
        }
    }

    fn flush_timeline_blocks(&mut self, lines: &[String], also_log_to_console: bool) {
        if let Some(writer) = &mut self.timeline {
            for line in lines {
                let _ = writeln!(writer, "{line}");
            }
        }
        if also_log_to_console {
            for line in lines {
                log::info!("{line}");
            }
        }
    }

    fn finish_pending_timelines(&mut self, verbose: bool, also_log_to_console: bool) {
        let blocks = self.timeline_projector.finish_all(verbose);
        for lines in blocks {
            self.flush_timeline_blocks(&lines, also_log_to_console);
        }
    }
}

impl TraceSink for SinkWriter {
    fn write_event(&mut self, event: &TraceEvent, verbose: bool, also_log_to_console: bool) {
        if let Some(writer) = &mut self.events {
            let _ = serde_json::to_writer(&mut *writer, event);
            let _ = writeln!(writer);
        }

        let blocks = self.timeline_projector.record_event(event.clone(), verbose);
        for lines in blocks {
            self.flush_timeline_blocks(&lines, also_log_to_console);
        }
    }

    fn flush(&mut self) {
        if let Some(writer) = &mut self.events {
            let _ = writer.flush();
        }
        if let Some(writer) = &mut self.timeline {
            let _ = writer.flush();
        }
    }
}

struct TimelineProjector {
    flows: HashMap<u64, TimelineFlowState>,
}

impl TimelineProjector {
    fn new() -> Self {
        Self { flows: HashMap::new() }
    }

    fn record_event(&mut self, event: TraceEvent, verbose: bool) -> Vec<Vec<String>> {
        let mut blocks = Vec::new();

        if let TraceEvent::FlowRegistered { flow_id, context, .. } = &event {
            self.flows.insert(
                *flow_id,
                TimelineFlowState {
                    context: context.clone(),
                    events: vec![event.clone()],
                    started_at_ms: event.timestamp_ms(),
                },
            );
            return blocks;
        }

        let flow_ids = event_flow_ids(&event);
        if flow_ids.is_empty() {
            blocks.push(TraceRenderer::render_misc_event(&event, verbose));
            return blocks;
        }

        for flow_id in &flow_ids {
            let state = self.flows.entry(*flow_id).or_insert_with(|| TimelineFlowState {
                context: placeholder_context(*flow_id, event.client_id()),
                events: Vec::new(),
                started_at_ms: event.timestamp_ms(),
            });
            if state.events.is_empty() {
                state.started_at_ms = event.timestamp_ms();
            }
            state.events.push(event.clone());
        }

        if let TraceEvent::DeliveryEvent { flow_id: Some(flow_id), terminal: true, .. } = event {
            if let Some(state) = self.flows.remove(&flow_id) {
                blocks.push(TraceRenderer::render_flow_block(&state, verbose, false));
            }
        }

        blocks
    }

    fn finish_all(&mut self, verbose: bool) -> Vec<Vec<String>> {
        let mut flow_ids: Vec<u64> = self.flows.keys().copied().collect();
        flow_ids.sort_unstable();
        let mut blocks = Vec::new();
        for flow_id in flow_ids {
            if let Some(state) = self.flows.remove(&flow_id) {
                blocks.push(TraceRenderer::render_flow_block(&state, verbose, true));
            }
        }
        blocks
    }
}

struct TimelineFlowState {
    context: TraceContext,
    events: Vec<TraceEvent>,
    started_at_ms: f64,
}

fn event_flow_ids(event: &TraceEvent) -> Vec<u64> {
    match event {
        TraceEvent::FlowRegistered { flow_id, .. }
        | TraceEvent::DependencyDeclared { flow_id, .. }
        | TraceEvent::TransportSelected { flow_id, .. }
        | TraceEvent::StreamQueued { flow_id, .. }
        | TraceEvent::StreamWrite { flow_id, .. }
        | TraceEvent::StreamBackpressure { flow_id, .. }
        | TraceEvent::DatagramAttempt { flow_id, .. }
        | TraceEvent::DatagramResult { flow_id, .. } => vec![*flow_id],
        TraceEvent::SchedulerEvent { flow_id: Some(flow_id), .. }
        | TraceEvent::DeliveryEvent { flow_id: Some(flow_id), .. } => vec![*flow_id],
        TraceEvent::QuicEgress { flow_ids, .. } => flow_ids.clone(),
        TraceEvent::SessionStart { .. }
        | TraceEvent::SessionSnapshot { .. }
        | TraceEvent::SchedulerEvent { flow_id: None, .. }
        | TraceEvent::DeliveryEvent { flow_id: None, .. }
        | TraceEvent::RxEvent { .. } => Vec::new(),
    }
}

fn placeholder_context(flow_id: u64, client_id: ClientId) -> TraceContext {
    TraceContext {
        flow_id,
        client_id,
        kind: FlowKind::Envelope,
        packet_label: "unknown".to_string(),
        message_id: None,
        payload_bytes: 0,
        target_label: "unknown".to_string(),
        priority: "unknown".to_string(),
        order: "unknown".to_string(),
        delivery: "unknown".to_string(),
        dependency_label: None,
        sequence_id: None,
        components: Vec::new(),
    }
}

fn run_sink_thread(receiver: Receiver<TraceCommand>, config: NetworkTraceConfig) {
    if let Err(err) = fs::create_dir_all(config.log_dir.join("clients")) {
        log::error!("failed to create network trace dir: {err}");
        return;
    }

    let mut global = open_global_sinks(&config).ok();
    let mut clients: HashMap<ClientId, SinkWriter> = HashMap::new();
    let batch_interval = match config.flush_policy {
        FlushPolicy::Batched { interval_ms } => Some(Duration::from_millis(interval_ms)),
        _ => None,
    };

    loop {
        let command = match batch_interval {
            Some(interval) => receiver.recv_timeout(interval).map_err(|err| err.into()),
            None => receiver.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected),
        };

        match command {
            Ok(TraceCommand::Event(event)) => {
                if let Some(writer) = global.as_mut() {
                    writer.write_event(&event, config.verbose, config.also_log_to_console);
                }
                let client_id = event.client_id();
                let writer = clients.entry(client_id).or_insert_with(|| {
                    open_client_sinks(&config.log_dir, client_id, &config).unwrap_or_else(|err| {
                        panic!("failed to open client network trace sink: {err}")
                    })
                });
                writer.write_event(&event, config.verbose, false);

                match config.flush_policy {
                    FlushPolicy::EveryEvent => {
                        if let Some(writer) = global.as_mut() {
                            writer.flush();
                        }
                        writer.flush();
                    },
                    FlushPolicy::OnFlowComplete if event.is_terminal() => {
                        if let Some(writer) = global.as_mut() {
                            writer.flush();
                        }
                        writer.flush();
                    },
                    FlushPolicy::OnFlowComplete | FlushPolicy::Batched { .. } => {},
                }
            },
            Ok(TraceCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(writer) = global.as_mut() {
                    writer.finish_pending_timelines(config.verbose, config.also_log_to_console);
                    writer.flush();
                }
                for writer in clients.values_mut() {
                    writer.finish_pending_timelines(config.verbose, false);
                    writer.flush();
                }
                break;
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(writer) = global.as_mut() {
                    writer.flush();
                }
                for writer in clients.values_mut() {
                    writer.flush();
                }
            },
        }
    }
}

fn open_global_sinks(config: &NetworkTraceConfig) -> std::io::Result<SinkWriter> {
    let events = if config.global_events {
        Some(open_file(&config.log_dir.join("global.events.ndjson"))?)
    } else {
        None
    };
    let timeline = if config.global_timeline {
        Some(open_file(&config.log_dir.join("global.timeline.log"))?)
    } else {
        None
    };
    Ok(SinkWriter::new(events, timeline))
}

fn open_client_sinks(
    log_dir: &Path,
    client_id: ClientId,
    config: &NetworkTraceConfig,
) -> std::io::Result<SinkWriter> {
    let client_dir = log_dir.join("clients");
    let events = if config.client_events {
        Some(open_file(&client_dir.join(format!("client_{client_id}.events.ndjson")))?)
    } else {
        None
    };
    let timeline = if config.client_timeline {
        Some(open_file(&client_dir.join(format!("client_{client_id}.timeline.log")))?)
    } else {
        None
    };
    Ok(SinkWriter::new(events, timeline))
}

fn open_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

struct TraceRenderer;

impl TraceRenderer {
    fn render_misc_event(event: &TraceEvent, verbose: bool) -> Vec<String> {
        match event {
            TraceEvent::SessionStart { timestamp_ms, client_id } => {
                vec![format!("[T+{}ms] client={client_id} session_start", fmt_ms(*timestamp_ms))]
            },
            TraceEvent::SessionSnapshot {
                timestamp_ms,
                client_id,
                established,
                rtt_ms,
                queued_stream_bytes,
                inflight_messages,
                active_streams,
                active_sequences,
                pending_pings,
                send_quantum,
            } => {
                vec![format!(
                    "[T+{}ms] client={client_id} session_snapshot established={} rtt_ms={} queued_stream_bytes={} inflight_messages={} active_streams={} active_sequences={} pending_pings={} send_quantum={}",
                    fmt_ms(*timestamp_ms),
                    established,
                    rtt_ms.map(|value| format!("{value:.2}")).unwrap_or_else(|| "none".to_string()),
                    queued_stream_bytes,
                    inflight_messages,
                    active_streams,
                    active_sequences,
                    pending_pings,
                    send_quantum
                )]
            },
            TraceEvent::SchedulerEvent {
                timestamp_ms,
                client_id,
                flow_id,
                action,
                queue_name,
                queue_depth,
                command,
                order_domain,
                reason,
                force_flush,
            } => {
                let mut detail = Vec::new();
                if let Some(flow_id) = flow_id {
                    detail.push(format!("flow={flow_id}"));
                }
                if let Some(queue_name) = queue_name {
                    detail.push(format!("queue={queue_name}"));
                }
                if let Some(queue_depth) = queue_depth {
                    detail.push(format!("depth={queue_depth}"));
                }
                if let Some(command) = command {
                    detail.push(format!("command={command}"));
                }
                if let Some(order_domain) = order_domain {
                    detail.push(format!("order={order_domain}"));
                }
                if let Some(reason) = reason {
                    detail.push(format!("reason={reason}"));
                }
                if let Some(force_flush) = force_flush {
                    detail.push(format!("force_flush={force_flush}"));
                }
                vec![format!(
                    "[T+{}ms] client={client_id} scheduler {action} {}",
                    fmt_ms(*timestamp_ms),
                    detail.join(" ")
                )]
            },
            TraceEvent::QuicEgress {
                timestamp_ms,
                client_id,
                bytes,
                destination,
                pacing_delay_ms,
                approximate,
                flow_ids,
                message_ids,
                sources,
            } => {
                vec![format!(
                    "[T+{}ms] client={client_id} quic_egress bytes={bytes} dest={destination} pace={pacing_delay_ms:.3}ms approx={approximate} flows={:?} messages={:?} sources={:?}",
                    fmt_ms(*timestamp_ms),
                    flow_ids,
                    message_ids,
                    sources
                )]
            },
            TraceEvent::RxEvent { timestamp_ms, client_id, direction, transport, bytes, packet_type, detail } => {
                let detail = detail
                    .as_ref()
                    .map(|value| format!(" detail={value}"))
                    .unwrap_or_default();
                vec![format!(
                    "[T+{}ms] client={client_id} {:?} {transport} {bytes}B {packet_type}{detail}",
                    fmt_ms(*timestamp_ms),
                    direction
                )]
            },
            TraceEvent::FlowRegistered { .. }
            | TraceEvent::DependencyDeclared { .. }
            | TraceEvent::TransportSelected { .. }
            | TraceEvent::StreamQueued { .. }
            | TraceEvent::StreamWrite { .. }
            | TraceEvent::StreamBackpressure { .. }
            | TraceEvent::DatagramAttempt { .. }
            | TraceEvent::DatagramResult { .. }
            | TraceEvent::DeliveryEvent { .. } => Vec::new(),
        }
        .into_iter()
        .filter(|line| verbose || !line.contains("snapshot"))
        .collect()
    }

    fn render_flow_block(
        state: &TimelineFlowState,
        verbose: bool,
        incomplete: bool,
    ) -> Vec<String> {
        let mut lines = Vec::new();
        let context = &state.context;
        let flow_id = context.flow_id;
        let final_outcome = state.events.iter().rev().find_map(|event| match event {
            TraceEvent::DeliveryEvent {
                outcome,
                terminal,
                detail,
                retry_count,
                retry_reason,
                ..
            } if *terminal => Some((outcome.clone(), detail.clone(), *retry_count, *retry_reason)),
            _ => None,
        });
        let total_ms = state
            .events
            .last()
            .map(|event| (event.timestamp_ms() - state.started_at_ms).max(0.0))
            .unwrap_or(0.0);

        lines.push(format!(
            "FLOW {}  {}  target={}  priority={}  order={}  delivery={}  bytes={}",
            flow_id,
            context.packet_label,
            context.target_label,
            context.priority,
            context.order,
            context.delivery,
            context.payload_bytes
        ));
        lines.push(format!(
            "  kind={}  message={}  components={}  total={}ms{}",
            flow_kind_label(context.kind),
            context.message_id.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string()),
            context.components.len(),
            fmt_ms(total_ms),
            if incomplete { "  status=incomplete" } else { "" }
        ));
        if let Some(dependency) = &context.dependency_label {
            lines.push(format!("  depends_on_message={dependency}"));
        }
        if let Some(sequence_id) = &context.sequence_id {
            lines.push(format!("  sequence={sequence_id}"));
        }
        if verbose && !context.components.is_empty() {
            let components = context
                .components
                .iter()
                .map(|component| {
                    format!(
                        "#{}:{}~{}B",
                        component.index, component.component_type, component.approx_payload_bytes
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  components={components}"));
        }
        lines.push("  steps:".to_string());

        let mut previous_ts = state.started_at_ms;
        for event in &state.events {
            let ts = event.timestamp_ms();
            let since_start = (ts - state.started_at_ms).max(0.0);
            let delta = (ts - previous_ts).max(0.0);
            previous_ts = ts;
            if let Some(description) = Self::render_flow_step(event, verbose) {
                lines.push(format!(
                    "    +{:>8}ms  Δ{:>8}ms  {}",
                    fmt_ms(since_start),
                    fmt_ms(delta),
                    description
                ));
            }
        }

        if let Some((outcome, detail, retry_count, retry_reason)) = final_outcome {
            lines.push(format!(
                "  outcome={} retry_count={}{}{}",
                outcome,
                retry_count,
                retry_reason
                    .map(|reason| format!(" retry_reason={}", reason.describe()))
                    .unwrap_or_default(),
                detail.map(|value| format!(" ({value})")).unwrap_or_default()
            ));
        } else if incomplete {
            lines.push("  outcome=incomplete".to_string());
        }
        lines.push(String::new());
        lines
    }

    fn render_flow_step(event: &TraceEvent, verbose: bool) -> Option<String> {
        match event {
            TraceEvent::FlowRegistered { context, .. } => Some(format!(
                "register kind={} target={} bytes={} msg={}",
                flow_kind_label(context.kind),
                context.target_label,
                context.payload_bytes,
                context
                    .message_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )),
            TraceEvent::DependencyDeclared { dependency_kind, dependency_value, .. } => {
                Some(format!("dependency {}={}", dependency_kind, dependency_value))
            },
            TraceEvent::SchedulerEvent {
                action,
                queue_name,
                queue_depth,
                command,
                order_domain,
                reason,
                force_flush,
                ..
            } => {
                let mut parts = vec![action.clone()];
                if let Some(queue_name) = queue_name {
                    parts.push(format!("queue={queue_name}"));
                }
                if let Some(queue_depth) = queue_depth {
                    parts.push(format!("depth={queue_depth}"));
                }
                if let Some(command) = command {
                    parts.push(format!("command={command}"));
                }
                if let Some(order_domain) = order_domain {
                    parts.push(format!("order={order_domain}"));
                }
                if let Some(reason) = reason {
                    parts.push(format!("reason={reason}"));
                }
                if let Some(force_flush) = force_flush {
                    parts.push(format!("force_flush={force_flush}"));
                }
                Some(format!("scheduler {}", parts.join(" ")))
            },
            TraceEvent::TransportSelected {
                route,
                stream_id,
                fin,
                eligible,
                reason,
                writable_len,
                ..
            } => {
                let mut parts =
                    vec![format!("transport route={route}"), format!("eligible={eligible}")];
                if let Some(stream_id) = stream_id {
                    parts.push(format!("stream={stream_id}"));
                }
                if let Some(fin) = fin {
                    parts.push(format!("fin={fin}"));
                }
                if let Some(writable_len) = writable_len {
                    parts.push(format!("writable={writable_len}"));
                }
                parts.push(format!("reason={reason}"));
                Some(parts.join(" "))
            },
            TraceEvent::StreamQueued {
                stream_id,
                queued_before,
                queued_after,
                inflight_before,
                inflight_after,
                message_id,
                ..
            } => Some(format!(
                "stream queued stream={} queued {}->{} inflight {}->{} msg={}",
                stream_id,
                queued_before,
                queued_after,
                inflight_before,
                inflight_after,
                message_id.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string())
            )),
            TraceEvent::StreamWrite {
                stream_id,
                written,
                total,
                offset,
                fin,
                queued_before,
                queued_after,
                remaining,
                backpressure_reason,
                ..
            } => {
                let mut line = format!(
                    "stream write stream={} wrote={}/{} offset={} remaining={} fin={} queued {}->{}",
                    stream_id, written, total, offset, remaining, fin, queued_before, queued_after
                );
                if let Some(reason) = backpressure_reason {
                    line.push_str(&format!(" backpressure={reason}"));
                }
                Some(line)
            },
            TraceEvent::StreamBackpressure {
                stream_id,
                remaining,
                available_capacity,
                blocked_bytes,
                reason,
                ..
            } => Some(format!(
                "stream backpressure stream={} reason={} remaining={} available={} blocked={}",
                stream_id,
                reason.describe(),
                remaining,
                available_capacity
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                blocked_bytes
            )),
            TraceEvent::DatagramAttempt { payload_bytes, writable_len, .. } => Some(format!(
                "datagram attempt bytes={} writable={}",
                payload_bytes,
                writable_len.map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
            )),
            TraceEvent::DatagramResult { status, detail, terminal_outcome, .. } => Some(format!(
                "datagram result status={} terminal={} {}",
                status,
                terminal_outcome.as_deref().unwrap_or("-"),
                detail
            )),
            TraceEvent::QuicEgress {
                bytes,
                destination,
                pacing_delay_ms,
                approximate,
                flow_ids,
                sources,
                ..
            } => Some(format!(
                "quic egress bytes={} dest={} pace={:.3}ms approx={} flows={:?} sources={:?}",
                bytes, destination, pacing_delay_ms, approximate, flow_ids, sources
            )),
            TraceEvent::DeliveryEvent {
                message_id,
                outcome,
                terminal,
                retry_count,
                retry_reason,
                detail,
                ..
            } => Some(format!(
                "delivery outcome={} terminal={} retry_count={}{} msg={}{}",
                outcome,
                terminal,
                retry_count,
                retry_reason
                    .map(|reason| format!(" retry_reason={}", reason.describe()))
                    .unwrap_or_default(),
                message_id.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string()),
                detail.as_ref().map(|value| format!(" detail={value}")).unwrap_or_default()
            )),
            TraceEvent::SessionStart { .. }
            | TraceEvent::SessionSnapshot { .. }
            | TraceEvent::RxEvent { .. } => {
                if verbose {
                    None
                } else {
                    None
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn make_tracer(log_dir: PathBuf) -> Arc<NetworkTracer> {
        Arc::new(NetworkTracer {
            next_flow_id: AtomicU64::new(1),
            sinks: TraceSinkManager::start(NetworkTraceConfig {
                enabled: true,
                log_dir,
                flush_policy: FlushPolicy::EveryEvent,
                also_log_to_console: false,
                verbose: true,
                global_events: true,
                client_events: true,
                global_timeline: true,
                client_timeline: true,
            }),
        })
    }

    fn temp_log_dir() -> PathBuf {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("widev-trace-test-{unique}"))
    }

    #[test]
    fn register_envelope_records_dependency_metadata() {
        let tracer = make_tracer(temp_log_dir());
        let mut session = SessionTracer::new(tracer, 7);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(7),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        )
        .id(99)
        .delivery(DeliveryPolicy::ObserveTransport)
        .dependency(42);

        let trace = session.register_envelope(&envelope, 128);
        assert_eq!(trace.message_id, Some(99));
        assert_eq!(trace.dependency_label.as_deref(), Some("42"));
        assert_eq!(trace.components.len(), 1);
    }

    #[test]
    fn register_bundle_records_components() {
        let tracer = make_tracer(temp_log_dir());
        let mut session = SessionTracer::new(tracer, 8);
        let envelope = PacketEnvelope::bundle(
            PacketTarget::Client(8),
            vec![
                crate::packets::S2CPacket::Ping { nonce: 1 },
                crate::packets::S2CPacket::Ping { nonce: 2 },
            ],
        );

        let trace = session.register_envelope(&envelope, 200);
        assert_eq!(trace.components.len(), 2);
        assert!(trace.packet_label.contains("bundle=2"));
    }

    #[test]
    fn client_receipt_keeps_flow_active_until_processed() {
        let tracer = make_tracer(temp_log_dir());
        let mut session = SessionTracer::new(tracer, 9);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(9),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        )
        .id(77)
        .delivery(DeliveryPolicy::RequireClientReceipt);

        let trace = session.register_envelope(&envelope, 64);
        session.on_transport_outcome(77, DeliveryOutcome::TransportDelivered);
        assert!(session.projector.get_flow(trace.flow_id).is_some());
        session.on_transport_outcome(77, DeliveryOutcome::ClientProcessed);
        assert!(session.projector.get_flow(trace.flow_id).is_none());
    }

    #[test]
    fn sink_manager_writes_global_and_client_logs() {
        let log_dir = temp_log_dir();
        let tracer = make_tracer(log_dir.clone());
        let mut session = SessionTracer::new(tracer, 11);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(11),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        );
        let trace = session.register_envelope(&envelope, 32);
        session.on_flow_outcome(trace.flow_id, DeliveryOutcome::TransportDelivered);
        std::thread::sleep(Duration::from_millis(50));

        let global_events = fs::read_to_string(log_dir.join("global.events.ndjson")).unwrap();
        let client_events =
            fs::read_to_string(log_dir.join("clients").join("client_11.events.ndjson")).unwrap();
        let client_timeline =
            fs::read_to_string(log_dir.join("clients").join("client_11.timeline.log")).unwrap();

        assert!(global_events.contains("\"flow_registered\""));
        assert!(client_events.contains("\"delivery_event\""));
        assert!(client_timeline.contains("delivery outcome=TransportDelivered"));
    }

    #[test]
    fn terminal_delivery_event_includes_retry_metadata() {
        let log_dir = temp_log_dir();
        let tracer = make_tracer(log_dir.clone());
        let mut session = SessionTracer::new(tracer, 12);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(12),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        );

        let trace = session.register_envelope(&envelope, 32);
        session.on_flow_retry(trace.flow_id, RetryReason::Congestion);
        session.on_flow_retry(trace.flow_id, RetryReason::Congestion);
        session.on_flow_outcome(trace.flow_id, DeliveryOutcome::TransportDelivered);
        std::thread::sleep(Duration::from_millis(50));

        let client_events =
            fs::read_to_string(log_dir.join("clients").join("client_12.events.ndjson")).unwrap();
        let client_timeline =
            fs::read_to_string(log_dir.join("clients").join("client_12.timeline.log")).unwrap();

        assert!(client_events.contains("\"retry_count\":2"));
        assert!(client_events.contains("\"retry_reason\":\"congestion\""));
        assert!(client_timeline.contains("retry_count=2"));
        assert!(client_timeline.contains("retry_reason=congestion"));
    }

    #[test]
    fn terminal_delivery_event_records_zero_retries_by_default() {
        let log_dir = temp_log_dir();
        let tracer = make_tracer(log_dir.clone());
        let mut session = SessionTracer::new(tracer, 13);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(13),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        );

        let trace = session.register_envelope(&envelope, 32);
        session.on_flow_outcome(trace.flow_id, DeliveryOutcome::TransportDelivered);
        std::thread::sleep(Duration::from_millis(50));

        let client_events =
            fs::read_to_string(log_dir.join("clients").join("client_13.events.ndjson")).unwrap();
        assert!(client_events.contains("\"retry_count\":0"));
        assert!(client_events.contains("\"retry_reason\":null"));
    }

    #[test]
    fn stream_backpressure_event_records_flow_control_window() {
        let log_dir = temp_log_dir();
        let tracer = make_tracer(log_dir.clone());
        let mut session = SessionTracer::new(tracer, 14);
        let envelope = PacketEnvelope::single(
            PacketTarget::Client(14),
            crate::packets::S2CPacket::Ping { nonce: 1 },
        );

        let trace = session.register_envelope(&envelope, 48);
        session.on_flow_control_backpressure(&trace, 9, 128, Some(0));
        session.on_flow_outcome(trace.flow_id, DeliveryOutcome::TransportDelivered);
        std::thread::sleep(Duration::from_millis(50));

        let client_events =
            fs::read_to_string(log_dir.join("clients").join("client_14.events.ndjson")).unwrap();
        let client_timeline =
            fs::read_to_string(log_dir.join("clients").join("client_14.timeline.log")).unwrap();

        assert!(client_events.contains("\"stream_backpressure\""));
        assert!(client_events.contains("\"flow_control_window\""));
        assert!(client_events.contains("\"blocked_bytes\":128"));
        assert!(client_timeline.contains("stream backpressure"));
        assert!(client_timeline.contains("flow_control_window"));
    }
}
