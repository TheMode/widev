use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::game::ClientId;
use crate::packets::{
    C2SPacket, DeliveryOutcome, DeliveryPolicy, DropReason, MessageId, PacketControl,
    PacketEnvelope, PacketOrder, PacketPriority, PacketResource, PacketTarget,
};

const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(1000);
const STEP_LABEL_WIDTH: usize = 16;

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
    enabled: bool,
    next_flow_id: AtomicU64,
    snapshot_interval: Duration,
}

impl NetworkTracer {
    pub fn from_env() -> Arc<Self> {
        let enabled = std::env::var("WIDEV_NET_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        let snapshot_interval = std::env::var("WIDEV_NET_TRACE_SNAPSHOT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .filter(|duration| !duration.is_zero())
            .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL);

        Arc::new(Self {
            enabled,
            next_flow_id: AtomicU64::new(1),
            snapshot_interval,
        })
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

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn snapshot_interval(&self) -> Duration {
        self.snapshot_interval
    }
}

pub struct SessionTracer {
    tracer: Arc<NetworkTracer>,
    client_id: ClientId,
    active_flows: HashMap<u64, FlowTrace>,
    last_snapshot_at: Instant,
}

impl SessionTracer {
    pub fn new(tracer: Arc<NetworkTracer>, client_id: ClientId) -> Self {
        Self {
            tracer,
            client_id,
            active_flows: HashMap::new(),
            last_snapshot_at: Instant::now(),
        }
    }

    pub fn register_envelope(
        &mut self,
        envelope: &PacketEnvelope,
        framed_len: usize,
    ) -> DispatchTraceMeta {
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
                "envelope",
                describe_priority(envelope.meta.priority),
                describe_order(envelope.meta.order),
                describe_delivery(envelope.meta.delivery),
                envelope.meta.delivery == DeliveryPolicy::RequireClientReceipt,
            ),
        );
        self.push_step(
            trace.flow_id,
            "tx.enqueue",
            format!(
                "priority={} order={} delivery={} framed={}B",
                describe_priority(envelope.meta.priority),
                describe_order(envelope.meta.order),
                describe_delivery(envelope.meta.delivery),
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
        self.active_flows.insert(
            trace.flow_id,
            FlowTrace::new(
                &trace,
                "resource",
                describe_priority(resource.meta.priority),
                describe_order(resource.meta.order),
                describe_delivery(resource.meta.delivery),
                resource.meta.delivery == DeliveryPolicy::RequireClientReceipt,
            ),
        );
        self.push_step(
            trace.flow_id,
            "tx.enqueue",
            format!(
                "priority={} order={} delivery={} framed={}B usage_count={}",
                describe_priority(resource.meta.priority),
                describe_order(resource.meta.order),
                describe_delivery(resource.meta.delivery),
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
                } => self.push_step(
                    trace.flow_id,
                    "sched.defer",
                    format!(
                        "queue={} queued_messages={} reason={} initial_defer=true",
                        queue_name, queued_messages, policy
                    ),
                ),
                SchedulerTraceEvent::RequeuedCongestion { trace, queued_messages } => self.push_step(
                    trace.flow_id,
                    "sched.requeue",
                    format!(
                        "queue_depth={} reason=transport_backpressure",
                        queued_messages
                    ),
                ),
                SchedulerTraceEvent::DispatchReady {
                    trace,
                    force_flush,
                    queue_name,
                } => self.push_step(
                    trace.flow_id,
                    "sched.dispatch",
                    format!("queue={} force_flush={}", queue_name, force_flush),
                ),
                SchedulerTraceEvent::Dropped {
                    trace,
                    reason,
                    queue_name,
                } => self.push_step(
                    trace.flow_id,
                    "sched.drop",
                    format!("queue={} reason={reason:?}", queue_name),
                ),
                SchedulerTraceEvent::BlockedByBarrier { flow_id, command } => self.push_maybe_known(
                    flow_id,
                    "sched.blocked",
                    format!("command={} reason=barrier_pending", command),
                ),
                SchedulerTraceEvent::BlockedByDeferred {
                    flow_id,
                    command,
                    order_domain,
                } => self.push_maybe_known(
                    flow_id,
                    "sched.blocked",
                    format!(
                        "command={} reason=deferred_conflict domain={}",
                        command, order_domain
                    ),
                ),
                SchedulerTraceEvent::BarrierBegin => {
                    self.log_standalone(format!(
                        "net.ctrl client={} barrier begin active_flows={}",
                        self.client_id,
                        self.active_flows.len()
                    ));
                },
                SchedulerTraceEvent::BarrierReleased => {
                    self.log_standalone(format!("net.ctrl client={} barrier released", self.client_id));
                },
                SchedulerTraceEvent::ClearedTransportState => {
                    self.log_standalone(format!(
                        "net.ctrl client={} cleared queued transport state",
                        self.client_id
                    ));
                },
            }
        }
    }

    pub fn on_datagram_attempt(
        &mut self,
        trace: &DispatchTraceMeta,
        writable_len: Option<usize>,
    ) {
        self.push_step(
            trace.flow_id,
            "tx.transport",
            format!(
                "route=datagram writable_len={}",
                writable_len
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
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
            "tx.delivered"
        } else {
            "tx.dgram"
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
            "tx.transport",
            format!("route=stream stream_id={} fin={} reason={}", stream_id, fin, reason),
        );
    }

    pub fn on_stream_queued(
        &mut self,
        trace: &DispatchTraceMeta,
        queued_before: usize,
        queued_after: usize,
        inflight_before: usize,
        inflight_after: usize,
    ) {
        self.push_step(
            trace.flow_id,
            "tx.queue",
            format!(
                "queued_stream_bytes {} -> {} inflight {} -> {}",
                queued_before, queued_after, inflight_before, inflight_after
            ),
        );
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
        self.push_step(
            trace.flow_id,
            "tx.flush",
            format!(
                "stream_id={} written={}/{} fin={} queued_stream_bytes {} -> {}",
                stream_id, written, total, fin, queued_before, queued_after
            ),
        );
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
            "tx.backpressure",
            format!("stream_id={} reason={} remaining={}B", stream_id, reason, remaining),
        );
    }

    pub fn on_transport_outcome(&mut self, message_id: MessageId, outcome: DeliveryOutcome) {
        if let Some(flow_id) = self.find_flow_by_message_id(message_id) {
            if matches!(outcome, DeliveryOutcome::TransportDelivered)
                && self
                    .active_flows
                    .get(&flow_id)
                    .is_some_and(|flow| flow.await_client_receipt)
            {
                self.push_step(flow_id, "tx.delivered", "transport=local_quic".to_string());
                return;
            }
            if matches!(outcome, DeliveryOutcome::ClientProcessed) {
                self.push_step(flow_id, "rx.receipt", "outcome=ClientProcessed".to_string());
            } else if matches!(outcome, DeliveryOutcome::TransportDelivered) {
                self.push_step(flow_id, "tx.delivered", "transport=local_quic".to_string());
            }
            self.finish_flow(flow_id, outcome);
        } else if self.tracer.enabled() {
            self.log_standalone(format!(
                "net.delivery client={} msg={} outcome={}",
                self.client_id,
                message_id,
                describe_delivery_outcome(outcome)
            ));
        }
    }

    pub fn on_flow_outcome(&mut self, flow_id: u64, outcome: DeliveryOutcome) {
        if matches!(outcome, DeliveryOutcome::TransportDelivered) {
            self.push_step(flow_id, "tx.delivered", "transport=local_quic".to_string());
        }
        self.finish_flow(flow_id, outcome);
    }

    pub fn on_flow_aborted(&mut self, flow_id: u64, reason: &str) {
        self.finish_flow_with_label(flow_id, format!("Aborted({reason})"));
    }

    pub fn on_control(&self, control: PacketControl) {
        if self.tracer.enabled() {
            self.log_standalone(format!(
                "net.ctrl client={} {}",
                self.client_id,
                describe_control(control)
            ));
        }
    }

    pub fn on_keepalive_ping(&self, bytes: usize, nonce: u64) {
        if self.tracer.enabled() {
            self.log_standalone(format!(
                "net.tx client={} transport=datagram bytes={} packet=Ping nonce={}",
                self.client_id, bytes, nonce
            ));
        }
    }

    pub fn on_rx_packet(&self, transport: &str, bytes: usize, packet: &C2SPacket) {
        if self.tracer.enabled() {
            self.log_standalone(format!(
                "net.rx client={} transport={} bytes={} packet={}",
                self.client_id,
                transport,
                bytes,
                variant_name(packet)
            ));
        }
    }

    pub fn on_rx_decode_failed(&self, transport: &str, bytes: usize) {
        if self.tracer.enabled() {
            self.log_standalone(format!(
                "net.rx client={} transport={} bytes={} packet=decode_failed",
                self.client_id, transport, bytes
            ));
        }
    }

    pub fn maybe_log_snapshot(&mut self, snapshot: SessionSnapshot) {
        if !self.tracer.enabled() {
            return;
        }
        if self.last_snapshot_at.elapsed() < self.tracer.snapshot_interval() {
            return;
        }
        self.last_snapshot_at = Instant::now();

        let oldest_flow_ms = self
            .active_flows
            .values()
            .map(|flow| flow.started_at.elapsed().as_millis())
            .max()
            .unwrap_or(0);
        self.log_standalone(format!(
            "net.snapshot client={} established={} rtt={} queued_stream_bytes={} inflight={} active_flows={} active_streams={} active_sequences={} pending_pings={} send_quantum={}B oldest_flow={}ms",
            self.client_id,
            snapshot.established,
            snapshot
                .rtt_ms
                .map(|value| format!("{value:.2}ms"))
                .unwrap_or_else(|| "n/a".to_string()),
            snapshot.queued_stream_bytes,
            snapshot.inflight_messages,
            self.active_flows.len(),
            snapshot.active_streams,
            snapshot.active_sequences,
            snapshot.pending_pings,
            snapshot.send_quantum,
            oldest_flow_ms
        ));
    }

    fn finish_flow(&mut self, flow_id: u64, outcome: DeliveryOutcome) {
        self.finish_flow_with_label(flow_id, describe_delivery_outcome(outcome));
    }

    fn finish_flow_with_label(&mut self, flow_id: u64, outcome_label: String) {
        let Some(flow) = self.active_flows.remove(&flow_id) else {
            return;
        };
        if !self.tracer.enabled() {
            return;
        }
        let rendered_steps = render_steps(&flow);
        log::info!(
            "FLOW msg={} kind={} packet={} target={} bytes={} priority={} order={} delivery={} total={:.3}ms outcome={}\n{}",
            render_message_id(flow.message_id),
            flow.kind,
            flow.packet_label,
            flow.target_label,
            flow.payload_bytes,
            flow.priority,
            flow.order,
            flow.delivery,
            flow.started_at.elapsed().as_secs_f64() * 1000.0,
            outcome_label,
            rendered_steps
        );
    }

    fn find_flow_by_message_id(&self, message_id: MessageId) -> Option<u64> {
        self.active_flows
            .iter()
            .find(|(_, flow)| flow.message_id == Some(message_id))
            .map(|(flow_id, _)| *flow_id)
    }

    fn push_step(&mut self, flow_id: u64, label: &str, detail: String) {
        if let Some(flow) = self.active_flows.get_mut(&flow_id) {
            flow.steps.push(FlowStep {
                label: label.to_string(),
                at: Instant::now(),
                detail,
            });
        }
    }

    fn push_maybe_known(&mut self, flow_id: Option<u64>, label: &str, detail: String) {
        if let Some(flow_id) = flow_id {
            if self.active_flows.contains_key(&flow_id) {
                self.push_step(flow_id, label, detail);
                return;
            }
        }
        self.log_standalone(format!("net.sched client={} {} {}", self.client_id, label, detail));
    }

    fn log_standalone(&self, line: String) {
        log::info!("{line}");
    }
}

struct FlowTrace {
    kind: &'static str,
    packet_label: String,
    message_id: Option<MessageId>,
    target_label: String,
    payload_bytes: usize,
    priority: String,
    order: String,
    delivery: &'static str,
    await_client_receipt: bool,
    started_at: Instant,
    steps: Vec<FlowStep>,
}

impl FlowTrace {
    fn new(
        trace: &DispatchTraceMeta,
        kind: &'static str,
        priority: String,
        order: String,
        delivery: &'static str,
        await_client_receipt: bool,
    ) -> Self {
        Self {
            kind,
            packet_label: trace.packet_label.clone(),
            message_id: trace.message_id,
            target_label: trace.target_label.clone(),
            payload_bytes: trace.payload_bytes,
            priority,
            order,
            delivery,
            await_client_receipt,
            started_at: Instant::now(),
            steps: Vec::new(),
        }
    }
}

struct FlowStep {
    label: String,
    at: Instant,
    detail: String,
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

fn render_steps(flow: &FlowTrace) -> String {
    flow.steps
        .iter()
        .map(|step| {
            format!(
                "  {:<width$} +{:>8.3}ms  {}",
                step.label,
                step.at.duration_since(flow.started_at).as_secs_f64() * 1000.0,
                step.detail,
                width = STEP_LABEL_WIDTH
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_message_id(message_id: Option<MessageId>) -> String {
    message_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn envelope_label(envelope: &PacketEnvelope) -> String {
    match &envelope.payload {
        crate::packets::PacketPayload::Single(packet) => variant_name(packet),
        crate::packets::PacketPayload::Bundle(bundle) => {
            let first = bundle
                .first()
                .map(variant_name)
                .unwrap_or_else(|| "EmptyBundle".to_string());
            format!("{first} bundle={}", bundle.len())
        },
    }
}

fn variant_name<T: fmt::Debug>(value: &T) -> String {
    let rendered = format!("{value:?}");
    rendered
        .split([' ', '{', '('])
        .next()
        .unwrap_or("Unknown")
        .to_string()
}

fn describe_priority(priority: PacketPriority) -> String {
    match priority {
        PacketPriority::Normal => "Normal".to_string(),
        PacketPriority::Droppable => "Droppable".to_string(),
        PacketPriority::Deadline { max_delay } => {
            format!("Deadline({:.0}ms)", max_delay.as_secs_f64() * 1000.0)
        },
        PacketPriority::Coalescing {
            target_payload_bytes,
        } => format!("Coalescing({target_payload_bytes}B)"),
    }
}

fn describe_order(order: PacketOrder) -> String {
    match order {
        PacketOrder::Independent => "Independent".to_string(),
        PacketOrder::Dependency(message_id) => format!("Dependency({message_id})"),
        PacketOrder::Sequence(sequence_id) => format!("Sequence({sequence_id})"),
        PacketOrder::SequenceEnd(sequence_id) => format!("SequenceEnd({sequence_id})"),
    }
}

fn describe_delivery(delivery: DeliveryPolicy) -> &'static str {
    match delivery {
        DeliveryPolicy::None => "None",
        DeliveryPolicy::ObserveTransport => "ObserveTransport",
        DeliveryPolicy::RequireClientReceipt => "RequireClientReceipt",
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

fn describe_control(control: PacketControl) -> String {
    match control {
        PacketControl::SequenceClose { sequence_id } => format!("sequence_close sequence={sequence_id}"),
        PacketControl::SequenceCloseAll { .. } => "sequence_close_all".to_string(),
        PacketControl::Clear { .. } => "clear_transport_state".to_string(),
        PacketControl::Barrier { .. } => "barrier".to_string(),
    }
}
