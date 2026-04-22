use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;

use crate::game::ClientId;
use crate::packets::{
    C2SPacket, DeliveryOutcome, DeliveryPolicy, DropReason, MessageId, PacketControl,
    PacketEnvelope, PacketOrder, PacketPriority, PacketResource, PacketTarget, RetryReason,
};

#[path = "network_trace_presentation.rs"]
mod network_trace_presentation;
use network_trace_presentation::TraceSinkManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    EveryEvent,
    OnFlowComplete,
    Batched {
        interval_ms: u64,
    },
}

#[derive(Debug, Clone)]
pub struct NetworkTraceConfig {
    pub enabled: bool,
    pub log_dir: PathBuf,
    pub flush_policy: FlushPolicy,
    pub also_log_to_console: bool,
    pub verbose: bool,
    pub global_events: bool,
    pub client_events: bool,
    pub global_timeline: bool,
    pub client_timeline: bool,
}

impl Default for NetworkTraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_dir: PathBuf::from("logs/network"),
            flush_policy: FlushPolicy::OnFlowComplete,
            also_log_to_console: false,
            verbose: true,
            global_events: true,
            client_events: true,
            global_timeline: true,
            client_timeline: true,
        }
    }
}

impl NetworkTraceConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: env_flag_any(&["WIDEV_NET_TRACE"], false),
            log_dir: env_string_any(&["WIDEV_NET_TRACE_DIR"])
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("logs/network")),
            flush_policy: env_string_any(&["WIDEV_NET_TRACE_FLUSH"])
                .and_then(|value| match value.as_str() {
                    "every" => Some(FlushPolicy::EveryEvent),
                    "batch" | "batched" => Some(FlushPolicy::Batched { interval_ms: 100 }),
                    "flow" | "complete" => Some(FlushPolicy::OnFlowComplete),
                    _ => None,
                })
                .unwrap_or(FlushPolicy::OnFlowComplete),
            also_log_to_console: env_flag_any(&["WIDEV_NET_TRACE_CONSOLE"], false),
            verbose: env_flag_any(&["WIDEV_NET_TRACE_VERBOSE"], true),
            global_events: env_flag_any(&["WIDEV_NET_TRACE_GLOBAL_EVENTS"], true),
            client_events: env_flag_any(&["WIDEV_NET_TRACE_CLIENT_EVENTS"], true),
            global_timeline: env_flag_any(&["WIDEV_NET_TRACE_GLOBAL_TIMELINE"], true),
            client_timeline: env_flag_any(&["WIDEV_NET_TRACE_CLIENT_TIMELINE"], true),
        }
    }
}

fn env_flag_any(keys: &[&str], default: bool) -> bool {
    env_string_any(keys)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn env_string_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| std::env::var(key).ok())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowKind {
    Envelope,
    Resource,
    Datagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceDirection {
    Rx,
    Tx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum BackpressureReason {
    FlowControlWindow,
    Other,
}

#[derive(Debug, Clone, Serialize)]
pub struct PacketComponent {
    pub index: usize,
    pub component_type: String,
    pub message_id: Option<u128>,
    pub approx_payload_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceContext {
    pub flow_id: u64,
    pub client_id: ClientId,
    pub kind: FlowKind,
    pub packet_label: String,
    pub message_id: Option<MessageId>,
    pub payload_bytes: usize,
    pub target_label: String,
    pub priority: String,
    pub order: String,
    pub delivery: String,
    pub dependency_label: Option<String>,
    pub sequence_id: Option<String>,
    pub components: Vec<PacketComponent>,
}

pub type DispatchTraceMeta = TraceContext;

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
        reason: RetryReason,
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

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TraceEvent {
    SessionStart {
        timestamp_ms: f64,
        client_id: ClientId,
    },
    SessionSnapshot {
        timestamp_ms: f64,
        client_id: ClientId,
        established: bool,
        rtt_ms: Option<f64>,
        queued_stream_bytes: usize,
        inflight_messages: usize,
        active_streams: usize,
        active_sequences: usize,
        pending_pings: usize,
        send_quantum: usize,
    },
    FlowRegistered {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        context: TraceContext,
    },
    DependencyDeclared {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        dependency_kind: String,
        dependency_value: String,
    },
    SchedulerEvent {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: Option<u64>,
        action: String,
        queue_name: Option<String>,
        queue_depth: Option<usize>,
        command: Option<String>,
        order_domain: Option<String>,
        reason: Option<String>,
        force_flush: Option<bool>,
    },
    TransportSelected {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        route: String,
        stream_id: Option<u64>,
        fin: Option<bool>,
        eligible: bool,
        reason: String,
        writable_len: Option<usize>,
    },
    StreamQueued {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        stream_id: u64,
        queued_before: usize,
        queued_after: usize,
        inflight_before: usize,
        inflight_after: usize,
        message_id: Option<u128>,
    },
    StreamWrite {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        stream_id: u64,
        written: usize,
        total: usize,
        offset: usize,
        fin: bool,
        queued_before: usize,
        queued_after: usize,
        remaining: usize,
        backpressure_reason: Option<String>,
    },
    StreamBackpressure {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        stream_id: u64,
        remaining: usize,
        available_capacity: Option<usize>,
        blocked_bytes: usize,
        reason: BackpressureReason,
    },
    DatagramAttempt {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        payload_bytes: usize,
        writable_len: Option<usize>,
    },
    DatagramResult {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: u64,
        status: String,
        detail: String,
        terminal_outcome: Option<String>,
    },
    QuicEgress {
        timestamp_ms: f64,
        client_id: ClientId,
        bytes: usize,
        destination: String,
        pacing_delay_ms: f64,
        approximate: bool,
        flow_ids: Vec<u64>,
        message_ids: Vec<u128>,
        sources: Vec<String>,
    },
    DeliveryEvent {
        timestamp_ms: f64,
        client_id: ClientId,
        flow_id: Option<u64>,
        message_id: Option<u128>,
        outcome: String,
        terminal: bool,
        retry_count: u32,
        retry_reason: Option<RetryReason>,
        detail: Option<String>,
    },
    RxEvent {
        timestamp_ms: f64,
        client_id: ClientId,
        direction: TraceDirection,
        transport: String,
        bytes: usize,
        packet_type: String,
        detail: Option<String>,
    },
}

impl TraceEvent {
    fn client_id(&self) -> ClientId {
        match self {
            Self::SessionStart { client_id, .. }
            | Self::SessionSnapshot { client_id, .. }
            | Self::FlowRegistered { client_id, .. }
            | Self::DependencyDeclared { client_id, .. }
            | Self::SchedulerEvent { client_id, .. }
            | Self::TransportSelected { client_id, .. }
            | Self::StreamQueued { client_id, .. }
            | Self::StreamWrite { client_id, .. }
            | Self::StreamBackpressure { client_id, .. }
            | Self::DatagramAttempt { client_id, .. }
            | Self::DatagramResult { client_id, .. }
            | Self::QuicEgress { client_id, .. }
            | Self::DeliveryEvent { client_id, .. }
            | Self::RxEvent { client_id, .. } => *client_id,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::DeliveryEvent { terminal: true, .. })
    }

    fn timestamp_ms(&self) -> f64 {
        match self {
            Self::SessionStart { timestamp_ms, .. }
            | Self::SessionSnapshot { timestamp_ms, .. }
            | Self::FlowRegistered { timestamp_ms, .. }
            | Self::DependencyDeclared { timestamp_ms, .. }
            | Self::SchedulerEvent { timestamp_ms, .. }
            | Self::TransportSelected { timestamp_ms, .. }
            | Self::StreamQueued { timestamp_ms, .. }
            | Self::StreamWrite { timestamp_ms, .. }
            | Self::StreamBackpressure { timestamp_ms, .. }
            | Self::DatagramAttempt { timestamp_ms, .. }
            | Self::DatagramResult { timestamp_ms, .. }
            | Self::QuicEgress { timestamp_ms, .. }
            | Self::DeliveryEvent { timestamp_ms, .. }
            | Self::RxEvent { timestamp_ms, .. } => *timestamp_ms,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingEgress {
    flow_id: u64,
    message_id: Option<u128>,
    source: String,
    approximate: bool,
}

#[derive(Debug, Clone)]
struct ActiveFlow {
    context: TraceContext,
    await_client_receipt: bool,
    retry_count: u32,
    retry_reason: Option<RetryReason>,
}

pub struct NetworkTracer {
    next_flow_id: AtomicU64,
    sinks: TraceSinkManager,
}

impl NetworkTracer {
    pub fn from_env() -> Arc<Self> {
        let config = NetworkTraceConfig::from_env();
        Arc::new(Self { next_flow_id: AtomicU64::new(1), sinks: TraceSinkManager::start(config) })
    }

    fn enabled(&self) -> bool {
        self.sinks.is_enabled()
    }

    fn next_flow_id(&self) -> u64 {
        self.next_flow_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct TraceEmitter {
    client_id: ClientId,
    session_start: Instant,
    sinks: TraceSinkManager,
}

impl TraceEmitter {
    fn new(client_id: ClientId, tracer: &Arc<NetworkTracer>, session_start: Instant) -> Self {
        Self { client_id, session_start, sinks: tracer.sinks.clone() }
    }

    fn now_ms(&self) -> f64 {
        self.session_start.elapsed().as_secs_f64() * 1000.0
    }

    fn emit(&self, event: TraceEvent) {
        self.sinks.send(event);
    }

    fn emit_session_start(&self) {
        self.emit(TraceEvent::SessionStart {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
        });
    }

    fn emit_flow_registered(&self, context: &TraceContext) {
        self.emit(TraceEvent::FlowRegistered {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: context.flow_id,
            context: context.clone(),
        });

        if let Some(dependency_label) = &context.dependency_label {
            self.emit(TraceEvent::DependencyDeclared {
                timestamp_ms: self.now_ms(),
                client_id: self.client_id,
                flow_id: context.flow_id,
                dependency_kind: "message".to_string(),
                dependency_value: dependency_label.clone(),
            });
        }

        if let Some(sequence_id) = &context.sequence_id {
            self.emit(TraceEvent::DependencyDeclared {
                timestamp_ms: self.now_ms(),
                client_id: self.client_id,
                flow_id: context.flow_id,
                dependency_kind: "sequence".to_string(),
                dependency_value: sequence_id.clone(),
            });
        }
    }

    fn emit_scheduler_event(
        &self,
        flow_id: Option<u64>,
        action: impl Into<String>,
        queue_name: Option<String>,
        queue_depth: Option<usize>,
        command: Option<String>,
        order_domain: Option<String>,
        reason: Option<String>,
        force_flush: Option<bool>,
    ) {
        self.emit(TraceEvent::SchedulerEvent {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id,
            action: action.into(),
            queue_name,
            queue_depth,
            command,
            order_domain,
            reason,
            force_flush,
        });
    }

    fn emit_transport_selected(
        &self,
        trace: &TraceContext,
        route: &str,
        stream_id: Option<u64>,
        fin: Option<bool>,
        eligible: bool,
        reason: String,
        writable_len: Option<usize>,
    ) {
        self.emit(TraceEvent::TransportSelected {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            route: route.to_string(),
            stream_id,
            fin,
            eligible,
            reason,
            writable_len,
        });
    }

    fn emit_stream_queued(
        &self,
        trace: &TraceContext,
        stream_id: u64,
        queued_before: usize,
        queued_after: usize,
        inflight_before: usize,
        inflight_after: usize,
        message_id: Option<MessageId>,
    ) {
        self.emit(TraceEvent::StreamQueued {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            stream_id,
            queued_before,
            queued_after,
            inflight_before,
            inflight_after,
            message_id: message_id.map(|id| id as u128),
        });
    }

    fn emit_stream_write(
        &self,
        trace: &TraceContext,
        stream_id: u64,
        written: usize,
        total: usize,
        offset: usize,
        fin: bool,
        queued_before: usize,
        queued_after: usize,
        backpressure_reason: Option<String>,
    ) {
        self.emit(TraceEvent::StreamWrite {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            stream_id,
            written,
            total,
            offset,
            fin,
            queued_before,
            queued_after,
            remaining: total.saturating_sub(offset),
            backpressure_reason,
        });
    }

    fn emit_stream_backpressure(
        &self,
        trace: &TraceContext,
        stream_id: u64,
        remaining: usize,
        available_capacity: Option<usize>,
        blocked_bytes: usize,
        reason: BackpressureReason,
    ) {
        self.emit(TraceEvent::StreamBackpressure {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            stream_id,
            remaining,
            available_capacity,
            blocked_bytes,
            reason,
        });
    }

    fn emit_datagram_attempt(&self, trace: &TraceContext, writable_len: Option<usize>) {
        self.emit(TraceEvent::DatagramAttempt {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            payload_bytes: trace.payload_bytes,
            writable_len,
        });
    }

    fn emit_datagram_result(
        &self,
        trace: &TraceContext,
        status: &str,
        detail: String,
        terminal_outcome: Option<String>,
    ) {
        self.emit(TraceEvent::DatagramResult {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id: trace.flow_id,
            status: status.to_string(),
            detail,
            terminal_outcome,
        });
    }

    fn emit_quic_egress(
        &self,
        bytes: usize,
        destination: SocketAddr,
        pacing_delay_ms: f64,
        approximate: bool,
        flow_ids: Vec<u64>,
        message_ids: Vec<u128>,
        sources: Vec<String>,
    ) {
        self.emit(TraceEvent::QuicEgress {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            bytes,
            destination: destination.to_string(),
            pacing_delay_ms,
            approximate,
            flow_ids,
            message_ids,
            sources,
        });
    }

    fn emit_delivery(
        &self,
        flow_id: Option<u64>,
        message_id: Option<MessageId>,
        outcome: String,
        terminal: bool,
        retry_count: u32,
        retry_reason: Option<RetryReason>,
        detail: Option<String>,
    ) {
        self.emit(TraceEvent::DeliveryEvent {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id,
            message_id: message_id.map(|id| id as u128),
            outcome,
            terminal,
            retry_count,
            retry_reason,
            detail,
        });
    }

    fn emit_rx(
        &self,
        direction: TraceDirection,
        transport: &str,
        bytes: usize,
        packet_type: String,
        detail: Option<String>,
    ) {
        self.emit(TraceEvent::RxEvent {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            direction,
            transport: transport.to_string(),
            bytes,
            packet_type,
            detail,
        });
    }
}

pub struct TraceProjector {
    active_flows: HashMap<u64, ActiveFlow>,
    flow_by_message_id: HashMap<MessageId, u64>,
    pending_egress: VecDeque<PendingEgress>,
}

impl TraceProjector {
    fn new() -> Self {
        Self {
            active_flows: HashMap::new(),
            flow_by_message_id: HashMap::new(),
            pending_egress: VecDeque::new(),
        }
    }

    fn insert_flow(&mut self, context: TraceContext, await_client_receipt: bool) {
        if let Some(message_id) = context.message_id {
            self.flow_by_message_id.insert(message_id, context.flow_id);
        }
        self.active_flows.insert(
            context.flow_id,
            ActiveFlow { context, await_client_receipt, retry_count: 0, retry_reason: None },
        );
    }

    fn get_flow(&self, flow_id: u64) -> Option<&ActiveFlow> {
        self.active_flows.get(&flow_id)
    }

    fn find_flow_id_by_message_id(&self, message_id: MessageId) -> Option<u64> {
        self.flow_by_message_id.get(&message_id).copied()
    }

    fn finish_flow(&mut self, flow_id: u64) -> Option<ActiveFlow> {
        let flow = self.active_flows.remove(&flow_id)?;
        if let Some(message_id) = flow.context.message_id {
            self.flow_by_message_id.remove(&message_id);
        }
        Some(flow)
    }

    fn record_retry(&mut self, flow_id: u64, reason: RetryReason) {
        let Some(flow) = self.active_flows.get_mut(&flow_id) else {
            return;
        };
        flow.retry_count = flow.retry_count.saturating_add(1);
        flow.retry_reason = Some(reason);
    }

    fn queue_egress(
        &mut self,
        flow_id: u64,
        message_id: Option<MessageId>,
        source: &str,
        approximate: bool,
    ) {
        self.pending_egress.push_back(PendingEgress {
            flow_id,
            message_id: message_id.map(|id| id as u128),
            source: source.to_string(),
            approximate,
        });
    }

    fn take_pending_egress(&mut self) -> Vec<PendingEgress> {
        self.pending_egress.drain(..).collect()
    }
}

pub struct SessionTracer {
    tracer: Arc<NetworkTracer>,
    client_id: ClientId,
    emitter: TraceEmitter,
    projector: TraceProjector,
}

impl SessionTracer {
    pub fn new(tracer: Arc<NetworkTracer>, client_id: ClientId) -> Self {
        let session_start = Instant::now();
        let emitter = TraceEmitter::new(client_id, &tracer, session_start);
        if tracer.enabled() {
            emitter.emit_session_start();
        }
        Self { tracer, client_id, emitter, projector: TraceProjector::new() }
    }

    pub fn register_envelope(
        &mut self,
        envelope: &PacketEnvelope,
        framed_len: usize,
    ) -> DispatchTraceMeta {
        let components = packet_components_from_envelope(envelope, framed_len);
        let kind =
            determine_flow_kind(envelope.meta.priority, envelope.meta.order, envelope.id.is_some());
        let context = TraceContext {
            flow_id: self.tracer.next_flow_id(),
            client_id: self.client_id,
            kind,
            packet_label: envelope_label(envelope),
            message_id: envelope.id,
            payload_bytes: framed_len,
            target_label: describe_target(envelope.meta.target),
            priority: envelope.meta.priority.describe(),
            order: envelope.meta.order.describe(),
            delivery: envelope.meta.delivery.describe(),
            dependency_label: dependency_message_label(envelope.meta.order),
            sequence_id: sequence_label(envelope.meta.order),
            components,
        };
        self.projector.insert_flow(
            context.clone(),
            envelope.meta.delivery == DeliveryPolicy::RequireClientReceipt,
        );
        if self.tracer.enabled() {
            self.emitter.emit_flow_registered(&context);
        }
        context
    }

    pub fn register_resource(
        &mut self,
        resource: &PacketResource,
        framed_len: usize,
    ) -> DispatchTraceMeta {
        let context = TraceContext {
            flow_id: self.tracer.next_flow_id(),
            client_id: self.client_id,
            kind: FlowKind::Resource,
            packet_label: format!("resource/{}", resource.resource_type),
            message_id: Some(resource.id),
            payload_bytes: framed_len,
            target_label: describe_target(resource.meta.target),
            priority: resource.meta.priority.describe(),
            order: resource.meta.order.describe(),
            delivery: resource.meta.delivery.describe(),
            dependency_label: dependency_message_label(resource.meta.order),
            sequence_id: sequence_label(resource.meta.order),
            components: vec![PacketComponent {
                index: 0,
                component_type: format!("resource/{}", resource.resource_type),
                message_id: Some(resource.id as u128),
                approx_payload_bytes: framed_len,
            }],
        };
        self.projector.insert_flow(
            context.clone(),
            resource.meta.delivery == DeliveryPolicy::RequireClientReceipt,
        );
        if self.tracer.enabled() {
            self.emitter.emit_flow_registered(&context);
        }
        context
    }

    pub fn on_scheduler_events(&mut self, events: Vec<SchedulerTraceEvent>) {
        if !self.tracer.enabled() {
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
                    self.emitter.emit_scheduler_event(
                        Some(trace.flow_id),
                        "deferred_initial",
                        Some(queue_name.to_string()),
                        Some(queued_messages),
                        None,
                        Some(trace.order.clone()),
                        Some(policy.to_string()),
                        Some(false),
                    );
                },
                SchedulerTraceEvent::RequeuedCongestion { trace, queued_messages, reason } => {
                    self.emitter.emit_scheduler_event(
                        Some(trace.flow_id),
                        "requeued_congestion",
                        Some(queue_name_for_trace(&trace).to_string()),
                        Some(queued_messages),
                        None,
                        Some(trace.order.clone()),
                        Some(reason.describe()),
                        None,
                    );
                },
                SchedulerTraceEvent::DispatchReady { trace, force_flush, queue_name } => {
                    self.emitter.emit_scheduler_event(
                        Some(trace.flow_id),
                        "dispatch_ready",
                        Some(queue_name.to_string()),
                        None,
                        None,
                        Some(trace.order.clone()),
                        None,
                        Some(force_flush),
                    );
                },
                SchedulerTraceEvent::Dropped { trace, reason, queue_name } => {
                    self.emitter.emit_scheduler_event(
                        Some(trace.flow_id),
                        "dropped",
                        Some(queue_name.to_string()),
                        None,
                        None,
                        Some(trace.order.clone()),
                        Some(format!("{reason:?}")),
                        None,
                    );
                },
                SchedulerTraceEvent::BlockedByBarrier { flow_id, command } => {
                    self.emitter.emit_scheduler_event(
                        flow_id,
                        "blocked_by_barrier",
                        None,
                        None,
                        Some(command.to_string()),
                        None,
                        None,
                        None,
                    );
                },
                SchedulerTraceEvent::BlockedByDeferred { flow_id, command, order_domain } => {
                    self.emitter.emit_scheduler_event(
                        flow_id,
                        "blocked_by_deferred",
                        None,
                        None,
                        Some(command.to_string()),
                        Some(order_domain),
                        None,
                        None,
                    );
                },
                SchedulerTraceEvent::BarrierBegin => {
                    self.emitter.emit_scheduler_event(
                        None,
                        "barrier_begin",
                        None,
                        None,
                        Some("barrier".to_string()),
                        None,
                        None,
                        None,
                    );
                },
                SchedulerTraceEvent::BarrierReleased => {
                    self.emitter.emit_scheduler_event(
                        None,
                        "barrier_released",
                        None,
                        None,
                        Some("barrier".to_string()),
                        None,
                        None,
                        None,
                    );
                },
                SchedulerTraceEvent::ClearedTransportState => {
                    self.emitter.emit_scheduler_event(
                        None,
                        "cleared_transport_state",
                        None,
                        None,
                        Some("clear".to_string()),
                        None,
                        None,
                        None,
                    );
                },
            }
        }
    }

    pub fn on_datagram_attempt(&mut self, trace: &DispatchTraceMeta, writable_len: Option<usize>) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_transport_selected(
            trace,
            "datagram",
            None,
            None,
            true,
            "datagram_candidate".to_string(),
            writable_len,
        );
        self.emitter.emit_datagram_attempt(trace, writable_len);
    }

    pub fn on_datagram_result(
        &mut self,
        trace: &DispatchTraceMeta,
        outcome: &str,
        extra: impl Into<String>,
        terminal: Option<DeliveryOutcome>,
    ) {
        let detail = extra.into();
        if self.tracer.enabled() {
            self.emitter.emit_datagram_result(
                trace,
                outcome,
                detail.clone(),
                terminal.map(|o| o.describe()),
            );
        }

        if outcome == "sent" {
            self.projector.queue_egress(trace.flow_id, trace.message_id, "datagram", false);
        }

        if let Some(outcome) = terminal {
            self.finish_flow(trace.flow_id, outcome, Some(detail));
        }
    }

    pub fn on_flow_retry(&mut self, flow_id: u64, reason: RetryReason) {
        self.projector.record_retry(flow_id, reason);
    }

    pub fn on_stream_transport_selected(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        fin: bool,
        reason: &str,
    ) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_transport_selected(
            trace,
            "stream",
            Some(stream_id),
            Some(fin),
            false,
            reason.to_string(),
            None,
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
        if !self.tracer.enabled() {
            return;
        }
        let stream_id = stream_id_hint_from_order(&trace.order).unwrap_or(0);
        self.emitter.emit_stream_queued(
            trace,
            stream_id,
            queued_before,
            queued_after,
            inflight_before,
            inflight_after,
            message_id,
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
        if self.tracer.enabled() {
            self.emitter.emit_stream_write(
                trace,
                stream_id,
                written,
                total,
                written,
                fin,
                queued_before,
                queued_after,
                None,
            );
        }
        self.projector.queue_egress(trace.flow_id, trace.message_id, "stream_write", true);
    }

    pub fn on_stream_backpressure(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        reason: &str,
        remaining: usize,
    ) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_stream_write(
            trace,
            stream_id,
            0,
            trace.payload_bytes,
            trace.payload_bytes.saturating_sub(remaining),
            false,
            0,
            0,
            Some(reason.to_string()),
        );
    }

    pub fn on_flow_control_backpressure(
        &mut self,
        trace: &DispatchTraceMeta,
        stream_id: u64,
        remaining: usize,
        available_capacity: Option<usize>,
    ) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_stream_backpressure(
            trace,
            stream_id,
            remaining,
            available_capacity,
            remaining.saturating_sub(available_capacity.unwrap_or(0)),
            BackpressureReason::FlowControlWindow,
        );
    }

    pub fn on_quic_egress(&mut self, bytes: usize, destination: SocketAddr, pacing_delay_ms: f64) {
        if !self.tracer.enabled() {
            return;
        }
        let pending = self.projector.take_pending_egress();
        let approximate = pending.is_empty()
            || pending.iter().any(|entry| entry.approximate)
            || pending.len() > 1;
        let flow_ids = pending.iter().map(|entry| entry.flow_id).collect();
        let message_ids = pending.iter().filter_map(|entry| entry.message_id).collect();
        let sources = pending.iter().map(|entry| entry.source.clone()).collect();
        self.emitter.emit_quic_egress(
            bytes,
            destination,
            pacing_delay_ms,
            approximate,
            flow_ids,
            message_ids,
            sources,
        );
    }

    pub fn on_transport_outcome(&mut self, message_id: MessageId, outcome: DeliveryOutcome) {
        let flow_id = self.projector.find_flow_id_by_message_id(message_id);
        let outcome_label = outcome.describe();
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                flow_id,
                Some(message_id),
                outcome_label.clone(),
                matches!(
                    outcome,
                    DeliveryOutcome::TransportDropped { .. } | DeliveryOutcome::ClientProcessed
                ),
                0,
                None,
                None,
            );
        }

        match outcome {
            DeliveryOutcome::ClientProcessed => {
                if let Some(flow_id) = flow_id {
                    self.projector.finish_flow(flow_id);
                }
            },
            DeliveryOutcome::TransportDelivered => {
                let await_receipt = flow_id
                    .and_then(|id| self.projector.get_flow(id))
                    .map(|flow| flow.await_client_receipt)
                    .unwrap_or(false);
                if !await_receipt {
                    if let Some(flow_id) = flow_id {
                        self.projector.finish_flow(flow_id);
                    }
                }
            },
            DeliveryOutcome::TransportDropped { .. } => {
                if let Some(flow_id) = flow_id {
                    self.projector.finish_flow(flow_id);
                }
            },
        }
    }

    pub fn on_flow_outcome(&mut self, flow_id: u64, outcome: DeliveryOutcome) {
        self.finish_flow(flow_id, outcome, None);
    }

    pub fn on_flow_aborted(&mut self, flow_id: u64, reason: &str) {
        let Some(flow) = self.projector.finish_flow(flow_id) else {
            return;
        };
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                Some(flow_id),
                flow.context.message_id,
                "Aborted".to_string(),
                true,
                flow.retry_count,
                flow.retry_reason,
                Some(reason.to_string()),
            );
        }
    }

    pub fn on_control(&self, control: PacketControl) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_scheduler_event(
            None,
            "control",
            None,
            None,
            Some(format!("{control:?}")),
            None,
            None,
            None,
        );
    }

    pub fn on_keepalive_ping(&self, bytes: usize, nonce: u64) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_rx(
            TraceDirection::Tx,
            "datagram",
            bytes,
            "Ping".to_string(),
            Some(format!("nonce={nonce}")),
        );
    }

    pub fn on_rx_packet(
        &self,
        transport: &str,
        bytes: usize,
        packet: &C2SPacket,
        rtt_ms: Option<f64>,
    ) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_rx(
            TraceDirection::Rx,
            transport,
            bytes,
            variant_name(packet),
            rtt_ms.map(|value| format!("rtt_ms={value:.2}")),
        );
    }

    pub fn on_rx_decode_failed(&self, transport: &str, bytes: usize) {
        if !self.tracer.enabled() {
            return;
        }
        self.emitter.emit_rx(
            TraceDirection::Rx,
            transport,
            bytes,
            "decode_failed".to_string(),
            None,
        );
    }

    pub fn maybe_log_snapshot(&mut self, snapshot: SessionSnapshot) {
        if !self.tracer.enabled() {
            return;
        }
        let timestamp_ms = self.emitter.now_ms();
        let client_id = self.emitter.client_id;
        self.emitter.emit(TraceEvent::SessionSnapshot {
            timestamp_ms,
            client_id,
            established: snapshot.established,
            rtt_ms: snapshot.rtt_ms,
            queued_stream_bytes: snapshot.queued_stream_bytes,
            inflight_messages: snapshot.inflight_messages,
            active_streams: snapshot.active_streams,
            active_sequences: snapshot.active_sequences,
            pending_pings: snapshot.pending_pings,
            send_quantum: snapshot.send_quantum,
        });
    }

    fn finish_flow(&mut self, flow_id: u64, outcome: DeliveryOutcome, detail: Option<String>) {
        let Some(flow) = self.projector.finish_flow(flow_id) else {
            return;
        };
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                Some(flow_id),
                flow.context.message_id,
                outcome.describe(),
                true,
                flow.retry_count,
                flow.retry_reason,
                detail,
            );
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

fn flow_kind_label(kind: FlowKind) -> &'static str {
    match kind {
        FlowKind::Envelope => "envelope",
        FlowKind::Resource => "resource",
        FlowKind::Datagram => "datagram",
    }
}

fn fmt_ms(value: f64) -> String {
    format!("{value:.3}")
}

fn packet_components_from_envelope(
    envelope: &PacketEnvelope,
    framed_len: usize,
) -> Vec<PacketComponent> {
    match &envelope.payload {
        crate::packets::PacketPayload::Single(packet) => vec![PacketComponent {
            index: 0,
            component_type: variant_name(packet),
            message_id: envelope.id.map(|id| id as u128),
            approx_payload_bytes: framed_len,
        }],
        crate::packets::PacketPayload::Bundle(bundle) => {
            let per_component = framed_len / bundle.len().max(1);
            bundle
                .iter()
                .enumerate()
                .map(|(index, packet)| PacketComponent {
                    index,
                    component_type: variant_name(packet),
                    message_id: None,
                    approx_payload_bytes: per_component,
                })
                .collect()
        },
    }
}

fn determine_flow_kind(priority: PacketPriority, order: PacketOrder, has_id: bool) -> FlowKind {
    match (priority, order) {
        (PacketPriority::Droppable, PacketOrder::Independent) if !has_id => FlowKind::Datagram,
        _ => FlowKind::Envelope,
    }
}

fn describe_target(target: PacketTarget) -> String {
    match target {
        PacketTarget::Client(client_id) => format!("Client({client_id})"),
        PacketTarget::Broadcast => "Broadcast".to_string(),
        PacketTarget::BroadcastExcept(client_id) => format!("BroadcastExcept({client_id})"),
    }
}

fn dependency_message_label(order: PacketOrder) -> Option<String> {
    match order {
        PacketOrder::Dependency(message_id) => Some(message_id.to_string()),
        _ => None,
    }
}

fn sequence_label(order: PacketOrder) -> Option<String> {
    match order {
        PacketOrder::Sequence(sequence_id) | PacketOrder::SequenceEnd(sequence_id) => {
            Some(sequence_id.to_string())
        },
        _ => None,
    }
}

fn queue_name_for_trace(trace: &DispatchTraceMeta) -> &'static str {
    if trace.sequence_id.is_some() { "sequence" } else { "independent" }
}

fn stream_id_hint_from_order(_order: &str) -> Option<u64> {
    None
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

trait Describe {
    fn describe(&self) -> String;
}

impl Describe for PacketPriority {
    fn describe(&self) -> String {
        match self {
            Self::Normal => "Normal".to_string(),
            Self::Droppable => "Droppable".to_string(),
            Self::MaxDelay { max_delay } => {
                format!("MaxDelay({:.0}ms)", max_delay.as_secs_f64() * 1000.0)
            },
            Self::Coalescing { target_payload_bytes } => {
                format!("Coalescing({target_payload_bytes}B)")
            },
        }
    }
}

impl Describe for PacketOrder {
    fn describe(&self) -> String {
        match self {
            Self::Independent => "Independent".to_string(),
            Self::Dependency(message_id) => format!("Dependency({message_id})"),
            Self::Sequence(sequence_id) => format!("Sequence({sequence_id})"),
            Self::SequenceEnd(sequence_id) => format!("SequenceEnd({sequence_id})"),
        }
    }
}

impl Describe for DeliveryPolicy {
    fn describe(&self) -> String {
        match self {
            Self::FireAndForget => "FireAndForget".to_string(),
            Self::ObserveTransport => "ObserveTransport".to_string(),
            Self::RequireClientReceipt => "RequireClientReceipt".to_string(),
        }
    }
}

impl Describe for DeliveryOutcome {
    fn describe(&self) -> String {
        match self {
            Self::TransportDelivered => "TransportDelivered".to_string(),
            Self::TransportDropped { reason } => format!("TransportDropped({reason:?})"),
            Self::ClientProcessed => "ClientProcessed".to_string(),
        }
    }
}

impl Describe for RetryReason {
    fn describe(&self) -> String {
        match self {
            Self::Congestion => "congestion".to_string(),
            Self::Timeout => "timeout".to_string(),
            Self::Nack => "nack".to_string(),
            Self::Other => "other".to_string(),
        }
    }
}

impl Describe for BackpressureReason {
    fn describe(&self) -> String {
        match self {
            Self::FlowControlWindow => "flow_control_window".to_string(),
            Self::Other => "other".to_string(),
        }
    }
}
