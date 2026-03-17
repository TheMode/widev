use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::game::ClientId;
use crate::packets::{
    C2SPacket, DeliveryOutcome, DeliveryPolicy, DropReason, MessageId, PacketControl,
    PacketEnvelope, PacketOrder, PacketPriority, PacketResource, PacketTarget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    EveryEvent,
    OnFlowComplete,
    Batched { interval_ms: u64 },
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
    pub delivery: &'static str,
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
            | Self::FlowRegistered { client_id, .. }
            | Self::DependencyDeclared { client_id, .. }
            | Self::SchedulerEvent { client_id, .. }
            | Self::TransportSelected { client_id, .. }
            | Self::StreamQueued { client_id, .. }
            | Self::StreamWrite { client_id, .. }
            | Self::DatagramAttempt { client_id, .. }
            | Self::DatagramResult { client_id, .. }
            | Self::QuicEgress { client_id, .. }
            | Self::DeliveryEvent { client_id, .. }
            | Self::RxEvent { client_id, .. } => *client_id,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::DeliveryEvent {
                terminal: true,
                ..
            }
        )
    }

    fn timestamp_ms(&self) -> f64 {
        match self {
            Self::SessionStart { timestamp_ms, .. }
            | Self::FlowRegistered { timestamp_ms, .. }
            | Self::DependencyDeclared { timestamp_ms, .. }
            | Self::SchedulerEvent { timestamp_ms, .. }
            | Self::TransportSelected { timestamp_ms, .. }
            | Self::StreamQueued { timestamp_ms, .. }
            | Self::StreamWrite { timestamp_ms, .. }
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
}

pub struct NetworkTracer {
    next_flow_id: AtomicU64,
    sinks: TraceSinkManager,
}

impl NetworkTracer {
    pub fn from_env() -> Arc<Self> {
        let config = NetworkTraceConfig::from_env();
        Arc::new(Self {
            next_flow_id: AtomicU64::new(1),
            sinks: TraceSinkManager::start(config),
        })
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
        self.emit(TraceEvent::SessionStart { timestamp_ms: self.now_ms(), client_id: self.client_id });
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
        detail: Option<String>,
    ) {
        self.emit(TraceEvent::DeliveryEvent {
            timestamp_ms: self.now_ms(),
            client_id: self.client_id,
            flow_id,
            message_id: message_id.map(|id| id as u128),
            outcome,
            terminal,
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
        self.active_flows.insert(context.flow_id, ActiveFlow { context, await_client_receipt });
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

    fn queue_egress(&mut self, flow_id: u64, message_id: Option<MessageId>, source: &str, approximate: bool) {
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
        let kind = determine_flow_kind(envelope.meta.priority, envelope.meta.order, envelope.id.is_some());
        let context = TraceContext {
            flow_id: self.tracer.next_flow_id(),
            client_id: self.client_id,
            kind,
            packet_label: envelope_label(envelope),
            message_id: envelope.id,
            payload_bytes: framed_len,
            target_label: describe_target(envelope.meta.target),
            priority: envelope.meta.priority.describe_long(),
            order: envelope.meta.order.describe_long(),
            delivery: envelope.meta.delivery.describe_short(),
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
            priority: resource.meta.priority.describe_long(),
            order: resource.meta.order.describe_long(),
            delivery: resource.meta.delivery.describe_short(),
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
                SchedulerTraceEvent::DeferredInitial { trace, policy, queue_name, queued_messages } => {
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
                SchedulerTraceEvent::RequeuedCongestion { trace, queued_messages } => {
                    self.emitter.emit_scheduler_event(
                        Some(trace.flow_id),
                        "requeued_congestion",
                        Some(queue_name_for_trace(&trace).to_string()),
                        Some(queued_messages),
                        None,
                        Some(trace.order.clone()),
                        Some("congestion".to_string()),
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
                terminal.map(describe_delivery_outcome),
            );
        }

        if outcome == "sent" {
            self.projector.queue_egress(trace.flow_id, trace.message_id, "datagram", false);
        }

        if let Some(outcome) = terminal {
            self.finish_flow(trace.flow_id, outcome, Some(detail));
        }
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

    pub fn on_quic_egress(&mut self, bytes: usize, destination: SocketAddr, pacing_delay_ms: f64) {
        if !self.tracer.enabled() {
            return;
        }
        let pending = self.projector.take_pending_egress();
        let approximate = pending.is_empty() || pending.iter().any(|entry| entry.approximate) || pending.len() > 1;
        let flow_ids = pending.iter().map(|entry| entry.flow_id).collect();
        let message_ids = pending.iter().filter_map(|entry| entry.message_id).collect();
        let sources = pending.iter().map(|entry| entry.source.clone()).collect();
        self.emitter
            .emit_quic_egress(bytes, destination, pacing_delay_ms, approximate, flow_ids, message_ids, sources);
    }

    pub fn on_transport_outcome(&mut self, message_id: MessageId, outcome: DeliveryOutcome) {
        let flow_id = self.projector.find_flow_id_by_message_id(message_id);
        let outcome_label = describe_delivery_outcome(outcome);
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                flow_id,
                Some(message_id),
                outcome_label.clone(),
                matches!(outcome, DeliveryOutcome::TransportDropped { .. } | DeliveryOutcome::ClientProcessed),
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
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                Some(flow_id),
                None,
                "Aborted".to_string(),
                true,
                Some(reason.to_string()),
            );
        }
        self.projector.finish_flow(flow_id);
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
        self.emitter.emit_scheduler_event(
            None,
            "snapshot",
            None,
            Some(snapshot.inflight_messages),
            Some(format!(
                "established={} active_streams={} active_sequences={} pending_pings={} send_quantum={} queued_stream_bytes={} rtt_ms={}",
                snapshot.established,
                snapshot.active_streams,
                snapshot.active_sequences,
                snapshot.pending_pings,
                snapshot.send_quantum,
                snapshot.queued_stream_bytes,
                snapshot.rtt_ms.map(|value| format!("{value:.2}")).unwrap_or_else(|| "none".to_string())
            )),
            None,
            None,
            None,
        );
    }

    fn finish_flow(&mut self, flow_id: u64, outcome: DeliveryOutcome, detail: Option<String>) {
        let Some(flow) = self.projector.finish_flow(flow_id) else {
            return;
        };
        if self.tracer.enabled() {
            self.emitter.emit_delivery(
                Some(flow_id),
                flow.context.message_id,
                describe_delivery_outcome(outcome),
                true,
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

#[derive(Clone)]
pub struct TraceSinkManager {
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
    fn start(config: NetworkTraceConfig) -> Self {
        let (sender, receiver) = mpsc::channel();
        if !config.enabled {
            return Self {
                inner: Arc::new(TraceSinkInner { sender, enabled: false }),
            };
        }

        thread::spawn(move || run_sink_thread(receiver, config));
        Self { inner: Arc::new(TraceSinkInner { sender, enabled: true }) }
    }

    fn send(&self, event: TraceEvent) {
        if self.inner.enabled {
            let _ = self.inner.sender.send(TraceCommand::Event(event));
        }
    }

    fn is_enabled(&self) -> bool {
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

        if let TraceEvent::DeliveryEvent {
            flow_id: Some(flow_id),
            terminal: true,
            ..
        } = event
        {
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
        | TraceEvent::DatagramAttempt { flow_id, .. }
        | TraceEvent::DatagramResult { flow_id, .. } => vec![*flow_id],
        TraceEvent::SchedulerEvent {
            flow_id: Some(flow_id),
            ..
        }
        | TraceEvent::DeliveryEvent {
            flow_id: Some(flow_id),
            ..
        } => vec![*flow_id],
        TraceEvent::QuicEgress { flow_ids, .. } => flow_ids.clone(),
        TraceEvent::SessionStart { .. }
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
        delivery: "unknown",
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
            | TraceEvent::DatagramAttempt { .. }
            | TraceEvent::DatagramResult { .. }
            | TraceEvent::DeliveryEvent { .. } => Vec::new(),
        }
        .into_iter()
        .filter(|line| verbose || !line.contains("snapshot"))
        .collect()
    }

    fn render_flow_block(state: &TimelineFlowState, verbose: bool, incomplete: bool) -> Vec<String> {
        let mut lines = Vec::new();
        let context = &state.context;
        let flow_id = context.flow_id;
        let final_outcome = state
            .events
            .iter()
            .rev()
            .find_map(|event| match event {
                TraceEvent::DeliveryEvent { outcome, terminal, detail, .. } if *terminal => {
                    Some((outcome.clone(), detail.clone()))
                },
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
            context
                .message_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
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
                .map(|component| format!("#{}:{}~{}B", component.index, component.component_type, component.approx_payload_bytes))
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

        if let Some((outcome, detail)) = final_outcome {
            lines.push(format!(
                "  outcome={}{}",
                outcome,
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
            TraceEvent::DependencyDeclared {
                dependency_kind,
                dependency_value,
                ..
            } => Some(format!("dependency {}={}", dependency_kind, dependency_value)),
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
                let mut parts = vec![format!("transport route={route}"), format!("eligible={eligible}")];
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
                message_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
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
            TraceEvent::DatagramAttempt {
                payload_bytes,
                writable_len,
                ..
            } => Some(format!(
                "datagram attempt bytes={} writable={}",
                payload_bytes,
                writable_len
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            )),
            TraceEvent::DatagramResult {
                status,
                detail,
                terminal_outcome,
                ..
            } => Some(format!(
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
                detail,
                ..
            } => Some(format!(
                "delivery outcome={} terminal={} msg={}{}",
                outcome,
                terminal,
                message_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                detail
                    .as_ref()
                    .map(|value| format!(" detail={value}"))
                    .unwrap_or_default()
            )),
            TraceEvent::SessionStart { .. } | TraceEvent::RxEvent { .. } => {
                if verbose {
                    None
                } else {
                    None
                }
            },
        }
    }
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

fn packet_components_from_envelope(envelope: &PacketEnvelope, framed_len: usize) -> Vec<PacketComponent> {
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
    if trace.sequence_id.is_some() {
        "sequence"
    } else {
        "independent"
    }
}

fn stream_id_hint_from_order(_order: &str) -> Option<u64> {
    None
}

fn envelope_label(envelope: &PacketEnvelope) -> String {
    match &envelope.payload {
        crate::packets::PacketPayload::Single(packet) => variant_name(packet),
        crate::packets::PacketPayload::Bundle(bundle) => {
            let first = bundle.first().map(variant_name).unwrap_or_else(|| "EmptyBundle".to_string());
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

impl DescribeLong for PacketPriority {
    fn describe_long(&self) -> String {
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

impl DescribeLong for PacketOrder {
    fn describe_long(&self) -> String {
        match self {
            Self::Independent => "Independent".to_string(),
            Self::Dependency(message_id) => format!("Dependency({message_id})"),
            Self::Sequence(sequence_id) => format!("Sequence({sequence_id})"),
            Self::SequenceEnd(sequence_id) => format!("SequenceEnd({sequence_id})"),
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
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
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
}
