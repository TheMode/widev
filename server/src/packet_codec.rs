use crate::packets::{
    decode_c2s, encode_s2c, C2SPacket, DeliveryPolicy, MessageId, PacketEnvelope, PacketMessage,
    PacketOrder, PacketPayload, PacketResource, S2CPacket,
};

pub fn serialize_s2c_packet(packet: &S2CPacket) -> Option<Vec<u8>> {
    encode_s2c(packet).ok()
}

pub fn serialize_s2c_packet_message(packet: &S2CPacket) -> Option<Vec<u8>> {
    let envelope = PacketEnvelope::single(crate::packets::PacketTarget::Broadcast, packet.clone());
    serialize_packet_message(&PacketMessage::Envelope(envelope))
}

pub fn serialize_packet_message(message: &PacketMessage) -> Option<Vec<u8>> {
    match message {
        PacketMessage::Envelope(envelope) => {
            let payload = serialize_envelope_payload(envelope)?;
            Some(frame_top_level_payload(FrameKind::Envelope, &payload))
        },
        PacketMessage::Resource(resource) => {
            let payload = serialize_resource_payload(resource)?;
            Some(frame_top_level_payload(FrameKind::Resource, &payload))
        },
        PacketMessage::Control(_) => None,
    }
}

fn serialize_envelope_payload(envelope: &PacketEnvelope) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    append_envelope_header(&mut payload, envelope);

    match &envelope.payload {
        PacketPayload::Single(packet) => {
            append_packet_frame(&mut payload, packet)?;
        },
        PacketPayload::Bundle(bundle) => {
            if bundle.is_empty() {
                return None;
            }

            for packet in bundle {
                let _ = append_packet_frame(&mut payload, packet);
            }
        },
    }

    if payload.is_empty() {
        return None;
    }

    Some(payload)
}

pub fn decode_c2s_packet(bytes: &[u8]) -> Option<C2SPacket> {
    decode_c2s(bytes).ok()
}

fn serialize_resource_payload(resource: &PacketResource) -> Option<Vec<u8>> {
    const FLAG_CLIENT_PROCESSED_RECEIPT: u8 = 1 << 0;
    const FLAG_HAS_DEPENDENCY: u8 = 1 << 1;

    let mut payload = Vec::new();
    let mut flags = 0u8;
    if resource.meta.delivery == DeliveryPolicy::RequireClientReceipt {
        flags |= FLAG_CLIENT_PROCESSED_RECEIPT;
    }
    if matches!(resource.meta.order, PacketOrder::Dependency(_)) {
        flags |= FLAG_HAS_DEPENDENCY;
    }

    payload.push(flags);
    payload.extend_from_slice(&resource.id.to_be_bytes());
    if let PacketOrder::Dependency(dependency_id) = resource.meta.order {
        payload.extend_from_slice(&dependency_id.to_be_bytes());
    }

    let resource_type = resource.resource_type.as_bytes();
    let resource_type_len = u16::try_from(resource_type.len()).ok()?;
    payload.extend_from_slice(&resource_type_len.to_be_bytes());
    payload.extend_from_slice(resource_type);
    let encoded_usage = resource.usage_count.unwrap_or(-1);
    payload.extend_from_slice(&encoded_usage.to_be_bytes());
    payload.extend_from_slice(&(resource.blob.len() as u32).to_be_bytes());
    payload.extend_from_slice(&resource.blob);
    Some(payload)
}

fn append_packet_frame(framed: &mut Vec<u8>, packet: &S2CPacket) -> Option<()> {
    let encoded = serialize_s2c_packet(packet)?;
    framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    framed.extend_from_slice(&encoded);
    Some(())
}

fn append_envelope_header(out: &mut Vec<u8>, envelope: &PacketEnvelope) {
    const ENVELOPE_VERSION: u8 = 1;
    const FLAG_HAS_ID: u8 = 1 << 0;
    const FLAG_CLIENT_PROCESSED_RECEIPT: u8 = 1 << 1;
    const FLAG_HAS_DEPENDENCY: u8 = 1 << 2;

    out.push(ENVELOPE_VERSION);

    let mut flags = 0u8;
    if envelope.id.is_some() {
        flags |= FLAG_HAS_ID;
    }
    if envelope.meta.delivery == DeliveryPolicy::RequireClientReceipt {
        flags |= FLAG_CLIENT_PROCESSED_RECEIPT;
    }
    if matches!(envelope.meta.order, crate::packets::PacketOrder::Dependency(_)) {
        flags |= FLAG_HAS_DEPENDENCY;
    }
    out.push(flags);

    if let Some(id) = envelope.id {
        out.extend_from_slice(&envelope_id_to_bytes(id));
    }
    if let crate::packets::PacketOrder::Dependency(dependency_id) = envelope.meta.order {
        out.extend_from_slice(&envelope_id_to_bytes(dependency_id));
    }
}

fn envelope_id_to_bytes(id: MessageId) -> [u8; 16] {
    id.to_be_bytes()
}

fn frame_top_level_payload(kind: FrameKind, payload: &[u8]) -> Vec<u8> {
    const FRAME_VERSION: u8 = 1;

    let total_len = 2usize.saturating_add(payload.len());
    let mut framed = Vec::with_capacity(4 + total_len);
    framed.extend_from_slice(&(total_len as u32).to_be_bytes());
    framed.push(FRAME_VERSION);
    framed.push(kind as u8);
    framed.extend_from_slice(payload);
    framed
}

#[repr(u8)]
enum FrameKind {
    Envelope = 1,
    Resource = 2,
}
