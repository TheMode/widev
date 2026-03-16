use crate::packets::{
    decode_c2s, encode_s2c, C2SPacket, DeliveryPolicy, EnvelopeId, PacketEnvelope, PacketPayload,
    S2CPacket,
};

pub enum DecodedC2SPacket {
    Packet(C2SPacket),
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

pub fn serialize_s2c_packet(packet: &S2CPacket) -> Option<Vec<u8>> {
    encode_s2c(packet).ok()
}

pub fn serialize_envelope_frames(envelope: &PacketEnvelope) -> Option<Vec<u8>> {
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

    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(&payload);
    Some(framed)
}

pub fn decode_c2s_packet(bytes: &[u8]) -> Option<DecodedC2SPacket> {
    let packet = decode_c2s(bytes).ok()?;

    Some(match packet {
        C2SPacket::Ping { nonce } => DecodedC2SPacket::Ping { nonce },
        C2SPacket::Pong { nonce } => DecodedC2SPacket::Pong { nonce },
        packet => DecodedC2SPacket::Packet(packet),
    })
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
    if envelope.delivery == DeliveryPolicy::RequireClientReceipt {
        flags |= FLAG_CLIENT_PROCESSED_RECEIPT;
    }
    if matches!(envelope.order, crate::packets::PacketOrder::Dependency(_)) {
        flags |= FLAG_HAS_DEPENDENCY;
    }
    out.push(flags);

    if let Some(id) = envelope.id {
        out.extend_from_slice(&envelope_id_to_bytes(id));
    }
    if let crate::packets::PacketOrder::Dependency(dependency_id) = envelope.order {
        out.extend_from_slice(&envelope_id_to_bytes(dependency_id));
    }
}

fn envelope_id_to_bytes(id: EnvelopeId) -> [u8; 16] {
    id.to_be_bytes()
}
