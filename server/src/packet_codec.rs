use crate::packets::{decode_c2s, encode_s2c, C2SPacket, PacketPayload, S2CPacket};

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

pub fn serialize_envelope_frames(payload: &PacketPayload) -> Option<Vec<u8>> {
    let mut framed = Vec::new();

    match payload {
        PacketPayload::Single(packet) => {
            append_packet_frame(&mut framed, packet)?;
        },
        PacketPayload::Bundle(bundle) => {
            if bundle.is_empty() {
                return None;
            }

            for packet in bundle {
                let _ = append_packet_frame(&mut framed, packet);
            }
        },
    }

    (!framed.is_empty()).then_some(framed)
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
