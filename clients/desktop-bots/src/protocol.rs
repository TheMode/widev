mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

pub use packets::{C2SPacket, S2CPacket};

pub struct DecodedEnvelope {
    pub receipt_id: Option<packets::EnvelopeId>,
    pub packets: Vec<S2CPacket>,
}

pub fn encode_c2s(packet: &C2SPacket) -> wincode::WriteResult<Vec<u8>> {
    packets::encode_c2s(packet)
}

pub fn decode_s2c(bytes: &[u8]) -> wincode::ReadResult<S2CPacket> {
    packets::decode_s2c(bytes)
}

pub fn decode_envelope(bytes: &[u8]) -> Option<DecodedEnvelope> {
    const ENVELOPE_VERSION: u8 = 1;
    const FLAG_HAS_ID: u8 = 1 << 0;
    const FLAG_CLIENT_PROCESSED_RECEIPT: u8 = 1 << 1;

    if bytes.len() < 2 || bytes[0] != ENVELOPE_VERSION {
        return None;
    }

    let flags = bytes[1];
    let mut cursor = 2usize;
    let id = if flags & FLAG_HAS_ID != 0 {
        if bytes.len() < cursor + 16 {
            return None;
        }
        let mut raw = [0u8; 16];
        raw.copy_from_slice(&bytes[cursor..cursor + 16]);
        cursor += 16;
        Some(u128::from_be_bytes(raw))
    } else {
        None
    };

    let receipt_id = if flags & FLAG_CLIENT_PROCESSED_RECEIPT != 0 { Some(id?) } else { None };

    let mut packets_buf = bytes[cursor..].to_vec();
    let mut packets = Vec::new();
    while let Some(frame) = pop_frame(&mut packets_buf) {
        packets.push(decode_s2c(&frame).ok()?);
    }
    Some(DecodedEnvelope { receipt_id, packets })
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
