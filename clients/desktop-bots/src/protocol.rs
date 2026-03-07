mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

pub use packets::{C2SPacket, S2CPacket};

pub fn encode_c2s(packet: &C2SPacket) -> wincode::WriteResult<Vec<u8>> {
    packets::encode_c2s(packet)
}

pub fn decode_s2c(bytes: &[u8]) -> wincode::ReadResult<S2CPacket> {
    packets::decode_s2c(bytes)
}
