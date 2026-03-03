#[allow(dead_code)]
mod packets {
    include!(concat!(env!("OUT_DIR"), "/packets_gen.rs"));
}

pub(super) use packets::{
    C2SPacket, InputType, PredictionKind, S2CPacket, TransformPredictionMask,
};

pub(super) fn encode_c2s(packet: &C2SPacket) -> Result<Vec<u8>, bincode::Error> {
    packets::encode_c2s(packet)
}

pub(super) fn decode_s2c(bytes: &[u8]) -> Result<S2CPacket, bincode::Error> {
    packets::decode_s2c(bytes)
}
