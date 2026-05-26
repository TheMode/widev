use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::RngExt;
use sha2::{Digest, Sha256};

const TOKEN_VERSION: u8 = 1;
const TOKEN_TTL: Duration = Duration::from_secs(5);
const HMAC_LEN: usize = 32;
const SECRET_LEN: usize = 32;
const SHA256_BLOCK_LEN: usize = 64;
const ADDR_V4: u8 = 4;
const ADDR_V6: u8 = 6;

pub struct RetryTokenizer {
    secret: [u8; SECRET_LEN],
}

impl RetryTokenizer {
    pub fn new() -> Self {
        let mut secret = [0u8; SECRET_LEN];
        rand::rng().fill(&mut secret);
        Self { secret }
    }

    pub fn mint(&self, peer: SocketAddr, odcid: &[u8]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(32 + odcid.len());
        payload.push(TOKEN_VERSION);
        payload.extend_from_slice(&now_millis().to_be_bytes());
        encode_addr(&mut payload, peer);
        let odcid_len = u8::try_from(odcid.len()).expect("odcid fits in a u8 per RFC 9000");
        payload.push(odcid_len);
        payload.extend_from_slice(odcid);

        let mac = hmac_sha256(&self.secret, &payload);
        payload.extend_from_slice(&mac);
        payload
    }

    /// Returns the original DCID encoded in the token on success.
    pub fn validate(&self, token: &[u8], peer: SocketAddr) -> Option<Vec<u8>> {
        let payload_len = token.len().checked_sub(HMAC_LEN)?;
        let (payload, provided_mac) = token.split_at(payload_len);
        if payload.first() != Some(&TOKEN_VERSION) {
            return None;
        }
        let expected_mac = hmac_sha256(&self.secret, payload);
        if !constant_time_eq(provided_mac, &expected_mac) {
            return None;
        }

        let mut cursor = 1usize;
        let ts_bytes: [u8; 8] = payload.get(cursor..cursor + 8)?.try_into().ok()?;
        cursor += 8;
        let age = now_millis().saturating_sub(u64::from_be_bytes(ts_bytes));
        if age > TOKEN_TTL.as_millis() as u64 {
            return None;
        }

        if decode_addr(payload, &mut cursor)? != peer {
            return None;
        }

        let odcid_len = *payload.get(cursor)? as usize;
        cursor += 1;
        Some(payload.get(cursor..cursor + odcid_len)?.to_vec())
    }
}

fn encode_addr(out: &mut Vec<u8>, addr: SocketAddr) {
    match addr.ip() {
        IpAddr::V4(v4) => {
            out.push(ADDR_V4);
            out.extend_from_slice(&v4.octets());
        },
        IpAddr::V6(v6) => {
            out.push(ADDR_V6);
            out.extend_from_slice(&v6.octets());
        },
    }
    out.extend_from_slice(&addr.port().to_be_bytes());
}

fn decode_addr(payload: &[u8], cursor: &mut usize) -> Option<SocketAddr> {
    let kind = *payload.get(*cursor)?;
    *cursor += 1;
    let ip = match kind {
        ADDR_V4 => {
            let bytes: [u8; 4] = payload.get(*cursor..*cursor + 4)?.try_into().ok()?;
            *cursor += 4;
            IpAddr::V4(bytes.into())
        },
        ADDR_V6 => {
            let bytes: [u8; 16] = payload.get(*cursor..*cursor + 16)?.try_into().ok()?;
            *cursor += 16;
            IpAddr::V6(bytes.into())
        },
        _ => return None,
    };
    let port_bytes: [u8; 2] = payload.get(*cursor..*cursor + 2)?.try_into().ok()?;
    *cursor += 2;
    Some(SocketAddr::new(ip, u16::from_be_bytes(port_bytes)))
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn hmac_sha256(key: &[u8; SECRET_LEN], message: &[u8]) -> [u8; HMAC_LEN] {
    let mut key_block = [0u8; SHA256_BLOCK_LEN];
    key_block[..SECRET_LEN].copy_from_slice(key);

    let mut ipad = [0x36u8; SHA256_BLOCK_LEN];
    let mut opad = [0x5cu8; SHA256_BLOCK_LEN];
    for i in 0..SHA256_BLOCK_LEN {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let inner = {
        let mut h = Sha256::new();
        h.update(ipad);
        h.update(message);
        h.finalize()
    };

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner);
    outer.finalize().into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4_peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 51234)
    }

    fn v6_peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)), 51234)
    }

    #[test]
    fn roundtrip_v4() {
        let tokenizer = RetryTokenizer::new();
        let odcid = [0xab, 0xcd, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05];
        let token = tokenizer.mint(v4_peer(), &odcid);
        assert_eq!(tokenizer.validate(&token, v4_peer()).as_deref(), Some(&odcid[..]));
    }

    #[test]
    fn roundtrip_v6() {
        let tokenizer = RetryTokenizer::new();
        let odcid = [0x10, 0x20, 0x30, 0x40];
        let token = tokenizer.mint(v6_peer(), &odcid);
        assert_eq!(tokenizer.validate(&token, v6_peer()).as_deref(), Some(&odcid[..]));
    }

    #[test]
    fn rejects_wrong_addr() {
        let tokenizer = RetryTokenizer::new();
        let token = tokenizer.mint(v4_peer(), &[1, 2, 3, 4]);
        let other = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)), 51234);
        assert!(tokenizer.validate(&token, other).is_none());
    }

    #[test]
    fn rejects_wrong_port() {
        let tokenizer = RetryTokenizer::new();
        let token = tokenizer.mint(v4_peer(), &[1, 2, 3, 4]);
        let mut other = v4_peer();
        other.set_port(other.port() + 1);
        assert!(tokenizer.validate(&token, other).is_none());
    }

    #[test]
    fn rejects_tampered_token() {
        let tokenizer = RetryTokenizer::new();
        let mut token = tokenizer.mint(v4_peer(), &[1, 2, 3, 4]);
        let last = token.len() - 1;
        token[last] ^= 0x01;
        assert!(tokenizer.validate(&token, v4_peer()).is_none());
    }

    #[test]
    fn rejects_foreign_secret() {
        let minter = RetryTokenizer::new();
        let validator = RetryTokenizer::new();
        let token = minter.mint(v4_peer(), &[1, 2, 3, 4]);
        assert!(validator.validate(&token, v4_peer()).is_none());
    }

    #[test]
    fn rejects_truncated_token() {
        let tokenizer = RetryTokenizer::new();
        let token = tokenizer.mint(v4_peer(), &[1, 2, 3, 4]);
        assert!(tokenizer.validate(&token[..token.len() - 1], v4_peer()).is_none());
        assert!(tokenizer.validate(&[], v4_peer()).is_none());
    }

    #[test]
    fn rejects_wrong_version() {
        let tokenizer = RetryTokenizer::new();
        let mut token = tokenizer.mint(v4_peer(), &[1, 2, 3, 4]);
        token[0] = TOKEN_VERSION.wrapping_add(1);
        assert!(tokenizer.validate(&token, v4_peer()).is_none());
    }
}
