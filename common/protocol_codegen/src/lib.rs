use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    #[serde(default)]
    pub typedefs: Vec<TypedefDef>,
    #[serde(default)]
    pub enums: Vec<EnumDef>,
    #[serde(default)]
    pub bitmasks: Vec<BitmaskDef>,
    #[serde(default)]
    pub common: Vec<PacketDef>,
    pub c2s: Vec<PacketDef>,
    pub s2c: Vec<PacketDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TypedefDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnumDef {
    pub name: String,
    pub variants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BitmaskDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub flags: Vec<BitmaskFlagDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BitmaskFlagDef {
    pub name: String,
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PacketDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

pub trait CodegenBackend {
    fn generate(&self, schema: &Schema) -> Result<String>;
}

pub fn load_schema(schema_path: &Path) -> Result<Schema> {
    let schema_text = fs::read_to_string(schema_path)
        .with_context(|| format!("failed to read schema file {}", schema_path.display()))?;
    let schema: Schema = serde_json::from_str(&schema_text)
        .with_context(|| format!("failed to parse JSON schema {}", schema_path.display()))?;
    Ok(schema)
}

pub fn generate_with_backend(
    schema_path: &Path,
    output_path: &Path,
    backend: &dyn CodegenBackend,
) -> Result<()> {
    let schema = load_schema(schema_path)?;
    let generated = backend.generate(&schema)?;
    fs::write(output_path, generated)
        .with_context(|| format!("failed to write generated file {}", output_path.display()))?;
    Ok(())
}

pub struct RustBackend;

impl CodegenBackend for RustBackend {
    fn generate(&self, schema: &Schema) -> Result<String> {
        let mut out = String::new();

        for typedef_def in &schema.typedefs {
            writeln!(&mut out, "pub type {} = {};", typedef_def.name, typedef_def.ty)
                .expect("writing to String should not fail");
        }
        if !schema.typedefs.is_empty() {
            out.push('\n');
        }

        for bitmask_def in &schema.bitmasks {
            write_bitmask(&mut out, bitmask_def);
        }

        for enum_def in &schema.enums {
            write_plain_enum(&mut out, enum_def);
        }

        for (name, packets) in [
            ("C2SPacket", schema.common.iter().chain(schema.c2s.iter())),
            ("S2CPacket", schema.common.iter().chain(schema.s2c.iter())),
        ] {
            write_packet_enum(&mut out, name, packets);
        }

        write_codec_fn(&mut out, "encode_c2s", "decode_c2s", "C2SPacket");
        write_codec_fn(&mut out, "encode_s2c", "decode_s2c", "S2CPacket");

        if schema.typedefs.iter().any(|typedef_def| typedef_def.name == "MessageId") {
            write_decode_module(&mut out);
        }

        out.truncate(out.trim_end().len());

        Ok(out)
    }
}

fn write_bitmask(out: &mut String, bitmask_def: &BitmaskDef) {
    writeln!(out, "{SCHEMA_DERIVE}").expect("writing to String should not fail");
    writeln!(out, "#[repr(transparent)]").expect("writing to String should not fail");
    writeln!(out, "pub struct {}(pub {});", bitmask_def.name, bitmask_def.ty)
        .expect("writing to String should not fail");
    writeln!(out, "impl {} {{", bitmask_def.name).expect("writing to String should not fail");
    writeln!(out, "    pub const NONE: Self = Self(0);")
        .expect("writing to String should not fail");
    for flag in &bitmask_def.flags {
        writeln!(out, "    pub const {}: Self = Self({});", to_upper_snake(&flag.name), flag.value)
            .expect("writing to String should not fail");
    }
    writeln!(out, "    pub fn bits(self) -> {} {{", bitmask_def.ty)
        .expect("writing to String should not fail");
    writeln!(out, "        self.0").expect("writing to String should not fail");
    writeln!(out, "    }}").expect("writing to String should not fail");
    writeln!(out, "    pub fn contains(self, other: Self) -> bool {{")
        .expect("writing to String should not fail");
    writeln!(out, "        (self.0 & other.0) == other.0")
        .expect("writing to String should not fail");
    writeln!(out, "    }}").expect("writing to String should not fail");
    writeln!(out, "}}").expect("writing to String should not fail");
    writeln!(out).expect("writing to String should not fail");
    writeln!(out, "impl std::ops::BitOr for {} {{", bitmask_def.name)
        .expect("writing to String should not fail");
    writeln!(out, "    type Output = Self;").expect("writing to String should not fail");
    writeln!(out, "    fn bitor(self, rhs: Self) -> Self::Output {{")
        .expect("writing to String should not fail");
    writeln!(out, "        Self(self.0 | rhs.0)").expect("writing to String should not fail");
    writeln!(out, "    }}").expect("writing to String should not fail");
    writeln!(out, "}}").expect("writing to String should not fail");
    writeln!(out).expect("writing to String should not fail");
    writeln!(out, "impl std::ops::BitOrAssign for {} {{", bitmask_def.name)
        .expect("writing to String should not fail");
    writeln!(out, "    fn bitor_assign(&mut self, rhs: Self) {{")
        .expect("writing to String should not fail");
    writeln!(out, "        self.0 |= rhs.0;").expect("writing to String should not fail");
    writeln!(out, "    }}").expect("writing to String should not fail");
    writeln!(out, "}}").expect("writing to String should not fail");
    writeln!(out).expect("writing to String should not fail");
}

fn write_plain_enum(out: &mut String, enum_def: &EnumDef) {
    writeln!(out, "{SCHEMA_DERIVE}").expect("writing to String should not fail");
    writeln!(out, "pub enum {} {{", enum_def.name).expect("writing to String should not fail");
    for variant in &enum_def.variants {
        writeln!(out, "    {},", variant).expect("writing to String should not fail");
    }
    writeln!(out, "}}").expect("writing to String should not fail");
    writeln!(out).expect("writing to String should not fail");
}

fn write_packet_enum<'a>(
    out: &mut String,
    name: &str,
    packets: impl IntoIterator<Item = &'a PacketDef>,
) {
    writeln!(out, "{PACKET_DERIVE}").expect("writing to String should not fail");
    writeln!(out, "pub enum {} {{", name).expect("writing to String should not fail");
    for packet in packets {
        if packet.fields.is_empty() {
            writeln!(out, "    {},", packet.name).expect("writing to String should not fail");
            continue;
        }

        writeln!(out, "    {} {{", packet.name).expect("writing to String should not fail");
        for field in &packet.fields {
            writeln!(out, "        {}: {},", field.name, field.ty)
                .expect("writing to String should not fail");
        }
        writeln!(out, "    }},").expect("writing to String should not fail");
    }
    writeln!(out, "}}").expect("writing to String should not fail");
    writeln!(out).expect("writing to String should not fail");
}

fn write_codec_fn(out: &mut String, encode_name: &str, decode_name: &str, packet_name: &str) {
    writeln!(
        out,
        "\
pub fn {encode_name}(packet: &{packet_name}) -> wincode::WriteResult<Vec<u8>> {{
    wincode::serialize(packet)
}}

pub fn {decode_name}(bytes: &[u8]) -> wincode::ReadResult<{packet_name}> {{
    wincode::deserialize(bytes)
}}
"
    )
    .expect("writing to String should not fail");
}

fn write_decode_module(out: &mut String) {
    writeln!(
        out,
        "{}",
        r#"
pub mod decode {
    use super::{decode_s2c, MessageId, S2CPacket};

    #[derive(Debug, Clone)]
    pub struct DecodedEnvelope {
        pub id: Option<MessageId>,
        pub receipt_id: Option<MessageId>,
        pub dependency_id: Option<MessageId>,
        pub packets: Vec<S2CPacket>,
    }

    #[derive(Debug, Clone)]
    pub struct DecodedResource {
        pub id: MessageId,
        pub receipt_id: Option<MessageId>,
        pub dependency_id: Option<MessageId>,
        pub resource_type: String,
        pub usage_count: i32,
        pub blob: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub enum DecodedServerMessage {
        Envelope(DecodedEnvelope),
        Resource(DecodedResource),
    }

    pub fn s2c_envelope(bytes: &[u8]) -> Option<DecodedEnvelope> {
        const ENVELOPE_VERSION: u8 = 1;
        const FLAG_HAS_ID: u8 = 1 << 0;
        const FLAG_CLIENT_PROCESSED_RECEIPT: u8 = 1 << 1;
        const FLAG_HAS_DEPENDENCY: u8 = 1 << 2;

        if bytes.len() < 2 || bytes[0] != ENVELOPE_VERSION {
            return None;
        }

        let flags = bytes[1];
        let mut cursor = 2usize;
        let id = if flags & FLAG_HAS_ID != 0 {
            read_message_id(bytes, &mut cursor)
        } else {
            None
        };
        let receipt_id = if flags & FLAG_CLIENT_PROCESSED_RECEIPT != 0 {
            Some(id?)
        } else {
            None
        };
        let dependency_id = if flags & FLAG_HAS_DEPENDENCY != 0 {
            read_message_id(bytes, &mut cursor)
        } else {
            None
        };
        let mut packets = Vec::new();
        while cursor < bytes.len() {
            let frame = read_frame(bytes, &mut cursor)?;
            packets.push(decode_s2c(frame).ok()?);
        }
        if packets.is_empty() {
            return None;
        }
        Some(DecodedEnvelope { id, receipt_id, dependency_id, packets })
    }

    pub fn server_message(bytes: &[u8]) -> Option<DecodedServerMessage> {
        const FRAME_VERSION: u8 = 1;
        const FRAME_KIND_ENVELOPE: u8 = 1;
        const FRAME_KIND_RESOURCE: u8 = 2;
        const FLAG_CLIENT_PROCESSED_RECEIPT: u8 = 1 << 0;
        const FLAG_HAS_DEPENDENCY: u8 = 1 << 1;

        if bytes.len() < 2 || bytes[0] != FRAME_VERSION {
            return None;
        }

        match bytes[1] {
            FRAME_KIND_ENVELOPE => {
                s2c_envelope(&bytes[2..]).map(DecodedServerMessage::Envelope)
            },
            FRAME_KIND_RESOURCE => {
                let payload = &bytes[2..];
                if payload.len() < 1 + 16 + 2 + 4 + 4 {
                    return None;
                }

                let flags = payload[0];
                let mut cursor = 1usize;
                let id = read_message_id(payload, &mut cursor)?;
                let receipt_id = if flags & FLAG_CLIENT_PROCESSED_RECEIPT != 0 {
                    Some(id)
                } else {
                    None
                };
                let dependency_id = if flags & FLAG_HAS_DEPENDENCY != 0 {
                    read_message_id(payload, &mut cursor)
                } else {
                    None
                };

                let resource_type_len = read_u16(payload, &mut cursor)? as usize;
                let resource_type =
                    String::from_utf8(payload.get(cursor..cursor + resource_type_len)?.to_vec())
                        .ok()?;
                cursor += resource_type_len;

                let usage_count = read_i32(payload, &mut cursor)?;
                let blob_len = read_u32(payload, &mut cursor)? as usize;
                let blob = payload.get(cursor..cursor + blob_len)?.to_vec();

                Some(DecodedServerMessage::Resource(DecodedResource {
                    id,
                    receipt_id,
                    dependency_id,
                    resource_type,
                    usage_count,
                    blob,
                }))
            },
            _ => None,
        }
    }

    fn read_message_id(bytes: &[u8], cursor: &mut usize) -> Option<MessageId> {
        if bytes.len() < *cursor + 16 {
            return None;
        }
        let mut raw = [0u8; 16];
        raw.copy_from_slice(&bytes[*cursor..*cursor + 16]);
        *cursor += 16;
        Some(u128::from_be_bytes(raw))
    }

    fn read_u16(bytes: &[u8], cursor: &mut usize) -> Option<u16> {
        if bytes.len() < *cursor + 2 {
            return None;
        }
        let raw = [bytes[*cursor], bytes[*cursor + 1]];
        *cursor += 2;
        Some(u16::from_be_bytes(raw))
    }

    fn read_u32(bytes: &[u8], cursor: &mut usize) -> Option<u32> {
        if bytes.len() < *cursor + 4 {
            return None;
        }
        let raw = [
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
        ];
        *cursor += 4;
        Some(u32::from_be_bytes(raw))
    }

    fn read_i32(bytes: &[u8], cursor: &mut usize) -> Option<i32> {
        if bytes.len() < *cursor + 4 {
            return None;
        }
        let raw = [
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
        ];
        *cursor += 4;
        Some(i32::from_be_bytes(raw))
    }

    fn read_frame<'a>(bytes: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
        if bytes.len() < *cursor + 4 {
            return None;
        }
        let len = u32::from_be_bytes([
            bytes[*cursor],
            bytes[*cursor + 1],
            bytes[*cursor + 2],
            bytes[*cursor + 3],
        ]) as usize;
        *cursor += 4;
        if bytes.len() < *cursor + len {
            return None;
        }
        let payload = &bytes[*cursor..*cursor + len];
        *cursor += len;
        Some(payload)
    }
}
"#
    )
    .expect("writing to String should not fail");
}

const SCHEMA_DERIVE: &str = r#"#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]"#;

const PACKET_DERIVE: &str = r#"#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]"#;

fn to_upper_snake(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 4);
    for (i, ch) in value.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_uppercase());
    }
    out
}
