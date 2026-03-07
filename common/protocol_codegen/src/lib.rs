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
            out.push_str(&format!("pub type {} = {};\n", typedef_def.name, typedef_def.ty));
        }
        if !schema.typedefs.is_empty() {
            out.push('\n');
        }

        for bitmask_def in &schema.bitmasks {
            out.push_str(&format_bitmask(bitmask_def));
        }

        for enum_def in &schema.enums {
            out.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]\n");
            out.push_str(&format!("pub enum {} {{\n", enum_def.name));
            for variant in &enum_def.variants {
                out.push_str(&format!("    {},\n", variant));
            }
            out.push_str("}\n\n");
        }

        out.push_str("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]\n");
        out.push_str("pub enum C2SPacket {\n");
        for packet in &schema.common {
            out.push_str(&format_variant(packet));
        }
        for packet in &schema.c2s {
            out.push_str(&format_variant(packet));
        }
        out.push_str("}\n\n");

        out.push_str("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]\n");
        out.push_str("pub enum S2CPacket {\n");
        for packet in &schema.common {
            out.push_str(&format_variant(packet));
        }
        for packet in &schema.s2c {
            out.push_str(&format_variant(packet));
        }
        out.push_str("}\n\n");

        out.push_str(
            "pub fn encode_c2s(packet: &C2SPacket) -> wincode::WriteResult<Vec<u8>> {\n",
        );
        out.push_str("    wincode::serialize(packet)\n");
        out.push_str("}\n\n");

        out.push_str("pub fn decode_c2s(bytes: &[u8]) -> wincode::ReadResult<C2SPacket> {\n");
        out.push_str("    wincode::deserialize(bytes)\n");
        out.push_str("}\n\n");

        out.push_str(
            "pub fn encode_s2c(packet: &S2CPacket) -> wincode::WriteResult<Vec<u8>> {\n",
        );
        out.push_str("    wincode::serialize(packet)\n");
        out.push_str("}\n\n");

        out.push_str("pub fn decode_s2c(bytes: &[u8]) -> wincode::ReadResult<S2CPacket> {\n");
        out.push_str("    wincode::deserialize(bytes)\n");
        out.push_str("}\n");

        Ok(out)
    }
}

fn format_variant(packet: &PacketDef) -> String {
    let mut out = String::new();
    if packet.fields.is_empty() {
        out.push_str(&format!("    {},\n", packet.name));
        return out;
    }

    out.push_str(&format!("    {} {{\n", packet.name));
    for field in &packet.fields {
        out.push_str(&format!("        {}: {},\n", field.name, field.ty));
    }
    out.push_str("    },\n");
    out
}

fn format_bitmask(bitmask: &BitmaskDef) -> String {
    let mut out = String::new();
    out.push_str(
        "#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]\n",
    );
    out.push_str("#[repr(transparent)]\n");
    out.push_str(&format!("pub struct {}(pub {});\n", bitmask.name, bitmask.ty));
    out.push_str(&format!("impl {} {{\n", bitmask.name));
    out.push_str("    pub const NONE: Self = Self(0);\n");
    for flag in &bitmask.flags {
        out.push_str(&format!(
            "    pub const {}: Self = Self({});\n",
            to_upper_snake(&flag.name),
            flag.value
        ));
    }
    out.push_str("    pub fn bits(self) -> ");
    out.push_str(&bitmask.ty);
    out.push_str(" {\n");
    out.push_str("        self.0\n");
    out.push_str("    }\n");
    out.push_str("    pub fn contains(self, other: Self) -> bool {\n");
    out.push_str("        (self.0 & other.0) == other.0\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str(&format!("impl std::ops::BitOr for {} {{\n", bitmask.name));
    out.push_str("    type Output = Self;\n");
    out.push_str("    fn bitor(self, rhs: Self) -> Self::Output {\n");
    out.push_str("        Self(self.0 | rhs.0)\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str(&format!("impl std::ops::BitOrAssign for {} {{\n", bitmask.name));
    out.push_str("    fn bitor_assign(&mut self, rhs: Self) {\n");
    out.push_str("        self.0 |= rhs.0;\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out
}

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
