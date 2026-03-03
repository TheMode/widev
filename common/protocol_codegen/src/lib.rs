use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    pub c2s: Vec<PacketDef>,
    pub s2c: Vec<PacketDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PacketDef {
    pub name: String,
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
        out.push_str("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]\n");
        out.push_str("pub enum C2SPacket {\n");
        for packet in &schema.c2s {
            out.push_str(&format_variant(packet));
        }
        out.push_str("}\n\n");

        out.push_str("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]\n");
        out.push_str("pub enum S2CPacket {\n");
        for packet in &schema.s2c {
            out.push_str(&format_variant(packet));
        }
        out.push_str("}\n\n");

        out.push_str(
            "pub fn encode_c2s(packet: &C2SPacket) -> Result<Vec<u8>, bincode::Error> {\n",
        );
        out.push_str("    bincode::serialize(packet)\n");
        out.push_str("}\n\n");

        out.push_str("pub fn decode_c2s(bytes: &[u8]) -> Result<C2SPacket, bincode::Error> {\n");
        out.push_str("    bincode::deserialize(bytes)\n");
        out.push_str("}\n\n");

        out.push_str(
            "pub fn encode_s2c(packet: &S2CPacket) -> Result<Vec<u8>, bincode::Error> {\n",
        );
        out.push_str("    bincode::serialize(packet)\n");
        out.push_str("}\n\n");

        out.push_str("pub fn decode_s2c(bytes: &[u8]) -> Result<S2CPacket, bincode::Error> {\n");
        out.push_str("    bincode::deserialize(bytes)\n");
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
