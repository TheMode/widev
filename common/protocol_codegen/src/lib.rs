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

        macro_rules! line {
            () => {
                out.push('\n');
            };
            ($text:expr) => {
                out.push_str($text);
                out.push('\n');
            };
            ($fmt:expr, $($arg:tt)*) => {
                writeln!(&mut out, $fmt, $($arg)*).expect("writing to String should not fail");
            };
        }

        for typedef_def in &schema.typedefs {
            line!("pub type {} = {};", typedef_def.name, typedef_def.ty);
        }
        if !schema.typedefs.is_empty() {
            line!();
        }

        for bitmask_def in &schema.bitmasks {
            line!(
                r#"#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]"#
            );
            line!("#[repr(transparent)]");
            line!("pub struct {}(pub {});", bitmask_def.name, bitmask_def.ty);
            line!("impl {} {{", bitmask_def.name);
            line!("    pub const NONE: Self = Self(0);");
            for flag in &bitmask_def.flags {
                line!("    pub const {}: Self = Self({});", to_upper_snake(&flag.name), flag.value);
            }
            line!("    pub fn bits(self) -> {} {{", bitmask_def.ty);
            line!("        self.0");
            line!("    }");
            line!("    pub fn contains(self, other: Self) -> bool {");
            line!("        (self.0 & other.0) == other.0");
            line!("    }");
            line!("}");
            line!();
            line!("impl std::ops::BitOr for {} {{", bitmask_def.name);
            line!("    type Output = Self;");
            line!("    fn bitor(self, rhs: Self) -> Self::Output {");
            line!("        Self(self.0 | rhs.0)");
            line!("    }");
            line!("}");
            line!();
            line!("impl std::ops::BitOrAssign for {} {{", bitmask_def.name);
            line!("    fn bitor_assign(&mut self, rhs: Self) {");
            line!("        self.0 |= rhs.0;");
            line!("    }");
            line!("}");
            line!();
        }

        for enum_def in &schema.enums {
            line!(
                r#"#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]"#
            );
            line!("pub enum {} {{", enum_def.name);
            for variant in &enum_def.variants {
                line!("    {},", variant);
            }
            line!("}");
            line!();
        }

        for (name, packets) in [
            ("C2SPacket", schema.common.iter().chain(schema.c2s.iter())),
            ("S2CPacket", schema.common.iter().chain(schema.s2c.iter())),
        ] {
            line!(
                r#"#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]"#
            );
            line!("pub enum {} {{", name);
            for packet in packets {
                if packet.fields.is_empty() {
                    line!("    {},", packet.name);
                } else {
                    line!("    {} {{", packet.name);
                    for field in &packet.fields {
                        line!("        {}: {},", field.name, field.ty);
                    }
                    line!("    },");
                }
            }
            line!("}");
            line!();
        }

        macro_rules! codec_fns {
            ($encode:ident, $decode:ident, $packet:ident) => {
                out.push_str(concat!(
                    "pub fn ",
                    stringify!($encode),
                    "(packet: &",
                    stringify!($packet),
                    ") -> wincode::WriteResult<Vec<u8>> {\n",
                    "    wincode::serialize(packet)\n",
                    "}\n\n",
                    "pub fn ",
                    stringify!($decode),
                    "(bytes: &[u8]) -> wincode::ReadResult<",
                    stringify!($packet),
                    "> {\n",
                    "    wincode::deserialize(bytes)\n",
                    "}\n\n",
                ));
            };
        }

        codec_fns!(encode_c2s, decode_c2s, C2SPacket);
        codec_fns!(encode_s2c, decode_s2c, S2CPacket);
        out.truncate(out.trim_end().len());

        Ok(out)
    }
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
