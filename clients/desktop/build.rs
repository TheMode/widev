use std::env;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let schema = manifest_dir.join("../../common/packets/packets.json");
    let out_file = PathBuf::from(env::var("OUT_DIR")?).join("packets_gen.rs");

    println!("cargo:rerun-if-changed={}", schema.display());
    protocol_codegen::generate_with_backend(&schema, &out_file, &protocol_codegen::RustBackend)?;

    Ok(())
}
