use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedData {
    servers: HashMap<String, PersistedServer>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedServer {
    bindings: HashMap<String, String>,
}

pub(super) struct BindingStore {
    path: PathBuf,
    data: PersistedData,
}

impl BindingStore {
    pub(super) fn load_default() -> Result<Self> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("state")
            .join("bindings.json");

        if !path.exists() {
            return Ok(Self {
                path,
                data: PersistedData::default(),
            });
        }

        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let data = match serde_json::from_str::<PersistedData>(&content) {
            Ok(data) => data,
            Err(err) => {
                eprintln!(
                    "failed to parse {}, resetting binding cache: {err}",
                    path.display()
                );
                if let Err(remove_err) = fs::remove_file(&path) {
                    eprintln!(
                        "failed to delete corrupt bindings file {}: {remove_err}",
                        path.display()
                    );
                }
                PersistedData::default()
            }
        };

        Ok(Self { path, data })
    }

    pub(super) fn get_binding_path(&self, cert_fp: &str, identifier: &str) -> Option<String> {
        self.data
            .servers
            .get(cert_fp)?
            .bindings
            .get(identifier)
            .cloned()
    }

    pub(super) fn binding_count(&self, cert_fp: &str) -> usize {
        self.data
            .servers
            .get(cert_fp)
            .map(|server| server.bindings.len())
            .unwrap_or(0)
    }

    pub(super) fn set_binding_path(&mut self, cert_fp: &str, identifier: &str, input_path: String) {
        let server = self.data.servers.entry(cert_fp.to_string()).or_default();
        server.bindings.insert(identifier.to_string(), input_path);
    }

    pub(super) fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let content =
            serde_json::to_string_pretty(&self.data).context("failed to serialize store")?;
        fs::write(&self.path, content)
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        Ok(())
    }
}
