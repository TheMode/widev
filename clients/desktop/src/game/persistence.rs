use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use minifb::Key;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedData {
    servers: HashMap<String, PersistedServer>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedServer {
    bindings: HashMap<String, u16>,
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
        let data = serde_json::from_str::<PersistedData>(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;

        Ok(Self { path, data })
    }

    pub(super) fn get_key(&self, cert_fp: &str, identifier: &str) -> Option<Key> {
        let key_code = self.data.servers.get(cert_fp)?.bindings.get(identifier)?;
        key_from_code(*key_code)
    }

    pub(super) fn binding_count(&self, cert_fp: &str) -> usize {
        self.data
            .servers
            .get(cert_fp)
            .map(|server| server.bindings.len())
            .unwrap_or(0)
    }

    pub(super) fn set_key(&mut self, cert_fp: &str, identifier: &str, key: Key) {
        let server = self.data.servers.entry(cert_fp.to_string()).or_default();
        server
            .bindings
            .insert(identifier.to_string(), key_to_code(key));
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

fn key_to_code(key: Key) -> u16 {
    key as u16
}

fn key_from_code(code: u16) -> Option<Key> {
    const ALL_KEYS: &[Key] = &[
        Key::Key0,
        Key::Key1,
        Key::Key2,
        Key::Key3,
        Key::Key4,
        Key::Key5,
        Key::Key6,
        Key::Key7,
        Key::Key8,
        Key::Key9,
        Key::A,
        Key::B,
        Key::C,
        Key::D,
        Key::E,
        Key::F,
        Key::G,
        Key::H,
        Key::I,
        Key::J,
        Key::K,
        Key::L,
        Key::M,
        Key::N,
        Key::O,
        Key::P,
        Key::Q,
        Key::R,
        Key::S,
        Key::T,
        Key::U,
        Key::V,
        Key::W,
        Key::X,
        Key::Y,
        Key::Z,
        Key::F1,
        Key::F2,
        Key::F3,
        Key::F4,
        Key::F5,
        Key::F6,
        Key::F7,
        Key::F8,
        Key::F9,
        Key::F10,
        Key::F11,
        Key::F12,
        Key::F13,
        Key::F14,
        Key::F15,
        Key::Down,
        Key::Left,
        Key::Right,
        Key::Up,
        Key::Apostrophe,
        Key::Backquote,
        Key::Backslash,
        Key::Comma,
        Key::Equal,
        Key::LeftBracket,
        Key::Minus,
        Key::Period,
        Key::RightBracket,
        Key::Semicolon,
        Key::Slash,
        Key::Backspace,
        Key::Delete,
        Key::End,
        Key::Enter,
        Key::Escape,
        Key::Home,
        Key::Insert,
        Key::Menu,
        Key::PageDown,
        Key::PageUp,
        Key::Pause,
        Key::Space,
        Key::Tab,
        Key::NumLock,
        Key::CapsLock,
        Key::ScrollLock,
        Key::LeftShift,
        Key::RightShift,
        Key::LeftCtrl,
        Key::RightCtrl,
        Key::NumPad0,
        Key::NumPad1,
        Key::NumPad2,
        Key::NumPad3,
        Key::NumPad4,
        Key::NumPad5,
        Key::NumPad6,
        Key::NumPad7,
        Key::NumPad8,
        Key::NumPad9,
        Key::NumPadDot,
        Key::NumPadSlash,
        Key::NumPadAsterisk,
        Key::NumPadMinus,
        Key::NumPadPlus,
        Key::NumPadEnter,
        Key::LeftAlt,
        Key::RightAlt,
        Key::LeftSuper,
        Key::RightSuper,
        Key::Unknown,
    ];

    ALL_KEYS.iter().copied().find(|key| (*key as u16) == code)
}
