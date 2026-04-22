use std::fmt;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, IntoStaticStr};

use super::protocol;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeviceType {
    Keyboard,
    Mouse,
    Gamepad,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum DeviceIdentity {
    Session {
        value: String,
    },
    Gamepad {
        uuid: [u8; 16],
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    #[cfg(target_os = "windows")]
    WindowsPersistent {
        value: String,
    },
}

fn deserialize_device_identity_opt<'de, D>(
    deserializer: D,
) -> Result<Option<DeviceIdentity>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(DeviceIdentity::session(value))),
        Some(other) => serde_json::from_value(other).map(Some).map_err(D::Error::custom),
    }
}

impl DeviceIdentity {
    pub(crate) fn session(value: impl Into<String>) -> Self {
        Self::Session { value: value.into() }
    }

    pub(crate) fn display_label(&self) -> String {
        match self {
            Self::Session { value } => value.clone(),
            Self::Gamepad { uuid, label } => {
                if let Some(label) = label {
                    return label.clone();
                }
                let mut out = String::with_capacity(32);
                for byte in uuid {
                    use std::fmt::Write as _;
                    let _ = write!(&mut out, "{byte:02x}");
                }
                out
            },
            #[cfg(target_os = "windows")]
            Self::WindowsPersistent { value } => value.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct DeviceFilter {
    #[serde(rename = "type")]
    pub(crate) device_type: DeviceType,
    #[serde(
        alias = "id",
        default,
        deserialize_with = "deserialize_device_identity_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) identity: Option<DeviceIdentity>,
}

impl DeviceFilter {
    pub(crate) fn any(device_type: DeviceType) -> Self {
        Self { device_type, identity: None }
    }

    #[allow(dead_code)]
    pub(crate) fn exact(device_type: DeviceType, id: impl Into<String>) -> Self {
        Self { device_type, identity: Some(DeviceIdentity::session(id)) }
    }

    pub(crate) fn exact_identity(device_type: DeviceType, identity: DeviceIdentity) -> Self {
        Self { device_type, identity: Some(identity) }
    }

    pub(crate) fn matches(&self, device_type: DeviceType, identity: &DeviceIdentity) -> bool {
        self.device_type == device_type
            && self.identity.as_ref().map(|expected| expected == identity).unwrap_or(true)
    }

    pub(crate) fn with_scope(&self, any_device: bool) -> Self {
        if any_device { Self::any(self.device_type) } else { self.clone() }
    }

    fn display_label(&self) -> String {
        self.identity.as_ref().map(DeviceIdentity::display_label).unwrap_or_else(|| "*".to_string())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct KeyModifiers {
    pub(crate) shift: bool,
    pub(crate) control: bool,
    pub(crate) alt: bool,
    pub(crate) super_key: bool,
}

impl KeyModifiers {
    pub(crate) fn is_empty(self) -> bool {
        !self.shift && !self.control && !self.alt && !self.super_key
    }
}

impl fmt::Display for KeyModifiers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.super_key {
            f.write_str("super+")?;
        }
        if self.alt {
            f.write_str("alt+")?;
        }
        if self.control {
            f.write_str("ctrl+")?;
        }
        if self.shift {
            f.write_str("shift+")?;
        }
        Ok(())
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    IntoStaticStr,
    PartialEq,
)]
pub(crate) enum MouseAxis {
    MotionX,
    MotionY,
    WheelX,
    WheelY,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    IntoStaticStr,
    PartialEq,
)]
pub(crate) enum GamepadButton {
    South,
    East,
    North,
    West,
    C,
    Z,
    LeftTrigger,
    LeftTrigger2,
    RightTrigger,
    RightTrigger2,
    Select,
    Start,
    Mode,
    LeftThumb,
    RightThumb,
    DPadUp,
    DPadDown,
    DPadLeft,
    DPadRight,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    IntoStaticStr,
    PartialEq,
)]
pub(crate) enum GamepadAxis {
    LeftStickX,
    LeftStickY,
    LeftZ,
    RightStickX,
    RightStickY,
    RightZ,
    DPadX,
    DPadY,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    IntoStaticStr,
    PartialEq,
)]
pub(crate) enum KeyboardKey {
    Digit0,
    Digit1,
    Digit2,
    Digit3,
    Digit4,
    Digit5,
    Digit6,
    Digit7,
    Digit8,
    Digit9,
    KeyA,
    KeyB,
    KeyC,
    KeyD,
    KeyE,
    KeyF,
    KeyG,
    KeyH,
    KeyI,
    KeyJ,
    KeyK,
    KeyL,
    KeyM,
    KeyN,
    KeyO,
    KeyP,
    KeyQ,
    KeyR,
    KeyS,
    KeyT,
    KeyU,
    KeyV,
    KeyW,
    KeyX,
    KeyY,
    KeyZ,
    Escape,
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    PrintScreen,
    ScrollLock,
    Pause,
    Insert,
    Home,
    Delete,
    End,
    PageDown,
    PageUp,
    ArrowLeft,
    ArrowUp,
    ArrowRight,
    ArrowDown,
    Backspace,
    Enter,
    Space,
    CapsLock,
    NumLock,
    Numpad0,
    Numpad1,
    Numpad2,
    Numpad3,
    Numpad4,
    Numpad5,
    Numpad6,
    Numpad7,
    Numpad8,
    Numpad9,
    NumpadAdd,
    NumpadComma,
    NumpadDecimal,
    NumpadDivide,
    NumpadEnter,
    NumpadEqual,
    NumpadMultiply,
    NumpadSubtract,
    Tab,
    ShiftLeft,
    ShiftRight,
    ControlLeft,
    ControlRight,
    AltLeft,
    AltRight,
    SuperLeft,
    SuperRight,
    ContextMenu,
    Semicolon,
    Equal,
    Comma,
    Minus,
    Period,
    Slash,
    Backquote,
    BracketLeft,
    Backslash,
    BracketRight,
    Quote,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum MouseButtonKind {
    Left,
    Right,
    Middle,
    Back,
    Forward,
    Other(u16),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Display, PartialEq, Eq, Hash)]
pub(crate) enum GamepadStick {
    Left,
    Right,
    DPad,
}

impl GamepadStick {
    pub(crate) fn axes(self) -> (GamepadAxis, GamepadAxis) {
        match self {
            Self::Left => (GamepadAxis::LeftStickX, GamepadAxis::LeftStickY),
            Self::Right => (GamepadAxis::RightStickX, GamepadAxis::RightStickY),
            Self::DPad => (GamepadAxis::DPadX, GamepadAxis::DPadY),
        }
    }

    pub(crate) fn from_axis(axis: GamepadAxis) -> Option<Self> {
        match axis {
            GamepadAxis::LeftStickX | GamepadAxis::LeftStickY => Some(Self::Left),
            GamepadAxis::RightStickX | GamepadAxis::RightStickY => Some(Self::Right),
            GamepadAxis::DPadX | GamepadAxis::DPadY => Some(Self::DPad),
            GamepadAxis::LeftZ | GamepadAxis::RightZ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ActionBinding {
    #[serde(rename = "type")]
    pub(crate) action_type: protocol::InputType,
    pub(crate) sources: Vec<RawSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct RawSource {
    pub(crate) device: DeviceFilter,
    pub(crate) input: InputDescriptor,
}

impl RawSource {
    pub(crate) fn with_device_scope(&self, any_device: bool) -> Self {
        Self { device: self.device.with_scope(any_device), input: self.input.clone() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum InputDescriptor {
    Key {
        code: KeyboardKey,
    },
    MouseButton {
        button: MouseButtonKind,
    },
    MouseAxis {
        axis: MouseAxis,
    },
    GamepadButton {
        button: GamepadButton,
    },
    GamepadAxis {
        axis: GamepadAxis,
    },
    KeyChord {
        key: KeyboardKey,
        modifiers: KeyModifiers,
    },
    VirtualStick {
        positive_x: KeyboardKey,
        negative_x: KeyboardKey,
        positive_y: KeyboardKey,
        negative_y: KeyboardKey,
    },
    Stick {
        stick: GamepadStick,
    },
}

impl InputDescriptor {
    pub(crate) fn is_toggle_compatible(&self) -> bool {
        matches!(
            self,
            Self::Key { .. }
                | Self::KeyChord { .. }
                | Self::MouseButton { .. }
                | Self::GamepadButton { .. }
        )
    }

    pub(crate) fn is_axis_1d_compatible(&self) -> bool {
        matches!(self, Self::MouseAxis { .. } | Self::GamepadAxis { .. })
    }

    pub(crate) fn is_joystick_2d_compatible(&self) -> bool {
        matches!(self, Self::VirtualStick { .. } | Self::Stick { .. })
    }

    pub(crate) fn default_action_type(&self) -> protocol::InputType {
        if self.is_toggle_compatible() {
            protocol::InputType::Toggle
        } else if self.is_axis_1d_compatible() {
            protocol::InputType::Axis1D
        } else {
            protocol::InputType::Joystick2D
        }
    }

    pub(crate) fn is_compatible_for(&self, input_type: protocol::InputType) -> bool {
        match input_type {
            protocol::InputType::Toggle => self.is_toggle_compatible(),
            protocol::InputType::Axis1D => self.is_axis_1d_compatible(),
            protocol::InputType::Joystick2D => self.is_joystick_2d_compatible(),
        }
    }
}

impl fmt::Display for ActionBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sources = self.sources.iter();
        if let Some(first) = sources.next() {
            write!(f, "{first}")?;
            for source in sources {
                write!(f, " | {source}")?;
            }
        } else {
            write!(f, "<no sources>")?;
        }
        Ok(())
    }
}

impl fmt::Display for RawSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.input {
            InputDescriptor::Key { code } => {
                write!(f, "keyboard/{}/key/{code}", self.device.display_label())
            },
            InputDescriptor::MouseButton { button } => {
                write!(f, "mouse/{}/button/{button}", self.device.display_label())
            },
            InputDescriptor::MouseAxis { axis } => {
                write!(f, "mouse/{}/axis/{axis}", self.device.display_label())
            },
            InputDescriptor::GamepadButton { button } => {
                write!(f, "gamepad/{}/button/{button}", self.device.display_label())
            },
            InputDescriptor::GamepadAxis { axis } => {
                write!(f, "gamepad/{}/axis/{axis}", self.device.display_label())
            },
            InputDescriptor::KeyChord { key, modifiers } => {
                write!(f, "keyboard/{}/chord({modifiers}{key})", self.device.display_label())
            },
            InputDescriptor::VirtualStick { positive_x, negative_x, positive_y, negative_y } => {
                write!(
                    f,
                    "keyboard/{}/virtual_stick(+x={positive_x},-x={negative_x},+y={positive_y},-y={negative_y})",
                    self.device.display_label()
                )
            },
            InputDescriptor::Stick { stick } => {
                write!(f, "gamepad/{}/stick/{stick}", self.device.display_label())
            },
        }
    }
}

impl fmt::Display for MouseButtonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Left => f.write_str("Left"),
            Self::Right => f.write_str("Right"),
            Self::Middle => f.write_str("Middle"),
            Self::Back => f.write_str("Back"),
            Self::Forward => f.write_str("Forward"),
            Self::Other(value) => write!(f, "Other({value})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_chord_formats_with_modifiers() {
        let source = RawSource {
            device: DeviceFilter::any(DeviceType::Keyboard),
            input: InputDescriptor::KeyChord {
                key: KeyboardKey::KeyK,
                modifiers: KeyModifiers { control: true, shift: true, ..Default::default() },
            },
        };

        assert_eq!(source.to_string(), "keyboard/*/chord(ctrl+shift+KeyK)");
    }

    #[test]
    fn key_chord_is_toggle_compatible() {
        let source = RawSource {
            device: DeviceFilter::any(DeviceType::Keyboard),
            input: InputDescriptor::KeyChord {
                key: KeyboardKey::KeyK,
                modifiers: KeyModifiers { control: true, ..Default::default() },
            },
        };

        assert!(source.input.is_toggle_compatible());
    }
}
