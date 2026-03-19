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
pub(crate) struct DeviceFilter {
    #[serde(rename = "type")]
    pub(crate) device_type: DeviceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
}

impl DeviceFilter {
    pub(crate) fn any(device_type: DeviceType) -> Self {
        Self { device_type, id: None }
    }

    pub(crate) fn exact(device_type: DeviceType, id: String) -> Self {
        Self { device_type, id: Some(id) }
    }

    pub(crate) fn matches(&self, device_type: DeviceType, device_id: &str) -> bool {
        self.device_type == device_type
            && self.id.as_deref().map(|expected| expected == device_id).unwrap_or(true)
    }

    pub(crate) fn with_scope(&self, any_device: bool) -> Self {
        if any_device {
            Self::any(self.device_type)
        } else {
            self.clone()
        }
    }

    fn scope_label(&self) -> &str {
        self.id.as_deref().unwrap_or("*")
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

    pub(crate) fn is_toggle_compatible(&self) -> bool {
        matches!(
            self.input,
            InputDescriptor::Key { .. }
                | InputDescriptor::MouseButton { .. }
                | InputDescriptor::GamepadButton { .. }
        )
    }

    pub(crate) fn is_axis_1d_compatible(&self) -> bool {
        matches!(
            self.input,
            InputDescriptor::MouseAxis { .. } | InputDescriptor::GamepadAxis { .. }
        )
    }

    pub(crate) fn is_joystick_2d_compatible(&self) -> bool {
        matches!(self.input, InputDescriptor::VirtualStick { .. } | InputDescriptor::Stick { .. })
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
                write!(f, "keyboard/{}/key/{code}", self.device.scope_label())
            },
            InputDescriptor::MouseButton { button } => {
                write!(f, "mouse/{}/button/{button}", self.device.scope_label())
            },
            InputDescriptor::MouseAxis { axis } => {
                write!(f, "mouse/{}/axis/{axis}", self.device.scope_label())
            },
            InputDescriptor::GamepadButton { button } => {
                write!(f, "gamepad/{}/button/{button}", self.device.scope_label())
            },
            InputDescriptor::GamepadAxis { axis } => {
                write!(f, "gamepad/{}/axis/{axis}", self.device.scope_label())
            },
            InputDescriptor::VirtualStick {
                positive_x,
                negative_x,
                positive_y,
                negative_y,
            } => write!(
                f,
                "keyboard/{}/virtual_stick(+x={positive_x},-x={negative_x},+y={positive_y},-y={negative_y})",
                self.device.scope_label()
            ),
            InputDescriptor::Stick { stick } => {
                write!(f, "gamepad/{}/stick/{stick}", self.device.scope_label())
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
