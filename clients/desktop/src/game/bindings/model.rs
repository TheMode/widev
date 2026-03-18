use std::fmt;

use strum::{Display, EnumIter, IntoEnumIterator, IntoStaticStr};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DeviceSelector {
    Any,
    Exact(String),
}

impl DeviceSelector {
    pub(crate) fn matches(&self, device_id: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected == device_id,
        }
    }

    fn as_path(&self) -> &str {
        match self {
            Self::Any => "*",
            Self::Exact(id) => id,
        }
    }

    fn parse(raw: &str) -> Self {
        if raw == "*" {
            Self::Any
        } else {
            Self::Exact(raw.to_string())
        }
    }
}

#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, IntoStaticStr, PartialEq)]
pub(crate) enum MouseAxis {
    MotionX,
    MotionY,
    WheelX,
    WheelY,
}

#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, IntoStaticStr, PartialEq)]
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

#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, IntoStaticStr, PartialEq)]
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

#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, IntoStaticStr, PartialEq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MouseButtonKind {
    Left,
    Right,
    Middle,
    Back,
    Forward,
    Other(u16),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BindingDeclaration {
    KeyboardKey {
        device: DeviceSelector,
        key: KeyboardKey,
    },
    MouseButton {
        device: DeviceSelector,
        button: MouseButtonKind,
    },
    MouseAxis {
        device: DeviceSelector,
        axis: MouseAxis,
    },
    GamepadButton {
        device: DeviceSelector,
        button: GamepadButton,
    },
    GamepadAxis {
        device: DeviceSelector,
        axis: GamepadAxis,
    },
}

pub(crate) type InputPath = BindingDeclaration;

impl BindingDeclaration {
    pub(crate) fn with_device_scope(&self, any_device: bool) -> Self {
        let selector = if any_device {
            DeviceSelector::Any
        } else {
            match self.device_selector() {
                DeviceSelector::Any => DeviceSelector::Any,
                DeviceSelector::Exact(id) => DeviceSelector::Exact(id.to_string()),
            }
        };

        match self {
            Self::KeyboardKey { key, .. } => Self::KeyboardKey { device: selector, key: *key },
            Self::MouseButton { button, .. } => {
                Self::MouseButton { device: selector, button: *button }
            },
            Self::MouseAxis { axis, .. } => Self::MouseAxis { device: selector, axis: *axis },
            Self::GamepadButton { button, .. } => {
                Self::GamepadButton { device: selector, button: *button }
            },
            Self::GamepadAxis { axis, .. } => Self::GamepadAxis { device: selector, axis: *axis },
        }
    }

    fn device_selector(&self) -> &DeviceSelector {
        match self {
            Self::KeyboardKey { device, .. }
            | Self::MouseButton { device, .. }
            | Self::MouseAxis { device, .. }
            | Self::GamepadButton { device, .. }
            | Self::GamepadAxis { device, .. } => device,
        }
    }

    pub(crate) fn is_toggle_compatible(&self) -> bool {
        matches!(
            self,
            Self::KeyboardKey { .. } | Self::MouseButton { .. } | Self::GamepadButton { .. }
        )
    }

    pub(crate) fn is_axis_1d_compatible(&self) -> bool {
        matches!(self, Self::MouseAxis { .. } | Self::GamepadAxis { .. })
    }
}

impl fmt::Display for BindingDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyboardKey { device, key } => {
                write!(f, "keyboard/{}/key/{key}", device.as_path())
            },
            Self::MouseButton { device, button } => {
                write!(f, "mouse/{}/button/{button}", device.as_path())
            },
            Self::MouseAxis { device, axis } => {
                write!(f, "mouse/{}/axis/{axis}", device.as_path())
            },
            Self::GamepadButton { device, button } => {
                write!(f, "gamepad/{}/button/{button}", device.as_path())
            },
            Self::GamepadAxis { device, axis } => {
                write!(f, "gamepad/{}/axis/{axis}", device.as_path())
            },
        }
    }
}

pub(crate) fn parse_input_path(path: &str) -> Option<InputPath> {
    let mut parts = path.split('/');
    let device_kind = parts.next()?;
    let device_selector = DeviceSelector::parse(parts.next()?);
    let control_kind = parts.next()?;
    let control_name = parts.next()?;

    if parts.next().is_some() {
        return None;
    }

    match (device_kind, control_kind) {
        ("keyboard", "key") => Some(InputPath::KeyboardKey {
            device: device_selector,
            key: parse_keyboard_key(control_name)?,
        }),
        ("mouse", "button") => Some(InputPath::MouseButton {
            device: device_selector,
            button: parse_mouse_button(control_name)?,
        }),
        ("mouse", "axis") => Some(InputPath::MouseAxis {
            device: device_selector,
            axis: parse_mouse_axis(control_name)?,
        }),
        ("gamepad", "button") => Some(InputPath::GamepadButton {
            device: device_selector,
            button: parse_gamepad_button(control_name)?,
        }),
        ("gamepad", "axis") => Some(InputPath::GamepadAxis {
            device: device_selector,
            axis: parse_gamepad_axis(control_name)?,
        }),
        _ => None,
    }
}

fn parse_keyboard_key(raw: &str) -> Option<KeyboardKey> {
    KeyboardKey::iter().find(|key| {
        let key_name: &'static str = (*key).into();
        key_name == raw
    })
}

fn parse_mouse_button(raw: &str) -> Option<MouseButtonKind> {
    match raw {
        "Left" => Some(MouseButtonKind::Left),
        "Right" => Some(MouseButtonKind::Right),
        "Middle" => Some(MouseButtonKind::Middle),
        "Back" => Some(MouseButtonKind::Back),
        "Forward" => Some(MouseButtonKind::Forward),
        _ => {
            let suffix = raw.strip_prefix("Other(")?.strip_suffix(')')?;
            let value = suffix.parse::<u16>().ok()?;
            Some(MouseButtonKind::Other(value))
        },
    }
}

fn parse_mouse_axis(raw: &str) -> Option<MouseAxis> {
    MouseAxis::iter().find(|axis| {
        let axis_name: &'static str = (*axis).into();
        axis_name == raw
    })
}

fn parse_gamepad_button(raw: &str) -> Option<GamepadButton> {
    GamepadButton::iter().find(|button| {
        let name: &'static str = (*button).into();
        name == raw
    })
}

fn parse_gamepad_axis(raw: &str) -> Option<GamepadAxis> {
    GamepadAxis::iter().find(|axis| {
        let name: &'static str = (*axis).into();
        name == raw
    })
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
