use std::collections::{HashMap, HashSet};

use gilrs::{Axis as GilrsAxis, Button as GilrsButton, EventType as GilrsEventType, Gilrs};
use winit::event::{DeviceEvent, ElementState, MouseButton, MouseScrollDelta, WindowEvent};
use winit::keyboard::PhysicalKey;

use super::model::{
    DeviceSelector, GamepadAxis, GamepadButton, InputPath, KeyboardKey, MouseAxis, MouseButtonKind,
};
use super::UiAction;

const GAMEPAD_AXIS_CAPTURE_THRESHOLD: f32 = 0.35;
const MOUSE_AXIS_CAPTURE_THRESHOLD: f32 = 0.1;

pub(crate) struct InputCapture {
    gamepad: Option<Gilrs>,
    pressed_keys: HashSet<(String, KeyboardKey)>,
    pressed_mouse_buttons: HashSet<(String, MouseButtonKind)>,
    pressed_gamepad_buttons: HashSet<(String, GamepadButton)>,
    mouse_axes: HashMap<(String, MouseAxis), f32>,
    gamepad_axes: HashMap<(String, GamepadAxis), f32>,
    just_pressed_keys: Vec<KeyboardKey>,
    just_captured_inputs: Vec<InputPath>,
}

impl InputCapture {
    pub(crate) fn new() -> Self {
        let gamepad = Gilrs::new()
            .map_err(|err| {
                log::warn!("gamepad input disabled: {err}");
                err
            })
            .ok();

        Self {
            gamepad,
            pressed_keys: HashSet::new(),
            pressed_mouse_buttons: HashSet::new(),
            pressed_gamepad_buttons: HashSet::new(),
            mouse_axes: HashMap::new(),
            gamepad_axes: HashMap::new(),
            just_pressed_keys: Vec::new(),
            just_captured_inputs: Vec::new(),
        }
    }

    pub(crate) fn poll_gamepads(&mut self) {
        let Some(gamepad) = self.gamepad.as_mut() else {
            return;
        };

        while let Some(event) = gamepad.next_event() {
            let device = format!("{:?}", event.id);
            match event.event {
                GilrsEventType::Connected => {
                    log::info!("gamepad connected: {device}");
                },
                GilrsEventType::Disconnected => {
                    self.pressed_gamepad_buttons.retain(|(d, _)| d != &device);
                    self.gamepad_axes.retain(|(d, _), _| d != &device);
                },
                GilrsEventType::ButtonPressed(button, _) => {
                    if let Some(button) = gamepad_button_from_gilrs(button) {
                        self.pressed_gamepad_buttons.insert((device.clone(), button));
                        self.just_captured_inputs.push(InputPath::GamepadButton {
                            device: DeviceSelector::Exact(device),
                            button,
                        });
                    }
                },
                GilrsEventType::ButtonReleased(button, _) => {
                    if let Some(button) = gamepad_button_from_gilrs(button) {
                        self.pressed_gamepad_buttons.remove(&(device, button));
                    }
                },
                GilrsEventType::AxisChanged(axis, value, _) => {
                    if let Some(axis) = gamepad_axis_from_gilrs(axis) {
                        self.gamepad_axes.insert((device.clone(), axis), value);
                        if value.abs() >= GAMEPAD_AXIS_CAPTURE_THRESHOLD {
                            self.just_captured_inputs.push(InputPath::GamepadAxis {
                                device: DeviceSelector::Exact(device),
                                axis,
                            });
                        }
                    }
                },
                _ => {},
            }
        }
    }

    pub(crate) fn consume_window_event(&mut self, event: &WindowEvent) {
        match event {
            WindowEvent::KeyboardInput { device_id, event, .. } => {
                let PhysicalKey::Code(code) = event.physical_key else {
                    return;
                };
                let device = format!("{device_id:?}");
                match event.state {
                    ElementState::Pressed => {
                        if let Some(key) = keyboard_key_from_winit(code) {
                            if !event.repeat && self.pressed_keys.insert((device.clone(), key)) {
                                self.just_pressed_keys.push(key);
                                self.just_captured_inputs.push(InputPath::KeyboardKey {
                                    device: DeviceSelector::Exact(device),
                                    key,
                                });
                            }
                        }
                    },
                    ElementState::Released => {
                        if let Some(key) = keyboard_key_from_winit(code) {
                            self.pressed_keys.remove(&(device, key));
                        }
                    },
                }
            },
            WindowEvent::MouseInput { device_id, state, button, .. } => {
                let device = format!("{device_id:?}");
                match state {
                    ElementState::Pressed => {
                        if let Some(button) = mouse_button_from_winit(*button) {
                            if self.pressed_mouse_buttons.insert((device.clone(), button)) {
                                self.just_captured_inputs.push(InputPath::MouseButton {
                                    device: DeviceSelector::Exact(device),
                                    button,
                                });
                            }
                        }
                    },
                    ElementState::Released => {
                        if let Some(button) = mouse_button_from_winit(*button) {
                            self.pressed_mouse_buttons.remove(&(device, button));
                        }
                    },
                }
            },
            WindowEvent::MouseWheel { device_id, delta, .. } => {
                let device = format!("{device_id:?}");
                let (x, y) = match delta {
                    MouseScrollDelta::LineDelta(x, y) => (*x, *y),
                    MouseScrollDelta::PixelDelta(pos) => (pos.x as f32, pos.y as f32),
                };

                if x.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                    self.mouse_axes.insert((device.clone(), MouseAxis::WheelX), x);
                    self.just_captured_inputs.push(InputPath::MouseAxis {
                        device: DeviceSelector::Exact(device.clone()),
                        axis: MouseAxis::WheelX,
                    });
                }
                if y.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                    self.mouse_axes.insert((device.clone(), MouseAxis::WheelY), y);
                    self.just_captured_inputs.push(InputPath::MouseAxis {
                        device: DeviceSelector::Exact(device),
                        axis: MouseAxis::WheelY,
                    });
                }
            },
            _ => {},
        }
    }

    pub(crate) fn consume_device_event(
        &mut self,
        device_id: winit::event::DeviceId,
        event: DeviceEvent,
    ) {
        if let DeviceEvent::MouseMotion { delta } = event {
            let device = format!("{device_id:?}");
            let dx = delta.0 as f32;
            let dy = delta.1 as f32;
            if dx.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                self.mouse_axes.insert((device.clone(), MouseAxis::MotionX), dx);
                self.just_captured_inputs.push(InputPath::MouseAxis {
                    device: DeviceSelector::Exact(device.clone()),
                    axis: MouseAxis::MotionX,
                });
            }
            if dy.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                self.mouse_axes.insert((device.clone(), MouseAxis::MotionY), dy);
                self.just_captured_inputs.push(InputPath::MouseAxis {
                    device: DeviceSelector::Exact(device),
                    axis: MouseAxis::MotionY,
                });
            }
        }
    }

    pub(crate) fn binding_actions(&self) -> Vec<UiAction> {
        let mut out = Vec::with_capacity(self.just_captured_inputs.len() + 3);
        out.extend(
            self.just_captured_inputs
                .iter()
                .filter(|input| !is_prompt_control_input(input))
                .cloned()
                .map(UiAction::Suggest),
        );
        if self.just_pressed_keys.contains(&KeyboardKey::Enter) {
            out.push(UiAction::Confirm);
        }
        if self.just_pressed_keys.contains(&KeyboardKey::Backspace) {
            out.push(UiAction::Skip);
        }
        if self.just_pressed_keys.contains(&KeyboardKey::Tab) {
            out.push(UiAction::ToggleDeviceScope);
        }
        out
    }

    pub(crate) fn read_binding_value(&self, path: &InputPath) -> f32 {
        match path {
            InputPath::KeyboardKey { device, key } => {
                if self
                    .pressed_keys
                    .iter()
                    .any(|(device_id, pressed)| device.matches(device_id) && pressed == key)
                {
                    1.0
                } else {
                    0.0
                }
            },
            InputPath::MouseButton { device, button } => {
                if self
                    .pressed_mouse_buttons
                    .iter()
                    .any(|(device_id, pressed)| device.matches(device_id) && pressed == button)
                {
                    1.0
                } else {
                    0.0
                }
            },
            InputPath::MouseAxis { device, axis } => self
                .mouse_axes
                .iter()
                .filter(|((device_id, current_axis), _)| {
                    device.matches(device_id) && current_axis == axis
                })
                .map(|(_, value)| *value)
                .max_by(|a, b| a.abs().total_cmp(&b.abs()))
                .unwrap_or(0.0),
            InputPath::GamepadButton { device, button } => {
                if self
                    .pressed_gamepad_buttons
                    .iter()
                    .any(|(device_id, pressed)| device.matches(device_id) && pressed == button)
                {
                    1.0
                } else {
                    0.0
                }
            },
            InputPath::GamepadAxis { device, axis } => self
                .gamepad_axes
                .iter()
                .filter(|((device_id, current_axis), _)| {
                    device.matches(device_id) && current_axis == axis
                })
                .map(|(_, value)| *value)
                .max_by(|a, b| a.abs().total_cmp(&b.abs()))
                .unwrap_or(0.0),
        }
    }

    pub(crate) fn end_frame(&mut self) {
        self.just_pressed_keys.clear();
        self.just_captured_inputs.clear();
        self.mouse_axes.clear();
    }

    pub(crate) fn clear_active_inputs(&mut self) {
        self.pressed_keys.clear();
        self.pressed_mouse_buttons.clear();
        self.pressed_gamepad_buttons.clear();
        self.mouse_axes.clear();
        self.gamepad_axes.clear();
        self.just_pressed_keys.clear();
        self.just_captured_inputs.clear();
    }
}

fn is_prompt_control_input(input: &InputPath) -> bool {
    matches!(
        input,
        InputPath::KeyboardKey {
            key: KeyboardKey::Enter
                | KeyboardKey::Backspace
                | KeyboardKey::Tab
                | KeyboardKey::Escape,
            ..
        }
    )
}

fn keyboard_key_from_winit(code: winit::keyboard::KeyCode) -> Option<KeyboardKey> {
    Some(match code {
        winit::keyboard::KeyCode::Digit0 => KeyboardKey::Digit0,
        winit::keyboard::KeyCode::Digit1 => KeyboardKey::Digit1,
        winit::keyboard::KeyCode::Digit2 => KeyboardKey::Digit2,
        winit::keyboard::KeyCode::Digit3 => KeyboardKey::Digit3,
        winit::keyboard::KeyCode::Digit4 => KeyboardKey::Digit4,
        winit::keyboard::KeyCode::Digit5 => KeyboardKey::Digit5,
        winit::keyboard::KeyCode::Digit6 => KeyboardKey::Digit6,
        winit::keyboard::KeyCode::Digit7 => KeyboardKey::Digit7,
        winit::keyboard::KeyCode::Digit8 => KeyboardKey::Digit8,
        winit::keyboard::KeyCode::Digit9 => KeyboardKey::Digit9,
        winit::keyboard::KeyCode::KeyA => KeyboardKey::KeyA,
        winit::keyboard::KeyCode::KeyB => KeyboardKey::KeyB,
        winit::keyboard::KeyCode::KeyC => KeyboardKey::KeyC,
        winit::keyboard::KeyCode::KeyD => KeyboardKey::KeyD,
        winit::keyboard::KeyCode::KeyE => KeyboardKey::KeyE,
        winit::keyboard::KeyCode::KeyF => KeyboardKey::KeyF,
        winit::keyboard::KeyCode::KeyG => KeyboardKey::KeyG,
        winit::keyboard::KeyCode::KeyH => KeyboardKey::KeyH,
        winit::keyboard::KeyCode::KeyI => KeyboardKey::KeyI,
        winit::keyboard::KeyCode::KeyJ => KeyboardKey::KeyJ,
        winit::keyboard::KeyCode::KeyK => KeyboardKey::KeyK,
        winit::keyboard::KeyCode::KeyL => KeyboardKey::KeyL,
        winit::keyboard::KeyCode::KeyM => KeyboardKey::KeyM,
        winit::keyboard::KeyCode::KeyN => KeyboardKey::KeyN,
        winit::keyboard::KeyCode::KeyO => KeyboardKey::KeyO,
        winit::keyboard::KeyCode::KeyP => KeyboardKey::KeyP,
        winit::keyboard::KeyCode::KeyQ => KeyboardKey::KeyQ,
        winit::keyboard::KeyCode::KeyR => KeyboardKey::KeyR,
        winit::keyboard::KeyCode::KeyS => KeyboardKey::KeyS,
        winit::keyboard::KeyCode::KeyT => KeyboardKey::KeyT,
        winit::keyboard::KeyCode::KeyU => KeyboardKey::KeyU,
        winit::keyboard::KeyCode::KeyV => KeyboardKey::KeyV,
        winit::keyboard::KeyCode::KeyW => KeyboardKey::KeyW,
        winit::keyboard::KeyCode::KeyX => KeyboardKey::KeyX,
        winit::keyboard::KeyCode::KeyY => KeyboardKey::KeyY,
        winit::keyboard::KeyCode::KeyZ => KeyboardKey::KeyZ,
        winit::keyboard::KeyCode::Escape => KeyboardKey::Escape,
        winit::keyboard::KeyCode::F1 => KeyboardKey::F1,
        winit::keyboard::KeyCode::F2 => KeyboardKey::F2,
        winit::keyboard::KeyCode::F3 => KeyboardKey::F3,
        winit::keyboard::KeyCode::F4 => KeyboardKey::F4,
        winit::keyboard::KeyCode::F5 => KeyboardKey::F5,
        winit::keyboard::KeyCode::F6 => KeyboardKey::F6,
        winit::keyboard::KeyCode::F7 => KeyboardKey::F7,
        winit::keyboard::KeyCode::F8 => KeyboardKey::F8,
        winit::keyboard::KeyCode::F9 => KeyboardKey::F9,
        winit::keyboard::KeyCode::F10 => KeyboardKey::F10,
        winit::keyboard::KeyCode::F11 => KeyboardKey::F11,
        winit::keyboard::KeyCode::F12 => KeyboardKey::F12,
        winit::keyboard::KeyCode::PrintScreen => KeyboardKey::PrintScreen,
        winit::keyboard::KeyCode::ScrollLock => KeyboardKey::ScrollLock,
        winit::keyboard::KeyCode::Pause => KeyboardKey::Pause,
        winit::keyboard::KeyCode::Insert => KeyboardKey::Insert,
        winit::keyboard::KeyCode::Home => KeyboardKey::Home,
        winit::keyboard::KeyCode::Delete => KeyboardKey::Delete,
        winit::keyboard::KeyCode::End => KeyboardKey::End,
        winit::keyboard::KeyCode::PageDown => KeyboardKey::PageDown,
        winit::keyboard::KeyCode::PageUp => KeyboardKey::PageUp,
        winit::keyboard::KeyCode::ArrowLeft => KeyboardKey::ArrowLeft,
        winit::keyboard::KeyCode::ArrowUp => KeyboardKey::ArrowUp,
        winit::keyboard::KeyCode::ArrowRight => KeyboardKey::ArrowRight,
        winit::keyboard::KeyCode::ArrowDown => KeyboardKey::ArrowDown,
        winit::keyboard::KeyCode::Backspace => KeyboardKey::Backspace,
        winit::keyboard::KeyCode::Enter => KeyboardKey::Enter,
        winit::keyboard::KeyCode::Space => KeyboardKey::Space,
        winit::keyboard::KeyCode::CapsLock => KeyboardKey::CapsLock,
        winit::keyboard::KeyCode::NumLock => KeyboardKey::NumLock,
        winit::keyboard::KeyCode::Numpad0 => KeyboardKey::Numpad0,
        winit::keyboard::KeyCode::Numpad1 => KeyboardKey::Numpad1,
        winit::keyboard::KeyCode::Numpad2 => KeyboardKey::Numpad2,
        winit::keyboard::KeyCode::Numpad3 => KeyboardKey::Numpad3,
        winit::keyboard::KeyCode::Numpad4 => KeyboardKey::Numpad4,
        winit::keyboard::KeyCode::Numpad5 => KeyboardKey::Numpad5,
        winit::keyboard::KeyCode::Numpad6 => KeyboardKey::Numpad6,
        winit::keyboard::KeyCode::Numpad7 => KeyboardKey::Numpad7,
        winit::keyboard::KeyCode::Numpad8 => KeyboardKey::Numpad8,
        winit::keyboard::KeyCode::Numpad9 => KeyboardKey::Numpad9,
        winit::keyboard::KeyCode::NumpadAdd => KeyboardKey::NumpadAdd,
        winit::keyboard::KeyCode::NumpadComma => KeyboardKey::NumpadComma,
        winit::keyboard::KeyCode::NumpadDecimal => KeyboardKey::NumpadDecimal,
        winit::keyboard::KeyCode::NumpadDivide => KeyboardKey::NumpadDivide,
        winit::keyboard::KeyCode::NumpadEnter => KeyboardKey::NumpadEnter,
        winit::keyboard::KeyCode::NumpadEqual => KeyboardKey::NumpadEqual,
        winit::keyboard::KeyCode::NumpadMultiply => KeyboardKey::NumpadMultiply,
        winit::keyboard::KeyCode::NumpadSubtract => KeyboardKey::NumpadSubtract,
        winit::keyboard::KeyCode::Tab => KeyboardKey::Tab,
        winit::keyboard::KeyCode::ShiftLeft => KeyboardKey::ShiftLeft,
        winit::keyboard::KeyCode::ShiftRight => KeyboardKey::ShiftRight,
        winit::keyboard::KeyCode::ControlLeft => KeyboardKey::ControlLeft,
        winit::keyboard::KeyCode::ControlRight => KeyboardKey::ControlRight,
        winit::keyboard::KeyCode::AltLeft => KeyboardKey::AltLeft,
        winit::keyboard::KeyCode::AltRight => KeyboardKey::AltRight,
        winit::keyboard::KeyCode::SuperLeft => KeyboardKey::SuperLeft,
        winit::keyboard::KeyCode::SuperRight => KeyboardKey::SuperRight,
        winit::keyboard::KeyCode::ContextMenu => KeyboardKey::ContextMenu,
        winit::keyboard::KeyCode::Semicolon => KeyboardKey::Semicolon,
        winit::keyboard::KeyCode::Equal => KeyboardKey::Equal,
        winit::keyboard::KeyCode::Comma => KeyboardKey::Comma,
        winit::keyboard::KeyCode::Minus => KeyboardKey::Minus,
        winit::keyboard::KeyCode::Period => KeyboardKey::Period,
        winit::keyboard::KeyCode::Slash => KeyboardKey::Slash,
        winit::keyboard::KeyCode::Backquote => KeyboardKey::Backquote,
        winit::keyboard::KeyCode::BracketLeft => KeyboardKey::BracketLeft,
        winit::keyboard::KeyCode::Backslash => KeyboardKey::Backslash,
        winit::keyboard::KeyCode::BracketRight => KeyboardKey::BracketRight,
        winit::keyboard::KeyCode::Quote => KeyboardKey::Quote,
        _ => return None,
    })
}

fn mouse_button_from_winit(button: MouseButton) -> Option<MouseButtonKind> {
    Some(match button {
        MouseButton::Left => MouseButtonKind::Left,
        MouseButton::Right => MouseButtonKind::Right,
        MouseButton::Middle => MouseButtonKind::Middle,
        MouseButton::Back => MouseButtonKind::Back,
        MouseButton::Forward => MouseButtonKind::Forward,
        MouseButton::Other(value) => MouseButtonKind::Other(value),
    })
}

fn gamepad_button_from_gilrs(button: GilrsButton) -> Option<GamepadButton> {
    match button {
        GilrsButton::South => Some(GamepadButton::South),
        GilrsButton::East => Some(GamepadButton::East),
        GilrsButton::North => Some(GamepadButton::North),
        GilrsButton::West => Some(GamepadButton::West),
        GilrsButton::C => Some(GamepadButton::C),
        GilrsButton::Z => Some(GamepadButton::Z),
        GilrsButton::LeftTrigger => Some(GamepadButton::LeftTrigger),
        GilrsButton::LeftTrigger2 => Some(GamepadButton::LeftTrigger2),
        GilrsButton::RightTrigger => Some(GamepadButton::RightTrigger),
        GilrsButton::RightTrigger2 => Some(GamepadButton::RightTrigger2),
        GilrsButton::Select => Some(GamepadButton::Select),
        GilrsButton::Start => Some(GamepadButton::Start),
        GilrsButton::Mode => Some(GamepadButton::Mode),
        GilrsButton::LeftThumb => Some(GamepadButton::LeftThumb),
        GilrsButton::RightThumb => Some(GamepadButton::RightThumb),
        GilrsButton::DPadUp => Some(GamepadButton::DPadUp),
        GilrsButton::DPadDown => Some(GamepadButton::DPadDown),
        GilrsButton::DPadLeft => Some(GamepadButton::DPadLeft),
        GilrsButton::DPadRight => Some(GamepadButton::DPadRight),
        GilrsButton::Unknown => None,
    }
}

fn gamepad_axis_from_gilrs(axis: GilrsAxis) -> Option<GamepadAxis> {
    match axis {
        GilrsAxis::LeftStickX => Some(GamepadAxis::LeftStickX),
        GilrsAxis::LeftStickY => Some(GamepadAxis::LeftStickY),
        GilrsAxis::LeftZ => Some(GamepadAxis::LeftZ),
        GilrsAxis::RightStickX => Some(GamepadAxis::RightStickX),
        GilrsAxis::RightStickY => Some(GamepadAxis::RightStickY),
        GilrsAxis::RightZ => Some(GamepadAxis::RightZ),
        GilrsAxis::DPadX => Some(GamepadAxis::DPadX),
        GilrsAxis::DPadY => Some(GamepadAxis::DPadY),
        GilrsAxis::Unknown => None,
    }
}
