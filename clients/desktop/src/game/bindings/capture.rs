use std::collections::{HashMap, HashSet};

use gilrs::{Axis as GilrsAxis, Button as GilrsButton, EventType as GilrsEventType, Gilrs};
use winit::event::{DeviceEvent, ElementState, MouseButton, MouseScrollDelta, WindowEvent};
use winit::keyboard::PhysicalKey;

use super::model::{
    DeviceFilter, DeviceIdentity, DeviceType, GamepadAxis, GamepadButton, GamepadStick,
    InputDescriptor, KeyModifiers, KeyboardKey, MouseAxis, MouseButtonKind, RawSource,
};
use super::protocol;
use super::UiAction;
use winit::keyboard::ModifiersState;
#[cfg(target_os = "windows")]
use winit::platform::windows::DeviceIdExtWindows;

const GAMEPAD_AXIS_CAPTURE_THRESHOLD: f32 = 0.35;
const GAMEPAD_STICK_CAPTURE_THRESHOLD: f32 = 0.45;
const MOUSE_AXIS_CAPTURE_THRESHOLD: f32 = 0.1;

pub(crate) struct InputCapture {
    gamepad: Option<Gilrs>,
    current_modifiers: ModifiersState,
    pressed_keys: HashSet<(DeviceIdentity, KeyboardKey)>,
    pressed_mouse_buttons: HashSet<(DeviceIdentity, MouseButtonKind)>,
    pressed_gamepad_buttons: HashSet<(DeviceIdentity, GamepadButton)>,
    mouse_axes: HashMap<(DeviceIdentity, MouseAxis), f32>,
    gamepad_axes: HashMap<(DeviceIdentity, GamepadAxis), f32>,
    just_pressed_keys: Vec<KeyboardKey>,
    just_captured_sources: Vec<RawSource>,
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
            current_modifiers: ModifiersState::empty(),
            pressed_keys: HashSet::new(),
            pressed_mouse_buttons: HashSet::new(),
            pressed_gamepad_buttons: HashSet::new(),
            mouse_axes: HashMap::new(),
            gamepad_axes: HashMap::new(),
            just_pressed_keys: Vec::new(),
            just_captured_sources: Vec::new(),
        }
    }

    pub(crate) fn poll_gamepads(&mut self) {
        let Some(gamepad) = self.gamepad.as_mut() else {
            return;
        };

        while let Some(event) = gamepad.next_event() {
            let gamepad_info = gamepad.gamepad(event.id);
            let device = gamepad_identity(gamepad_info.uuid(), Some(gamepad_info.os_name()));
            match event.event {
                GilrsEventType::Connected => {
                    log::info!("gamepad connected: {}", gamepad_info.os_name());
                },
                GilrsEventType::Disconnected => {
                    self.pressed_gamepad_buttons.retain(|(d, _)| d != &device);
                    self.gamepad_axes.retain(|(d, _), _| d != &device);
                },
                GilrsEventType::ButtonPressed(button, _) => {
                    if let Some(button) = gamepad_button_from_gilrs(button) {
                        self.pressed_gamepad_buttons.insert((device.clone(), button));
                        self.just_captured_sources.push(captured_source(
                            DeviceType::Gamepad,
                            device,
                            InputDescriptor::GamepadButton { button },
                        ));
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
                            self.just_captured_sources.push(captured_source(
                                DeviceType::Gamepad,
                                device.clone(),
                                InputDescriptor::GamepadAxis { axis },
                            ));
                        }
                        if let Some(stick) = GamepadStick::from_axis(axis) {
                            let magnitude =
                                read_gamepad_stick_magnitude(&self.gamepad_axes, &device, stick);
                            if magnitude >= GAMEPAD_STICK_CAPTURE_THRESHOLD {
                                self.just_captured_sources.push(captured_source(
                                    DeviceType::Gamepad,
                                    device,
                                    InputDescriptor::Stick { stick },
                                ));
                            }
                        }
                    }
                },
                _ => {},
            }
        }
    }

    pub(crate) fn consume_window_event(&mut self, event: &WindowEvent) {
        match event {
            WindowEvent::ModifiersChanged(modifiers) => {
                self.current_modifiers = modifiers.state();
            },
            WindowEvent::KeyboardInput { device_id, event, .. } => {
                let PhysicalKey::Code(code) = event.physical_key else {
                    return;
                };
                let device = keyboard_mouse_identity(&device_id);
                match event.state {
                    ElementState::Pressed => {
                        if let Some(key) = keyboard_key_from_winit(code) {
                            if !event.repeat && self.pressed_keys.insert((device.clone(), key)) {
                                self.just_pressed_keys.push(key);
                                let input = if is_modifier_key(key) {
                                    InputDescriptor::Key { code: key }
                                } else {
                                    let modifiers =
                                        key_modifiers_from_state(self.current_modifiers);
                                    if modifiers.is_empty() {
                                        InputDescriptor::Key { code: key }
                                    } else {
                                        InputDescriptor::KeyChord { key, modifiers }
                                    }
                                };
                                self.just_captured_sources.push(captured_source(
                                    DeviceType::Keyboard,
                                    device,
                                    input,
                                ));
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
                let device = keyboard_mouse_identity(&device_id);
                match state {
                    ElementState::Pressed => {
                        if let Some(button) = mouse_button_from_winit(*button) {
                            if self.pressed_mouse_buttons.insert((device.clone(), button)) {
                                self.just_captured_sources.push(captured_source(
                                    DeviceType::Mouse,
                                    device,
                                    InputDescriptor::MouseButton { button },
                                ));
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
                let device = keyboard_mouse_identity(&device_id);
                let (x, y) = match delta {
                    MouseScrollDelta::LineDelta(x, y) => (*x, *y),
                    MouseScrollDelta::PixelDelta(pos) => (pos.x as f32, pos.y as f32),
                };

                if x.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                    self.mouse_axes.insert((device.clone(), MouseAxis::WheelX), x);
                    self.just_captured_sources.push(captured_source(
                        DeviceType::Mouse,
                        device.clone(),
                        InputDescriptor::MouseAxis { axis: MouseAxis::WheelX },
                    ));
                }
                if y.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                    self.mouse_axes.insert((device.clone(), MouseAxis::WheelY), y);
                    self.just_captured_sources.push(captured_source(
                        DeviceType::Mouse,
                        device,
                        InputDescriptor::MouseAxis { axis: MouseAxis::WheelY },
                    ));
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
            let device = keyboard_mouse_identity(&device_id);
            let dx = delta.0 as f32;
            let dy = delta.1 as f32;
            if dx.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                self.mouse_axes.insert((device.clone(), MouseAxis::MotionX), dx);
                self.just_captured_sources.push(captured_source(
                    DeviceType::Mouse,
                    device.clone(),
                    InputDescriptor::MouseAxis { axis: MouseAxis::MotionX },
                ));
            }
            if dy.abs() >= MOUSE_AXIS_CAPTURE_THRESHOLD {
                self.mouse_axes.insert((device.clone(), MouseAxis::MotionY), dy);
                self.just_captured_sources.push(captured_source(
                    DeviceType::Mouse,
                    device,
                    InputDescriptor::MouseAxis { axis: MouseAxis::MotionY },
                ));
            }
        }
    }

    pub(crate) fn binding_actions(&self) -> Vec<UiAction> {
        let mut out = Vec::with_capacity(self.just_captured_sources.len() + 3);
        out.extend(
            self.just_captured_sources
                .iter()
                .filter(|source| !is_prompt_control_input(source))
                .cloned()
                .map(UiAction::Suggest),
        );
        if self.current_modifiers.is_empty() && self.just_pressed_keys.contains(&KeyboardKey::Enter)
        {
            out.push(UiAction::Confirm);
        }
        if self.current_modifiers.is_empty()
            && self.just_pressed_keys.contains(&KeyboardKey::Backspace)
        {
            out.push(UiAction::Skip);
        }
        if self.current_modifiers.is_empty() && self.just_pressed_keys.contains(&KeyboardKey::Tab) {
            out.push(UiAction::ToggleDeviceScope);
        }
        out
    }

    pub(crate) fn read_binding_value(&self, source: &RawSource) -> protocol::InputPayload {
        match &source.input {
            InputDescriptor::Key { code } => protocol::InputPayload::Toggle {
                pressed: self.read_pressed_key(&source.device, *code),
            },
            InputDescriptor::MouseButton { button } => protocol::InputPayload::Toggle {
                pressed: self.read_pressed_mouse_button(&source.device, *button),
            },
            InputDescriptor::MouseAxis { axis } => protocol::InputPayload::Axis1D {
                value: self.read_mouse_axis(&source.device, *axis),
            },
            InputDescriptor::GamepadButton { button } => protocol::InputPayload::Toggle {
                pressed: self.read_pressed_gamepad_button(&source.device, *button),
            },
            InputDescriptor::GamepadAxis { axis } => protocol::InputPayload::Axis1D {
                value: self.read_gamepad_axis(&source.device, *axis),
            },
            InputDescriptor::KeyChord { key, modifiers } => protocol::InputPayload::Toggle {
                pressed: self.read_pressed_key_chord(&source.device, *key, *modifiers),
            },
            InputDescriptor::VirtualStick { positive_x, negative_x, positive_y, negative_y } => {
                let x =
                    axis_from_keys(&self.pressed_keys, &source.device, *positive_x, *negative_x);
                let y =
                    axis_from_keys(&self.pressed_keys, &source.device, *positive_y, *negative_y);
                protocol::InputPayload::Joystick2D { x, y }
            },
            InputDescriptor::Stick { stick } => {
                let (x_axis, y_axis) = stick.axes();
                protocol::InputPayload::Joystick2D {
                    x: self.read_gamepad_axis(&source.device, x_axis),
                    y: self.read_gamepad_axis(&source.device, y_axis),
                }
            },
        }
    }

    pub(crate) fn end_frame(&mut self) {
        self.just_pressed_keys.clear();
        self.just_captured_sources.clear();
        self.mouse_axes.clear();
    }

    pub(crate) fn clear_active_inputs(&mut self) {
        self.current_modifiers = ModifiersState::empty();
        self.pressed_keys.clear();
        self.pressed_mouse_buttons.clear();
        self.pressed_gamepad_buttons.clear();
        self.mouse_axes.clear();
        self.gamepad_axes.clear();
        self.just_pressed_keys.clear();
        self.just_captured_sources.clear();
    }

    fn read_pressed_key(&self, filter: &DeviceFilter, key: KeyboardKey) -> bool {
        self.pressed_keys.iter().any(|(device_id, pressed)| {
            filter.matches(DeviceType::Keyboard, device_id) && pressed == &key
        })
    }

    fn read_pressed_mouse_button(&self, filter: &DeviceFilter, button: MouseButtonKind) -> bool {
        self.pressed_mouse_buttons.iter().any(|(device_id, pressed)| {
            filter.matches(DeviceType::Mouse, device_id) && pressed == &button
        })
    }

    fn read_pressed_gamepad_button(&self, filter: &DeviceFilter, button: GamepadButton) -> bool {
        self.pressed_gamepad_buttons.iter().any(|(device_id, pressed)| {
            filter.matches(DeviceType::Gamepad, device_id) && pressed == &button
        })
    }

    fn read_pressed_key_chord(
        &self,
        filter: &DeviceFilter,
        key: KeyboardKey,
        modifiers: KeyModifiers,
    ) -> bool {
        self.read_pressed_key(filter, key) && self.read_required_modifiers(filter, modifiers)
    }

    fn read_required_modifiers(&self, filter: &DeviceFilter, modifiers: KeyModifiers) -> bool {
        (!modifiers.shift
            || self.read_pressed_modifier(filter, KeyboardKey::ShiftLeft, KeyboardKey::ShiftRight))
            && (!modifiers.control
                || self.read_pressed_modifier(
                    filter,
                    KeyboardKey::ControlLeft,
                    KeyboardKey::ControlRight,
                ))
            && (!modifiers.alt
                || self.read_pressed_modifier(filter, KeyboardKey::AltLeft, KeyboardKey::AltRight))
            && (!modifiers.super_key
                || self.read_pressed_modifier(
                    filter,
                    KeyboardKey::SuperLeft,
                    KeyboardKey::SuperRight,
                ))
    }

    fn read_pressed_modifier(
        &self,
        filter: &DeviceFilter,
        left: KeyboardKey,
        right: KeyboardKey,
    ) -> bool {
        self.pressed_keys.iter().any(|(device_id, pressed)| {
            filter.matches(DeviceType::Keyboard, device_id)
                && (*pressed == left || *pressed == right)
        })
    }

    fn read_mouse_axis(&self, filter: &DeviceFilter, axis: MouseAxis) -> f32 {
        self.mouse_axes
            .iter()
            .filter(|((device_id, current_axis), _)| {
                filter.matches(DeviceType::Mouse, device_id) && current_axis == &axis
            })
            .map(|(_, value)| *value)
            .max_by(|a, b| a.abs().total_cmp(&b.abs()))
            .unwrap_or(0.0)
    }

    fn read_gamepad_axis(&self, filter: &DeviceFilter, axis: GamepadAxis) -> f32 {
        self.gamepad_axes
            .iter()
            .filter(|((device_id, current_axis), _)| {
                filter.matches(DeviceType::Gamepad, device_id) && current_axis == &axis
            })
            .map(|(_, value)| *value)
            .max_by(|a, b| a.abs().total_cmp(&b.abs()))
            .unwrap_or(0.0)
    }
}

fn axis_from_keys(
    pressed_keys: &HashSet<(DeviceIdentity, KeyboardKey)>,
    filter: &DeviceFilter,
    positive: KeyboardKey,
    negative: KeyboardKey,
) -> f32 {
    let positive_pressed = pressed_keys.iter().any(|(device_id, key)| {
        filter.matches(DeviceType::Keyboard, device_id) && key == &positive
    });
    let negative_pressed = pressed_keys.iter().any(|(device_id, key)| {
        filter.matches(DeviceType::Keyboard, device_id) && key == &negative
    });
    (positive_pressed as i8 - negative_pressed as i8) as f32
}

fn key_modifiers_from_state(state: ModifiersState) -> KeyModifiers {
    KeyModifiers {
        shift: state.shift_key(),
        control: state.control_key(),
        alt: state.alt_key(),
        super_key: state.super_key(),
    }
}

fn captured_source(
    device_type: DeviceType,
    identity: DeviceIdentity,
    input: InputDescriptor,
) -> RawSource {
    RawSource { device: DeviceFilter::exact_identity(device_type, identity), input }
}

fn read_gamepad_stick_magnitude(
    gamepad_axes: &HashMap<(DeviceIdentity, GamepadAxis), f32>,
    device_id: &DeviceIdentity,
    stick: GamepadStick,
) -> f32 {
    let (x_axis, y_axis) = stick.axes();
    let x = gamepad_axes.get(&(device_id.clone(), x_axis)).copied().unwrap_or(0.0);
    let y = gamepad_axes.get(&(device_id.clone(), y_axis)).copied().unwrap_or(0.0);
    x.hypot(y)
}

fn is_prompt_control_input(source: &RawSource) -> bool {
    matches!(
        source.input,
        InputDescriptor::Key {
            code: KeyboardKey::Enter
                | KeyboardKey::Backspace
                | KeyboardKey::Tab
                | KeyboardKey::Escape,
        }
    )
}

fn is_modifier_key(key: KeyboardKey) -> bool {
    matches!(
        key,
        KeyboardKey::ShiftLeft
            | KeyboardKey::ShiftRight
            | KeyboardKey::ControlLeft
            | KeyboardKey::ControlRight
            | KeyboardKey::AltLeft
            | KeyboardKey::AltRight
            | KeyboardKey::SuperLeft
            | KeyboardKey::SuperRight
    )
}

fn keyboard_mouse_identity(device_id: &winit::event::DeviceId) -> DeviceIdentity {
    #[cfg(target_os = "windows")]
    {
        if let Some(value) = device_id.persistent_identifier() {
            return DeviceIdentity::WindowsPersistent { value };
        }
    }

    DeviceIdentity::session(format!("{device_id:?}"))
}

fn gamepad_identity(uuid: [u8; 16], label: Option<&str>) -> DeviceIdentity {
    DeviceIdentity::Gamepad { uuid, label: label.map(|label| label.to_string()) }
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
    Some(match button {
        GilrsButton::South => GamepadButton::South,
        GilrsButton::East => GamepadButton::East,
        GilrsButton::North => GamepadButton::North,
        GilrsButton::West => GamepadButton::West,
        GilrsButton::C => GamepadButton::C,
        GilrsButton::Z => GamepadButton::Z,
        GilrsButton::LeftTrigger => GamepadButton::LeftTrigger,
        GilrsButton::LeftTrigger2 => GamepadButton::LeftTrigger2,
        GilrsButton::RightTrigger => GamepadButton::RightTrigger,
        GilrsButton::RightTrigger2 => GamepadButton::RightTrigger2,
        GilrsButton::Select => GamepadButton::Select,
        GilrsButton::Start => GamepadButton::Start,
        GilrsButton::Mode => GamepadButton::Mode,
        GilrsButton::LeftThumb => GamepadButton::LeftThumb,
        GilrsButton::RightThumb => GamepadButton::RightThumb,
        GilrsButton::DPadUp => GamepadButton::DPadUp,
        GilrsButton::DPadDown => GamepadButton::DPadDown,
        GilrsButton::DPadLeft => GamepadButton::DPadLeft,
        GilrsButton::DPadRight => GamepadButton::DPadRight,
        _ => return None,
    })
}

fn gamepad_axis_from_gilrs(axis: GilrsAxis) -> Option<GamepadAxis> {
    Some(match axis {
        GilrsAxis::LeftStickX => GamepadAxis::LeftStickX,
        GilrsAxis::LeftStickY => GamepadAxis::LeftStickY,
        GilrsAxis::LeftZ => GamepadAxis::LeftZ,
        GilrsAxis::RightStickX => GamepadAxis::RightStickX,
        GilrsAxis::RightStickY => GamepadAxis::RightStickY,
        GilrsAxis::RightZ => GamepadAxis::RightZ,
        GilrsAxis::DPadX => GamepadAxis::DPadX,
        GilrsAxis::DPadY => GamepadAxis::DPadY,
        _ => return None,
    })
}
