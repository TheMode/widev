use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use anyhow::Result;
use gilrs::{Axis as GilrsAxis, Button as GilrsButton, EventType as GilrsEventType, Gilrs};
use winit::event::{DeviceEvent, ElementState, MouseButton, MouseScrollDelta, WindowEvent};
use winit::keyboard::{KeyCode, PhysicalKey};

use super::persistence::BindingStore;
use super::protocol;

const INPUT_RESEND_EVERY_FRAMES: u16 = 8;
const GAMEPAD_AXIS_CAPTURE_THRESHOLD: f32 = 0.35;
const MOUSE_AXIS_CAPTURE_THRESHOLD: f32 = 0.1;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum DeviceSelector {
    Any,
    Exact(String),
}

impl DeviceSelector {
    pub(super) fn matches(&self, device_id: &str) -> bool {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum MouseAxis {
    MotionX,
    MotionY,
    WheelX,
    WheelY,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum GamepadButton {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum GamepadAxis {
    LeftStickX,
    LeftStickY,
    LeftZ,
    RightStickX,
    RightStickY,
    RightZ,
    DPadX,
    DPadY,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum InputPath {
    KeyboardKey {
        device: DeviceSelector,
        key: KeyCode,
    },
    MouseButton {
        device: DeviceSelector,
        button: MouseButton,
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

impl InputPath {
    pub(super) fn with_device_scope(&self, any_device: bool) -> Self {
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

    fn is_toggle_compatible(&self) -> bool {
        matches!(
            self,
            Self::KeyboardKey { .. } | Self::MouseButton { .. } | Self::GamepadButton { .. }
        )
    }

    fn is_axis_1d_compatible(&self) -> bool {
        matches!(self, Self::MouseAxis { .. } | Self::GamepadAxis { .. })
    }
}

impl fmt::Display for InputPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyboardKey { device, key } => {
                write!(f, "keyboard/{}/key/{key:?}", device.as_path())
            },
            Self::MouseButton { device, button } => {
                write!(f, "mouse/{}/button/{}", device.as_path(), mouse_button_name(*button))
            },
            Self::MouseAxis { device, axis } => {
                write!(f, "mouse/{}/axis/{}", device.as_path(), mouse_axis_name(*axis))
            },
            Self::GamepadButton { device, button } => {
                write!(f, "gamepad/{}/button/{}", device.as_path(), gamepad_button_name(*button))
            },
            Self::GamepadAxis { device, axis } => {
                write!(f, "gamepad/{}/axis/{}", device.as_path(), gamepad_axis_name(*axis))
            },
        }
    }
}

pub(super) struct BindingPromptState {
    pub(super) identifier: String,
    pub(super) input_type: protocol::InputType,
    pub(super) suggestion: Option<InputPath>,
    pub(super) any_device_scope: bool,
    pub(super) allows_toggle: bool,
    pub(super) allows_axis: bool,
    pub(super) joystick_scalar_fallback: bool,
}

impl BindingPromptState {
    pub(super) fn log_sections(&self) -> Vec<String> {
        let mut lines = Vec::new();
        lines.push(format!("Input type: {:?}", self.input_type));
        lines.push(format!(
            "Device scope: {}",
            if self.any_device_scope { "Any device (*)" } else { "Exact device id" }
        ));
        if self.allows_toggle {
            lines.push(
                "Compatible controls: keyboard keys, mouse buttons, gamepad buttons".to_string(),
            );
        }
        if self.allows_axis {
            lines.push("Compatible controls: mouse axes, gamepad axes".to_string());
        }
        if self.joystick_scalar_fallback {
            lines.push(
                "Note: packet transport is scalar-only today; Joystick2D currently degrades to axis-style capture."
                    .to_string(),
            );
        }
        lines
    }
}

#[derive(Clone)]
struct BindingDefinition {
    id: u16,
    identifier: String,
    input_type: protocol::InputType,
}

struct BindingAssignment {
    id: u16,
    input: InputPath,
    last_value: f32,
    frames_since_send: u16,
}

pub(super) struct ConfirmedBinding {
    pub(super) binding_id: u16,
    pub(super) identifier: String,
    pub(super) input: InputPath,
}

pub(super) enum DeclareBindingOutcome {
    Restored {
        binding_id: u16,
        input: InputPath,
        saved_path: String,
        identifier: String,
    },
    Pending,
}

pub(super) enum UiAction {
    Suggest(InputPath),
    Confirm,
    Skip,
    ToggleDeviceScope,
}

pub(super) struct BindingState {
    pending_bindings: VecDeque<BindingDefinition>,
    suggestion: Option<InputPath>,
    bind_any_device_scope: bool,
    active_bindings: Vec<BindingAssignment>,
    store: BindingStore,
}

impl BindingState {
    pub(super) fn new(store: BindingStore) -> Self {
        Self {
            pending_bindings: VecDeque::new(),
            suggestion: None,
            bind_any_device_scope: true,
            active_bindings: Vec::new(),
            store,
        }
    }

    pub(super) fn binding_count(&self, cert_fp: &str) -> usize {
        self.store.binding_count(cert_fp)
    }

    pub(super) fn binding_prompt(&self) -> Option<BindingPromptState> {
        let current = self.pending_bindings.front()?;
        let (allows_toggle, allows_axis, joystick_scalar_fallback) =
            prompt_capabilities(current.input_type);
        Some(BindingPromptState {
            identifier: current.identifier.clone(),
            input_type: current.input_type,
            suggestion: self.suggestion.clone(),
            any_device_scope: self.bind_any_device_scope,
            allows_toggle,
            allows_axis,
            joystick_scalar_fallback,
        })
    }

    pub(super) fn apply_ui_action(&mut self, action: UiAction) {
        match action {
            UiAction::Suggest(input) => self.suggest_input(input),
            UiAction::ToggleDeviceScope => self.bind_any_device_scope = !self.bind_any_device_scope,
            UiAction::Skip => {
                self.skip_binding();
            },
            UiAction::Confirm => {},
        }
    }

    pub(super) fn confirm_binding(
        &mut self,
        server_cert_fingerprint: Option<&str>,
    ) -> Result<Option<ConfirmedBinding>> {
        let Some(definition) = self.pending_bindings.pop_front() else {
            return Ok(None);
        };
        let Some(input) = self.suggestion.take() else {
            return Ok(None);
        };

        let scoped = input.with_device_scope(self.bind_any_device_scope);
        let persisted_path = scoped.to_string();

        if let Some(cert_fp) = server_cert_fingerprint {
            self.store.set_binding_path(cert_fp, &definition.identifier, persisted_path.clone());
            self.store.save()?;
        }

        self.activate_binding(definition.id, scoped.clone());

        Ok(Some(ConfirmedBinding {
            binding_id: definition.id,
            identifier: definition.identifier,
            input: scoped,
        }))
    }

    pub(super) fn skip_binding(&mut self) -> Option<String> {
        let skipped = self.pending_bindings.pop_front().map(|d| d.identifier);
        self.suggestion = None;
        skipped
    }

    pub(super) fn declare_binding(
        &mut self,
        cert_fp: Option<&str>,
        binding_id: u16,
        identifier: String,
        input_type: protocol::InputType,
    ) -> DeclareBindingOutcome {
        if let Some(cert_fp) = cert_fp {
            if let Some(saved_path) = self.store.get_binding_path(cert_fp, &identifier) {
                if let Some(saved_input) = parse_input_path(&saved_path) {
                    self.activate_binding(binding_id, saved_input.clone());
                    return DeclareBindingOutcome::Restored {
                        binding_id,
                        input: saved_input,
                        saved_path,
                        identifier,
                    };
                }
            }
        }

        self.pending_bindings.push_back(BindingDefinition {
            id: binding_id,
            identifier,
            input_type,
        });
        DeclareBindingOutcome::Pending
    }

    pub(super) fn sample_values<F>(&mut self, mut read_value: F) -> Vec<(u16, f32)>
    where
        F: FnMut(&InputPath) -> f32,
    {
        let mut outgoing = Vec::new();
        for binding in &mut self.active_bindings {
            binding.frames_since_send = binding.frames_since_send.saturating_add(1);
            let value = read_value(&binding.input).clamp(-1.0, 1.0);
            let changed = (value - binding.last_value).abs() >= f32::EPSILON;
            let should_resend = binding.frames_since_send >= INPUT_RESEND_EVERY_FRAMES;
            if !changed && !should_resend {
                continue;
            }

            binding.last_value = value;
            binding.frames_since_send = 0;
            outgoing.push((binding.id, value));
        }
        outgoing
    }

    fn suggest_input(&mut self, input: InputPath) {
        let Some(current) = self.pending_bindings.front() else {
            return;
        };
        if !is_path_compatible(&input, current.input_type) {
            return;
        }
        self.suggestion = Some(input);
    }

    fn activate_binding(&mut self, id: u16, input: InputPath) {
        self.active_bindings.push(BindingAssignment {
            id,
            input,
            last_value: 0.0,
            frames_since_send: 0,
        });
    }
}

pub(super) struct InputCapture {
    gamepad: Option<Gilrs>,
    pressed_keys: HashSet<(String, KeyCode)>,
    pressed_mouse_buttons: HashSet<(String, MouseButton)>,
    pressed_gamepad_buttons: HashSet<(String, GamepadButton)>,
    mouse_axes: HashMap<(String, MouseAxis), f32>,
    gamepad_axes: HashMap<(String, GamepadAxis), f32>,
    just_pressed_keys: Vec<KeyCode>,
    just_captured_inputs: Vec<InputPath>,
}

impl InputCapture {
    pub(super) fn new() -> Self {
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

    pub(super) fn poll_gamepads(&mut self) {
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

    pub(super) fn consume_window_event(&mut self, event: &WindowEvent) {
        match event {
            WindowEvent::KeyboardInput { device_id, event, .. } => {
                let PhysicalKey::Code(code) = event.physical_key else {
                    return;
                };
                let device = format!("{device_id:?}");
                match event.state {
                    ElementState::Pressed => {
                        if !event.repeat && self.pressed_keys.insert((device.clone(), code)) {
                            self.just_pressed_keys.push(code);
                            self.just_captured_inputs.push(InputPath::KeyboardKey {
                                device: DeviceSelector::Exact(device),
                                key: code,
                            });
                        }
                    },
                    ElementState::Released => {
                        self.pressed_keys.remove(&(device, code));
                    },
                }
            },
            WindowEvent::MouseInput { device_id, state, button, .. } => {
                let device = format!("{device_id:?}");
                match state {
                    ElementState::Pressed => {
                        if self.pressed_mouse_buttons.insert((device.clone(), *button)) {
                            self.just_captured_inputs.push(InputPath::MouseButton {
                                device: DeviceSelector::Exact(device),
                                button: *button,
                            });
                        }
                    },
                    ElementState::Released => {
                        self.pressed_mouse_buttons.remove(&(device, *button));
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

    pub(super) fn consume_device_event(
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

    pub(super) fn binding_actions(&self) -> Vec<UiAction> {
        let mut out = Vec::with_capacity(self.just_captured_inputs.len() + 3);
        out.extend(
            self.just_captured_inputs
                .iter()
                .filter(|input| !is_prompt_control_input(input))
                .cloned()
                .map(UiAction::Suggest),
        );
        if self.just_pressed_keys.contains(&KeyCode::Enter) {
            out.push(UiAction::Confirm);
        }
        if self.just_pressed_keys.contains(&KeyCode::Backspace) {
            out.push(UiAction::Skip);
        }
        if self.just_pressed_keys.contains(&KeyCode::Tab) {
            out.push(UiAction::ToggleDeviceScope);
        }
        out
    }

    pub(super) fn is_exit_requested(&self) -> bool {
        self.pressed_keys.iter().any(|(_, code)| *code == KeyCode::Escape)
    }

    pub(super) fn read_binding_value(&self, path: &InputPath) -> f32 {
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

    pub(super) fn end_frame(&mut self) {
        self.just_pressed_keys.clear();
        self.just_captured_inputs.clear();
        self.mouse_axes.clear();
    }
}

fn is_prompt_control_input(input: &InputPath) -> bool {
    matches!(
        input,
        InputPath::KeyboardKey {
            key: KeyCode::Enter | KeyCode::Backspace | KeyCode::Tab | KeyCode::Escape,
            ..
        }
    )
}

fn is_path_compatible(path: &InputPath, input_type: protocol::InputType) -> bool {
    match input_type {
        protocol::InputType::Toggle => path.is_toggle_compatible(),
        protocol::InputType::Axis1D | protocol::InputType::Joystick2D => {
            path.is_axis_1d_compatible()
        },
    }
}

fn prompt_capabilities(input_type: protocol::InputType) -> (bool, bool, bool) {
    match input_type {
        protocol::InputType::Toggle => (true, false, false),
        protocol::InputType::Axis1D => (false, true, false),
        protocol::InputType::Joystick2D => (false, true, true),
    }
}

fn parse_input_path(path: &str) -> Option<InputPath> {
    if let Some(old_key) = parse_legacy_keyboard_path(path) {
        return Some(InputPath::KeyboardKey { device: DeviceSelector::Any, key: old_key });
    }

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
            key: parse_key_code(control_name)?,
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

fn parse_legacy_keyboard_path(path: &str) -> Option<KeyCode> {
    let raw = path.strip_prefix("keyboard.")?;
    parse_key_code(raw)
}

fn parse_key_code(raw: &str) -> Option<KeyCode> {
    all_keys().iter().copied().find(|key| format!("{key:?}") == raw)
}

fn parse_mouse_button(raw: &str) -> Option<MouseButton> {
    match raw {
        "Left" => Some(MouseButton::Left),
        "Right" => Some(MouseButton::Right),
        "Middle" => Some(MouseButton::Middle),
        "Back" => Some(MouseButton::Back),
        "Forward" => Some(MouseButton::Forward),
        _ => {
            let suffix = raw.strip_prefix("Other(")?.strip_suffix(')')?;
            let value = suffix.parse::<u16>().ok()?;
            Some(MouseButton::Other(value))
        },
    }
}

fn parse_mouse_axis(raw: &str) -> Option<MouseAxis> {
    match raw {
        "MotionX" => Some(MouseAxis::MotionX),
        "MotionY" => Some(MouseAxis::MotionY),
        "WheelX" => Some(MouseAxis::WheelX),
        "WheelY" => Some(MouseAxis::WheelY),
        _ => None,
    }
}

fn parse_gamepad_button(raw: &str) -> Option<GamepadButton> {
    all_gamepad_buttons().iter().copied().find(|button| gamepad_button_name(*button) == raw)
}

fn parse_gamepad_axis(raw: &str) -> Option<GamepadAxis> {
    all_gamepad_axes().iter().copied().find(|axis| gamepad_axis_name(*axis) == raw)
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

fn all_gamepad_buttons() -> &'static [GamepadButton] {
    const BUTTONS: &[GamepadButton] = &[
        GamepadButton::South,
        GamepadButton::East,
        GamepadButton::North,
        GamepadButton::West,
        GamepadButton::C,
        GamepadButton::Z,
        GamepadButton::LeftTrigger,
        GamepadButton::LeftTrigger2,
        GamepadButton::RightTrigger,
        GamepadButton::RightTrigger2,
        GamepadButton::Select,
        GamepadButton::Start,
        GamepadButton::Mode,
        GamepadButton::LeftThumb,
        GamepadButton::RightThumb,
        GamepadButton::DPadUp,
        GamepadButton::DPadDown,
        GamepadButton::DPadLeft,
        GamepadButton::DPadRight,
    ];

    BUTTONS
}

fn all_gamepad_axes() -> &'static [GamepadAxis] {
    const AXES: &[GamepadAxis] = &[
        GamepadAxis::LeftStickX,
        GamepadAxis::LeftStickY,
        GamepadAxis::LeftZ,
        GamepadAxis::RightStickX,
        GamepadAxis::RightStickY,
        GamepadAxis::RightZ,
        GamepadAxis::DPadX,
        GamepadAxis::DPadY,
    ];

    AXES
}

fn all_keys() -> &'static [KeyCode] {
    const ALL_KEYS: &[KeyCode] = &[
        KeyCode::Digit0,
        KeyCode::Digit1,
        KeyCode::Digit2,
        KeyCode::Digit3,
        KeyCode::Digit4,
        KeyCode::Digit5,
        KeyCode::Digit6,
        KeyCode::Digit7,
        KeyCode::Digit8,
        KeyCode::Digit9,
        KeyCode::KeyA,
        KeyCode::KeyB,
        KeyCode::KeyC,
        KeyCode::KeyD,
        KeyCode::KeyE,
        KeyCode::KeyF,
        KeyCode::KeyG,
        KeyCode::KeyH,
        KeyCode::KeyI,
        KeyCode::KeyJ,
        KeyCode::KeyK,
        KeyCode::KeyL,
        KeyCode::KeyM,
        KeyCode::KeyN,
        KeyCode::KeyO,
        KeyCode::KeyP,
        KeyCode::KeyQ,
        KeyCode::KeyR,
        KeyCode::KeyS,
        KeyCode::KeyT,
        KeyCode::KeyU,
        KeyCode::KeyV,
        KeyCode::KeyW,
        KeyCode::KeyX,
        KeyCode::KeyY,
        KeyCode::KeyZ,
        KeyCode::F1,
        KeyCode::F2,
        KeyCode::F3,
        KeyCode::F4,
        KeyCode::F5,
        KeyCode::F6,
        KeyCode::F7,
        KeyCode::F8,
        KeyCode::F9,
        KeyCode::F10,
        KeyCode::F11,
        KeyCode::F12,
        KeyCode::ArrowUp,
        KeyCode::ArrowDown,
        KeyCode::ArrowLeft,
        KeyCode::ArrowRight,
        KeyCode::Space,
        KeyCode::Enter,
        KeyCode::Escape,
        KeyCode::Backspace,
        KeyCode::Tab,
        KeyCode::ShiftLeft,
        KeyCode::ShiftRight,
        KeyCode::ControlLeft,
        KeyCode::ControlRight,
        KeyCode::AltLeft,
        KeyCode::AltRight,
        KeyCode::SuperLeft,
        KeyCode::SuperRight,
    ];

    ALL_KEYS
}

fn mouse_button_name(button: MouseButton) -> &'static str {
    match button {
        MouseButton::Left => "Left",
        MouseButton::Right => "Right",
        MouseButton::Middle => "Middle",
        MouseButton::Back => "Back",
        MouseButton::Forward => "Forward",
        MouseButton::Other(_) => "Other",
    }
}

fn mouse_axis_name(axis: MouseAxis) -> &'static str {
    match axis {
        MouseAxis::MotionX => "MotionX",
        MouseAxis::MotionY => "MotionY",
        MouseAxis::WheelX => "WheelX",
        MouseAxis::WheelY => "WheelY",
    }
}

fn gamepad_button_name(button: GamepadButton) -> &'static str {
    match button {
        GamepadButton::South => "South",
        GamepadButton::East => "East",
        GamepadButton::North => "North",
        GamepadButton::West => "West",
        GamepadButton::C => "C",
        GamepadButton::Z => "Z",
        GamepadButton::LeftTrigger => "LeftTrigger",
        GamepadButton::LeftTrigger2 => "LeftTrigger2",
        GamepadButton::RightTrigger => "RightTrigger",
        GamepadButton::RightTrigger2 => "RightTrigger2",
        GamepadButton::Select => "Select",
        GamepadButton::Start => "Start",
        GamepadButton::Mode => "Mode",
        GamepadButton::LeftThumb => "LeftThumb",
        GamepadButton::RightThumb => "RightThumb",
        GamepadButton::DPadUp => "DPadUp",
        GamepadButton::DPadDown => "DPadDown",
        GamepadButton::DPadLeft => "DPadLeft",
        GamepadButton::DPadRight => "DPadRight",
    }
}

fn gamepad_axis_name(axis: GamepadAxis) -> &'static str {
    match axis {
        GamepadAxis::LeftStickX => "LeftStickX",
        GamepadAxis::LeftStickY => "LeftStickY",
        GamepadAxis::LeftZ => "LeftZ",
        GamepadAxis::RightStickX => "RightStickX",
        GamepadAxis::RightStickY => "RightStickY",
        GamepadAxis::RightZ => "RightZ",
        GamepadAxis::DPadX => "DPadX",
        GamepadAxis::DPadY => "DPadY",
    }
}
