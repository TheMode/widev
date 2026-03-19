mod capture;
mod model;

use std::collections::VecDeque;

use anyhow::Result;

pub(crate) use capture::InputCapture;
#[allow(unused_imports)]
pub(crate) use model::{
    ActionBinding, DeviceFilter, DeviceType, GamepadStick, InputDescriptor, KeyboardKey, RawSource,
};

use super::persistence::BindingStore;
use super::protocol;

pub(super) struct BindingPromptState {
    pub(super) identifier: String,
    pub(super) input_type: protocol::InputType,
    pub(super) suggestion: Option<String>,
    pub(super) any_device_scope: bool,
    pub(super) allows_toggle: bool,
    pub(super) allows_axis: bool,
    pub(super) allows_joystick: bool,
    pub(super) capture_hint: Option<String>,
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
        if self.allows_joystick {
            lines.push("Compatible controls: gamepad sticks, keyboard virtual sticks".to_string());
        }
        if let Some(hint) = &self.capture_hint {
            lines.push(hint.clone());
        }
        lines
    }
}

#[derive(Clone)]
struct PendingBinding {
    id: u16,
    identifier: String,
    input_type: protocol::InputType,
    capture: BindingCapture,
}

struct BindingAssignment {
    id: u16,
    binding: ActionBinding,
    last_value: protocol::InputPayload,
}

pub(super) struct ConfirmedBinding {
    pub(super) binding_id: u16,
    pub(super) identifier: String,
    pub(super) binding: ActionBinding,
}

pub(super) enum DeclareBindingOutcome {
    Restored {
        binding_id: u16,
        binding: ActionBinding,
        identifier: String,
    },
    Pending,
}

pub(super) enum UiAction {
    Suggest(RawSource),
    Confirm,
    Skip,
    ToggleDeviceScope,
}

#[derive(Clone)]
enum BindingCapture {
    Single {
        suggested_source: Option<RawSource>,
    },
    Joystick {
        direct_source: Option<RawSource>,
        virtual_keys: [Option<KeyboardKey>; 4],
        virtual_device_id: Option<String>,
    },
}

pub(super) struct BindingState {
    pending_bindings: VecDeque<PendingBinding>,
    bind_any_device_scope: bool,
    active_bindings: Vec<BindingAssignment>,
    store: BindingStore,
}

impl BindingState {
    pub(super) fn new(store: BindingStore) -> Self {
        Self {
            pending_bindings: VecDeque::new(),
            bind_any_device_scope: true,
            active_bindings: Vec::new(),
            store,
        }
    }

    pub(super) fn binding_count(&self, cert_fp: &str) -> usize {
        self.store.binding_count(cert_fp)
    }

    pub(super) fn binding_prompt(&self) -> Option<BindingPromptState> {
        self.pending_bindings
            .front()
            .map(|binding| binding.prompt_state(self.bind_any_device_scope))
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
        let Some(pending) = self.pending_bindings.pop_front() else {
            return Ok(None);
        };
        let Some(binding) = pending.capture.into_binding() else {
            return Ok(None);
        };
        let binding = scope_binding(binding, self.bind_any_device_scope);

        if let Some(cert_fp) = server_cert_fingerprint {
            self.store.set_binding(cert_fp, &pending.identifier, binding.clone());
            self.store.save()?;
        }

        self.activate_binding(pending.id, binding.clone());

        Ok(Some(ConfirmedBinding {
            binding_id: pending.id,
            identifier: pending.identifier,
            binding,
        }))
    }

    pub(super) fn skip_binding(&mut self) -> Option<String> {
        self.pending_bindings.pop_front().map(|d| d.identifier)
    }

    pub(super) fn declare_binding(
        &mut self,
        cert_fp: Option<&str>,
        binding_id: u16,
        identifier: String,
        input_type: protocol::InputType,
    ) -> DeclareBindingOutcome {
        if let Some(cert_fp) = cert_fp {
            if let Some(saved_binding) = self.store.get_binding(cert_fp, &identifier) {
                if saved_binding.action_type == input_type {
                    self.activate_binding(binding_id, saved_binding.clone());
                    return DeclareBindingOutcome::Restored {
                        binding_id,
                        binding: saved_binding,
                        identifier,
                    };
                }
            }
        }

        self.pending_bindings.push_back(PendingBinding {
            id: binding_id,
            identifier,
            input_type,
            capture: default_capture(input_type),
        });
        DeclareBindingOutcome::Pending
    }

    pub(super) fn sample_values<F>(
        &mut self,
        mut read_value: F,
    ) -> Vec<(u16, protocol::InputPayload)>
    where
        F: FnMut(&RawSource) -> protocol::InputPayload,
    {
        let mut outgoing = Vec::new();
        for binding in &mut self.active_bindings {
            let value = aggregate_binding_value(&binding.binding, &mut read_value);
            if !payload_changed(&binding.last_value, &value) {
                continue;
            }

            binding.last_value = value.clone();
            outgoing.push((binding.id, value));
        }
        outgoing
    }

    fn suggest_input(&mut self, input: RawSource) {
        let Some(current) = self.pending_bindings.front_mut() else {
            return;
        };
        current.apply_input(input);
    }

    fn activate_binding(&mut self, id: u16, binding: ActionBinding) {
        self.active_bindings.retain(|assignment| assignment.id != id);
        self.active_bindings.push(BindingAssignment {
            id,
            last_value: default_payload(binding.action_type),
            binding,
        });
    }
}

impl PendingBinding {
    fn prompt_state(&self, any_device_scope: bool) -> BindingPromptState {
        let (allows_toggle, allows_axis, allows_joystick) = prompt_capabilities(self.input_type);
        BindingPromptState {
            identifier: self.identifier.clone(),
            input_type: self.input_type,
            suggestion: self.capture.preview(any_device_scope),
            any_device_scope,
            allows_toggle,
            allows_axis,
            allows_joystick,
            capture_hint: self.capture.hint(),
        }
    }

    fn apply_input(&mut self, input: RawSource) {
        self.capture.apply_input(self.input_type, input);
    }
}

impl BindingCapture {
    fn apply_input(&mut self, input_type: protocol::InputType, input: RawSource) {
        match self {
            Self::Single { suggested_source } => {
                if is_source_compatible(&input, input_type) {
                    *suggested_source = Some(input);
                }
            },
            Self::Joystick { direct_source, virtual_keys, virtual_device_id } => {
                match input.input {
                    InputDescriptor::Stick { .. } if is_source_compatible(&input, input_type) => {
                        *direct_source = Some(input);
                        *virtual_keys = [None, None, None, None];
                        *virtual_device_id = None;
                    },
                    InputDescriptor::Key { code } => {
                        *direct_source = None;
                        if virtual_device_id.is_none() {
                            *virtual_device_id = input.device.id.clone();
                        }
                        if let Some(slot) = virtual_keys.iter_mut().find(|slot| slot.is_none()) {
                            *slot = Some(code);
                        }
                    },
                    _ => {},
                }
            },
        }
    }

    fn into_binding(self) -> Option<ActionBinding> {
        match self {
            Self::Single { suggested_source } => suggested_source.map(|source| ActionBinding {
                action_type: if source.is_toggle_compatible() {
                    protocol::InputType::Toggle
                } else {
                    protocol::InputType::Axis1D
                },
                sources: vec![source],
            }),
            Self::Joystick { direct_source: Some(source), .. } => Some(ActionBinding {
                action_type: protocol::InputType::Joystick2D,
                sources: vec![source],
            }),
            Self::Joystick {
                direct_source: None,
                virtual_keys:
                    [Some(positive_x), Some(negative_x), Some(positive_y), Some(negative_y)],
                virtual_device_id,
            } => Some(ActionBinding {
                action_type: protocol::InputType::Joystick2D,
                sources: vec![RawSource {
                    device: model::DeviceFilter {
                        device_type: DeviceType::Keyboard,
                        id: virtual_device_id,
                    },
                    input: InputDescriptor::VirtualStick {
                        positive_x,
                        negative_x,
                        positive_y,
                        negative_y,
                    },
                }],
            }),
            Self::Joystick { .. } => None,
        }
    }

    fn preview(&self, any_device_scope: bool) -> Option<String> {
        match self {
            Self::Single { suggested_source } => suggested_source
                .as_ref()
                .map(|source| source.with_device_scope(any_device_scope).to_string()),
            Self::Joystick { direct_source: Some(source), .. } => {
                Some(source.with_device_scope(any_device_scope).to_string())
            },
            Self::Joystick { virtual_keys, virtual_device_id, .. } => {
                if virtual_keys.iter().all(Option::is_none) {
                    None
                } else {
                    let device_label = if any_device_scope {
                        "*".to_string()
                    } else {
                        virtual_device_id.clone().unwrap_or_else(|| "*".to_string())
                    };
                    Some(format!(
                        "keyboard/{device_label}/virtual_stick(+x={},-x={},+y={},-y={})",
                        key_slot_label(virtual_keys[0]),
                        key_slot_label(virtual_keys[1]),
                        key_slot_label(virtual_keys[2]),
                        key_slot_label(virtual_keys[3]),
                    ))
                }
            },
        }
    }

    fn hint(&self) -> Option<String> {
        match self {
            Self::Single { .. } => None,
            Self::Joystick { direct_source: Some(_), .. } => Some(
                "Captured a gamepad stick. Press Enter to confirm or capture keys instead."
                    .to_string(),
            ),
            Self::Joystick { virtual_keys, .. } => {
                let labels = ["+X", "-X", "+Y", "-Y"];
                let next = virtual_keys.iter().position(|slot| slot.is_none());
                next.map(|index| {
                    format!("Capture keyboard key for {} or move a gamepad stick.", labels[index])
                })
                .or_else(|| Some("Virtual stick complete. Press Enter to confirm.".to_string()))
            },
        }
    }
}

fn key_slot_label(key: Option<KeyboardKey>) -> String {
    key.map(|key| key.to_string()).unwrap_or_else(|| "?".to_string())
}

fn scope_binding(binding: ActionBinding, any_device_scope: bool) -> ActionBinding {
    ActionBinding {
        action_type: binding.action_type,
        sources: binding
            .sources
            .into_iter()
            .map(|source| source.with_device_scope(any_device_scope))
            .collect(),
    }
}

fn default_capture(input_type: protocol::InputType) -> BindingCapture {
    match input_type {
        protocol::InputType::Toggle | protocol::InputType::Axis1D => {
            BindingCapture::Single { suggested_source: None }
        },
        protocol::InputType::Joystick2D => BindingCapture::Joystick {
            direct_source: None,
            virtual_keys: [None, None, None, None],
            virtual_device_id: None,
        },
    }
}

fn is_source_compatible(source: &RawSource, input_type: protocol::InputType) -> bool {
    match input_type {
        protocol::InputType::Toggle => source.is_toggle_compatible(),
        protocol::InputType::Axis1D => source.is_axis_1d_compatible(),
        protocol::InputType::Joystick2D => source.is_joystick_2d_compatible(),
    }
}

fn aggregate_binding_value<F>(binding: &ActionBinding, read_value: &mut F) -> protocol::InputPayload
where
    F: FnMut(&RawSource) -> protocol::InputPayload,
{
    match binding.action_type {
        protocol::InputType::Toggle => protocol::InputPayload::Toggle {
            pressed: binding
                .sources
                .iter()
                .any(|source| matches!(read_value(source), protocol::InputPayload::Toggle { pressed: true })),
        },
        protocol::InputType::Axis1D => {
            let value = binding
                .sources
                .iter()
                .filter_map(|source| match read_value(source) {
                    protocol::InputPayload::Axis1D { value } => Some(value.clamp(-1.0, 1.0)),
                    _ => None,
                })
                .max_by(|a, b| a.abs().total_cmp(&b.abs()))
                .unwrap_or(0.0);
            protocol::InputPayload::Axis1D { value }
        },
        protocol::InputType::Joystick2D => {
            let (x, y) = binding
                .sources
                .iter()
                .filter_map(|source| match read_value(source) {
                    protocol::InputPayload::Joystick2D { x, y } => {
                        Some((x.clamp(-1.0, 1.0), y.clamp(-1.0, 1.0)))
                    },
                    _ => None,
                })
                .max_by(|(ax, ay), (bx, by)| ax.hypot(*ay).total_cmp(&bx.hypot(*by)))
                .unwrap_or((0.0, 0.0));
            protocol::InputPayload::Joystick2D { x, y }
        },
    }
}

fn payload_changed(previous: &protocol::InputPayload, next: &protocol::InputPayload) -> bool {
    previous != next
}

fn default_payload(input_type: protocol::InputType) -> protocol::InputPayload {
    match input_type {
        protocol::InputType::Toggle => protocol::InputPayload::Toggle { pressed: false },
        protocol::InputType::Axis1D => protocol::InputPayload::Axis1D { value: 0.0 },
        protocol::InputType::Joystick2D => protocol::InputPayload::Joystick2D { x: 0.0, y: 0.0 },
    }
}

fn prompt_capabilities(input_type: protocol::InputType) -> (bool, bool, bool) {
    match input_type {
        protocol::InputType::Toggle => (true, false, false),
        protocol::InputType::Axis1D => (false, true, false),
        protocol::InputType::Joystick2D => (false, false, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::game::bindings::model::{DeviceFilter, InputDescriptor};

    #[test]
    fn aggregates_toggle_from_multiple_sources() {
        let binding = ActionBinding {
            action_type: protocol::InputType::Toggle,
            sources: vec![
                RawSource {
                    device: DeviceFilter::any(DeviceType::Keyboard),
                    input: InputDescriptor::Key { code: KeyboardKey::KeyW },
                },
                RawSource {
                    device: DeviceFilter::any(DeviceType::Keyboard),
                    input: InputDescriptor::Key { code: KeyboardKey::ArrowUp },
                },
            ],
        };

        let value = aggregate_binding_value(&binding, &mut |source| match source.input {
            InputDescriptor::Key { code: KeyboardKey::ArrowUp } => {
                protocol::InputPayload::Toggle { pressed: true }
            },
            _ => protocol::InputPayload::Toggle { pressed: false },
        });

        assert_eq!(value, protocol::InputPayload::Toggle { pressed: true });
    }

    #[test]
    fn aggregates_virtual_stick_by_magnitude() {
        let binding = ActionBinding {
            action_type: protocol::InputType::Joystick2D,
            sources: vec![
                RawSource {
                    device: DeviceFilter::any(DeviceType::Keyboard),
                    input: InputDescriptor::VirtualStick {
                        positive_x: KeyboardKey::KeyD,
                        negative_x: KeyboardKey::KeyA,
                        positive_y: KeyboardKey::KeyW,
                        negative_y: KeyboardKey::KeyS,
                    },
                },
                RawSource {
                    device: DeviceFilter::any(DeviceType::Gamepad),
                    input: InputDescriptor::Stick { stick: GamepadStick::Left },
                },
            ],
        };

        let value = aggregate_binding_value(&binding, &mut |source| match source.input {
            InputDescriptor::VirtualStick { .. } => {
                protocol::InputPayload::Joystick2D { x: 1.0, y: 0.0 }
            },
            InputDescriptor::Stick { .. } => protocol::InputPayload::Joystick2D { x: 0.2, y: 0.1 },
            _ => unreachable!(),
        });

        assert_eq!(value, protocol::InputPayload::Joystick2D { x: 1.0, y: 0.0 });
    }

    #[test]
    fn joystick_capture_builds_virtual_stick() {
        let mut capture = BindingCapture::Joystick {
            direct_source: None,
            virtual_keys: [None, None, None, None],
            virtual_device_id: None,
        };

        for key in [KeyboardKey::KeyD, KeyboardKey::KeyA, KeyboardKey::KeyW, KeyboardKey::KeyS] {
            capture.apply_input(
                protocol::InputType::Joystick2D,
                RawSource {
                    device: DeviceFilter::exact(DeviceType::Keyboard, "kbd".to_string()),
                    input: InputDescriptor::Key { code: key },
                },
            );
        }

        let binding = capture.into_binding().expect("virtual stick should be complete");
        assert_eq!(binding.action_type, protocol::InputType::Joystick2D);
        assert!(matches!(binding.sources[0].input, InputDescriptor::VirtualStick { .. }));
    }

    #[test]
    fn confirm_binding_initializes_next_pending_capture() {
        let mut state = BindingState::new(crate::game::persistence::BindingStore::new_for_tests());
        state.declare_binding(None, 1, "move_up".to_string(), protocol::InputType::Toggle);
        state.declare_binding(None, 2, "move_down".to_string(), protocol::InputType::Toggle);

        state.apply_ui_action(UiAction::Suggest(RawSource {
            device: DeviceFilter::exact(DeviceType::Keyboard, "kbd".to_string()),
            input: InputDescriptor::Key { code: KeyboardKey::KeyW },
        }));

        let confirmed = state.confirm_binding(None).expect("confirm should succeed");
        assert!(confirmed.is_some());
        assert_eq!(state.pending_bindings.len(), 1);

        state.apply_ui_action(UiAction::Suggest(RawSource {
            device: DeviceFilter::exact(DeviceType::Keyboard, "kbd".to_string()),
            input: InputDescriptor::Key { code: KeyboardKey::KeyS },
        }));

        let prompt = state.binding_prompt().expect("second binding should remain active");
        assert_eq!(prompt.suggestion.as_deref(), Some("keyboard/*/key/KeyS"));
    }
}
