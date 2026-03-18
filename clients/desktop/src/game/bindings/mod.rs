mod capture;
mod model;

use std::collections::VecDeque;

use anyhow::Result;

pub(crate) use capture::InputCapture;
pub(crate) use model::InputPath;

use self::model::parse_input_path;
use super::persistence::BindingStore;
use super::protocol;

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
            let value = read_value(&binding.input).clamp(-1.0, 1.0);
            let changed = (value - binding.last_value).abs() >= f32::EPSILON;
            if !changed {
                continue;
            }

            binding.last_value = value;
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
        self.active_bindings.push(BindingAssignment { id, input, last_value: 0.0 });
    }
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
