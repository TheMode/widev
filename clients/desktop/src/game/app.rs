use std::sync::Arc;

use anyhow::{Context, Result};
use winit::application::ApplicationHandler;
use winit::dpi::LogicalSize;
use winit::event::{DeviceEvent, WindowEvent};
use winit::event_loop::{ActiveEventLoop, EventLoop};
use winit::window::Window;

use super::bindings::InputCapture;
use super::renderer::Renderer;
use super::ClientGame;

const WIDTH: u32 = 800;
const HEIGHT: u32 = 600;
const MAIN_SURFACE_ID: u32 = 1;

pub(super) fn run(game: ClientGame) -> Result<()> {
    let event_loop = EventLoop::new().context("failed to create event loop")?;
    let mut app = App::new(game);
    event_loop.run_app(&mut app).context("event loop failed")
}

struct App {
    game: ClientGame,
    window: Option<Arc<Window>>,
    renderer: Option<Renderer>,
    input_capture: InputCapture,
    window_focused: bool,
    window_occluded: bool,
    surface_list_sent: bool,
    last_reported_surface_size: Option<(u32, u32)>,
    last_prompt_signature: Option<String>,
}

impl App {
    fn new(game: ClientGame) -> Self {
        Self {
            game,
            window: None,
            renderer: None,
            input_capture: InputCapture::new(),
            window_focused: true,
            window_occluded: false,
            surface_list_sent: false,
            last_reported_surface_size: None,
            last_prompt_signature: None,
        }
    }

    fn render_active(&self) -> bool {
        self.window_focused && !self.window_occluded
    }

    fn tick_frame(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        let Some(window) = self.window.clone() else {
            return Ok(());
        };
        if self.renderer.is_none() {
            return Ok(());
        }

        self.input_capture.poll_gamepads();
        self.game.tick_network()?;
        if self.game.is_connected() && !self.surface_list_sent {
            let size = window.inner_size();
            self.game.send_surface_list(vec![(
                MAIN_SURFACE_ID,
                "main".to_string(),
                size.width,
                size.height,
            )])?;
            self.surface_list_sent = true;
            self.last_reported_surface_size = Some((size.width, size.height));
        }
        if let Some(renderer) = self.renderer.as_mut() {
            let surface = self.game.surface_state(MAIN_SURFACE_ID);
            renderer.set_surface_constraints(
                surface.dimension_lock,
                surface.aspect_ratio_lock,
                surface.clear_background,
            );
        }

        let mut overlay_states = Vec::new();
        if self.game.binding_prompt().is_some() {
            window.set_title(&format!("{} - Input Bindings", self.game.game_name()));

            for action in self.input_capture.binding_actions() {
                self.game.apply_binding_ui_action(action)?;
            }

            self.update_binding_overlay_and_logs(&mut overlay_states);
        } else {
            self.last_prompt_signature = None;
            window.set_title(self.game.game_name());
            let input_capture = &self.input_capture;
            self.game.send_bound_inputs(|path| input_capture.read_binding_value(path))?;
        }

        if self.input_capture.is_exit_requested() {
            event_loop.exit();
            return Ok(());
        }

        let mut states = self.game.render_states();
        states.extend(overlay_states);
        if let Some(renderer) = self.renderer.as_mut() {
            renderer.render(&states)?;
        }

        self.input_capture.end_frame();
        Ok(())
    }

    fn update_binding_overlay_and_logs(&mut self, overlay_states: &mut Vec<super::RenderState>) {
        let Some(prompt) = self.game.binding_prompt() else {
            self.last_prompt_signature = None;
            return;
        };

        if let Some(renderer) = self.renderer.as_ref() {
            *overlay_states = renderer.build_binding_overlay(&prompt);
        }

        let signature = format!(
            "{}:{:?}:{}:{:?}",
            prompt.identifier, prompt.input_type, prompt.any_device_scope, prompt.suggestion
        );
        if self.last_prompt_signature.as_deref() == Some(signature.as_str()) {
            return;
        }

        log::info!("binding prompt: {} [{:?}]", prompt.identifier, prompt.input_type);
        for line in prompt.log_sections() {
            log::info!("  {line}");
        }
        if let Some(suggestion) = prompt.suggestion {
            log::info!("  captured: {}", suggestion.with_device_scope(prompt.any_device_scope));
        }
        self.last_prompt_signature = Some(signature);
    }
}

impl ApplicationHandler for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_some() {
            return;
        }

        let window_attributes = Window::default_attributes()
            .with_title(self.game.game_name().to_string())
            .with_inner_size(LogicalSize::new(WIDTH as f64, HEIGHT as f64));

        let window = match event_loop.create_window(window_attributes) {
            Ok(window) => Arc::new(window),
            Err(err) => {
                log::error!("failed to create desktop window: {err:#}");
                event_loop.exit();
                return;
            },
        };

        let renderer = match pollster::block_on(Renderer::new(window.clone())) {
            Ok(renderer) => renderer,
            Err(err) => {
                log::error!("failed to initialize renderer: {err:#}");
                event_loop.exit();
                return;
            },
        };

        self.window = Some(window);
        self.renderer = Some(renderer);
        self.window_focused = true;
        self.window_occluded = false;
        event_loop.set_control_flow(winit::event_loop::ControlFlow::Wait);
        if let Some(window) = &self.window {
            window.request_redraw();
        }
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        event_loop.set_control_flow(winit::event_loop::ControlFlow::Wait);
    }

    fn device_event(
        &mut self,
        _event_loop: &ActiveEventLoop,
        device_id: winit::event::DeviceId,
        event: DeviceEvent,
    ) {
        self.input_capture.consume_device_event(device_id, event);
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: winit::window::WindowId,
        event: WindowEvent,
    ) {
        self.input_capture.consume_window_event(&event);

        match event {
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::Focused(focused) => {
                self.window_focused = focused;
                if self.render_active() {
                    if let Some(window) = &self.window {
                        window.request_redraw();
                    }
                }
            },
            WindowEvent::Occluded(occluded) => {
                self.window_occluded = occluded;
                if self.render_active() {
                    if let Some(window) = &self.window {
                        window.request_redraw();
                    }
                }
            },
            WindowEvent::Resized(size) => {
                if let Some(renderer) = self.renderer.as_mut() {
                    renderer.resize(size);
                }
                if self.game.is_connected() {
                    let next_size = (size.width, size.height);
                    if self.last_reported_surface_size != Some(next_size) {
                        if let Err(err) =
                            self.game.send_surface_resized(MAIN_SURFACE_ID, size.width, size.height)
                        {
                            log::warn!("failed to send surface resize: {err:#}");
                        } else {
                            self.last_reported_surface_size = Some(next_size);
                        }
                    }
                }
            },
            WindowEvent::RedrawRequested => {
                if let Err(err) = self.tick_frame(event_loop) {
                    log::error!("client frame error: {err:#}");
                    event_loop.exit();
                    return;
                }
                if self.render_active() {
                    if let Some(window) = &self.window {
                        window.request_redraw();
                    }
                }
            },
            WindowEvent::Destroyed => {
                self.window_focused = false;
            },
            _ => {},
        }
    }
}
