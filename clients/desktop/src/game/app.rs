use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use winit::application::ApplicationHandler;
use winit::dpi::LogicalSize;
use winit::event::{ElementState, WindowEvent};
use winit::event_loop::{ActiveEventLoop, EventLoop};
use winit::keyboard::{KeyCode, PhysicalKey};
use winit::window::Window;

use super::renderer::Renderer;
use super::ClientGame;

const WIDTH: u32 = 800;
const HEIGHT: u32 = 600;

pub(super) fn run(game: ClientGame) -> Result<()> {
    let event_loop = EventLoop::new().context("failed to create event loop")?;
    let mut app = App::new(game);
    event_loop.run_app(&mut app).context("event loop failed")
}

struct App {
    game: ClientGame,
    window: Option<Arc<Window>>,
    renderer: Option<Renderer>,
    pressed_keys: HashSet<KeyCode>,
    just_pressed: Vec<KeyCode>,
}

impl App {
    fn new(game: ClientGame) -> Self {
        Self {
            game,
            window: None,
            renderer: None,
            pressed_keys: HashSet::new(),
            just_pressed: Vec::new(),
        }
    }

    fn tick_frame(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        let Some(window) = self.window.as_ref() else {
            return Ok(());
        };
        let Some(renderer) = self.renderer.as_mut() else {
            return Ok(());
        };

        self.game.tick_network()?;

        if let Some(prompt) = self.game.binding_prompt() {
            let prompt_title = match prompt.suggestion {
                Some(key) => format!(
                    "{} - Bind {} [{:?}] - press Enter to confirm {:?}, Backspace to skip, Esc to quit",
                    self.game.game_name(),
                    prompt.identifier,
                    prompt.input_type,
                    key
                ),
                None => format!(
                    "{} - Bind {} [{:?}] - press a key, Enter to confirm, Backspace to skip, Esc to quit",
                    self.game.game_name(),
                    prompt.identifier,
                    prompt.input_type
                ),
            };
            window.set_title(&prompt_title);

            for code in self.just_pressed.iter().copied() {
                if matches!(code, KeyCode::Enter | KeyCode::Backspace | KeyCode::Escape) {
                    continue;
                }
                self.game.suggest_binding_key(code);
            }
            if self.just_pressed.contains(&KeyCode::Enter) {
                self.game.confirm_binding()?;
            }
            if self.just_pressed.contains(&KeyCode::Backspace) {
                self.game.skip_binding();
            }
        } else {
            window.set_title(self.game.game_name());
            self.game.send_bound_inputs(|code| self.pressed_keys.contains(&code))?;
        }

        if self.pressed_keys.contains(&KeyCode::Escape) {
            event_loop.exit();
            return Ok(());
        }

        let states = self.game.render_states();
        renderer.render(&states)?;
        self.just_pressed.clear();
        Ok(())
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
    }

    fn about_to_wait(&mut self, _event_loop: &ActiveEventLoop) {
        if let Some(window) = &self.window {
            window.request_redraw();
        }
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: winit::window::WindowId,
        event: WindowEvent,
    ) {
        match event {
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::Resized(size) => {
                if let Some(renderer) = self.renderer.as_mut() {
                    renderer.resize(size);
                }
            },
            WindowEvent::RedrawRequested => {
                if let Err(err) = self.tick_frame(event_loop) {
                    log::error!("client frame error: {err:#}");
                    event_loop.exit();
                }
            },
            WindowEvent::KeyboardInput { event, .. } => {
                let PhysicalKey::Code(code) = event.physical_key else {
                    return;
                };
                match event.state {
                    ElementState::Pressed => {
                        if !event.repeat && self.pressed_keys.insert(code) {
                            self.just_pressed.push(code);
                        }
                    },
                    ElementState::Released => {
                        self.pressed_keys.remove(&code);
                    },
                }
            },
            _ => {},
        }
    }
}
