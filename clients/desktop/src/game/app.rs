use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
#[cfg(target_os = "macos")]
use muda::{CheckMenuItem, Menu, MenuEvent, MenuId, PredefinedMenuItem, Submenu};
use winit::application::ApplicationHandler;
use winit::dpi::LogicalSize;
use winit::event::{DeviceEvent, WindowEvent};
use winit::event_loop::{ActiveEventLoop, EventLoop};
#[cfg(target_os = "macos")]
use winit::platform::macos::WindowAttributesExtMacOS;
use winit::window::Window;

use super::bindings::InputCapture;
use super::renderer::Renderer;
use super::ClientGame;

const WIDTH: u32 = 800;
const HEIGHT: u32 = 600;
const MAIN_SURFACE_ID: u32 = 1;
const TICK_INTERVAL: Duration = Duration::from_millis(16);

pub(super) fn run(game: ClientGame) -> Result<()> {
    let event_loop = EventLoop::new().context("failed to create event loop")?;
    let mut app = App::new(game);
    event_loop.run_app(&mut app).context("event loop failed")
}

struct App {
    game: ClientGame,
    window: Option<Arc<Window>>,
    renderer: Option<Renderer>,
    settings: AppSettings,
    input_capture: InputCapture,
    window_occluded: bool,
    force_redraw: bool,
    next_tick_at: Instant,
    render_cache: RenderCache,
    last_prompt_signature: Option<String>,
    #[cfg(target_os = "macos")]
    menu: Option<AppMenu>,
}

struct AppSettings {
    show_latency: bool,
    full_screen_content: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self { show_latency: true, full_screen_content: false }
    }
}

#[derive(Default)]
struct RenderCache {
    last_rendered_states: Vec<super::RenderState>,
    last_rendered_text: Vec<super::renderer::TextCommand>,
    last_surface_state: Option<super::SurfaceState>,
    surface_list_sent: bool,
    last_reported_surface_size: Option<(u32, u32)>,
}

#[cfg(target_os = "macos")]
struct AppMenu {
    _menu: Menu,
    quit_id: MenuId,
    show_latency_id: MenuId,
    show_latency_item: CheckMenuItem,
    full_screen_content_id: MenuId,
    full_screen_content_item: CheckMenuItem,
}

#[cfg(target_os = "macos")]
impl AppMenu {
    fn create(app_name: &str, settings: &AppSettings) -> Result<Self> {
        let menu = Menu::new();
        let app_menu = Submenu::new(app_name, true);
        let quit_item = PredefinedMenuItem::quit(None);
        let view_menu = Submenu::new("View", true);
        let show_latency_item =
            CheckMenuItem::new("Show Latency", true, settings.show_latency, None);
        let full_screen_content_item =
            CheckMenuItem::new("Full Screen Content", true, settings.full_screen_content, None);
        app_menu.append(&quit_item)?;
        view_menu.append(&show_latency_item)?;
        view_menu.append(&full_screen_content_item)?;
        menu.append(&app_menu)?;
        menu.append(&view_menu)?;
        menu.init_for_nsapp();

        Ok(Self {
            _menu: menu,
            quit_id: quit_item.id().clone(),
            show_latency_id: show_latency_item.id().clone(),
            show_latency_item,
            full_screen_content_id: full_screen_content_item.id().clone(),
            full_screen_content_item,
        })
    }
}

#[cfg(target_os = "macos")]
enum MenuAction {
    Quit,
    ToggleShowLatency(CheckMenuItem),
    ToggleFullScreenContent(CheckMenuItem),
}

impl App {
    fn new(game: ClientGame) -> Self {
        Self {
            game,
            window: None,
            renderer: None,
            settings: AppSettings::default(),
            input_capture: InputCapture::new(),
            window_occluded: false,
            force_redraw: true,
            next_tick_at: Instant::now(),
            render_cache: RenderCache::default(),
            last_prompt_signature: None,
            #[cfg(target_os = "macos")]
            menu: None,
        }
    }

    fn build_window_attributes(&self) -> winit::window::WindowAttributes {
        let mut window_attributes = Window::default_attributes()
            .with_title(self.game.game_name().to_string())
            .with_inner_size(LogicalSize::new(WIDTH as f64, HEIGHT as f64));
        #[cfg(target_os = "macos")]
        {
            window_attributes = window_attributes
                .with_fullsize_content_view(self.settings.full_screen_content)
                .with_title_hidden(self.settings.full_screen_content)
                .with_titlebar_transparent(self.settings.full_screen_content);
        }
        window_attributes
    }

    fn create_window_and_renderer(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        let window = event_loop
            .create_window(self.build_window_attributes())
            .map(Arc::new)
            .context("failed to create desktop window")?;
        let renderer = pollster::block_on(Renderer::new(window.clone()))
            .context("failed to initialize renderer")?;

        self.window = Some(window);
        self.renderer = Some(renderer);
        self.render_cache = RenderCache::default();
        self.window_occluded = false;
        self.wake_for_render();
        Ok(())
    }

    fn rebuild_window(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        self.renderer = None;
        self.window = None;
        self.create_window_and_renderer(event_loop)
    }

    fn poll_menu_events(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(target_os = "macos")]
        {
            let Some(action_menu) = self.menu.as_ref().map(|menu| {
                (
                    menu.quit_id.clone(),
                    menu.show_latency_id.clone(),
                    menu.show_latency_item.clone(),
                    menu.full_screen_content_id.clone(),
                    menu.full_screen_content_item.clone(),
                )
            }) else {
                return;
            };
            let (
                quit_id,
                show_latency_id,
                show_latency_item,
                full_screen_content_id,
                full_screen_content_item,
            ) = action_menu;
            while let Ok(event) = MenuEvent::receiver().try_recv() {
                let action = if event.id == quit_id {
                    Some(MenuAction::Quit)
                } else if event.id == show_latency_id {
                    Some(MenuAction::ToggleShowLatency(show_latency_item.clone()))
                } else if event.id == full_screen_content_id {
                    Some(MenuAction::ToggleFullScreenContent(full_screen_content_item.clone()))
                } else {
                    None
                };
                match action {
                    Some(MenuAction::Quit) => {
                        event_loop.exit();
                        return;
                    },
                    Some(MenuAction::ToggleShowLatency(item)) => {
                        self.settings.show_latency = !self.settings.show_latency;
                        item.set_checked(self.settings.show_latency);
                        self.wake_for_render();
                    },
                    Some(MenuAction::ToggleFullScreenContent(item)) => {
                        self.settings.full_screen_content = !self.settings.full_screen_content;
                        item.set_checked(self.settings.full_screen_content);
                        if let Err(err) = self.rebuild_window(event_loop) {
                            log::error!(
                                "failed to rebuild window after full screen content toggle: {err:#}"
                            );
                            event_loop.exit();
                            return;
                        }
                    },
                    None => {},
                }
            }
        }
    }

    fn render_active(&self) -> bool {
        !self.window_occluded
    }

    fn request_redraw(&self) {
        if let Some(window) = &self.window {
            window.request_redraw();
        }
    }

    fn wake_for_render(&mut self) {
        self.next_tick_at = Instant::now();
        self.force_redraw = true;
        self.request_redraw();
    }

    fn apply_render_activity_change(&mut self) {
        if self.render_active() {
            self.wake_for_render();
        }
    }

    fn update_control_flow(&mut self, event_loop: &ActiveEventLoop) {
        if !self.render_active() {
            event_loop.set_control_flow(winit::event_loop::ControlFlow::Wait);
            return;
        }

        let now = Instant::now();
        if now >= self.next_tick_at {
            self.next_tick_at = now + TICK_INTERVAL;
            self.request_redraw();
        }
        event_loop.set_control_flow(winit::event_loop::ControlFlow::WaitUntil(self.next_tick_at));
    }

    fn tick_frame(&mut self, _event_loop: &ActiveEventLoop) -> Result<()> {
        let Some(window) = self.window.clone() else {
            return Ok(());
        };
        if self.renderer.is_none() {
            return Ok(());
        }

        self.input_capture.poll_gamepads();
        self.game.tick_network()?;
        if self.game.is_connected() && !self.render_cache.surface_list_sent {
            let size = window.inner_size();
            self.game
                .send_surface_list(vec![(MAIN_SURFACE_ID, size.width, size.height)])?;
            self.render_cache.surface_list_sent = true;
            self.render_cache.last_reported_surface_size = Some((size.width, size.height));
        }
        let surface = self.game.surface_state(MAIN_SURFACE_ID);
        if let Some(renderer) = self.renderer.as_mut() {
            renderer.set_surface_constraints(
                surface.dimension_lock,
                surface.aspect_ratio_lock,
                surface.clear_background,
            );
        }

        let mut overlay_states = Vec::new();
        let mut overlay_text = Vec::new();
        if self.game.binding_prompt().is_some() {
            window.set_title(&format!("{} - Input Bindings", self.game.game_name()));

            for action in self.input_capture.binding_actions() {
                self.game.apply_binding_ui_action(action)?;
            }

            self.update_binding_overlay_and_logs(&mut overlay_states, &mut overlay_text);
        } else {
            self.last_prompt_signature = None;
            window.set_title(self.game.game_name());
            let input_capture = &self.input_capture;
            self.game.send_bound_inputs(|path| input_capture.read_binding_value(path))?;
        }

        let mut states = self.game.render_states();
        if self.settings.show_latency {
            if let Some(renderer) = self.renderer.as_ref() {
                renderer
                    .build_latency_overlay(self.game.latency_snapshot())
                    .merge_into(&mut states, &mut overlay_text);
            }
        }
        states.extend(overlay_states);
        let render_needed = self.force_redraw
            || self.render_cache.last_surface_state != Some(surface)
            || self.render_cache.last_rendered_states != states
            || self.render_cache.last_rendered_text != overlay_text;
        if render_needed {
            if let Some(renderer) = self.renderer.as_mut() {
                renderer.render(&states, &overlay_text)?;
            }
            self.render_cache.last_rendered_states = states;
            self.render_cache.last_rendered_text = overlay_text;
            self.render_cache.last_surface_state = Some(surface);
            self.force_redraw = false;
        }

        self.input_capture.end_frame();
        Ok(())
    }

    fn update_binding_overlay_and_logs(
        &mut self,
        overlay_states: &mut Vec<super::RenderState>,
        overlay_text: &mut Vec<super::renderer::TextCommand>,
    ) {
        let Some(prompt) = self.game.binding_prompt() else {
            self.last_prompt_signature = None;
            return;
        };

        if let Some(renderer) = self.renderer.as_ref() {
            renderer.build_binding_overlay(&prompt).merge_into(overlay_states, overlay_text);
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

        if let Err(err) = self.create_window_and_renderer(event_loop) {
            log::error!("{err:#}");
            event_loop.exit();
            return;
        }
        #[cfg(target_os = "macos")]
        if self.menu.is_none() {
            match AppMenu::create(self.game.game_name(), &self.settings) {
                Ok(menu) => self.menu = Some(menu),
                Err(err) => log::warn!("failed to initialize menu bar: {err:#}"),
            }
        }
        self.update_control_flow(event_loop);
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        self.poll_menu_events(event_loop);
        self.update_control_flow(event_loop);
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
        self.poll_menu_events(event_loop);
        self.input_capture.consume_window_event(&event);

        match event {
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::Focused(_) => self.apply_render_activity_change(),
            WindowEvent::Occluded(occluded) => {
                self.window_occluded = occluded;
                self.apply_render_activity_change();
            },
            WindowEvent::Resized(size) => {
                if let Some(renderer) = self.renderer.as_mut() {
                    renderer.resize(size);
                }
                self.wake_for_render();
                if self.game.is_connected() {
                    let next_size = (size.width, size.height);
                    if self.render_cache.last_reported_surface_size != Some(next_size) {
                        if let Err(err) =
                            self.game.send_surface_resized(MAIN_SURFACE_ID, size.width, size.height)
                        {
                            log::warn!("failed to send surface resize: {err:#}");
                        } else {
                            self.render_cache.last_reported_surface_size = Some(next_size);
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
            },
            WindowEvent::Destroyed => {},
            _ => {},
        }
    }
}
