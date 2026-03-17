use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
#[cfg(target_os = "macos")]
use muda::{CheckMenuItem, Menu, MenuEvent, MenuId, PredefinedMenuItem, Submenu};
use winit::application::ApplicationHandler;
use winit::dpi::{LogicalSize, PhysicalSize};
use winit::event::{DeviceEvent, WindowEvent};
use winit::event_loop::{ActiveEventLoop, EventLoop, EventLoopProxy};
#[cfg(target_os = "macos")]
use winit::platform::macos::WindowAttributesExtMacOS;
use winit::window::Window;

use super::bindings::InputCapture;
use super::network_migration::NetworkMigrationCoordinator;
use super::renderer::Renderer;
use super::{ClientGame, ClientPhase};

const WIDTH: u32 = 800;
const HEIGHT: u32 = 600;
const MAIN_SURFACE_ID: u32 = 1;
const TICK_INTERVAL: Duration = Duration::from_millis(16);

#[derive(Clone, Copy, Debug)]
enum AppEvent {
    NetworkReady,
}

pub(super) fn run(game: ClientGame) -> Result<()> {
    let event_loop =
        EventLoop::<AppEvent>::with_user_event().build().context("failed to create event loop")?;
    let mut app = App::new(game, event_loop.create_proxy());
    event_loop.run_app(&mut app).context("event loop failed")
}

struct App {
    game: ClientGame,
    window: Option<Arc<Window>>,
    renderer: Option<Renderer>,
    settings: AppSettings,
    input_capture: InputCapture,
    window_focused: bool,
    window_occluded: bool,
    force_redraw: bool,
    next_tick_at: Instant,
    render_cache: RenderCache,
    last_prompt_signature: Option<String>,
    network_migration: NetworkMigrationCoordinator,
    #[cfg(target_os = "macos")]
    menu: Option<AppMenu>,
}

struct AppSettings {
    show_latency: bool,
    full_screen_content: bool,
    lock_window_to_surface: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self { show_latency: true, full_screen_content: false, lock_window_to_surface: false }
    }
}

#[derive(Default)]
struct RenderCache {
    last_rendered_states: Vec<super::RenderState>,
    last_rendered_text: Vec<super::renderer::TextCommand>,
    last_surface_state: Option<super::SurfaceState>,
    last_render_revision: u64,
    surface_list_sent: bool,
    last_reported_surface_size: Option<(u32, u32)>,
    last_applied_window_lock: Option<WindowLockState>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WindowLockState {
    enabled: bool,
    dimensions: Option<(u32, u32)>,
}

#[cfg(target_os = "macos")]
struct AppMenu {
    _menu: Menu,
    quit_id: MenuId,
    show_latency_id: MenuId,
    show_latency_item: CheckMenuItem,
    full_screen_content_id: MenuId,
    full_screen_content_item: CheckMenuItem,
    lock_window_to_surface_id: MenuId,
    lock_window_to_surface_item: CheckMenuItem,
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
        let lock_window_to_surface_item = CheckMenuItem::new(
            "Lock Window to Game Resolution",
            true,
            settings.lock_window_to_surface,
            None,
        );
        app_menu.append(&quit_item)?;
        view_menu.append(&show_latency_item)?;
        view_menu.append(&full_screen_content_item)?;
        view_menu.append(&lock_window_to_surface_item)?;
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
            lock_window_to_surface_id: lock_window_to_surface_item.id().clone(),
            lock_window_to_surface_item,
        })
    }
}

#[cfg(target_os = "macos")]
enum MenuAction {
    Quit,
    ToggleShowLatency(CheckMenuItem),
    ToggleFullScreenContent(CheckMenuItem),
    ToggleLockWindowToSurface(CheckMenuItem),
}

impl App {
    fn new(mut game: ClientGame, proxy: EventLoopProxy<AppEvent>) -> Self {
        let wake_notifier: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = proxy.send_event(AppEvent::NetworkReady);
        });
        game.set_network_waker(wake_notifier);

        Self {
            game,
            window: None,
            renderer: None,
            settings: AppSettings::default(),
            input_capture: InputCapture::new(),
            window_focused: true,
            window_occluded: false,
            force_redraw: true,
            next_tick_at: Instant::now(),
            render_cache: RenderCache::default(),
            last_prompt_signature: None,
            network_migration: NetworkMigrationCoordinator::new(),
            #[cfg(target_os = "macos")]
            menu: None,
        }
    }

    fn build_window_attributes(&self) -> winit::window::WindowAttributes {
        let locked_dimensions = self.current_window_lock_dimensions();
        let mut window_attributes = Window::default_attributes()
            .with_title(self.game.game_name().to_string())
            .with_inner_size(LogicalSize::new(WIDTH as f64, HEIGHT as f64));
        if let Some((width, height)) = locked_dimensions {
            let size = LogicalSize::new(width as f64, height as f64);
            window_attributes = window_attributes
                .with_inner_size(size)
                .with_min_inner_size(size)
                .with_max_inner_size(size)
                .with_resizable(false);
        }
        #[cfg(target_os = "macos")]
        {
            window_attributes = window_attributes
                .with_fullsize_content_view(self.settings.full_screen_content)
                .with_title_hidden(self.settings.full_screen_content)
                .with_titlebar_transparent(self.settings.full_screen_content);
        }
        window_attributes
    }

    fn current_window_lock_dimensions(&self) -> Option<(u32, u32)> {
        self.settings
            .lock_window_to_surface
            .then(|| self.game.surface_state(MAIN_SURFACE_ID).dimension_lock)
            .flatten()
    }

    fn apply_window_lock_if_needed(&mut self, surface: super::SurfaceState) {
        let next_lock = WindowLockState {
            enabled: self.settings.lock_window_to_surface,
            dimensions: self
                .settings
                .lock_window_to_surface
                .then_some(surface.dimension_lock)
                .flatten(),
        };
        if self.render_cache.last_applied_window_lock == Some(next_lock) {
            return;
        }
        let Some(window) = self.window.as_ref() else {
            self.render_cache.last_applied_window_lock = Some(next_lock);
            return;
        };

        if let Some((width, height)) = next_lock.dimensions {
            let size = LogicalSize::new(width as f64, height as f64);
            window.set_min_inner_size(Some(size));
            window.set_max_inner_size(Some(size));
            window.set_resizable(false);
            let _ = window.request_inner_size(size);
        } else {
            window.set_min_inner_size(None::<LogicalSize<f64>>);
            window.set_max_inner_size(None::<LogicalSize<f64>>);
            window.set_resizable(true);
        }

        self.render_cache.last_applied_window_lock = Some(next_lock);
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
        self.sync_surface_state()?;
        self.game.mark_window_running();
        self.wake_for_render();
        Ok(())
    }

    fn ensure_window_ready(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        if self.window.is_some() {
            return Ok(());
        }

        self.game.tick_network()?;
        if self.game.phase() == ClientPhase::JoinedPendingWindow {
            self.create_window_and_renderer(event_loop)?;
        }
        Ok(())
    }

    fn sync_surface_state(&mut self) -> Result<()> {
        let surface = self.game.surface_state(MAIN_SURFACE_ID);
        self.apply_window_lock_if_needed(surface);
        if let Some(renderer) = self.renderer.as_mut() {
            renderer.set_surface_constraints(
                surface.dimension_lock,
                surface.aspect_ratio_lock,
                surface.clear_background,
            );
        }
        let Some(window) = self.window.as_ref() else {
            return Ok(());
        };
        if !self.game.is_connected() || self.render_cache.surface_list_sent {
            return Ok(());
        }
        let size: PhysicalSize<u32> = window.inner_size();

        self.game.send_surface_list(vec![(MAIN_SURFACE_ID, size.width, size.height)])?;
        self.render_cache.surface_list_sent = true;
        self.render_cache.last_reported_surface_size = Some((size.width, size.height));
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
                    menu.lock_window_to_surface_id.clone(),
                    menu.lock_window_to_surface_item.clone(),
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
                lock_window_to_surface_id,
                lock_window_to_surface_item,
            ) = action_menu;
            while let Ok(event) = MenuEvent::receiver().try_recv() {
                let action = if event.id == quit_id {
                    Some(MenuAction::Quit)
                } else if event.id == show_latency_id {
                    Some(MenuAction::ToggleShowLatency(show_latency_item.clone()))
                } else if event.id == full_screen_content_id {
                    Some(MenuAction::ToggleFullScreenContent(full_screen_content_item.clone()))
                } else if event.id == lock_window_to_surface_id {
                    Some(MenuAction::ToggleLockWindowToSurface(lock_window_to_surface_item.clone()))
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
                    Some(MenuAction::ToggleLockWindowToSurface(item)) => {
                        self.settings.lock_window_to_surface =
                            !self.settings.lock_window_to_surface;
                        item.set_checked(self.settings.lock_window_to_surface);
                        self.render_cache.last_applied_window_lock = None;
                        self.apply_window_lock_if_needed(self.game.surface_state(MAIN_SURFACE_ID));
                        self.wake_for_render();
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
        self.sync_surface_state()?;
        let surface = self.game.surface_state(MAIN_SURFACE_ID);

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
            if self.window_focused {
                let input_capture = &self.input_capture;
                self.game.send_bound_inputs(|path| input_capture.read_binding_value(path))?;
            }
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
            || self.render_cache.last_render_revision != self.game.render_revision()
            || self.render_cache.last_rendered_states != states
            || self.render_cache.last_rendered_text != overlay_text;
        if render_needed {
            if let Some(renderer) = self.renderer.as_mut() {
                renderer.render(&states, self.game.resources(), &overlay_text)?;
            }
            self.render_cache.last_rendered_states = states;
            self.render_cache.last_rendered_text = overlay_text;
            self.render_cache.last_surface_state = Some(surface);
            self.render_cache.last_render_revision = self.game.render_revision();
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

impl ApplicationHandler<AppEvent> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.network_migration.on_resumed(Instant::now(), self.game.is_connected()) {
            if let Err(err) = self.game.handle_network_change() {
                log::warn!("failed to migrate QUIC connection after app resume: {err:#}");
            }
        }

        if let Err(err) = self.ensure_window_ready(event_loop) {
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
        if self.network_migration.poll_network_change(Instant::now(), self.game.is_connected()) {
            if let Err(err) = self.game.handle_network_change() {
                log::warn!("failed to migrate QUIC connection after network change: {err:#}");
            }
        }
        if let Err(err) = self.ensure_window_ready(event_loop) {
            log::error!("client network error before join: {err:#}");
            event_loop.exit();
            return;
        }
        self.update_control_flow(event_loop);
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: AppEvent) {
        match event {
            AppEvent::NetworkReady => {
                if let Err(err) = self.game.tick_network() {
                    log::error!("client network event error: {err:#}");
                    event_loop.exit();
                    return;
                }
                if let Err(err) = self.ensure_window_ready(event_loop) {
                    log::error!("client network wake error: {err:#}");
                    event_loop.exit();
                    return;
                }
                if self.window.is_some() {
                    self.wake_for_render();
                }
                self.update_control_flow(event_loop);
            },
        }
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
            WindowEvent::Focused(focused) => {
                self.window_focused = focused;
                if !focused {
                    self.input_capture.clear_active_inputs();
                }
                self.apply_render_activity_change();
            },
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
