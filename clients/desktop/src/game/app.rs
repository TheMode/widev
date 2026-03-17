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
    settings: AppSettings,
    runtime: RuntimeState,
    #[cfg(target_os = "macos")]
    menu: Option<AppMenu>,
}

impl App {
    fn new(mut game: ClientGame, proxy: EventLoopProxy<AppEvent>) -> Self {
        let wake_notifier: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = proxy.send_event(AppEvent::NetworkReady);
        });
        game.set_network_waker(wake_notifier);

        Self {
            game,
            settings: AppSettings::default(),
            runtime: RuntimeState::default(),
            #[cfg(target_os = "macos")]
            menu: None,
        }
    }

    fn ensure_window_ready(&mut self, event_loop: &ActiveEventLoop) -> Result<()> {
        if self.runtime.has_window() {
            return Ok(());
        }

        self.game.tick_network()?;
        if self.game.phase() == ClientPhase::JoinedPendingWindow {
            self.runtime.create_window_and_renderer(event_loop, &self.game, &self.settings)?;
            self.runtime.sync_surface_state(&mut self.game, &self.settings)?;
            self.game.mark_window_running();
            self.runtime.wake_for_render();
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn poll_menu_events(&mut self, event_loop: &ActiveEventLoop) {
        while let Some(action) = self.menu.as_ref().and_then(AppMenu::next_action) {
            if self.handle_menu_action(event_loop, action) {
                return;
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn poll_menu_events(&mut self, _event_loop: &ActiveEventLoop) {}

    #[cfg(target_os = "macos")]
    fn handle_menu_action(&mut self, event_loop: &ActiveEventLoop, action: MenuAction) -> bool {
        match action {
            MenuAction::Quit => {
                event_loop.exit();
                true
            },
            MenuAction::ToggleShowLatency(item) => {
                self.settings.show_latency = !self.settings.show_latency;
                item.set_checked(self.settings.show_latency);
                self.runtime.wake_for_render();
                false
            },
            MenuAction::ToggleFullScreenContent(item) => {
                self.settings.full_screen_content = !self.settings.full_screen_content;
                item.set_checked(self.settings.full_screen_content);
                if let Err(err) =
                    self.runtime.rebuild_window(event_loop, &self.game, &self.settings)
                {
                    log::error!(
                        "failed to rebuild window after full screen content toggle: {err:#}"
                    );
                    event_loop.exit();
                    return true;
                }
                if let Err(err) = self.runtime.sync_surface_state(&mut self.game, &self.settings) {
                    log::error!("failed to sync surface state after window rebuild: {err:#}");
                    event_loop.exit();
                    return true;
                }
                self.game.mark_window_running();
                self.runtime.wake_for_render();
                false
            },
            MenuAction::ToggleLockWindowToSurface(item) => {
                self.settings.lock_window_to_surface = !self.settings.lock_window_to_surface;
                item.set_checked(self.settings.lock_window_to_surface);
                self.runtime.rendering.invalidate_window_lock();
                if let Err(err) = self.runtime.sync_surface_state(&mut self.game, &self.settings) {
                    log::error!("failed to apply game resolution window lock: {err:#}");
                    event_loop.exit();
                    return true;
                }
                self.runtime.wake_for_render();
                false
            },
        }
    }

    fn handle_network_change(&mut self, context: &str) {
        if let Err(err) = self.game.handle_network_change() {
            log::warn!("{context}: {err:#}");
        }
    }
}

impl ApplicationHandler<AppEvent> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.runtime.network_migration.on_resumed(Instant::now(), self.game.is_connected()) {
            self.handle_network_change("failed to migrate QUIC connection after app resume");
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
        self.runtime.update_control_flow(event_loop);
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        self.poll_menu_events(event_loop);
        if self
            .runtime
            .network_migration
            .poll_network_change(Instant::now(), self.game.is_connected())
        {
            self.handle_network_change("failed to migrate QUIC connection after network change");
        }
        if let Err(err) = self.ensure_window_ready(event_loop) {
            log::error!("client network error before join: {err:#}");
            event_loop.exit();
            return;
        }
        self.runtime.update_control_flow(event_loop);
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
                if self.runtime.has_window() {
                    self.runtime.wake_for_render();
                }
                self.runtime.update_control_flow(event_loop);
            },
        }
    }

    fn device_event(
        &mut self,
        _event_loop: &ActiveEventLoop,
        device_id: winit::event::DeviceId,
        event: DeviceEvent,
    ) {
        self.runtime.input.consume_device_event(device_id, event);
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: winit::window::WindowId,
        event: WindowEvent,
    ) {
        self.poll_menu_events(event_loop);
        self.runtime.input.consume_window_event(&event);

        match event {
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::Focused(focused) => {
                self.runtime.windowing.set_focused(focused, &mut self.runtime.input);
                self.runtime.apply_render_activity_change();
            },
            WindowEvent::Occluded(occluded) => {
                self.runtime.windowing.occluded = occluded;
                self.runtime.apply_render_activity_change();
            },
            WindowEvent::Resized(size) => {
                self.runtime.handle_window_resized(&mut self.game, size);
            },
            WindowEvent::RedrawRequested => {
                if let Err(err) = self.runtime.tick_frame(&mut self.game, &self.settings) {
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

struct RuntimeState {
    windowing: WindowState,
    rendering: RenderingState,
    input: InputState,
    network_migration: NetworkMigrationCoordinator,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            windowing: WindowState::default(),
            rendering: RenderingState::default(),
            input: InputState::default(),
            network_migration: NetworkMigrationCoordinator::new(),
        }
    }
}

impl RuntimeState {
    fn has_window(&self) -> bool {
        self.windowing.window.is_some()
    }

    fn build_window_attributes(
        &self,
        game: &ClientGame,
        settings: &AppSettings,
    ) -> winit::window::WindowAttributes {
        let locked_dimensions = settings
            .lock_window_to_surface
            .then(|| game.surface_state(MAIN_SURFACE_ID).dimension_lock)
            .flatten();
        let mut window_attributes = Window::default_attributes()
            .with_title(game.game_name().to_string())
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
                .with_fullsize_content_view(settings.full_screen_content)
                .with_title_hidden(settings.full_screen_content)
                .with_titlebar_transparent(settings.full_screen_content);
        }
        window_attributes
    }

    fn create_window_and_renderer(
        &mut self,
        event_loop: &ActiveEventLoop,
        game: &ClientGame,
        settings: &AppSettings,
    ) -> Result<()> {
        let window = event_loop
            .create_window(self.build_window_attributes(game, settings))
            .map(Arc::new)
            .context("failed to create desktop window")?;
        let renderer = pollster::block_on(Renderer::new(window.clone()))
            .context("failed to initialize renderer")?;

        self.windowing.window = Some(window);
        self.rendering = RenderingState::default();
        self.rendering.renderer = Some(renderer);
        self.windowing.occluded = false;
        Ok(())
    }

    fn rebuild_window(
        &mut self,
        event_loop: &ActiveEventLoop,
        game: &ClientGame,
        settings: &AppSettings,
    ) -> Result<()> {
        self.rendering.renderer = None;
        self.windowing.window = None;
        self.create_window_and_renderer(event_loop, game, settings)
    }

    fn sync_surface_state(&mut self, game: &mut ClientGame, settings: &AppSettings) -> Result<()> {
        let surface = game.surface_state(MAIN_SURFACE_ID);
        self.windowing.apply_window_lock_if_needed(
            settings.lock_window_to_surface,
            surface,
            &mut self.rendering,
        );
        if let Some(renderer) = self.rendering.renderer.as_mut() {
            renderer.set_surface_constraints(
                surface.dimension_lock,
                surface.aspect_ratio_lock,
                surface.clear_background,
            );
        }
        let Some(window) = self.windowing.window.as_ref() else {
            return Ok(());
        };
        if !game.is_connected() || self.rendering.cache.surface_list_sent {
            return Ok(());
        }
        let size: PhysicalSize<u32> = window.inner_size();

        game.send_surface_list(vec![(MAIN_SURFACE_ID, size.width, size.height)])?;
        self.rendering.cache.surface_list_sent = true;
        self.rendering.cache.last_reported_surface_size = Some((size.width, size.height));
        Ok(())
    }

    fn handle_window_resized(&mut self, game: &mut ClientGame, size: PhysicalSize<u32>) {
        if let Some(renderer) = self.rendering.renderer.as_mut() {
            renderer.resize(size);
        }
        self.wake_for_render();
        if game.is_connected() {
            let next_size = (size.width, size.height);
            if self.rendering.cache.last_reported_surface_size != Some(next_size) {
                if let Err(err) =
                    game.send_surface_resized(MAIN_SURFACE_ID, size.width, size.height)
                {
                    log::warn!("failed to send surface resize: {err:#}");
                } else {
                    self.rendering.cache.last_reported_surface_size = Some(next_size);
                }
            }
        }
    }

    fn render_active(&self) -> bool {
        !self.windowing.occluded
    }

    fn request_redraw(&self) {
        if let Some(window) = &self.windowing.window {
            window.request_redraw();
        }
    }

    fn wake_for_render(&mut self) {
        self.rendering.next_tick_at = Instant::now();
        self.rendering.force_redraw = true;
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
        if now >= self.rendering.next_tick_at {
            self.rendering.next_tick_at = now + TICK_INTERVAL;
            self.request_redraw();
        }
        event_loop.set_control_flow(winit::event_loop::ControlFlow::WaitUntil(
            self.rendering.next_tick_at,
        ));
    }

    fn tick_frame(&mut self, game: &mut ClientGame, settings: &AppSettings) -> Result<()> {
        let Some(window) = self.windowing.window.clone() else {
            return Ok(());
        };
        if self.rendering.renderer.is_none() {
            return Ok(());
        }

        self.input.poll_gamepads();
        self.sync_surface_state(game, settings)?;
        let surface = game.surface_state(MAIN_SURFACE_ID);

        let mut overlay_states = Vec::new();
        let mut overlay_text = Vec::new();
        if game.binding_prompt().is_some() {
            window.set_title(&format!("{} - Input Bindings", game.game_name()));

            for action in self.input.binding_actions() {
                game.apply_binding_ui_action(action)?;
            }

            self.rendering.update_binding_overlay_and_logs(
                game,
                &mut overlay_states,
                &mut overlay_text,
            );
        } else {
            self.rendering.last_prompt_signature = None;
            window.set_title(game.game_name());
            if self.windowing.focused {
                let input_capture = &self.input.capture;
                game.send_bound_inputs(|path| input_capture.read_binding_value(path))?;
            }
        }

        let mut states = game.render_states();
        if settings.show_latency {
            if let Some(renderer) = self.rendering.renderer.as_ref() {
                renderer
                    .build_latency_overlay(game.latency_snapshot())
                    .merge_into(&mut states, &mut overlay_text);
            }
        }
        states.extend(overlay_states);
        let render_needed = self.rendering.force_redraw
            || self.rendering.cache.last_surface_state != Some(surface)
            || self.rendering.cache.last_render_revision != game.render_revision()
            || self.rendering.cache.last_rendered_states != states
            || self.rendering.cache.last_rendered_text != overlay_text;
        if render_needed {
            if let Some(renderer) = self.rendering.renderer.as_mut() {
                renderer.render(&states, game.resources(), &overlay_text)?;
            }
            self.rendering.cache.last_rendered_states = states;
            self.rendering.cache.last_rendered_text = overlay_text;
            self.rendering.cache.last_surface_state = Some(surface);
            self.rendering.cache.last_render_revision = game.render_revision();
            self.rendering.force_redraw = false;
        }

        self.input.end_frame();
        Ok(())
    }
}

struct WindowState {
    window: Option<Arc<Window>>,
    focused: bool,
    occluded: bool,
}

impl Default for WindowState {
    fn default() -> Self {
        Self { window: None, focused: true, occluded: false }
    }
}

impl WindowState {
    fn set_focused(&mut self, focused: bool, input: &mut InputState) {
        self.focused = focused;
        if !focused {
            input.clear_active_inputs();
        }
    }

    fn apply_window_lock_if_needed(
        &self,
        enabled: bool,
        surface: super::SurfaceState,
        rendering: &mut RenderingState,
    ) {
        let next_lock = WindowLockState {
            enabled,
            dimensions: enabled.then_some(surface.dimension_lock).flatten(),
        };
        if rendering.cache.last_applied_window_lock == Some(next_lock) {
            return;
        }
        let Some(window) = self.window.as_ref() else {
            rendering.cache.last_applied_window_lock = Some(next_lock);
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

        rendering.cache.last_applied_window_lock = Some(next_lock);
    }
}

struct InputState {
    capture: InputCapture,
}

impl Default for InputState {
    fn default() -> Self {
        Self { capture: InputCapture::new() }
    }
}

impl InputState {
    fn poll_gamepads(&mut self) {
        self.capture.poll_gamepads();
    }

    fn binding_actions(&mut self) -> Vec<super::bindings::UiAction> {
        self.capture.binding_actions()
    }

    fn consume_device_event(&mut self, device_id: winit::event::DeviceId, event: DeviceEvent) {
        self.capture.consume_device_event(device_id, event);
    }

    fn consume_window_event(&mut self, event: &WindowEvent) {
        self.capture.consume_window_event(event);
    }

    fn clear_active_inputs(&mut self) {
        self.capture.clear_active_inputs();
    }

    fn end_frame(&mut self) {
        self.capture.end_frame();
    }
}

struct RenderingState {
    renderer: Option<Renderer>,
    force_redraw: bool,
    next_tick_at: Instant,
    cache: RenderCache,
    last_prompt_signature: Option<String>,
}

impl Default for RenderingState {
    fn default() -> Self {
        Self {
            renderer: None,
            force_redraw: true,
            next_tick_at: Instant::now(),
            cache: RenderCache::default(),
            last_prompt_signature: None,
        }
    }
}

impl RenderingState {
    fn invalidate_window_lock(&mut self) {
        self.cache.last_applied_window_lock = None;
    }

    fn update_binding_overlay_and_logs(
        &mut self,
        game: &ClientGame,
        overlay_states: &mut Vec<super::RenderState>,
        overlay_text: &mut Vec<super::renderer::TextCommand>,
    ) {
        let Some(prompt) = game.binding_prompt() else {
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

    fn next_action(&self) -> Option<MenuAction> {
        while let Ok(event) = MenuEvent::receiver().try_recv() {
            if event.id == self.quit_id {
                return Some(MenuAction::Quit);
            }
            if event.id == self.show_latency_id {
                return Some(MenuAction::ToggleShowLatency(self.show_latency_item.clone()));
            }
            if event.id == self.full_screen_content_id {
                return Some(MenuAction::ToggleFullScreenContent(
                    self.full_screen_content_item.clone(),
                ));
            }
            if event.id == self.lock_window_to_surface_id {
                return Some(MenuAction::ToggleLockWindowToSurface(
                    self.lock_window_to_surface_item.clone(),
                ));
            }
        }
        None
    }
}

#[cfg(target_os = "macos")]
enum MenuAction {
    Quit,
    ToggleShowLatency(CheckMenuItem),
    ToggleFullScreenContent(CheckMenuItem),
    ToggleLockWindowToSurface(CheckMenuItem),
}
