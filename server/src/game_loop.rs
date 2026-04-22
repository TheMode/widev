use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::LoopOptions;
use crate::game::{Game, NetworkEvent};
use crate::game_state::GameState;
use crate::net::network::NetworkRuntime;

pub struct GameLoop {
    config: LoopOptions,
    start: Instant,
    completed_ticks: u64,
}

impl GameLoop {
    pub fn new(config: LoopOptions, start: Instant) -> Self {
        Self { config, start, completed_ticks: 0 }
    }

    pub fn spin_threshold(&self) -> Duration {
        self.config.spin_threshold()
    }

    pub fn until_next_tick(&self, now: Instant) -> Duration {
        self.next_tick_at().saturating_duration_since(now)
    }

    pub fn coarse_wait(&self, now: Instant) -> Duration {
        self.until_next_tick(now)
            .saturating_sub(self.config.spin_threshold())
            .min(self.config.shutdown_poll_interval())
    }

    pub fn run_due_ticks(
        &mut self,
        network: &NetworkRuntime,
        state: &mut GameState,
        game: &mut dyn Game,
    ) {
        let mut ticks_ran = 0u32;
        while ticks_ran < self.config.max_catch_up_ticks && self.is_tick_due(Instant::now()) {
            self.run_single_tick(network, state, game);
            ticks_ran += 1;
        }

        if ticks_ran == self.config.max_catch_up_ticks {
            self.skip_missed_ticks(Instant::now());
        }
    }

    pub fn run_single_tick(
        &mut self,
        network: &NetworkRuntime,
        state: &mut GameState,
        game: &mut dyn Game,
    ) {
        let tick_number = self.completed_ticks + 1;
        let tick_at = self.tick_at(tick_number);
        let dt = self.tick_duration(tick_number);
        game.on_tick(state, tick_at, dt);
        network.dispatch_messages(state.drain_outbox());
        self.completed_ticks = tick_number;
    }

    pub fn spin_until_next_tick(
        &self,
        shutdown: &ShutdownSignal,
        network: &NetworkRuntime,
        state: &mut GameState,
        game: &mut dyn Game,
    ) -> bool {
        while !shutdown.is_requested() && !self.is_tick_due(Instant::now()) {
            let events = network.drain_events();
            if events.is_empty() {
                std::hint::spin_loop();
            } else {
                handle_network_events(events, state, game);
            }
        }

        !shutdown.is_requested()
    }

    fn skip_missed_ticks(&mut self, now: Instant) {
        if !self.is_tick_due(now) {
            return;
        }

        let due_ticks = self.ticks_due_by(now);
        let skipped = due_ticks.saturating_sub(self.completed_ticks);
        if skipped > 0 {
            self.completed_ticks = due_ticks;
            log::warn!(
                "game loop fell behind by {} tick(s); skipped ahead to preserve schedule",
                skipped
            );
        }
    }

    fn next_tick_at(&self) -> Instant {
        self.tick_at(self.completed_ticks + 1)
    }

    fn tick_at(&self, tick_number: u64) -> Instant {
        self.start + self.elapsed_for_tick(tick_number)
    }

    fn is_tick_due(&self, now: Instant) -> bool {
        now >= self.next_tick_at()
    }

    fn tick_duration(&self, tick_number: u64) -> Duration {
        self.elapsed_for_tick(tick_number) - self.elapsed_for_tick(tick_number.saturating_sub(1))
    }

    fn elapsed_for_tick(&self, tick_number: u64) -> Duration {
        let nanos = (u128::from(tick_number) * 1_000_000_000u128)
            / u128::from(self.config.ticks_per_second);
        Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
    }

    fn ticks_due_by(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.start).as_nanos();
        let due = (elapsed * u128::from(self.config.ticks_per_second)) / 1_000_000_000u128;
        due.min(u128::from(u64::MAX)) as u64
    }
}

pub struct ShutdownSignal {
    requested: Arc<AtomicBool>,
}

impl ShutdownSignal {
    pub fn install() -> Result<Self> {
        let requested = Arc::new(AtomicBool::new(false));
        let handler_flag = Arc::clone(&requested);
        ctrlc::set_handler(move || {
            handler_flag.store(true, Ordering::SeqCst);
        })
        .context("failed to install Ctrl-C shutdown handler")?;

        Ok(Self { requested })
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

pub fn handle_network_events(
    events: Vec<NetworkEvent>,
    state: &mut GameState,
    game: &mut dyn Game,
) {
    for event in events {
        game.on_event(state, event);
    }
}
