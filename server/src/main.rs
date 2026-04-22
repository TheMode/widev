use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Args as ClapArgs, Parser};

mod game;
mod game_loop;
mod game_state;
mod games;
mod net;

pub use net::{network_trace, packet_codec, packet_scheduler, packets};

use game_loop::{GameLoop, ShutdownSignal, handle_network_events};
use game_state::GameState;
use net::network::NetworkRuntime;

#[derive(Debug, Clone, ClapArgs)]
pub(crate) struct LoopOptions {
    /// Fixed simulation tick rate in ticks per second.
    #[arg(long = "tick-rate", default_value_t = 60)]
    pub ticks_per_second: u16,
    /// Maximum number of catch-up ticks to run in one loop iteration before skipping ahead.
    #[arg(long, default_value_t = 8)]
    pub max_catch_up_ticks: u32,
    /// Busy-spin window before each tick deadline, in microseconds.
    #[arg(long = "spin-threshold-us", default_value_t = 250)]
    spin_threshold_us: u64,
    /// Maximum time to block waiting for events before re-checking shutdown, in milliseconds.
    #[arg(long = "shutdown-poll-ms", default_value_t = 10)]
    shutdown_poll_ms: u64,
}

impl LoopOptions {
    fn spin_threshold(&self) -> Duration {
        Duration::from_micros(self.spin_threshold_us)
    }

    fn shutdown_poll_interval(&self) -> Duration {
        Duration::from_millis(self.shutdown_poll_ms)
    }
}

#[derive(Debug, Parser)]
#[command(name = "widev-server")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    bind: SocketAddr,
    /// Game to run (e.g. pong, red_square)
    #[arg(default_value = "")]
    game: String,
    #[command(flatten)]
    loop_options: LoopOptions,
}

fn main() -> Result<()> {
    init_logging();

    let args = Args::parse();
    let shutdown = ShutdownSignal::install()?;
    let loop_options = args.loop_options.clone();

    let game_names = games::game_names();
    let game_name = if args.game.is_empty() {
        game_names.first().copied().unwrap_or("pong")
    } else {
        &args.game
    };

    log::info!("starting game: {}", game_name);

    let network = NetworkRuntime::start(args.bind)?;

    log::info!(
        "game loop config: tick_rate={}Hz, max_catch_up_ticks={}, spin_threshold={}us, shutdown_poll={}ms",
        loop_options.ticks_per_second,
        loop_options.max_catch_up_ticks,
        loop_options.spin_threshold().as_micros(),
        loop_options.shutdown_poll_interval().as_millis()
    );

    let start = Instant::now();
    let mut game_state = GameState::new(loop_options.ticks_per_second);
    let mut game = match games::create_game(game_name, start, &mut game_state) {
        Some(g) => g,
        None => {
            log::error!("unknown game: {}", game_name);
            anyhow::bail!("unknown game: {}", game_name);
        },
    };
    let mut game_loop = GameLoop::new(loop_options, start);

    while !shutdown.is_requested() {
        handle_network_events(network.drain_events(), &mut game_state, game.as_mut());

        game_loop.run_due_ticks(&network, &mut game_state, game.as_mut());

        if shutdown.is_requested() {
            break;
        }

        let now = Instant::now();
        let until_next_tick = game_loop.until_next_tick(now);
        if until_next_tick.is_zero() {
            continue;
        }

        if until_next_tick > game_loop.spin_threshold() {
            let wait_for = game_loop.coarse_wait(now);
            handle_network_events(
                network.wait_for_events(wait_for),
                &mut game_state,
                game.as_mut(),
            );
            continue;
        }

        if !game_loop.spin_until_next_tick(&shutdown, &network, &mut game_state, game.as_mut()) {
            break;
        }
    }

    log::info!("shutdown requested, stopping server");
    Ok(())
}

fn init_logging() {
    use std::io::Write;

    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    builder
        .format(|buf, record| {
            let ts = buf.timestamp_millis();
            let (c0, c1) = match record.level() {
                log::Level::Error => ("\x1b[31m", "\x1b[0m"),
                log::Level::Warn => ("\x1b[33m", "\x1b[0m"),
                log::Level::Info => ("\x1b[36m", "\x1b[0m"),
                log::Level::Debug => ("\x1b[90m", "\x1b[0m"),
                log::Level::Trace => ("\x1b[90m", "\x1b[0m"),
            };
            writeln!(buf, "[{} {}{}{}] {}", ts, c0, record.level(), c1, record.args())
        })
        .init();
}
