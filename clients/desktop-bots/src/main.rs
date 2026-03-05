use std::net::SocketAddr;

use anyhow::{bail, Result};
use clap::Parser;
use widev_desktop_bots::{run_with_flow, AckAndMoveFlow, BotRunnerConfig, PassiveFlow};

#[derive(Debug, Parser)]
#[command(name = "widev-desktop-bots")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    server: SocketAddr,

    /// Total bots to create.
    #[arg(long, default_value_t = 100)]
    bots: usize,

    /// Bot joins per second.
    #[arg(long, default_value_t = 100.0)]
    join_rate: f64,

    /// Per-bot local flow tick rate.
    #[arg(long, default_value_t = 60)]
    bot_tick_hz: u32,

    /// Worker thread count (0 = auto based on available_parallelism).
    #[arg(long, default_value_t = 0)]
    workers: usize,

    /// Built-in flow preset: passive | ack-move
    #[arg(long, default_value = "ack-move")]
    flow: String,
}

fn main() -> Result<()> {
    init_logging();

    let args = Args::parse();
    let cfg = BotRunnerConfig {
        server_addr: args.server,
        bot_count: args.bots,
        joins_per_second: args.join_rate,
        bot_tick_hz: args.bot_tick_hz,
        worker_threads: args.workers,
    };

    match args.flow.as_str() {
        "passive" => run_with_flow(cfg, |_| Box::new(PassiveFlow)),
        "ack-move" => run_with_flow(cfg, |_| Box::new(AckAndMoveFlow::new())),
        other => bail!("unknown flow '{}'; expected: passive | ack-move", other),
    }
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
