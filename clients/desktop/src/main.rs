use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;

mod game;

#[derive(Debug, Parser)]
#[command(name = "widev-desktop-client")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    server: SocketAddr,
}

fn main() -> Result<()> {
    init_logging();

    let args = Args::parse();
    game::run(game::GameConfig { server_addr: args.server })
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
