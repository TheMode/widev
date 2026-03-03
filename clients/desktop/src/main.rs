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
    let args = Args::parse();
    game::run(game::GameConfig {
        server_addr: args.server,
    })
}
