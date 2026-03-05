use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;

mod game;
mod game_state;
mod games;
mod network;
mod network_platform;
#[allow(dead_code)]
mod packets;

use game::Game;
use game::NetworkEvent;
use game_state::GameState;
use network::NetworkRuntime;

const IDLE_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug, Parser)]
#[command(name = "widev-server")]
struct Args {
    /// Server bind address (IP:PORT)
    #[arg(default_value = "127.0.0.1:4433")]
    bind: SocketAddr,
}

fn main() -> Result<()> {
    init_logging();

    let args = Args::parse();
    let network = NetworkRuntime::start(args.bind)?;

    let mut game_state = GameState::new(60);
    let mut game = games::default_game(Instant::now(), &mut game_state);
    let mut last_tick = Instant::now();

    loop {
        handle_network_events(&network, &mut game_state, game.as_mut());

        let now = Instant::now();
        let dt = now.duration_since(last_tick);
        if dt >= game_state.tick_interval() {
            game.on_tick(&mut game_state, now, dt);
            let envelopes = game_state.drain_outbox();
            network.dispatch_envelopes(envelopes);
            last_tick = now;
        }

        std::thread::sleep(IDLE_SLEEP);
    }
}

fn handle_network_events(network: &NetworkRuntime, state: &mut GameState, game: &mut dyn Game) {
    for event in network.drain_events() {
        match &event {
            NetworkEvent::ClientConnected(client_id) => {
                state.connect_client(*client_id);
            },
            NetworkEvent::ClientDisconnected(client_id) => {
                state.disconnect_client(*client_id);
            },
            NetworkEvent::ClientPacket { .. } => {},
        }
        game.on_event(state, event);
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
