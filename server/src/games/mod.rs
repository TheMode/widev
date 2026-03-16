use std::time::Instant;

use crate::game::Game;
use crate::game_state::GameState;

pub mod pong;
pub mod red_square;

pub type GameFactory = fn(Instant, &mut GameState) -> Box<dyn Game>;

const GAMES: [(&str, GameFactory); 2] = [
    ("pong", |s, st| Box::new(pong::PongGame::new(s, st))),
    ("red_square", |s, st| Box::new(red_square::RedSquareGame::new(s, st))),
];

pub fn create_game(
    name: &str,
    started_at: Instant,
    state: &mut GameState,
) -> Option<Box<dyn Game>> {
    GAMES.iter().find(|(n, _)| *n == name).map(|(_, factory)| factory(started_at, state))
}

pub fn game_names() -> Vec<&'static str> {
    GAMES.iter().map(|(name, _)| *name).collect()
}
