use std::time::Instant;

use crate::game::Game;

pub mod red_square;

pub fn default_game(started_at: Instant) -> Box<dyn Game> {
    Box::new(red_square::RedSquareGame::new(started_at))
}
