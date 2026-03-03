use std::time::Instant;

use crate::game::Game;
use crate::game_state::GameState;

pub mod red_square;

pub fn default_game(started_at: Instant, state: &mut GameState) -> Box<dyn Game> {
    Box::new(red_square::RedSquareGame::new(started_at, state))
}
