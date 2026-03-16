use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::game::{ClientId, Game, NetworkEvent};
use crate::game_state::GameState;
use crate::packets::{
    DeliveryPolicy, InputType, MessageId, PacketBundle, PacketEnvelope, PacketResource,
    PacketTarget, S2CPacket,
};

const GAME_WIDTH: f32 = 800.0;
const GAME_HEIGHT: f32 = 600.0;
const PADDLE_WIDTH: f32 = 20.0;
const PADDLE_HEIGHT: f32 = 80.0;
const PADDLE_SPEED: f32 = 300.0;
const BALL_SIZE: f32 = 16.0;
const BALL_SPEED_INITIAL: f32 = 250.0;
const BALL_SPEED_MAX: f32 = 450.0;
const PADDLE_MARGIN: f32 = 30.0;

#[derive(Default)]
struct PaddleInput {
    up: bool,
    down: bool,
}

struct Paddle {
    x: f32,
    y: f32,
    input: PaddleInput,
}

struct Ball {
    x: f32,
    y: f32,
    vx: f32,
    vy: f32,
    speed: f32,
}

struct Match {
    player1: ClientId,
    player2: ClientId,
    paddle1: Paddle,
    paddle2: Paddle,
    ball: Ball,
    score1: u8,
    score2: u8,
    winner: Option<ClientId>,
}

pub struct PongGame {
    matchmaking_queue: VecDeque<ClientId>,
    matches: HashMap<u64, Match>,
    next_match_id: u64,
    texture_id: MessageId,
    texture_png: Vec<u8>,
}

impl PongGame {
    pub fn new(_started_at: Instant, state: &mut GameState) -> Self {
        Self {
            matchmaking_queue: VecDeque::new(),
            matches: HashMap::new(),
            next_match_id: 1,
            texture_id: state.alloc_message_id(),
            texture_png: encode_white_png(32),
        }
    }

    fn try_start_match(&mut self, state: &mut GameState) {
        while self.matchmaking_queue.len() >= 2 {
            let player1 = self.matchmaking_queue.pop_front().unwrap();
            let player2 = self.matchmaking_queue.pop_front().unwrap();

            let match_id = self.next_match_id;
            self.next_match_id += 1;

            let paddle1 = Paddle {
                x: PADDLE_MARGIN,
                y: GAME_HEIGHT / 2.0 - PADDLE_HEIGHT / 2.0,
                input: PaddleInput::default(),
            };
            let paddle2 = Paddle {
                x: GAME_WIDTH - PADDLE_MARGIN - PADDLE_WIDTH,
                y: GAME_HEIGHT / 2.0 - PADDLE_HEIGHT / 2.0,
                input: PaddleInput::default(),
            };

            let ball = Self::spawn_ball(player1 < player2);

            let m = Match {
                player1,
                player2,
                paddle1,
                paddle2,
                ball,
                score1: 0,
                score2: 0,
                winner: None,
            };

            self.matches.insert(match_id, m);

            self.send_game_bootstrap(state, match_id, player1);
            self.send_game_bootstrap(state, match_id, player2);

            log::info!("match {} started: {} vs {}", match_id, player1, player2);
        }
    }

    fn spawn_ball(serve_left: bool) -> Ball {
        let angle = (rand_simple() * std::f32::consts::PI / 4.0) - std::f32::consts::PI / 8.0;
        let dir_x = if serve_left { 1.0 } else { -1.0 };
        Ball {
            x: GAME_WIDTH / 2.0 - BALL_SIZE / 2.0,
            y: GAME_HEIGHT / 2.0 - BALL_SIZE / 2.0,
            vx: angle.cos() * BALL_SPEED_INITIAL * dir_x,
            vy: angle.sin() * BALL_SPEED_INITIAL,
            speed: BALL_SPEED_INITIAL,
        }
    }

    fn send_waiting_screen(&mut self, state: &mut GameState, client_id: ClientId) {
        let message_id = state.alloc_message_id();

        let bundle = vec![
            S2CPacket::ServerHello { tick_rate_hz: state.ticks_per_second() },
            S2CPacket::SetGameName { name: "Pong".to_string() },
            S2CPacket::SurfaceLockAspectRatio { surface_id: 1, numerator: 4, denominator: 3 },
            S2CPacket::SurfaceClearBackground { surface_id: 1, color: [0.1, 0.0, 0.15, 1.0] },
            S2CPacket::BindingDeclare {
                binding_id: 1,
                identifier: "move_up".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::BindingDeclare {
                binding_id: 2,
                identifier: "move_down".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::Join {},
        ];

        state.send(
            PacketEnvelope::bundle(PacketTarget::Client(client_id), bundle)
                .id(message_id)
                .delivery(DeliveryPolicy::RequireClientReceipt)
                .sequence(Uuid::now_v7()),
        );
    }

    fn send_game_bootstrap(&mut self, state: &mut GameState, match_id: u64, client_id: ClientId) {
        let message_id = state.alloc_message_id();
        let bootstrap_sequence_id = Uuid::now_v7();
        let Some(m) = self.matches.get(&match_id) else { return };

        let is_player1 = m.player1 == client_id;
        let player_element = if is_player1 { m.player1 } else { m.player2 };
        let opponent_element = if is_player1 { m.player2 } else { m.player1 };

        let paddle_y = if is_player1 { m.paddle1.y } else { m.paddle2.y };
        let opponent_paddle_y = if is_player1 { m.paddle2.y } else { m.paddle1.y };

        state.send_resource(
            PacketResource::new(
                PacketTarget::Client(client_id),
                self.texture_id,
                "image/png",
                self.texture_png.clone(),
                -1,
            )
            .sequence(bootstrap_sequence_id),
        );

        let bundle = vec![
            S2CPacket::ServerHello { tick_rate_hz: state.ticks_per_second() },
            S2CPacket::SetGameName { name: "Pong".to_string() },
            S2CPacket::SurfaceLockAspectRatio { surface_id: 1, numerator: 4, denominator: 3 },
            S2CPacket::SurfaceClearBackground { surface_id: 1, color: [0.1, 0.0, 0.15, 1.0] },
            S2CPacket::BindingDeclare {
                binding_id: 1,
                identifier: "move_up".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::BindingDeclare {
                binding_id: 2,
                identifier: "move_down".to_string(),
                input_type: InputType::Toggle,
            },
            S2CPacket::ElementAdd { element_id: player_element },
            S2CPacket::ElementSetTexture {
                element_id: player_element,
                resource_id: self.texture_id,
            },
            S2CPacket::ElementSetColor { element_id: player_element, color: [0.6, 0.8, 1.0, 1.0] },
            S2CPacket::ElementSetSize {
                element_id: player_element,
                width: PADDLE_WIDTH,
                height: PADDLE_HEIGHT,
            },
            S2CPacket::ElementMove {
                element_id: player_element,
                x: if is_player1 { m.paddle1.x } else { m.paddle2.x },
                y: paddle_y,
            },
            S2CPacket::ElementAdd { element_id: opponent_element },
            S2CPacket::ElementSetTexture {
                element_id: opponent_element,
                resource_id: self.texture_id,
            },
            S2CPacket::ElementSetColor {
                element_id: opponent_element,
                color: [1.0, 0.4, 0.4, 1.0],
            },
            S2CPacket::ElementSetSize {
                element_id: opponent_element,
                width: PADDLE_WIDTH,
                height: PADDLE_HEIGHT,
            },
            S2CPacket::ElementMove {
                element_id: opponent_element,
                x: if is_player1 { m.paddle2.x } else { m.paddle1.x },
                y: opponent_paddle_y,
            },
            S2CPacket::ElementAdd { element_id: 0 },
            S2CPacket::ElementSetTexture { element_id: 0, resource_id: self.texture_id },
            S2CPacket::ElementSetColor { element_id: 0, color: [1.0, 1.0, 1.0, 1.0] },
            S2CPacket::ElementSetSize { element_id: 0, width: BALL_SIZE, height: BALL_SIZE },
            S2CPacket::ElementMove { element_id: 0, x: m.ball.x, y: m.ball.y },
            S2CPacket::Join {},
        ];

        state.send(
            PacketEnvelope::bundle(PacketTarget::Client(client_id), bundle)
                .id(message_id)
                .delivery(DeliveryPolicy::RequireClientReceipt)
                .sequence(bootstrap_sequence_id),
        );
    }

    fn send_score_update(&mut self, state: &mut GameState, match_id: u64) {
        let Some(m) = self.matches.get(&match_id) else { return };

        let bundle: PacketBundle = vec![
            S2CPacket::ElementMove { element_id: m.player1, x: m.paddle1.x, y: m.paddle1.y },
            S2CPacket::ElementMove { element_id: m.player2, x: m.paddle2.x, y: m.paddle2.y },
            S2CPacket::ElementMove { element_id: 0, x: m.ball.x, y: m.ball.y },
        ];

        state.send(PacketEnvelope::bundle(PacketTarget::Broadcast, bundle).droppable());
    }

    fn remove_from_queue(&mut self, client_id: ClientId) {
        self.matchmaking_queue.retain(|&id| id != client_id);
    }

    fn find_match_and_remove_player(&mut self, client_id: ClientId) -> Option<(u64, Match)> {
        let mut match_to_remove = None;
        for (&match_id, m) in &self.matches {
            if m.player1 == client_id || m.player2 == client_id {
                match_to_remove = Some(match_id);
                break;
            }
        }

        if let Some(match_id) = match_to_remove {
            let m = self.matches.remove(&match_id).unwrap();
            Some((match_id, m))
        } else {
            None
        }
    }
}

fn rand_simple() -> f32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    (nanos as f32) / (u32::MAX as f32)
}

impl Game for PongGame {
    fn on_event(&mut self, state: &mut GameState, event: NetworkEvent) {
        match event {
            NetworkEvent::ClientConnected(client_id) => {
                log::info!("client {} connected, joining matchmaking", client_id);
                self.remove_from_queue(client_id);
                self.matchmaking_queue.push_back(client_id);
                self.try_start_match(state);
                if !self.matches.values().any(|m| m.player1 == client_id || m.player2 == client_id)
                {
                    self.send_waiting_screen(state, client_id);
                }
            },
            NetworkEvent::ClientDisconnected(client_id) => {
                log::info!("client {} disconnected", client_id);
                self.remove_from_queue(client_id);

                if let Some((match_id, m)) = self.find_match_and_remove_player(client_id) {
                    let remaining = if m.player1 == client_id { m.player2 } else { m.player1 };
                    log::info!("match {} ended, player {} wins by disconnect", match_id, remaining);
                    self.matchmaking_queue.push_back(remaining);
                    self.try_start_match(state);
                }
            },
            NetworkEvent::ClientPacket { client_id, packet } => match packet {
                crate::packets::C2SPacket::InputValue { binding_id, value } => {
                    let pressed = value >= 0.5;
                    for m in self.matches.values_mut() {
                        let paddle = if m.player1 == client_id {
                            Some((&mut m.paddle1, true))
                        } else if m.player2 == client_id {
                            Some((&mut m.paddle2, false))
                        } else {
                            None
                        };

                        if let Some((paddle, _is_player1)) = paddle {
                            match binding_id {
                                1 => paddle.input.up = pressed,
                                2 => paddle.input.down = pressed,
                                _ => {},
                            }
                        }
                    }
                },
                _ => {},
            },
            _ => {},
        }
    }

    fn on_tick(&mut self, state: &mut GameState, _now: Instant, dt: Duration) {
        let dt_seconds = dt.as_secs_f32();

        let match_ids: Vec<u64> = self.matches.keys().copied().collect();
        let mut matches_to_end = Vec::new();

        for match_id in match_ids {
            let Some(m) = self.matches.get_mut(&match_id) else { continue };
            if m.winner.is_some() {
                continue;
            }

            let dy1 = (m.paddle1.input.down as i8 - m.paddle1.input.up as i8) as f32;
            m.paddle1.y = (m.paddle1.y + dy1 * PADDLE_SPEED * dt_seconds)
                .clamp(0.0, GAME_HEIGHT - PADDLE_HEIGHT);

            let dy2 = (m.paddle2.input.down as i8 - m.paddle2.input.up as i8) as f32;
            m.paddle2.y = (m.paddle2.y + dy2 * PADDLE_SPEED * dt_seconds)
                .clamp(0.0, GAME_HEIGHT - PADDLE_HEIGHT);

            m.ball.x += m.ball.vx * dt_seconds;
            m.ball.y += m.ball.vy * dt_seconds;

            if m.ball.y <= 0.0 {
                m.ball.y = 0.0;
                m.ball.vy = -m.ball.vy;
            } else if m.ball.y + BALL_SIZE >= GAME_HEIGHT {
                m.ball.y = GAME_HEIGHT - BALL_SIZE;
                m.ball.vy = -m.ball.vy;
            }

            let paddle1_hit = m.ball.x <= m.paddle1.x + PADDLE_WIDTH
                && m.ball.x + BALL_SIZE >= m.paddle1.x
                && m.ball.y + BALL_SIZE >= m.paddle1.y
                && m.ball.y <= m.paddle1.y + PADDLE_HEIGHT;
            if paddle1_hit {
                m.ball.x = m.paddle1.x + PADDLE_WIDTH;
                m.ball.vx = -m.ball.vx.abs();
                m.ball.speed = (m.ball.speed * 1.05).min(BALL_SPEED_MAX);
                let rel_y = (m.ball.y + BALL_SIZE / 2.0 - m.paddle1.y - PADDLE_HEIGHT / 2.0)
                    / (PADDLE_HEIGHT / 2.0);
                m.ball.vy = rel_y * m.ball.speed * 0.5;
                m.ball.vx = m.ball.speed;
            }

            let paddle2_hit = m.ball.x + BALL_SIZE >= m.paddle2.x
                && m.ball.x <= m.paddle2.x + PADDLE_WIDTH
                && m.ball.y + BALL_SIZE >= m.paddle2.y
                && m.ball.y <= m.paddle2.y + PADDLE_HEIGHT;
            if paddle2_hit {
                m.ball.x = m.paddle2.x - BALL_SIZE;
                m.ball.vx = m.ball.vx.abs();
                m.ball.speed = (m.ball.speed * 1.05).min(BALL_SPEED_MAX);
                let rel_y = (m.ball.y + BALL_SIZE / 2.0 - m.paddle2.y - PADDLE_HEIGHT / 2.0)
                    / (PADDLE_HEIGHT / 2.0);
                m.ball.vy = rel_y * m.ball.speed * 0.5;
                m.ball.vx = -m.ball.speed;
            }

            if m.ball.x + BALL_SIZE < 0.0 {
                m.score2 += 1;
                log::info!("score: {} - {}", m.score1, m.score2);
                if m.score2 >= 7 {
                    m.winner = Some(m.player2);
                } else {
                    m.ball = Self::spawn_ball(true);
                }
            } else if m.ball.x > GAME_WIDTH {
                m.score1 += 1;
                log::info!("score: {} - {}", m.score1, m.score2);
                if m.score1 >= 7 {
                    m.winner = Some(m.player1);
                } else {
                    m.ball = Self::spawn_ball(false);
                }
            }
        }

        let match_ids: Vec<u64> = self.matches.keys().copied().collect();
        for match_id in match_ids {
            let Some(m) = self.matches.get(&match_id) else { continue };
            if let Some(winner) = m.winner {
                log::info!("match {} won by player {}", match_id, winner);
                matches_to_end.push(match_id);
            }
        }

        for match_id in matches_to_end {
            if let Some(m) = self.matches.remove(&match_id) {
                self.matchmaking_queue.push_back(m.player1);
                self.matchmaking_queue.push_back(m.player2);
                self.try_start_match(state);
            }
        }

        let match_ids: Vec<u64> = self.matches.keys().copied().collect();
        for match_id in match_ids {
            let Some(m) = self.matches.get(&match_id) else { continue };
            if m.winner.is_none() {
                self.send_score_update(state, match_id);
            }
        }
    }
}

fn encode_white_png(size: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(Cursor::new(&mut bytes), size, size);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().expect("encoding generated texture header");
        let mut rgba = Vec::with_capacity((size * size * 4) as usize);
        for _ in 0..(size * size) {
            rgba.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        }
        writer.write_image_data(&rgba).expect("encoding generated texture payload");
    }
    bytes
}
