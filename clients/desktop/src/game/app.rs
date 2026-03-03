use anyhow::{Context, Result};
use minifb::{Key, KeyRepeat, Window, WindowOptions};

use super::ClientGame;

const WIDTH: usize = 800;
const HEIGHT: usize = 600;

pub(super) fn run(game: &mut ClientGame) -> Result<()> {
    let mut window = Window::new("widev desktop POC", WIDTH, HEIGHT, WindowOptions::default())
        .context("failed to create desktop window")?;
    window.set_target_fps(60);

    let mut buffer = vec![0x101010u32; WIDTH * HEIGHT];

    while window.is_open() {
        game.tick_network()?;

        if let Some(prompt) = game.binding_prompt() {
            let prompt_title = match prompt.suggestion {
                Some(key) => format!(
                    "Bind {} [{}] - press Enter to confirm {:?}, Backspace to skip, Esc to quit",
                    prompt.identifier, prompt.input_type, key
                ),
                None => format!(
                    "Bind {} [{}] - press a key, Enter to confirm, Backspace to skip, Esc to quit",
                    prompt.identifier, prompt.input_type
                ),
            };
            window.set_title(&prompt_title);

            for key in window.get_keys_pressed(KeyRepeat::No) {
                if matches!(key, Key::Enter | Key::Backspace | Key::Escape) {
                    continue;
                }
                game.suggest_binding_key(key);
            }

            if window.is_key_pressed(Key::Enter, KeyRepeat::No) {
                game.confirm_binding()?;
            }
            if window.is_key_pressed(Key::Backspace, KeyRepeat::No) {
                game.skip_binding();
            }
        } else {
            window.set_title("widev desktop POC");
            game.send_bound_inputs(|key| window.is_key_down(key))?;
        }

        if window.is_key_down(Key::Escape) {
            break;
        }

        clear(&mut buffer, 0x101010);
        let state = game.render_state();
        draw_square(
            &mut buffer,
            state.x as i32,
            state.y as i32,
            state.size as i32,
            state.color,
        );

        window
            .update_with_buffer(&buffer, WIDTH, HEIGHT)
            .context("failed to update frame buffer")?;
    }

    Ok(())
}

fn clear(buf: &mut [u32], color: u32) {
    buf.fill(color);
}

fn draw_square(buf: &mut [u32], x: i32, y: i32, size: i32, color: u32) {
    let half = size / 2;
    let x_min = (x - half).max(0);
    let y_min = (y - half).max(0);
    let x_max = (x + half).min(WIDTH as i32 - 1);
    let y_max = (y + half).min(HEIGHT as i32 - 1);

    for yy in y_min..=y_max {
        for xx in x_min..=x_max {
            let idx = yy as usize * WIDTH + xx as usize;
            buf[idx] = color;
        }
    }
}
