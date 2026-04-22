# widev

`widev` is a workspace for experimenting with an authoritative multiplayer game setup over QUIC.

## Workspace

- `server/`: game server (`widev-server`)
- `clients/desktop/`: desktop client (`widev-desktop-client`)
- `clients/desktop-bots/`: bot client (`widev-desktop-bots`)
- `common/`: shared packet schema and code generation

## Run

From the workspace root:

```bash
cargo run -p xtask -- server
```

Start the client against the default local server:

```bash
cargo run -p xtask -- client
```

Or launch both in one command for local testing:

```bash
cargo run -p xtask -- play
```

Optional bot runner:

```bash
cargo run -p xtask -- bots
```

`xtask` uses debug builds by default so edit-test cycles stay fast. Add `--release` when you want optimized binaries, for example:

```bash
cargo run -p xtask -- play --release
cargo run -p xtask -- bots --release --count=1000
```

For profiling, generate a flamegraph for a running server with:

```bash
cargo run -p xtask -- flame --name widev-server --duration 30 --output server-flame.svg
```

## Notes

- The server runs `pong` by default. Pass `red_square` to start that game instead.
- Packet enums are generated from `common/packets/packets.json` at build time.
- Local development certificates are generated automatically in `server/certs/` if they are missing.
