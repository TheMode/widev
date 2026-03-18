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
cargo run -p widev-server -- 127.0.0.1:4433
```

In another terminal:

```bash
cargo run -p widev-desktop-client -- 127.0.0.1:4433
```

Optional bot runner:

```bash
cargo run -p widev-desktop-bots -- 127.0.0.1:4433
```

## Notes

- The server runs `pong` by default. Pass `red_square` to start that game instead.
- Packet enums are generated from `common/packets/packets.json` at build time.
- Local development certificates are generated automatically in `server/certs/` if they are missing.
