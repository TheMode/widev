# widev POC

Authoritative-server 2D game proof-of-concept with strict separation:

- `server/`: Rust QUIC game server (uses `quiche`)
- `clients/desktop/`: lightweight desktop client (Rust + `minifb`)
- `common/`: shared packet schema + packet code generator

## Architecture (POC)

- Networking: UDP/QUIC via `quiche` datagrams.
- Authority: server owns the simulation and sends world state.
- Client role: barebones renderer + input collection.
- Protocol: packet enums are generated from `common/packets/packets.json` for both server and client at build time.
 - Codegen is backend-based (`common/protocol_codegen`) so additional language targets can be added later.

## Run

From workspace root:

```bash
cargo run -p widev-server -- 127.0.0.1:4433
```

In another terminal:

```bash
cargo run -p widev-desktop-client -- 127.0.0.1:4433
```

Controls:

- `WASD`: move square
- `Esc`: close client

## Notes

- Server cert/key are auto-generated in `server/certs/` for local development if missing.
- This is a single-player/single-connection POC to validate the architecture and packet/codegen flow.
