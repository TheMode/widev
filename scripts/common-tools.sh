#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ADDR="127.0.0.1:4433"

usage() {
  cat <<'EOF'
Usage:
  scripts/common-tools.sh server [addr]
  scripts/common-tools.sh client [addr]
  scripts/common-tools.sh bots [addr] [count] [flow]
  scripts/common-tools.sh flame [name] [duration] [output]

Defaults:
  addr     = 127.0.0.1:4433
  count    = 600
  flow     = ack-move
  name     = widev-server
  duration = 30
  output   = server-flame.svg
EOF
}

cmd="${1:-}"
if [[ -z "$cmd" ]]; then
  usage
  exit 1
fi
shift || true

case "$cmd" in
  server)
    addr="${1:-$DEFAULT_ADDR}"
    cd "$ROOT_DIR"
    cargo run -p widev-server --release -- "$addr"
    ;;

  client)
    addr="${1:-$DEFAULT_ADDR}"
    cd "$ROOT_DIR"
    cargo run -p widev-desktop-client --release -- "$addr"
    ;;

  bots)
    addr="${1:-$DEFAULT_ADDR}"
    count="${2:-600}"
    flow="${3:-ack-move}"
    cd "$ROOT_DIR"
    cargo run -p widev-desktop-bots --release -- "$addr" --bots "$count" --flow "$flow"
    ;;

  flame)
    name="${1:-widev-server}"
    duration="${2:-30}"
    output="${3:-server-flame.svg}"
    cd "$ROOT_DIR"
    scripts/flamegraph.sh --name "$name" --duration "$duration" --output "$output"
    ;;

  *)
    usage
    exit 1
    ;;
esac
