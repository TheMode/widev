#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ADDR="127.0.0.1:4433"
DEFAULT_LOG_DIR="./logs/network"

run_release_binary() {
  local pkg="$1"
  local bin="$2"
  shift 2

  cargo build -p "$pkg" --release
  exec "$ROOT_DIR/target/release/$bin" "$@"
}

setup_net_log_env() {
  local log_dir="${1:-$DEFAULT_LOG_DIR}"
  local flush_policy="${2:-flow}"
  local console="${3:-1}"

  export WIDEV_NET_TRACE=1
  export WIDEV_NET_TRACE_DIR="$log_dir"
  export WIDEV_NET_TRACE_FLUSH="$flush_policy"
  export WIDEV_NET_TRACE_CONSOLE="$console"
}

usage() {
  cat <<'EOF'
Usage:
  scripts/common-tools.sh server [addr] [--log[=dir]]
  scripts/common-tools.sh client [addr]
  scripts/common-tools.sh bots [addr] [count] [flow] [--log[=dir]]
  scripts/common-tools.sh flame [name] [duration] [output]

Options:
  --log[=dir]    Enable network tracing to specified directory
                 Default directory: ./logs/network

Defaults:
  addr     = 127.0.0.1:4433
  count    = 600
  flow     = ack-move
  name     = widev-server
  duration = 30
  output   = server-flame.svg

Environment Variables (for network tracing):
  WIDEV_NET_TRACE=1                  Enable tracing
  WIDEV_NET_TRACE_DIR=./logs/network Output directory
  WIDEV_NET_TRACE_FLUSH=flow         Flush policy: every|batch|flow
  WIDEV_NET_TRACE_CONSOLE=1          Also log to console

Examples:
  scripts/common-tools.sh server                           # Run server without tracing
  scripts/common-tools.sh server --log                     # Run server with tracing to ./logs/network
  scripts/common-tools.sh server --log=./custom-logs       # Run server with tracing to custom directory
  WIDEV_NET_TRACE_FLUSH=every scripts/common-tools.sh bots --log  # Bots with immediate flush
EOF
}

parse_log_arg() {
  local arg="$1"
  if [[ "$arg" == "--log" ]]; then
    echo "$DEFAULT_LOG_DIR"
  elif [[ "$arg" == --log=* ]]; then
    echo "${arg#--log=}"
  else
    echo ""
  fi
}

cmd="${1:-}"
if [[ -z "$cmd" ]]; then
  usage
  exit 1
fi
shift || true

case "$cmd" in
  server)
    addr="$DEFAULT_ADDR"
    log_dir=""
    
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --log| --log=*)
          log_dir="$(parse_log_arg "$1")"
          shift
          ;;
        *)
          if [[ -z "$addr" || "$addr" == "$DEFAULT_ADDR" ]]; then
            addr="$1"
          fi
          shift
          ;;
      esac
    done

    if [[ -n "$log_dir" ]]; then
      mkdir -p "$log_dir"
      setup_net_log_env "$log_dir"
      echo "Network logging enabled: $log_dir"
    fi

    cd "$ROOT_DIR"
    run_release_binary "widev-server" "widev-server" "$addr"
    ;;

  client)
    addr="${1:-$DEFAULT_ADDR}"
    cd "$ROOT_DIR"
    run_release_binary "widev-desktop-client" "widev-desktop-client" "$addr"
    ;;

  bots)
    addr="$DEFAULT_ADDR"
    count="600"
    flow="ack-move"
    log_dir=""

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --log|--log=*)
          log_dir="$(parse_log_arg "$1")"
          shift
          ;;
        *)
          if [[ "$addr" == "$DEFAULT_ADDR" && "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+$ ]]; then
            addr="$1"
          elif [[ "$count" == "600" && "$1" =~ ^[0-9]+$ ]]; then
            count="$1"
          elif [[ "$flow" == "ack-move" ]]; then
            flow="$1"
          fi
          shift
          ;;
      esac
    done

    if [[ -n "$log_dir" ]]; then
      mkdir -p "$log_dir"
      setup_net_log_env "$log_dir"
      echo "Network logging enabled: $log_dir"
    fi

    cd "$ROOT_DIR"
    run_release_binary "widev-desktop-bots" "widev-desktop-bots" "$addr" --bots "$count" --flow "$flow"
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
