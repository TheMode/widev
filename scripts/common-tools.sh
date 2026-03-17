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
  scripts/common-tools.sh server [addr] [game] [--log[=dir]]
  scripts/common-tools.sh server --addr=ADDR --game=GAME [--log[=dir]]
  scripts/common-tools.sh client [addr]
  scripts/common-tools.sh client --addr=ADDR
  scripts/common-tools.sh bots [addr] [count] [flow] [--log[=dir]]
  scripts/common-tools.sh bots --addr=ADDR --count=N --flow=NAME [--log[=dir]]
  scripts/common-tools.sh flame [name] [duration] [output]
  scripts/common-tools.sh flame --name=NAME --duration=SECS --output=FILE

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
  scripts/common-tools.sh server red_square                # Run the red_square game
  scripts/common-tools.sh server 127.0.0.1:4433 red_square
  scripts/common-tools.sh server --game=red_square         # Run the red_square game
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
    game=""
    log_dir=""
    
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --log|--log=*)
          log_dir="$(parse_log_arg "$1")"
          shift
          ;;
        --addr=*)
          addr="${1#--addr=}"
          shift
          ;;
        --bind=*)
          addr="${1#--bind=}"
          shift
          ;;
        --game=*)
          game="${1#--game=}"
          shift
          ;;
        *)
          if [[ "$1" =~ ^[0-9a-zA-Z.-]+:[0-9]+$ ]]; then
            addr="$1"
          elif [[ -z "$game" ]]; then
            game="$1"
          else
            echo "server: unexpected argument: $1" >&2
            exit 1
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
    if [[ -n "$game" ]]; then
      run_release_binary "widev-server" "widev-server" "$addr" "$game"
    else
      run_release_binary "widev-server" "widev-server" "$addr"
    fi
    ;;

  client)
    addr="$DEFAULT_ADDR"
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --addr=*)
          addr="${1#--addr=}"
          shift
          ;;
        *)
          if [[ "$addr" == "$DEFAULT_ADDR" ]]; then
            addr="$1"
          else
            echo "client: unexpected argument: $1" >&2
            exit 1
          fi
          shift
          ;;
      esac
    done
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
        --addr=*)
          addr="${1#--addr=}"
          shift
          ;;
        --count=*)
          count="${1#--count=}"
          shift
          ;;
        --flow=*)
          flow="${1#--flow=}"
          shift
          ;;
        *)
          if [[ "$addr" == "$DEFAULT_ADDR" && "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+$ ]]; then
            addr="$1"
          elif [[ "$count" == "600" && "$1" =~ ^[0-9]+$ ]]; then
            count="$1"
          elif [[ "$flow" == "ack-move" ]]; then
            flow="$1"
          else
            echo "bots: unexpected argument: $1" >&2
            exit 1
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
    name="widev-server"
    duration="30"
    output="server-flame.svg"

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --name=*)
          name="${1#--name=}"
          shift
          ;;
        --duration=*)
          duration="${1#--duration=}"
          shift
          ;;
        --output=*)
          output="${1#--output=}"
          shift
          ;;
        *)
          if [[ "$name" == "widev-server" ]]; then
            name="$1"
          elif [[ "$duration" == "30" ]]; then
            duration="$1"
          elif [[ "$output" == "server-flame.svg" ]]; then
            output="$1"
          else
            echo "flame: unexpected argument: $1" >&2
            exit 1
          fi
          shift
          ;;
      esac
    done

    cd "$ROOT_DIR"
    scripts/flamegraph.sh --name "$name" --duration "$duration" --output "$output"
    ;;

  *)
    usage
    exit 1
    ;;
esac
