#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Generate a flamegraph for a running process (Linux + macOS).

Usage:
  scripts/flamegraph.sh --pid <PID> [options]
  scripts/flamegraph.sh --name <pattern> [options]

Options:
  --pid <PID>             Target process id.
  --name <pattern>        Process match pattern for pgrep -f.
  --duration <seconds>    Sampling duration (default: 30).
  --frequency <hz>        Sampling frequency (default: 199).
  --output <file.svg>     Output flamegraph path (default: flamegraph.svg).
  --sudo                  Run profilers with sudo (mainly Linux perf).
  -h, --help              Show this help.

Linux requirements:
  - perf
  - inferno-collapse-perf
  - inferno-flamegraph

macOS requirements:
  - sample
  - stackcollapse-sample.awk
  - flamegraph.pl

Examples:
  scripts/flamegraph.sh --name widev-server --duration 30 --output server.svg
  scripts/flamegraph.sh --pid 12345 --duration 20 --sudo
USAGE
}

OS="$(uname -s)"
PID=""
NAME_PATTERN=""
DURATION=30
FREQUENCY=199
OUTPUT="flamegraph.svg"
USE_SUDO=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid)
      PID="${2:-}"
      shift 2
      ;;
    --name)
      NAME_PATTERN="${2:-}"
      shift 2
      ;;
    --duration)
      DURATION="${2:-}"
      shift 2
      ;;
    --frequency)
      FREQUENCY="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:-}"
      shift 2
      ;;
    --sudo)
      USE_SUDO=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$PID" && -z "$NAME_PATTERN" ]]; then
  echo "Provide --pid or --name." >&2
  usage
  exit 1
fi

if [[ -n "$PID" && -n "$NAME_PATTERN" ]]; then
  echo "Use only one of --pid or --name." >&2
  exit 1
fi

if [[ -n "$NAME_PATTERN" ]]; then
  matches=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && matches+=("$line")
  done < <(pgrep -f "$NAME_PATTERN" || true)
  if [[ ${#matches[@]} -eq 0 ]]; then
    echo "No process matched --name '$NAME_PATTERN'." >&2
    exit 1
  fi
  PID="${matches[0]}"
  if [[ ${#matches[@]} -gt 1 ]]; then
    echo "Multiple PIDs matched; using first: $PID (all: ${matches[*]})" >&2
  fi
fi

if ! [[ "$PID" =~ ^[0-9]+$ ]]; then
  echo "Invalid PID: $PID" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap cleanup EXIT

run_cmd() {
  if [[ "$USE_SUDO" -eq 1 ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

find_mac_tool() {
  local tool="$1"
  if command -v "$tool" >/dev/null 2>&1; then
    command -v "$tool"
    return 0
  fi

  local candidate
  for candidate in \
    "/opt/homebrew/opt/flamegraph/bin/$tool" \
    "/usr/local/opt/flamegraph/bin/$tool" \
    "/opt/homebrew/bin/$tool" \
    "/usr/local/bin/$tool"
  do
    if [[ -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

mkdir -p "$(dirname "$OUTPUT")"

case "$OS" in
  Linux)
    command -v perf >/dev/null 2>&1 || {
      echo "Missing 'perf'." >&2
      exit 1
    }
    command -v inferno-collapse-perf >/dev/null 2>&1 || {
      echo "Missing 'inferno-collapse-perf' (install: cargo install inferno)." >&2
      exit 1
    }
    command -v inferno-flamegraph >/dev/null 2>&1 || {
      echo "Missing 'inferno-flamegraph' (install: cargo install inferno)." >&2
      exit 1
    }

    perf_data="$tmpdir/perf.data"
    perf_script="$tmpdir/perf.script"
    folded="$tmpdir/folded.txt"

    echo "[linux] recording PID=$PID for ${DURATION}s at ${FREQUENCY}Hz"
    run_cmd perf record -F "$FREQUENCY" -g -p "$PID" -o "$perf_data" -- sleep "$DURATION"
    run_cmd perf script -i "$perf_data" > "$perf_script"
    inferno-collapse-perf "$perf_script" > "$folded"
    inferno-flamegraph "$folded" > "$OUTPUT"
    ;;

  Darwin)
    command -v sample >/dev/null 2>&1 || {
      echo "Missing 'sample' (part of macOS developer tools)." >&2
      exit 1
    }

    STACKCOLLAPSE="$(find_mac_tool stackcollapse-sample.awk || true)"
    FLAMEGRAPH_PL="$(find_mac_tool flamegraph.pl || true)"

    if [[ -z "$STACKCOLLAPSE" ]]; then
      echo "Missing stackcollapse-sample.awk (install FlameGraph scripts, e.g. 'brew install flamegraph')." >&2
      exit 1
    fi
    if [[ -z "$FLAMEGRAPH_PL" ]]; then
      echo "Missing flamegraph.pl (install FlameGraph scripts, e.g. 'brew install flamegraph')." >&2
      exit 1
    fi

    sample_out="$tmpdir/sample.txt"
    folded="$tmpdir/folded.txt"

    # sample interval is milliseconds.
    interval_ms=$(( 1000 / FREQUENCY ))
    if (( interval_ms < 1 )); then
      interval_ms=1
    fi

    echo "[macos] sampling PID=$PID for ${DURATION}s every ${interval_ms}ms"
    run_cmd sample "$PID" "$DURATION" "$interval_ms" -file "$sample_out" >/dev/null
    "$STACKCOLLAPSE" "$sample_out" > "$folded"
    "$FLAMEGRAPH_PL" "$folded" > "$OUTPUT"
    ;;

  *)
    echo "Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

echo "Flamegraph written to $OUTPUT"
