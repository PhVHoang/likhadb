#!/usr/bin/env bash
set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
DATA_DIR="${LIKHADB_DATA:-/tmp/likhadb-dev}"
HTTP_ADDR="${HTTP_ADDR:-0.0.0.0:8080}"
GRPC_ADDR="${GRPC_ADDR:-[::]:50051}"
RUST_LOG="${RUST_LOG:-info}"
HOT_RELOAD=0
CLEAN=0

# ── helpers ───────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  -w, --watch       Enable hot reload (requires cargo-watch, installs if absent)
  -c, --clean       Wipe data directory before starting
  -d, --data DIR    Data directory  (default: $DATA_DIR)
  --http ADDR       REST listen address  (default: $HTTP_ADDR)
  --grpc ADDR       gRPC listen address  (default: $GRPC_ADDR)
  --log LEVEL       RUST_LOG filter      (default: $RUST_LOG)
  -h, --help        Show this help
EOF
}

log()  { printf '\033[1;34m[dev]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[err]\033[0m %s\n' "$*" >&2; exit 1; }

# ── arg parse ─────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    -w|--watch)   HOT_RELOAD=1 ;;
    -c|--clean)   CLEAN=1 ;;
    -d|--data)    DATA_DIR="$2"; shift ;;
    --http)       HTTP_ADDR="$2"; shift ;;
    --grpc)       GRPC_ADDR="$2"; shift ;;
    --log)        RUST_LOG="$2"; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)            die "Unknown option: $1" ;;
  esac
  shift
done

# ── repo root check ───────────────────────────────────────────────────────────
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || die "Run this script from inside the likhadb repository."
cd "$REPO_ROOT"

# ── data directory ────────────────────────────────────────────────────────────
if [[ $CLEAN -eq 1 && -d "$DATA_DIR" ]]; then
  warn "Wiping data directory: $DATA_DIR"
  rm -rf "$DATA_DIR"
fi
mkdir -p "$DATA_DIR"
ok "Data directory: $DATA_DIR"

# ── port conflict check ───────────────────────────────────────────────────────
HTTP_PORT="${HTTP_ADDR##*:}"
GRPC_PORT="${GRPC_ADDR##*:}"

for port in "$HTTP_PORT" "$GRPC_PORT"; do
  if lsof -iTCP:"$port" -sTCP:LISTEN -t &>/dev/null; then
    die "Port $port is already in use. Stop the existing process or choose a different address."
  fi
done

# ── export env for child processes ────────────────────────────────────────────
export RUST_LOG HTTP_ADDR GRPC_ADDR

# ── hot-reload mode ───────────────────────────────────────────────────────────
if [[ $HOT_RELOAD -eq 1 ]]; then
  if ! command -v cargo-watch &>/dev/null; then
    log "cargo-watch not found — installing..."
    cargo install cargo-watch --locked
    ok "cargo-watch installed."
  fi

  log "Starting with hot reload (cargo-watch)..."
  log "Watching: crates/ and build.rs files"
  log "REST → http://$HTTP_ADDR   gRPC → $GRPC_ADDR"
  log "Data → $DATA_DIR"
  log "Press Ctrl+C to stop."
  echo

  # -x: cargo subcommand to run
  # -w: paths to watch (only source, not target/)
  # --ignore: skip files that don't affect the binary
  exec cargo watch \
    --watch crates \
    --watch Cargo.toml \
    --watch Cargo.lock \
    --ignore "*.snap" \
    --ignore "*.log" \
    --ignore "benches/" \
    --clear \
    --delay 1 \
    -x "run -p likhadb-server -- $DATA_DIR"
fi

# ── normal mode ───────────────────────────────────────────────────────────────
log "Building likhadb-server (debug)..."
cargo build -p likhadb-server 2>&1

BINARY="$REPO_ROOT/target/debug/likhadb-server"
[[ -x "$BINARY" ]] || die "Build succeeded but binary not found at $BINARY"

ok "Build complete."
log "REST → http://$HTTP_ADDR"
log "gRPC → $GRPC_ADDR"
log "Data → $DATA_DIR"
log "Log  → $RUST_LOG"
log "Press Ctrl+C to stop."
echo

# Trap so data dir is not left in a partially-written state on interrupt.
cleanup() {
  echo
  warn "Shutting down..."
}
trap cleanup INT TERM

exec "$BINARY" "$DATA_DIR"
