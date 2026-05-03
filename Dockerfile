# syntax=docker/dockerfile:1

# ── Stage 1: install cargo-chef on a slim Rust image ─────────────────────────
FROM rust:1.88-slim AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /build

# ── Stage 2: compute the dependency recipe ───────────────────────────────────
# Rebuilds only when Cargo.toml / Cargo.lock change.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json
# ── Stage 3: build dependencies, then the binary ─────────────────────────────
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Cook just the server package's dependency tree so bench crates are excluded.
RUN cargo chef cook --release -p likhadb-server --recipe-path recipe.json
COPY . .
RUN cargo build --release -p likhadb-server

# ── Stage 4: minimal runtime image ───────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/likhadb-server /app/likhadb-server

# Persistent data directory — mount a named volume or host path here.
RUN mkdir /data
VOLUME ["/data"]

EXPOSE 8080
EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/likhadb-server", "/data"]
