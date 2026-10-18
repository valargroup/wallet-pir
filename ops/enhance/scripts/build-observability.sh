#!/usr/bin/env bash
# Production binaries must not inherit this repo's developer target-cpu=native.
set -euo pipefail
export RUSTFLAGS='-C target-cpu=haswell'
export CFLAGS='-march=haswell'
export CXXFLAGS='-march=haswell'
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/observability-haswell}"
cargo build --locked --release -p pir-apm -p pir-monitor -p enhance-pir-server \
  --features pir-monitor/native-reinspiring,enhance-pir-server/native-reinspiring
