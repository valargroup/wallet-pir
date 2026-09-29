#!/bin/bash
set -euo pipefail
cd /root/wallet-pir-8e69ea75
export CARGO_TARGET_DIR=/tmp/wallet-pir-v10-production-target
export RUSTFLAGS='-C target-cpu=x86-64-v3'
export CXXFLAGS=-mpclmul CFLAGS=-mpclmul LIBCLANG_PATH=/usr/lib/llvm-18/lib CARGO_BUILD_JOBS=5
/root/.cargo/bin/cargo build --locked --release -p transparent-shard-server --example rate-query
/root/.cargo/bin/cargo test --locked --release -p transparent-shard-server --example rate-query
