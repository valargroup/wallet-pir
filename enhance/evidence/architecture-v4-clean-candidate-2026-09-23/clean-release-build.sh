#!/bin/sh
set -eu
export PATH=/opt/cargo/bin:$PATH RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo CARGO_TARGET_DIR=/opt/target
export CARGO_BUILD_JOBS=1 CFLAGS=-mpclmul CXXFLAGS=-mpclmul
export RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq'
mkdir -p /clean-candidate
cd /clean-candidate
tar -xf /out/clean-candidate-source.tar
cargo build --locked --offline --release -p enhance-pir-server --bin enhance-pir-v4 -p enhance-pir --bin enhance-pir-cli -p enhance-pir-load-test --bin enhance-pir-load-test --features enhance-pir/cli
mkdir -p /out/clean-release/release
cp /opt/target/release/enhance-pir-v4 /opt/target/release/enhance-pir-cli /opt/target/release/enhance-pir-load-test /out/clean-release/release/
sha256sum /out/clean-release/release/enhance-pir-* > /out/clean-release/binaries.sha256
