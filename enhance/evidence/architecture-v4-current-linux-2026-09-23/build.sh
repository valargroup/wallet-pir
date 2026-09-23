#!/bin/sh
set -eu
export PATH=/opt/cargo/bin:$PATH RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo CARGO_TARGET_DIR=/opt/target
export CARGO_BUILD_JOBS=2 CFLAGS=-mpclmul CXXFLAGS=-mpclmul
export RUSTFLAGS='-Dwarnings -C target-cpu=x86-64-v3'
cd /work
cargo build --locked --offline --profile release-fast -p enhance-pir-server --bin enhance-pir-v4 -p enhance-pir --bin enhance-pir-cli -p enhance-pir-load-test --bin enhance-pir-load-test --features enhance-pir/cli
mkdir -p /out/current-candidate
cp /opt/target/release-fast/enhance-pir-v4 /opt/target/release-fast/enhance-pir-cli /opt/target/release-fast/enhance-pir-load-test /out/current-candidate/
sha256sum /out/current-candidate/enhance-pir-* > /out/current-candidate/binaries.sha256
