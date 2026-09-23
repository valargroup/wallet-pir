#!/bin/sh
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates curl git build-essential clang libclang-dev cmake pkg-config protobuf-compiler libssl-dev python3 > /out/dependencies.log 2>&1
export RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo CARGO_TARGET_DIR=/opt/target
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain 1.91.0 --no-modify-path
export PATH=/opt/cargo/bin:$PATH
rustc --version
mkdir /work
cp -a /source/. /work/
cd /work
export CARGO_BUILD_JOBS=2 CFLAGS=-mpclmul CXXFLAGS=-mpclmul
export RUSTFLAGS='-Dwarnings -C target-cpu=x86-64-v3'
cargo build --locked --profile release-fast -p enhance-pir-server --bin enhance-pir-v4 -p enhance-pir --bin enhance-pir-cli -p enhance-pir-load-test --bin enhance-pir-load-test --features enhance-pir/cli
cp /opt/target/release-fast/enhance-pir-v4 /opt/target/release-fast/enhance-pir-cli /opt/target/release-fast/enhance-pir-load-test /out/
sha256sum /out/enhance-pir-* > /out/binaries.sha256
/out/enhance-pir-v4 --help > /out/v4-help.txt
