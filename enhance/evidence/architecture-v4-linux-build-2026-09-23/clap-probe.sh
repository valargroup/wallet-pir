set -eu
mkdir -p /tmp/clap-probe/src
cp /out/clap-probe.rs /tmp/clap-probe/src/main.rs
cat > /tmp/clap-probe/Cargo.toml <<'END'
[package]
name="clap-probe"
version="0.1.0"
edition="2021"
[dependencies]
clap="=4.6.0"
END
cd /tmp/clap-probe
export PATH=/opt/cargo/bin:$PATH CARGO_BUILD_JOBS=2
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --offline --release
./target/release/clap-probe --help
