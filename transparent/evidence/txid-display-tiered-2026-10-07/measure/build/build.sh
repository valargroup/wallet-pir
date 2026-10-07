#!/bin/bash
set -euo pipefail
source ~/.cargo/env
cd /root/txid-measure-d191f86b/src
export CARGO_TARGET_DIR=/root/txid-measure-d191f86b/target
cargo build --locked --release -p transparent-shard-server --example txid-rate --example txid-bandwidth
cd $CARGO_TARGET_DIR/release/examples && sha256sum txid-rate txid-bandwidth > /root/txid-measure-d191f86b/binaries.sha256
echo BUILD_DONE
