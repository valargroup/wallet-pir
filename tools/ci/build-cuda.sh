#!/usr/bin/env bash
# Build in Ubuntu 22.04; CUDA libraries are dynamically loaded on the GPU host.
set -euo pipefail
# shellcheck disable=SC1091
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 22.04 ]]
[[ "$(getconf GNU_LIBC_VERSION)" == 'glibc 2.35' ]]
[[ "$(rustc --version | cut -d' ' -f2)" == 1.91.0 ]]
[[ "$(rustc -vV | sed -n 's/^host: //p')" == x86_64-unknown-linux-gnu ]]
export CARGO_TARGET_DIR=target/native-cuda
export RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq'
export CFLAGS=-mpclmul CXXFLAGS=-mpclmul
python3 tools/ci/stage.py compile:cuda -- cargo build --locked --release \
  -p enhance-pir-server -p enhance-pir -p enhance-pir-load-test \
  --bin enhance-pir-server --bin enhance-pir-cli --bin enhance-pir-load-test \
  --features enhance-pir/cli,enhance-pir/native-reinspiring,enhance-pir-server/native-reinspiring,enhance-pir-server/cuda,enhance-pir-load-test/native-reinspiring
# Loading driver libraries remains a live qualification gate; --help must work
# on a hosted build runner without a CUDA driver.
for binary in enhance-pir-server enhance-pir-cli enhance-pir-load-test; do
  "$CARGO_TARGET_DIR/release/$binary" --help >/dev/null
done
python3 - <<'PY'
import json
from pathlib import Path
import sys
sys.path.insert(0, 'tools/ci')
from release import CUDA_BUILD
Path('target/native-cuda/build.json').write_text(json.dumps(CUDA_BUILD, indent=2) + '\n')
PY
