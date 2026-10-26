#!/usr/bin/env bash
# Build in Ubuntu 22.04; CUDA libraries are dynamically loaded on the GPU host.
set -euo pipefail
products=enhance,native-reinspiring,cuda
if [[ ${1:-} == --unified ]]; then
  products=enhance,status,transparent-history,transparent-detail,native-reinspiring,cuda
  shift
fi
[[ $# == 0 ]] || { echo 'usage: build-cuda.sh [--unified]' >&2; exit 2; }
# shellcheck disable=SC1091
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 22.04 ]]
[[ "$(getconf GNU_LIBC_VERSION)" == 'glibc 2.35' ]]
[[ "$(rustc --version | cut -d' ' -f2)" == 1.97.1 ]]
[[ "$(rustc -vV | sed -n 's/^host: //p')" == x86_64-unknown-linux-gnu ]]
export CARGO_TARGET_DIR=target/native-cuda
export RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq'
export CFLAGS=-mpclmul CXXFLAGS=-mpclmul
export WALLET_PIR_SOURCE_REVISION
WALLET_PIR_SOURCE_REVISION=${WALLET_PIR_SOURCE_REVISION:-$(git rev-parse HEAD)}
mkdir -p "$CARGO_TARGET_DIR"
# Clients stay their own invocation. The query server is another, so the cuda
# feature is not unified into the publisher. GPU hosts do not run the publisher.
python3 tools/ci/stage.py compile:cuda-clients -- cargo build --locked --release \
  -p enhance-pir-client -p enhance-pir-load-test \
  --bin enhance-pir-cli --bin enhance-pir-load-test \
  --features enhance-pir-client/native-reinspiring,enhance-pir-load-test/native-reinspiring
python3 tools/ci/stage.py compile:cuda -- cargo build --locked --release \
  -p pir-query-server --bin pir-query-server \
  --features "$products" \
  --message-format=json-render-diagnostics > "$CARGO_TARGET_DIR/build-artifacts.jsonl"
python3 tools/ci/verify_cuda_build.py --messages "$CARGO_TARGET_DIR/build-artifacts.jsonl" \
  --target "$CARGO_TARGET_DIR" --revision "$WALLET_PIR_SOURCE_REVISION"
# Loading driver libraries remains a live qualification gate; --help must work
# on a hosted build runner without a CUDA driver.
for binary in pir-query-server enhance-pir-cli enhance-pir-load-test; do
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
