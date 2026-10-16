#!/usr/bin/env bash
# Compile first, then release clean compiler file cache so cgroup-aware service
# tests measure their own working set. Cargo artifacts remain reusable on disk.
set -euo pipefail
ci_dir=$(dirname "$0")
# One owned directory across all test executables in this invocation. Production
# cache validation/keys remain authoritative; cold/cache-corruption tests use
# their own explicit directories. Traps clean up successful and failed runs.
fixture_cache=$(mktemp -d "${TMPDIR:-/tmp}/wallet-pir-test-runtime.XXXXXX")
export WALLET_PIR_TEST_RUNTIME_CACHE="$fixture_cache"
trap 'rm -rf -- "$fixture_cache"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 "$ci_dir/stage.py" compile -- cargo test --no-run "$@"
python3 "$ci_dir/stage.py" reclaim -- python3 "$ci_dir/reclaim-build-cache.py"
python3 "$ci_dir/stage.py" tests -- cargo test "$@"
