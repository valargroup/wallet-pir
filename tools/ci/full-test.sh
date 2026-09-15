#!/usr/bin/env bash
# Compile first, then release clean compiler file cache so cgroup-aware service
# tests measure their own working set. Cargo artifacts remain reusable on disk.
set -euo pipefail
cargo test --no-run "$@"
python3 "$(dirname "$0")/reclaim-build-cache.py"
exec cargo test "$@"
