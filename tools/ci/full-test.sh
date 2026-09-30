#!/usr/bin/env bash
# Compile first, then release clean compiler file cache so cgroup-aware service
# tests measure their own working set. Cargo artifacts remain reusable on disk.
set -euo pipefail
ci_dir=$(dirname "$0")
python3 "$ci_dir/stage.py" compile -- cargo test --no-run "$@"
python3 "$ci_dir/stage.py" reclaim -- python3 "$ci_dir/reclaim-build-cache.py"
exec python3 "$ci_dir/stage.py" tests -- cargo test "$@"
