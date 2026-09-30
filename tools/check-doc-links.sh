#!/usr/bin/env bash
# Keep the established entry point; scan and cache anchors in one process.
set -euo pipefail
exec python3 "$(dirname "$0")/check-doc-links.py"
