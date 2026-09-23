#!/bin/sh
set -eu
cd /work
python3 enhance/ops/scripts/test-v4-local.py --bin-dir /out --out /out/linux-smoke --seconds 10 --concurrency 1,2
