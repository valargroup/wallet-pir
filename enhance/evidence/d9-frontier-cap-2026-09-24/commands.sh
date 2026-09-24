#!/bin/sh
# Run from repository root on a disposable machine with enough free disk.
set -eu
cargo build --locked --release -p enhance-pir-server --example frontier_cap
mkdir -p enhance/evidence/d9-frontier-cap-2026-09-24
/usr/bin/time -l target/release/examples/frontier_cap --output /tmp/wallet-pir-d9-final-8k-20260924 --cap 8192 --queries 12 > enhance/evidence/d9-frontier-cap-2026-09-24/8k.jsonl 2> enhance/evidence/d9-frontier-cap-2026-09-24/8k.time
/usr/bin/time -l target/release/examples/frontier_cap --output /tmp/wallet-pir-d9-final-2k-20260924 --cap 2048 --queries 12 > enhance/evidence/d9-frontier-cap-2026-09-24/2k.jsonl 2> enhance/evidence/d9-frontier-cap-2026-09-24/2k.time
/usr/bin/time -l target/release/examples/frontier_cap --output /tmp/wallet-pir-d9-retained-8k-20260924 --cap 8192 --queries 2 --retained-revisions 5 > enhance/evidence/d9-frontier-cap-2026-09-24/8k-retained.jsonl 2> enhance/evidence/d9-frontier-cap-2026-09-24/8k-retained.time
/usr/bin/time -l target/release/examples/frontier_cap --output /tmp/wallet-pir-d9-retained-2k-20260924 --cap 2048 --queries 2 --retained-revisions 5 > enhance/evidence/d9-frontier-cap-2026-09-24/2k-retained.jsonl 2> enhance/evidence/d9-frontier-cap-2026-09-24/2k-retained.time
