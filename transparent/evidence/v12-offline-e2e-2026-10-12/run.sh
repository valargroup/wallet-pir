#!/usr/bin/env bash
# Offline v12 end to end on the development hub: publish the recent journal
# slice as a v12 set, verify it, serve it locally and recover every sample
# wallet the slice covers, exactly, over HTTP.
# Usage: transparent/evidence/v12-offline-e2e-2026-10-12/run.sh SLICE WORK
#   SLICE  the read-only recent slice (heights 3,262,749-3,473,686)
#   WORK   a new scratch directory (journal links, the set, wallet stores)
set -euo pipefail
cd "$(dirname "$0")/../../.."
slice=${1:?recent journal slice}
work=${2:?new scratch directory}
out=transparent/evidence/v12-offline-e2e-2026-10-12
bin="${CARGO_TARGET_DIR:-target}/release-fast"
# The block before the slice, as the 2026-09-08 publication recorded it
# (continuous-publication-2026-09-08: shard 159 ends at 3,262,748).
parent=0000000000b5d4b73a0544dd9d9bab7850e88a8ab5069148c53e6f1899b4176a
source_sha=$(git rev-parse HEAD)

cargo build --locked --profile release-fast -p transparent-filter-server \
  --bin shard-publish --bin shard-verify --bin sample-redigest
cargo build --locked --profile release-fast -p transparent-shard-server --bin transparent-shard-server
cargo build --locked --profile release-fast -p transparent-loadtest --example sample_recovery

mkdir "$work"
# Hard links: shard-publish takes a shared lock file beside the journal, which
# must not be created in the retained slice.
cp -al "$slice" "$work/journal"
/usr/bin/time -v "$bin/shard-publish" --data-dir "$work/journal" --output "$work/set" \
  --recent-geometry decoupled-2k-8k --directory-choice all --parent-block-hash "$parent" \
  --zakura-cookie /dev/null --record "$out/publication.json" --source-sha "$source_sha" \
  2>"$out/publish.log"
"$bin/shard-verify" --shard-dir "$work/set" --data-dir "$work/journal" --rebuild 6 \
  --expect-start 3262749 --expect-through 3473686 \
  --expect-anchor-hash 0000000000755137ff64d560a32b9190e62ae085923c9e69930c05550f50be1d \
  >"$out/verify.json" 2>"$out/verify.log"
cp "$work/set/shards.json" "$out/shards.json"

"$bin/transparent-shard-server" --listen 127.0.0.1:18093 --shard-dir "$work/set" \
  --cache-bytes $((6 << 30)) --query-slots 4 >"$work/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null || true' EXIT
until curl -fsS http://127.0.0.1:18093/v1/ready >/dev/null 2>&1; do sleep 1; done

# The frozen sample's digests are over an older event codec; re-derive them
# from the slice, which refuses unless every event count is unchanged.
rm -f "$out/sample-recent-slice-redigested.json"
"$bin/sample-redigest" --sample "$out/sample-recent-slice.json" --data-dir "$work/journal" \
  --out "$out/sample-recent-slice-redigested.json" --source-sha "$source_sha"
# Every wallet the slice covers, once, eight at a time.
/usr/bin/time -v "$bin/examples/sample_recovery" http://127.0.0.1:18093 \
  "$out/sample-recent-slice-redigested.json" 8 >"$out/wallets.ndjson" 2>"$out/recovery.log"
curl -fsS http://127.0.0.1:18093/metrics >"$out/server-metrics.txt"
python3 "$out/summarize.py" "$out/wallets.ndjson" >"$out/results.json"
