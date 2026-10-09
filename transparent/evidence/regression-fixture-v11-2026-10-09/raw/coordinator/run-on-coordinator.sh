#!/usr/bin/env bash
# Export the schema v11 regression fixture by a read-only replay of the v3 journal
# on the coordinator, following tier-boundary-sealed-2026-10-08.
#
# Writes only /srv/claude-regression-export-$SHA (build tree, removed at the end)
# and /root/claude-regression-v11 (inputs and outputs, kept until the fixture is
# frozen). Reads /srv/transparent-activity/full-v3/journal without its writer
# lock. Takes no production lock; publishes, restarts and reconfigures nothing.
# Build and replay run under MemoryMax=12G, no swap, CPUQuota=200%, nice 19 and
# idle I/O priority.
set -euo pipefail
SHA=66b0b9fb20bfd112e6f083f5097bdb1e90ba7cde
HOST=root@167.99.42.60
REPO=/Users/roman/projects/wallet-pir/.claude/worktrees/affectionate-wright-3a72f9
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$HERE/coordinator
BUILD=/srv/claude-regression-export-$SHA
WORK=/root/claude-regression-v11
LIMITS="-p MemoryMax=12G -p MemorySwapMax=0 -p CPUQuota=200%"
mkdir -p "$OUT"

if ssh "$HOST" 'fuser /run/lock/wallet-pir-production.lock 2>/dev/null' | grep -q '[0-9]'; then
  echo 'production lock is held; not starting' >&2
  exit 75
fi

# RESUME=1 continues after a verified source transfer.
if [ -z "${RESUME:-}" ]; then
  echo "== source $SHA"
  git -C "$REPO" archive --format=tar.gz "$SHA" > "$OUT/src.tar.gz"
  ssh "$HOST" "test ! -e $BUILD && test ! -e $WORK && mkdir -p $BUILD $WORK"
  ssh "$HOST" "tar -xzf - -C $BUILD" < "$OUT/src.tar.gz"
fi
scp -q "$HERE/remote/build.sh" "$HERE/remote/run-export.sh" \
  "$REPO/transparent/tools/transparent-regression/fixtures/mainnet-cases.json" "$HOST:$WORK/"

echo "== build regression-export (about 13 min)"
ssh "$HOST" "cd $WORK && systemd-run --quiet --scope --unit=claude-regression-build $LIMITS \
  nice -n 19 ionice -c 3 bash build.sh $SHA > build.log 2>&1; tail -4 build.log"
ssh "$HOST" "grep -q BUILD_DONE $WORK/build.log"

echo "== served map from both public origins"
for attempt in 1 2 3 4 5; do
  for o in transparent-pir enhance-pir; do
    curl -sS -D "$OUT/$o.headers" -o "$OUT/$o.shards.json" "https://$o.valargroup.dev/v1/filters/shards"
  done
  cmp -s "$OUT/transparent-pir.shards.json" "$OUT/enhance-pir.shards.json" && break
  [ "$attempt" = 5 ] && { echo 'origins never served identical maps' >&2; exit 1; }
  sleep 20
done
shasum -a 256 "$OUT"/*.shards.json
scp -q "$OUT/transparent-pir.shards.json" "$HOST:$WORK/shards.json"

echo "== read-only journal replay (about 20 min)"
ssh "$HOST" "cd $WORK && (date -u +%FT%TZ; free -b; df -B1 / /srv/transparent-activity) > headroom-before.txt \
  && systemd-run --quiet --scope --unit=claude-regression-export $LIMITS \
     nice -n 19 ionice -c 3 bash run-export.sh $SHA; \
  (date -u +%FT%TZ; free -b; df -B1 / /srv/transparent-activity) > headroom-after.txt; cat export.time; tail -3 export.stderr"

echo "== copy results back, remove the build tree"
mkdir -p "$OUT/remote"
ssh "$HOST" "tar -C $WORK -cf - ." | tar -xf - -C "$OUT/remote"
ssh "$HOST" "sha256sum $BUILD/target/release-fast/regression-export" > "$OUT/remote/regression-export.sha256"
ssh "$HOST" "rm -rf $BUILD"
ls -la "$OUT/remote"
test -s "$OUT/remote/mainnet-v11.json" && shasum -a 256 "$OUT/remote/mainnet-v11.json"
