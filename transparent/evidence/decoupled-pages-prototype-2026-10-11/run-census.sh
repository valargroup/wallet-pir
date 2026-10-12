#!/usr/bin/env bash
# Run every step-1 layout over both journal windows, in parallel.
#
#   run-census.sh BIN SLICES OUT [JOBS] [LAYOUT ...]
#
# BIN is a release-fast shard-census from this branch, SLICES the directory
# fetch-slices.sh filled (recent/, archive/), OUT a new directory for one log
# per (window, layout) plus its /usr/bin/time report. With no LAYOUT names,
# all layouts run over both windows (or over $WINDOWS). coupled-2k-8k was
# added after the first pass and run on the recent window only.
#
# Coupled layouts pass their derived policy explicitly (capacity minus 1/7
# scripts, minus 1/32 page rows, 58 minimal entries per 4,096-byte row), so
# the run seals exactly as the publisher would and skips the census's
# default script-target sweep.
set -euo pipefail

BIN=$1 SLICES=$2 OUT=$3 JOBS=${4:-8}
shift 3; [ $# -gt 0 ] && shift
SAMPLE=$(cd "$(dirname "$0")/../workload-sample-2026-09-08" && pwd)/sample.json

policy() { # directory_rows page_rows
  local scripts=$(($1 * 58)) pages=$2
  echo "$((scripts - scripts / 7)):$scripts,$((pages - pages / 32)):$pages"
}

declare -A LAYOUTS=(
  [archive-wide]="--geometry archive-wide --policy $(policy 32768 65536) --per-shard"
  [coupled-16k-64k]="--geometry archive-wide --directory-rows-per-segment 16384 --policy $(policy 16384 65536) --per-shard"
  [coupled-8k-64k]="--geometry archive-wide --directory-rows-per-segment 8192 --policy $(policy 8192 65536) --per-shard"
  [recent-4k-8k]="--geometry recent-4k-8k --policy $(policy 4096 8192) --per-shard"
  [coupled-2k-8k]="--geometry recent-4k-8k --directory-rows-per-segment 2048 --policy $(policy 2048 8192) --per-shard"
  [bulk-archive-wide]="--geometry archive-wide --policy $(policy 32768 65536) --per-shard --bulk-long"
  [bulk-recent-4k-8k]="--geometry recent-4k-8k --policy $(policy 4096 8192) --per-shard --bulk-long"
  [decoupled-32k-64k]="--decoupled --directory-rows 32768 --page-rows 65536"
  [decoupled-32k-32k]="--decoupled --directory-rows 32768 --page-rows 32768"
  [decoupled-32k-16k]="--decoupled --directory-rows 32768 --page-rows 16384"
  [decoupled-4k-8k]="--decoupled --directory-rows 4096 --page-rows 8192"
  [decoupled-8k-8k]="--decoupled --directory-rows 8192 --page-rows 8192"
)
names=("$@")
[ ${#names[@]} -gt 0 ] || names=("${!LAYOUTS[@]}")

mkdir "$OUT"
sha256sum "$BIN" > "$OUT/BIN.sha256"
jobs=()
# Archive runs first: they are the long ones.
for window in ${WINDOWS:-archive recent}; do
  for name in "${names[@]}"; do
    jobs+=("$window $name ${LAYOUTS[$name]}")
  done
done
printf '%s\n' "${jobs[@]}" > "$OUT/jobs.txt"
printf '%s\n' "${jobs[@]}" | xargs -P "$JOBS" -L 1 bash -c '
  window=$1 name=$2; shift 2
  log='"$OUT"'/$window.$name
  nice -n 10 /usr/bin/time -v -o $log.time '"$BIN"' --data-dir '"$SLICES"'/$window \
    --sample '"$SAMPLE"' "$@" > $log.txt 2> $log.err \
    && echo "done $window $name" || echo "FAILED $window $name ($log.err)"
' _
