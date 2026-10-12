#!/usr/bin/env bash
# Tip catch-up at several anchors, so the bill does not depend on where one
# anchor happens to fall inside each layout's open shard.
#
#   catch-up-anchors.sh BIN SLICES OUT [JOBS [LAYOUT ...]]
#
# For each anchor A = 3,473,686 - i * 6,500 (i = 0..7, spanning more than one
# decoupled 4k directory shard of the recent window), censuses the recent
# window up to A for the four recent-tier layouts, and shifts the sample's
# catch-up wallets by the same distance (required_from - i * 6,500). The
# scripts stay the frozen sample's; only their ranges move. Naming layouts
# runs only those, adding to an existing OUT.
set -euo pipefail

BIN=$1 SLICES=$2 OUT=$3 JOBS=${4:-8}
shift 3; [ $# -gt 0 ] && shift
HERE=$(cd "$(dirname "$0")" && pwd)
SAMPLE=$HERE/../workload-sample-2026-09-08/sample.json
ANCHOR=3473686 STEP=6500

policy() { local s=$(($1 * 58)) p=$2; echo "$((s - s / 7)):$s,$((p - p / 32)):$p"; }
declare -A LAYOUTS=(
  [recent-4k-8k]="--geometry recent-4k-8k --policy $(policy 4096 8192) --per-shard"
  [bulk-recent-4k-8k]="--geometry recent-4k-8k --policy $(policy 4096 8192) --per-shard --bulk-long"
  [decoupled-4k-8k]="--decoupled --directory-rows 4096 --page-rows 8192"
  [decoupled-8k-8k]="--decoupled --directory-rows 8192 --page-rows 8192"
  [coupled-2k-8k]="--geometry recent-4k-8k --directory-rows-per-segment 2048 --policy $(policy 2048 8192) --per-shard"
)
names=("$@")
[ ${#names[@]} -gt 0 ] || names=(recent-4k-8k bulk-recent-4k-8k decoupled-4k-8k decoupled-8k-8k)
mkdir -p "$OUT"
jobs=()
for i in 0 1 2 3 4 5 6 7; do
  end=$((ANCHOR - i * STEP))
  python3 - "$SAMPLE" "$OUT/sample-$end.json" $((i * STEP)) <<'EOF'
import json, sys
sample = json.load(open(sys.argv[1]))
shift = int(sys.argv[3])
sample["clients"] = [dict(c, required_from=c["required_from"] - shift)
                     for c in sample["clients"] if c["class"].startswith("catch-up")]
json.dump(sample, open(sys.argv[2], "w"))
EOF
  for name in "${names[@]}"; do jobs+=("$end $name ${LAYOUTS[$name]}"); done
done
printf '%s\n' "${jobs[@]}" | xargs -P "$JOBS" -L 1 bash -c '
  end=$1 name=$2; shift 2
  nice -n 10 '"$BIN"' --data-dir '"$SLICES"'/recent --end-height $end \
    --sample '"$OUT"'/sample-$end.json "$@" > '"$OUT"'/$end.$name.txt 2> '"$OUT"'/$end.$name.err \
    && echo "done $end $name" || echo "FAILED $end $name"
' _
for i in 0 1 2 3 4 5 6 7; do
  end=$((ANCHOR - i * STEP))
  for name in "${names[@]}"; do
    python3 "$HERE/pairs.py" --sample "$OUT/sample-$end.json" --anchor "$end" "$OUT/$end.$name.txt"
  done
done > "$OUT/pairs-${names[0]}.txt"
