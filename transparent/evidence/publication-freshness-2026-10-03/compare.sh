#!/usr/bin/env bash
# Alternating baseline/final runs of the deterministic tail benchmark.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p cmp
for fill in 10.2 13.5 15.9; do
  for round in 1 2; do
    for bin in base final; do
      work=cmp/$fill-$bin
      echo "$(date -u +%FT%TZ) fill=$fill round=$round bin=$bin load=$(cut -d' ' -f1-3 /proc/loadavg)" >> cmp/runs.log
      TRANSPARENT_BUILD_THREADS=2 bin/publication_bench.$bin --day day.jsonl --work "$work" \
        --initial-copies "$fill" --cycles 3 --runtime --runtime-repeats 2 \
        --record "cmp/$fill-$bin-r$round.json" > "cmp/$fill-$bin-r$round.log" 2>&1
    done
  done
done
echo done >> cmp/runs.log
