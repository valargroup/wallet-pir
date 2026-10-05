#!/usr/bin/env bash
# Alternates baseline and final hint_bench binaries over the final-cycle tail
# candidate of each retained fill, then runs the in-process hint comparison.
# Usage: prewarm-compare.sh <a1 bench cmp dir> <out dir>
set -euo pipefail
cmp=$1; out=$2; bin=$(dirname "$0")/bin
mkdir -p "$out"
export TRANSPARENT_BUILD_THREADS=2
for fill in 10.2 13.5 15.9; do
  candidate=$(ls -d "$cmp/$fill-final/cycles"/candidate-* | sort | tail -1)
  tail=$(python3 -c "import json,sys;m=json.load(open(sys.argv[1]));print([s for s in m['shards'] if not s['sealed']][0]['manifest_digest'])" "$candidate/shards.json")
  tables=("directory=$candidate/$tail/directory.0.bin" "pages=$candidate/$tail/pages.0.bin")
  sha256sum "$candidate/$tail/directory.0.bin" "$candidate/$tail/pages.0.bin" > "$out/$fill-inputs.sha256"
  for round in 1 2; do
    for which in base final; do
      echo "$(date -u +%FT%TZ) $fill $which r$round load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$out/runs.log"
      "$bin/hint_bench.$which" --runtime-only --geometry recent-8k --repeats 3 "${tables[@]}" > "$out/$fill-$which-r$round.json"
    done
  done
  "$bin/hint_bench.final" --geometry recent-8k --repeats 3 "${tables[@]}" > "$out/$fill-hints.jsonl"
done
