#!/usr/bin/env bash
# Recent-01-shaped rebuild bench: one recent-replica worker under the planned
# unit limits, fed per-block recent rebuilds by the controller in replay mode.
# Seals are disabled (a huge archive target), so the recent shard holds every
# synthetic record and each cycle rebuilds and prepares a shard of that size.
#
#   recent-bench.sh BIN_DIR WORK_DIR OUT_DIR [SIZES...]
#
# Limits default to recent-01's planned unit: CPUQuota=100%, MemoryMax=1.5G,
# one build thread. Override with CPU_QUOTA, MEMORY_MAX and BUILD_THREADS.
set -euo pipefail
BIN=$1 WORK=$2 OUT=$3
shift 3
SIZES=${*:-10000 20000 30000 40000 50000 60000 70000}
MEAN=${MEAN:-8}
CYCLES=${CYCLES:-15}
STEP_MS=${STEP_MS:-1000}
CPU_QUOTA=${CPU_QUOTA:-100%}
MEMORY_MAX=${MEMORY_MAX:-1536M}
BUILD_THREADS=${BUILD_THREADS:-1}
TAG=${TAG:-cpu${CPU_QUOTA%\%}-t$BUILD_THREADS}

mkdir -p "$OUT"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/run.log"; }

for size in $SIZES; do
  dir="$WORK/$TAG-$size"
  rm -rf "$dir"; mkdir -p "$dir"/{sockets,state}
  blocks=$(( size / MEAN + CYCLES + 1 ))
  through=$(( 1000 + blocks - CYCLES - 1 ))
  "$BIN/txid-display-controller" synth-journal --out "$dir/journal" --start 1000 \
    --blocks "$blocks" --mean-records "$MEAN" --seed 11 >"$dir/synth.json"
  "$BIN/txid-display-controller" bootstrap --root "$dir/root" --journal "$dir/journal" \
    --start 1001 --through "$through" --archive-target 1000000000 >"$dir/bootstrap.json"
  unit="txid-recent-bench-$TAG-$size"
  systemd-run --quiet --unit "$unit" --collect -p CPUQuota="$CPU_QUOTA" -p MemoryMax="$MEMORY_MAX" \
    -p MemorySwapMax=0 -E TRANSPARENT_BUILD_THREADS="$BUILD_THREADS" \
    "$BIN/transparent-txid-server" --listen 127.0.0.1:18196 --role recent-replica \
    --cache-bytes $((512 << 20)) --build-slots 1 --query-slots 2 --retain-revisions 1 \
    --control-socket "$dir/sockets/recent.sock" --active-record "$dir/state/recent.json"
  for _ in $(seq 50); do [ -S "$dir/sockets/recent.sock" ] && break; sleep 0.2; done
  cat >"$dir/workers.json" <<EOF
{"workers":[{"name":"recent","role":"recent-replica","transport":{"socket":"$dir/sockets/recent.sock"}}]}
EOF
  log "size $size: $(jq -c .recent "$dir/bootstrap.json")"
  "$BIN/txid-display-controller" run --root "$dir/root" --journal "$dir/journal" --mode replay \
    --workers "$dir/workers.json" --blocks-per-step 1 --step-interval-ms "$STEP_MS" \
    --status-listen 127.0.0.1:18199 --exit-when-idle >"$dir/controller.log" 2>&1 \
    || log "size $size: controller exited $?"
  peak=$(systemctl show "$unit" -p MemoryPeak --value 2>/dev/null || echo unknown)
  systemctl stop "$unit" || true
  jq -c --arg tag "$TAG" --argjson size "$size" --arg peak "$peak" \
    'select(.kind == "cycle") | del(.freshness_ms) | {tag: $tag, size: $size, memory_peak: $peak} + .' \
    "$dir/root/timeline.jsonl" >>"$OUT/cycles.jsonl"
  log "size $size: $(grep -c '"kind":"cycle"' "$dir/root/timeline.jsonl") cycles, memory peak $peak"
done
