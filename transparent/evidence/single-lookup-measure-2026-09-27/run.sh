#!/usr/bin/env bash
# Paired local measurement: the same recent journal slice published without
# (off) and with (all) directory choice tables, served by one local shard
# server at a time, synced by transparent-loadtest from the pinned sample.
set -euo pipefail
B=/root/wallet-pir-single-lookup/target/release
SAMPLE=/root/single-lookup-sample-recent.json
OUT=/root/sl-measure
CLASSES=restore-6m,multi-script,catch-up-1d,catch-up-7d,catch-up-30d
PORT=8601
mkdir -p "$OUT"

run() {
  local mode=$1 rep=$2
  local url=http://127.0.0.1:$PORT
  "$B/transparent-shard-server" --listen 127.0.0.1:$PORT --shard-dir /root/sl-shards-$mode \
    --cache-bytes 12884901888 --pilot-cold > "$OUT/server-$mode-$rep.log" 2>&1 &
  local server=$!
  for _ in $(seq 1 120); do curl -sf $url/v1/ready > /dev/null && break; sleep 1; done
  # Warm-up: builds every runtime the classes touch; not recorded.
  "$B/transparent-loadtest" --shard-url $url --sample $SAMPLE --classes $CLASSES \
    --steps 8 --step-duration 90s --min-completed-per-class 1 --timeout 300s \
    > "$OUT/warmup-$mode-$rep.log" 2>&1 || true
  "$B/transparent-loadtest" --shard-url $url --sample $SAMPLE --classes $CLASSES \
    --steps 1,8,32 --step-duration 180s --min-completed-per-class 5 --timeout 300s \
    --run-id "single-lookup-$mode-$rep" --host-sku roman-ipir-bench-8vcpu \
    --client-label "loopback, same host" \
    --json-out "$OUT/report-$mode-$rep.json" > "$OUT/loadtest-$mode-$rep.log" 2>&1 || true
  kill $server; wait $server 2>/dev/null || true
}

run off 1
run all 1
run all 2
run off 2
echo done > "$OUT/DONE"
