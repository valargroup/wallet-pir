#!/usr/bin/env bash
# Paired off/all syncs under emulated round-trip delay: netem on loopback adds
# DELAY each way, so a request/response pays 2*DELAY. One server at a time;
# one client at a time, so latency is sequential request rounds plus work.
set -euo pipefail
ulimit -n 1048576
B=/root/sl-target-v3/release
SAMPLE=/root/single-lookup-sample-recent.json
CLASSES=restore-6m,multi-script,catch-up-1d,catch-up-7d,catch-up-30d
OUT=/root/sl-latency
mkdir -p "$OUT"
trap 'tc qdisc del dev lo root 2>/dev/null || true' EXIT

run() {
  local mode=$1 delay=$2
  "$B/transparent-shard-server" --listen 127.0.0.1:8603 --shard-dir /root/sl-shards-$mode \
    --cache-bytes 12884901888 --runtime-cache-dir /root/sl-latency-cache-$mode \
    --runtime-cache-max-bytes 21474836480 --pilot-cold > "$OUT/server-$mode-$delay.log" 2>&1 &
  local server=$!
  for _ in $(seq 1 120); do curl -sf http://127.0.0.1:8603/v1/ready > /dev/null && break; sleep 1; done
  tc qdisc del dev lo root 2>/dev/null || true
  "$B/transparent-loadtest" --shard-url http://127.0.0.1:8603 --sample $SAMPLE --classes $CLASSES \
    --steps 8 --step-duration 60s --min-completed-per-class 1 --timeout 300s > "$OUT/warmup-$mode-$delay.log" 2>&1 || true
  if [ "$delay" != 0 ]; then tc qdisc add dev lo root netem delay ${delay}ms; fi
  "$B/transparent-loadtest" --shard-url http://127.0.0.1:8603 --sample $SAMPLE --classes $CLASSES \
    --steps 1 --step-duration 600s --min-completed-per-class 10 --timeout 600s \
    --run-id "single-lookup-latency-$mode-$delay" --client-label "loopback, netem ${delay}ms each way" \
    --json-out "$OUT/report-$mode-$delay.json" > "$OUT/loadtest-$mode-$delay.log" 2>&1 || true
  tc qdisc del dev lo root 2>/dev/null || true
  kill $server; wait $server 2>/dev/null || true
}

for delay in 25 50; do
  run off $delay
  run all $delay
done
echo done > "$OUT/DONE"
