#!/usr/bin/env bash
# Temporary bench fleet: 4 workers behind one Caddy router, driven from a
# dedicated load generator over the VPC. The same recent slice is served
# without (off) and with (all) directory choice tables, alternating.
# Load settings follow fleet series r3, capped at 128 wallets.
set -euo pipefail
# The default 1,024 open files cannot hold 128 wallets' connections.
ulimit -n 1048576
WORKERS="10.110.0.14 10.110.0.12 10.110.0.10 10.110.0.15"
URL=http://10.110.0.11:8080
SAMPLE=/root/single-lookup-sample-recent.json
CLASSES=restore-6m,multi-script,catch-up-1d,catch-up-7d,catch-up-30d
OUT=/root/sl-fleet
METRICS=""
for w in $WORKERS; do METRICS="$METRICS --metrics-url http://$w:8093"; done
mkdir -p "$OUT"
# The first pass (steps 8,32,128) ran with the default open-file limit and
# its 128 step failed on the client; the rerun is STEPS=128 TAG=-c128.

serve() {
  for w in $WORKERS; do ssh -o BatchMode=yes root@$w "/root/serve.sh $1" & done
  wait
  sleep 15 # router health interval
}

ready() {
  for w in $WORKERS; do echo "$w $(curl -s http://$w:8093/v1/ready)"; done > "$OUT/ready-$1.txt"
}

run() {
  local mode=$1 rep=$2
  serve "$mode"
  # Warm-up: builds the runtimes the classes touch on every worker; not recorded.
  transparent-loadtest --shard-url $URL --sample $SAMPLE --classes $CLASSES \
    --steps 32 --step-duration 5m --min-completed-per-class 1 --max-queries 6000 --timeout 300s \
    > "$OUT/warmup-$mode-$rep${TAG:-}.log" 2>&1 || true
  ready "$mode-$rep${TAG:-}"
  /usr/bin/time -v transparent-loadtest --shard-url $URL --sample $SAMPLE --classes $CLASSES \
    --steps "${STEPS:-8,32,128}" --step-duration 10m --min-completed-per-class 100 --max-queries 6000 --timeout 300s \
    $METRICS --run-id "single-lookup-fleet-$mode-$rep" --source-sha 72c9031d \
    --host-region ams3 --host-sku "bench 4x s-4vcpu-8gb workers, s-2vcpu-4gb Caddy router" \
    --client-label "sl-bench-loadgen (c-16 Xeon 8280), VPC, plain HTTP" \
    --json-out "$OUT/report-$mode-$rep${TAG:-}.json" > "$OUT/loadtest-$mode-$rep${TAG:-}.log" 2> "$OUT/time-$mode-$rep${TAG:-}.txt" || true
}

run off 1
run all 1
run all 2
run off 2
echo done > "$OUT/DONE${TAG:-}"
