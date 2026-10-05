#!/usr/bin/env bash
# Local end-to-end run of the tiered txid display proof of concept.
#
# A synthetic journal, two real `transparent-txid-server` workers (archive owner
# and recent replica), a path-splitting proxy standing in for the router
# snippet, and the controller replaying blocks across several seals and a
# window drop while `txid-rate` looks txids up through the proxy. Nothing here
# contacts a node or a production host.
#
#   local-e2e.sh BIN_DIR EXAMPLES_DIR WORK_DIR OUT_DIR
set -euo pipefail
BIN=$1 EXAMPLES=$2 WORK=$3 OUT=$4
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
START=${START:-1000}
BLOCKS=${BLOCKS:-12000}
MEAN=${MEAN:-8}
BOOT_THROUGH=${BOOT_THROUGH:-6200}
MAX_ARCHIVES=${MAX_ARCHIVES:-2}
STEP_BLOCKS=${STEP_BLOCKS:-25}
STEP_MS=${STEP_MS:-2000}
RATE=${RATE:-10}

rm -rf "$WORK"; mkdir -p "$WORK"/{sockets,state} "$OUT"
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

stamp() { date -u +%s.%N; }
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/run.log"; }

log "synthetic journal $START+$BLOCKS mean $MEAN"
"$BIN/txid-display-controller" synth-journal --out "$WORK/journal" --start "$START" \
  --blocks "$BLOCKS" --mean-records "$MEAN" --seed 7 | tee "$OUT/synth-journal.json"

log "bootstrap through $BOOT_THROUGH"
"$BIN/txid-display-controller" bootstrap --root "$WORK/root" --journal "$WORK/journal" \
  --start $((START + 1)) --through "$BOOT_THROUGH" --max-archive-shards "$MAX_ARCHIVES" \
  | tee "$OUT/bootstrap.jsonl"

for role in archive recent; do
  if [ "$role" = archive ]; then r=archive-owner port=18095; else r=recent-replica port=18096; fi
  "$BIN/transparent-txid-server" --listen 127.0.0.1:$port --role $r --cache-bytes $((3 << 30)) \
    --build-slots 1 --query-slots 2 --retain-revisions 2 \
    --control-socket "$WORK/sockets/$role.sock" --active-record "$WORK/state/$role.json" \
    >"$OUT/worker-$role.log" 2>&1 &
  pids+=($!)
done
python3 "$HERE/proxy.py" --listen 127.0.0.1:18090 --archive 127.0.0.1:18095 --recent 127.0.0.1:18096 &
pids+=($!)
cat >"$WORK/workers.json" <<EOF
{"workers":[{"name":"archive","role":"archive-owner","transport":{"socket":"$WORK/sockets/archive.sock"}},
            {"name":"recent","role":"recent-replica","transport":{"socket":"$WORK/sockets/recent.sock"}}]}
EOF
sleep 2

log "controller replay $STEP_BLOCKS blocks per ${STEP_MS} ms"
"$BIN/txid-display-controller" run --root "$WORK/root" --journal "$WORK/journal" --mode replay \
  --workers "$WORK/workers.json" --blocks-per-step "$STEP_BLOCKS" --step-interval-ms "$STEP_MS" \
  --status-listen 127.0.0.1:18099 --exit-when-idle >"$OUT/controller.log" 2>&1 &
controller=$!
pids+=($controller)

# Wait for the first activation, then sample a fixture from what is served.
for _ in $(seq 120); do curl -fsS -o /dev/null http://127.0.0.1:18090/v1/txid/shards && break; sleep 1; done
python3 "$REPO/transparent/ops/scripts/txid-display-observe.py" mapwatch \
  --url http://127.0.0.1:18090/v1/txid/shards --interval-ms 1000 --out-dir "$OUT/mapwatch" &
pids+=($!)
candidate=$(ls -d "$WORK"/root/candidate-* | sort | tail -1)
"$BIN/txid-inventory" fixture --publication "$candidate" --heights "$WORK/root/tooling/heights.bin" \
  --natural 400 --absent 50 --out "$WORK/fixture.json"
echo allow >"$WORK/permit"
( while kill -0 "$controller" 2>/dev/null; do echo allow >"$WORK/permit"; sleep 10; done; echo deny >"$WORK/permit" ) &
pids+=($!)
"$EXAMPLES/txid-rate" --url http://127.0.0.1:18090 --fixture "$WORK/fixture.json" --rate "$RATE" \
  --permit "$WORK/permit" --mix recent=0.8,archive=0.2 --cold-every 25 >"$OUT/rate.jsonl" 2>"$OUT/rate.log" &
rate=$!
pids+=($rate)

wait "$controller" || { log "controller exited $?"; }
log "replay finished"
sleep 15
kill "$rate" 2>/dev/null || true

cp "$WORK/root/timeline.jsonl" "$OUT/timeline.jsonl"
log "verify"
"$BIN/txid-display-controller" verify --root "$WORK/root" --journal "$WORK/journal" >"$OUT/verify.json" 2>"$OUT/verify.log" \
  && log "verify ok" || log "verify FAILED"
log "census and audit"
"$BIN/txid-inventory" census --publication "$WORK/root" --floor 10000 --out "$OUT/census.json" >"$OUT/census.txt"
"$BIN/txid-inventory" audit --maps "$OUT/mapwatch/maps" --order "$OUT/mapwatch/mapwatch.jsonl" --out "$OUT/audit.json" >"$OUT/audit.txt" || log "audit FAILED"
log "bandwidth"
candidate=$(ls -d "$WORK"/root/candidate-* | sort | tail -1)
"$BIN/txid-inventory" fixture --publication "$candidate" --heights "$WORK/root/tooling/heights.bin" \
  --per-class 3 --absent 3 --out "$WORK/fixture-final.json"
"$EXAMPLES/txid-bandwidth" measure --url http://127.0.0.1:18090 --fixture "$WORK/fixture-final.json" \
  --per-class 3 --out "$OUT/bandwidth.json" >"$OUT/bandwidth.log" 2>&1 || log "bandwidth FAILED"
"$EXAMPLES/txid-bandwidth" formula --out "$OUT/bandwidth-formula.json" >/dev/null 2>&1 || log "formula FAILED"
log "done"
