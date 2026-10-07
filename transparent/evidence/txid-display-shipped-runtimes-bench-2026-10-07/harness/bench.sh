#!/usr/bin/env bash
# Pre-deploy bench of runtimes shipped with txid display candidates, on one host.
#
# A synthetic journal is bootstrapped once, then replayed one block per cycle
# twice from copies of the same root: without and with `--ship-runtimes`. Each
# run drives one real `transparent-txid-server --role recent-replica` with
# production's flags over its control socket. The controller and the worker are
# pinned to two CPUs each with two build threads: the coordinator unit's
# CPUQuota=200% and recent-01's zz-latency.conf. A sampler records the
# worker's CPU time from /proc. Afterwards the newest two candidates of the
# shipped run are copied with the fleet adapter's rsync flags, as one block's
# delta. Nothing here contacts a node or a production host.
#
#   bench.sh BIN_DIR WORK_DIR OUT_DIR
set -euo pipefail
BIN=$1 WORK=$2 OUT=$3
START=${START:-1000}
BLOCKS=${BLOCKS:-12000}
MEAN=${MEAN:-8}
BOOT_THROUGH=${BOOT_THROUGH:-11400}
REPLAY_BLOCKS=${REPLAY_BLOCKS:-40}
STEP_MS=${STEP_MS:-4000}
CONTROLLER_CPUS=${CONTROLLER_CPUS:-2,3}
WORKER_CPUS=${WORKER_CPUS:-6,7}
RSYNC_REPEATS=${RSYNC_REPEATS:-5}
# Production's seal parameters (txid-display-freshness-2026-10-07 manifest).
SEAL=(--geometry txid-2k --n-archive 1 --n-recent 1 --archive-target 40000 --recent-floor 10000
      --reorg-margin 100 --max-archive-shards 2)

rm -rf "$WORK"; mkdir -p "$WORK" "$OUT"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/run.log"; }
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

log "synthetic journal $START+$BLOCKS mean $MEAN"
"$BIN/txid-display-controller" synth-journal --out "$WORK/journal" --start "$START" \
  --blocks "$BLOCKS" --mean-records "$MEAN" --seed 7 >"$OUT/synth-journal.json"
log "bootstrap through $BOOT_THROUGH"
"$BIN/txid-display-controller" bootstrap --root "$WORK/root0" --journal "$WORK/journal" \
  --start $((START + 1)) --through "$BOOT_THROUGH" "${SEAL[@]}" >"$OUT/bootstrap.jsonl"

run() {
  local name=$1; shift
  local dir=$WORK/$name
  mkdir -p "$dir/state"
  cp -a "$WORK/root0" "$dir/root"
  cp -a "$WORK/journal" "$dir/journal"
  TRANSPARENT_BUILD_THREADS=2 taskset -c "$WORKER_CPUS" "$BIN/transparent-txid-server" \
    --listen 127.0.0.1:18196 --role recent-replica --cache-bytes 536870912 --build-slots 1 \
    --query-slots 2 --retain-revisions 1 --control-socket "$dir/control.sock" \
    --active-record "$dir/state/active.json" >"$OUT/$name-worker.log" 2>&1 &
  local worker=$!
  pids+=("$worker")
  for _ in $(seq 100); do [ -S "$dir/control.sock" ] && break; sleep 0.1; done
  # Unix ms, then the worker's utime + stime in clock ticks.
  python3 - "$worker" >"$OUT/$name-worker-cpu.tsv" <<'EOF' &
import os, sys, time
pid = sys.argv[1]
print('unix_ms\tticks\tclk_tck', flush=True)
while True:
    try:
        fields = open('/proc/%s/stat' % pid).read().rsplit(')', 1)[1].split()
    except OSError:
        break
    print('%d\t%d\t%d' % (time.time() * 1000, int(fields[11]) + int(fields[12]),
                          os.sysconf('SC_CLK_TCK')), flush=True)
    time.sleep(0.25)
EOF
  pids+=($!)
  cat >"$dir/workers.json" <<EOF
{"workers":[{"name":"recent","role":"recent-replica","transport":{"socket":"$dir/control.sock"}}]}
EOF
  log "$name: replay $REPLAY_BLOCKS blocks, one per ${STEP_MS} ms, controller flags: $*"
  TRANSPARENT_BUILD_THREADS=2 taskset -c "$CONTROLLER_CPUS" "$BIN/txid-display-controller" run \
    --root "$dir/root" --journal "$dir/journal" --mode replay --workers "$dir/workers.json" \
    --blocks-per-step 1 --step-interval-ms "$STEP_MS" --replay-end $((BOOT_THROUGH + REPLAY_BLOCKS)) \
    --status-listen 127.0.0.1:18199 --exit-when-idle "$@" >"$OUT/$name-controller.log" 2>&1
  cp "$dir/root/timeline.jsonl" "$OUT/$name-timeline.jsonl"
  kill "$worker"; wait "$worker" 2>/dev/null || true
  log "$name: done"
}

run baseline
run shipped --ship-runtimes

# One block's delta as the fleet adapter copies it: the previous candidate is
# already on the worker, the next is copied with --link-dest to it.
mapfile -t candidates < <(python3 - "$WORK/shipped/root" <<'EOF'
import os, sys
root = sys.argv[1]
names = [n for n in os.listdir(root) if n.startswith('candidate-')]
for name in sorted(names, key=lambda n: (int(n.split('-')[1]), int(n.rsplit('-', 1)[1])))[-2:]:
    print(os.path.join(root, name))
EOF
)
previous=${candidates[0]} next=${candidates[1]}
log "rsync delta $(basename "$previous") -> $(basename "$next")"
ls -ln "$next" >"$OUT/rsync-next-candidate.txt"
for exclude in none runtimes; do
  for i in $(seq "$RSYNC_REPEATS"); do
    dest=$WORK/rsync-$exclude-$i
    rm -rf "$dest"; mkdir -p "$dest"
    rsync -a --numeric-ids "$previous/" "$dest/previous/"
    sync
    flags=()
    [ "$exclude" = runtimes ] && flags=(--exclude='*.runtime')
    started=$(date +%s.%N)
    rsync -a --delete --numeric-ids --stats "${flags[@]}" --link-dest="$dest/previous" \
      "$next/" "$dest/.tmp-next/" >"$OUT/rsync-$exclude-$i.stats"
    sync -f "$dest/.tmp-next"
    ended=$(date +%s.%N)
    echo "{\"exclude\": \"$exclude\", \"repeat\": $i, \"seconds\": $(echo "$ended - $started" | bc)}" \
      >>"$OUT/rsync.jsonl"
    rm -rf "$dest"
  done
done
log "done"
