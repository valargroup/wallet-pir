#!/usr/bin/env bash
# Bucket-count ablation on one synthetic chain: the same records published as
# txid-2k at N=1 (20k and 40k txids per archive) and at N=4 (10k per bucket,
# so about 40k per archive). Each publication is served statically by an
# archive owner and a recent replica behind the local proxy, then censused and
# metered. The history-attached geometries are formula-only (`txid-bandwidth
# formula`), since their per-query bytes depend on rows, not on the records.
#
#   ablation.sh BIN_DIR EXAMPLES_DIR WORK_DIR OUT_DIR
set -euo pipefail
BIN=$1 EXAMPLES=$2 WORK=$3 OUT=$4
HERE=$(cd "$(dirname "$0")" && pwd)
START=${START:-1000}
BLOCKS=${BLOCKS:-30000}
MEAN=${MEAN:-8}

rm -rf "$WORK"; mkdir -p "$WORK" "$OUT"
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/run.log"; }

"$BIN/txid-display-controller" synth-journal --out "$WORK/journal" --start "$START" \
  --blocks "$BLOCKS" --mean-records "$MEAN" --seed 7 >"$OUT/synth-journal.json"
through=$((START + BLOCKS - 1))

# name n archive_target recent_floor
for config in "n1-t20k 1 20000 10000" "n1-t40k 1 40000 10000" "n4-t10k 4 10000 2500"; do
  set -- $config
  name=$1 n=$2 target=$3 floor=$4
  root="$WORK/$name"
  "$BIN/txid-display-controller" bootstrap --root "$root" --journal "$WORK/journal" \
    --start $((START + 1)) --through "$through" --n-archive "$n" --n-recent "$n" \
    --archive-target "$target" --recent-floor "$floor" --max-archive-shards 100 \
    >"$OUT/$name-bootstrap.json"
  candidate=$(jq -r .candidate "$OUT/$name-bootstrap.json")
  [ -d "$candidate" ] || candidate=$(ls -d "$root"/candidate-* | sort | tail -1)
  "$BIN/txid-inventory" census --publication "$root" --floor 10000 --out "$OUT/$name-census.json" \
    >"$OUT/$name-census.txt"
  "$BIN/transparent-txid-server" --listen 127.0.0.1:18295 --role archive-owner \
    --publication-dir "$candidate" --cache-bytes $((12 << 30)) >"$WORK/$name-archive.log" 2>&1 &
  pids+=($!)
  "$BIN/transparent-txid-server" --listen 127.0.0.1:18296 --role recent-replica \
    --publication-dir "$candidate" --cache-bytes $((2 << 30)) >"$WORK/$name-recent.log" 2>&1 &
  pids+=($!)
  python3 "$HERE/proxy.py" --listen 127.0.0.1:18290 --archive 127.0.0.1:18295 --recent 127.0.0.1:18296 &
  pids+=($!)
  for _ in $(seq 600); do
    curl -fsS -o /dev/null http://127.0.0.1:18295/v1/ready && curl -fsS -o /dev/null http://127.0.0.1:18296/v1/ready && break
    sleep 1
  done
  "$BIN/txid-inventory" fixture --publication "$candidate" --heights "$root/tooling/heights.bin" \
    --per-class 3 --absent 3 --out "$WORK/$name-fixture.json"
  "$EXAMPLES/txid-bandwidth" measure --url http://127.0.0.1:18290 --fixture "$WORK/$name-fixture.json" \
    --per-class 3 --stale-wait-seconds 0 --out "$OUT/$name-bandwidth.json" >"$OUT/$name-bandwidth.log" 2>&1 \
    || log "$name: bandwidth FAILED"
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  pids=()
  log "$name: $(jq -c '{seals}' "$OUT/$name-bootstrap.json")"
done
"$EXAMPLES/txid-bandwidth" formula --out "$OUT/formula.json" >/dev/null
log done
