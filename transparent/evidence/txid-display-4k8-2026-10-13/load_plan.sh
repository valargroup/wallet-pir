#!/usr/bin/env bash
# Runs V0 (5/s, 60 s) and, only if V0 is clean, A (20/s, 600 s) with load_run.sh.
# A watcher stops a window on any inexact answer or 20 errors.
set -uo pipefail
cd /root/txid-4k8-load
bin=/root/wallet-pir-build-4k8/target-load/release-fast/examples/txid-rate
sha256sum $bin > binary.sha256
guard() { # guard NAME
  while sleep 5; do
    f=runs/$1/rate.jsonl; [ -f runs/$1/exit ] && return
    [ -f $f ] || continue
    bad=$(grep -c '"exact":false' $f); err=$(grep -c '"event":"error"' $f)
    if [ "$bad" -gt 0 ] || [ "$err" -ge 20 ]; then
      echo "TRIPPED $1 inexact=$bad errors=$err $(date -u +%T)" | tee runs/$1/TRIPPED
      pkill -x txid-rate; return
    fi
  done
}
mkdir -p runs/V0 runs/A
guard V0 & ./run.sh V0 5 60 --cold-every 25; wait
v0bad=$(grep -c '"exact":false' runs/V0/rate.jsonl); v0err=$(grep -c '"event":"error"' runs/V0/rate.jsonl)
echo "V0 lookups=$(grep -c '"event":"lookup"' runs/V0/rate.jsonl) inexact=$v0bad errors=$v0err"
if [ "$v0bad" -gt 0 ] || [ "$v0err" -gt 0 ] || [ -f runs/V0/TRIPPED ]; then echo "V0 not clean; A skipped"; exit 1; fi
guard A & ./run.sh A 20 600 --cold-every 25; wait
echo "A lookups=$(grep -c '"event":"lookup"' runs/A/rate.jsonl) inexact=$(grep -c '"exact":false' runs/A/rate.jsonl) errors=$(grep -c '"event":"error"' runs/A/rate.jsonl) $(cat runs/A/exit)"
