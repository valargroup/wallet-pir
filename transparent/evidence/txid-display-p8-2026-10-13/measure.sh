#!/usr/bin/env bash
# Memory, rebuild time and query time for one 8-bit txid-2k8 runtime, run
# while the host is quiet. Everything at nice 19 / idle IO class.
set -euo pipefail
M=/root/txid-p8-measure; B=$M/bin; R=$M/raw; cd $M
mkdir -p $R/residency $R/rebuild $R/query
L="nice -n 19 ionice -c3"
recent=$(awk -F'\t' '$2=="recent"{print $3}' inputs/plan.tsv)
archive=$(awk -F'\t' '$1=="100"{print $3}' inputs/plan.tsv)
snap() { { date -u +%FT%TZ; uptime; free -m; systemctl show transparent-txid-display-worker -p MemoryCurrent -p CPUUsageNSec; } >"$1"; }
mkdir -p $R/worker
worker() { { date -u +%FT%TZ; systemctl show transparent-txid-display-worker -p MemoryCurrent -p MemoryPeak -p MemoryMax -p ActiveEnterTimestamp -p ExecStart; } >$R/worker/systemd-$1.txt
  curl -s http://10.142.0.18:8095/metrics >$R/worker/metrics-$1.txt; }
worker before
snap $R/host-before.txt
TRANSPARENT_BUILD_THREADS=4 $L /usr/bin/time -v $B/shard-residency --geometry txid-2k8 --table txdirectory --runtimes 12 \
  >$R/residency/txid-2k8-txdirectory-12.txt 2>$R/residency/time.txt
for round in 1 2 3; do
  snap $R/rebuild/load-before-$round.txt
  TRANSPARENT_BUILD_THREADS=4 $L $B/hint_bench --runtime-only --geometry txid-2k8 --repeats 10 \
    txdirectory=tables/$recent/directory-0.0.bin >$R/rebuild/recent-round$round.json 2>>$R/rebuild/stderr.log
done
snap $R/rebuild/load-after.txt
for t in recent:$recent archive:$archive; do
  $L $B/p8_query_bench tables/${t#*:}/directory-0.0.bin 40 >$R/query/${t%%:*}.json 2>>$R/query/stderr.log
done
snap $R/host-after.txt
worker after
echo measure-done
