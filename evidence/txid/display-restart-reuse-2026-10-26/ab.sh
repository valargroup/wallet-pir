#!/bin/sh
# Before/after restart measurement for transaction-detail restart reuse.
# Every run starts from a cold page cache. Output: one JSON line per pass.
set -eu
cd /root/restart-reuse/fx
BASE=${BASE:-/root/restart-reuse/bin/pir-publisher-base}
PR3=/root/restart-reuse/bin/pir-publisher-pr3
OUT=${OUT:-/root/restart-reuse/ab.jsonl}
: > "$OUT"

run() { # label binary
  sync; echo 3 > /proc/sys/vm/drop_caches
  start=$(date +%s.%N)
  /usr/bin/time -f '{"label":"'"$1"'","peak_rss_kib":%M,"wall_seconds":%e}' -o /root/restart-reuse/time.json \
    "$2" detail bench-restore --root root --passes 2 > /root/restart-reuse/run.out
  sed "s/^{/{\"label\":\"$1\",/" /root/restart-reuse/run.out >> "$OUT"
  cat /root/restart-reuse/time.json >> "$OUT"
}

rm -f root/display-preparation.json
# The first run of the change has no saved record yet: it checks everything
# and writes one. Wait past the 1 s settling window before any adoption.
run pr3-without-record "$PR3"
sleep 2
for pair in 1 2 3; do
  if [ $((pair % 2)) -eq 1 ]; then run base "$BASE"; run pr3 "$PR3"
  else run pr3 "$PR3"; run base "$BASE"; fi
  sleep 2
done
echo done >> "$OUT"
