#!/usr/bin/env bash
# Certify every snapshotted display table under the 8-bit profile's served
# seeds, bound to the masks the worker serves, four at a time, then check each report at the 50-bit floor.
set -uo pipefail
M=/root/txid-4k8-measure; cd $M
mkdir -p raw/reports raw/verdicts
CK=$M/checker/reinspiring/tools/security/certify_native.py
one() {
  shard=$1 d=$3
  pub=$(awk -F'\t' -v d=$d '$2==d{print $3}' inputs/served-setup.tsv)
  n=raw/reports/s$(printf %03d $shard)-${d:0:12}
  [ -s $n.report.json ] || nice -n 19 ionice -c3 /usr/bin/time -f "%e s %M KiB $n" \
    bin/native_certificate segment --geometry txid-4k8 --table txdirectory \
    --rows-bin tables/$d/directory-0.0.bin --public-sha256 $pub >$n.report.json 2>>raw/native.log || { echo "$shard report" >>raw/failures.txt; return; }
  v=raw/verdicts/$(basename $n).verdict.json
  nice -n 19 python3 $CK --require-bits 50 $n.report.json >$v 2>$v.err; echo "$shard $?" >>raw/checker-exit.txt
}
export -f one; export M CK
date -u +%FT%TZ >raw/certify-started
# The recent table first: it is the one the next publication replaces.
sort -t$'\t' -k2,2r -k1,1n inputs/plan.tsv | xargs -P4 -L1 bash -c 'one $0 $1 $2' 
date -u +%FT%TZ >raw/certify-finished
echo certify-done
