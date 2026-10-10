#!/usr/bin/env bash
# Re-fetch history shard 89 (sealed, digest 5a6a8eac...) from the current active
# publication: the run's snapshot directory was pruned before the sealed pass.
set -euo pipefail
C=root@167.99.42.60; H=/srv/transparent-activity/full-v11/publications
TOOL=/root/wallet-pir-06a972db/target/release-fast/examples/native_certificate
CERT=/root/ipir-sp-d76e61a/reinspiring/tools/security/certify_native.py
d=5a6a8eacf2d79b9e18c0f290cea3a41a3d66fd35fa050555f8592012eefd1d94
cd /root/served-certs-06a972db
r() { ssh -n -o BatchMode=yes "$C" "$@"; }
r "cat $H/active.json" >inputs/history-active-rerun-89.json
AD=$(jq -r .directory inputs/history-active-rerun-89.json)
r "ionice -c3 nice -n19 cat $AD/$d/manifest.json" >/dev/shm/m.json
[ "$(sha256sum /dev/shm/m.json | cut -d' ' -f1)" = "$d" ]
for t in directory pages; do
  r "ionice -c3 nice -n19 cat $AD/$d/$t.0.bin" >/dev/shm/seg.bin
  want=$(jq -r ".directory_segments[0].sha256" /dev/shm/m.json)
  [ "$t" = pages ] && want=$(jq -r ".page_segments[0].sha256" /dev/shm/m.json)
  [ "$(sha256sum /dev/shm/seg.bin | cut -d' ' -f1)" = "$want" ]
  for q in nearest dithered; do
    n=reports/history-s89-$t.0-${d:0:12}-$q
    /usr/bin/time -f "%e s %M KiB $n" "$TOOL" segment --geometry recent-4k-8k --table $t --rows-bin /dev/shm/seg.bin --query-rounding $q >"$n.report.json" 2>>native.log </dev/null
    python3 "$CERT" "$n.report.json" >"$n.certificate.json" 2>"$n.certify.err"
  done
done
rm -f /dev/shm/seg.bin /dev/shm/m.json
printf 'history\t89\tdirectory,pages\t%s\trefetched from %s\n' "$d" "$AD" >rerun-89.tsv
echo rerun-89 ok
