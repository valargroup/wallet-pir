#!/bin/bash
# v12 whole-chain census, anchor 3505000, journal v2, read-only.
B=/srv/wallet-pir-build-e29cda78/target/release-fast/shard-census
D=/srv/zakura/transparent-event-data-v2
cd /srv/zakura/v12-census-20261012
run() { name=$1; shift; nice -n 19 ionice -c3 /usr/bin/time -v -o $name.time $B --data-dir $D --end-height 3505000 --sample /srv/zakura/v12-sample.json "$@" > $name.txt 2> $name.err && echo "done $name" >> status || echo "FAILED $name" >> status; }
run archive-32k-32k --decoupled --directory-rows 32768 --page-rows 32768 --per-shard &
run recent-2k-8k --decoupled --directory-rows 2048 --page-rows 8192 --per-shard &
wait
echo ALL_DONE >> status
