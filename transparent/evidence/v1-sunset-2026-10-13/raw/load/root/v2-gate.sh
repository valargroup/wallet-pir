#!/bin/bash
# /v2 20 QPS gate: three 7 QPS rate-query clients for 600 s on top of the continuous 5 QPS load.
set -u
D=/srv/transparent-activity/v2-gate-20261013-run2; L=/srv/transparent-activity/canonical-load/v12
mkdir -p $D
( while true; do echo allow > $D/permit.tmp; mv $D/permit.tmp $D/permit; sleep 10; done ) & refresher=$!
pids=()
for i in 1 2 3; do
  nice -n 10 $L/rate-query --url https://transparent-pir.valargroup.dev --fixture $L/fixture.json --qps 7 --workers 8 --seconds 600 --permit $D/permit > $D/client-$i.jsonl 2> $D/client-$i.stderr & pids+=($!)
done
rc=0; for p in "${pids[@]}"; do wait $p || rc=1; done
kill $refresher; echo deny > $D/permit; echo "clients exited rc=$rc"
