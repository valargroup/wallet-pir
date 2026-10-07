#!/bin/bash
# run-window.sh NAME UNIT TOTAL_RATE PROCS SECONDS [extra txid-rate args]
set -u
name=$1 unit=$2 total=$3 procs=$4 secs=$5; shift 5
W=/root/txid-measure-d191f86b; out=$W/runs/$name; mkdir -p $out
rate=$(python3 -c "print($total/$procs)")
echo "{\"name\":\"$name\",\"unit\":\"$unit\",\"total_rate\":$total,\"procs\":$procs,\"seconds\":$secs,\"extra\":\"$*\",\"start_unix\":$(date +%s.%N)}" > $out/window.json
pids=()
for i in $(seq 0 $((procs-1))); do
  $W/target/release/examples/txid-rate --url https://transparent-pir.valargroup.dev --fixture $W/fixture-natural-p$i.json \
    --rate $rate --seconds $secs --permit $W/permit --unit $unit --mix recent=0.8,archive=0.2 "$@" \
    > $out/rate-$i.jsonl 2> $out/rate-$i.err &
  pids+=($!)
done
for p in "${pids[@]}"; do wait $p; echo "pid $p exit $?" >> $out/exits.txt; done
echo "{\"end_unix\":$(date +%s.%N)}" >> $out/window.json
echo WINDOW_DONE > $out/DONE
