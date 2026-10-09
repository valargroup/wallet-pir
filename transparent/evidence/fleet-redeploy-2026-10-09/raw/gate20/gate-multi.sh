#!/bin/bash
# N independent rate-query clients (each with its own admission loop), merged.
set -euo pipefail
G=$1; CLIENTS=$2; QPS=$3; SECONDS_TOTAL=$4
mkdir -p $G
for i in $(seq 1 $CLIENTS); do
  /root/gate-v11-20261009/gate-client.sh $G/c$i $QPS $SECONDS_TOTAL 16 &
done
wait
cat $G/c*/queries.jsonl > $G/queries.jsonl
echo 0 > $G/exit_code
