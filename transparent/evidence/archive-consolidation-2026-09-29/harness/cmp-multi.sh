#!/bin/bash
# N independent clients (each with its own admission loop), merged.
# Usage: cmp-multi.sh DIR BINARY MIX CLIENTS QPS SECONDS
set -euo pipefail
G=$1; BIN=$2; MIX=$3; CLIENTS=$4; QPS=$5; S=$6
mkdir -p $G; sha256sum $BIN > $G/client.sha256
for i in $(seq 1 $CLIENTS); do /root/cmp/cmp-client.sh $G/c$i $BIN $MIX $QPS $S 16 & done
wait
cat $G/c*/queries.jsonl > $G/queries.jsonl
cat $G/c*/exit_code > $G/exit_codes
