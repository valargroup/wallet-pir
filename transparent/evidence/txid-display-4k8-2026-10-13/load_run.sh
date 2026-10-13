#!/usr/bin/env bash
# run.sh NAME RATE SECONDS [txid-rate args...]: one open-loop txid-rate window against /v2/txid.
set -euo pipefail
cd /root/txid-4k8-load
name=$1 rate=$2 secs=$3; shift 3
bin=/root/wallet-pir-build-4k8/target-load/release-fast/examples/txid-rate
mkdir -p runs/$name
( while true; do echo allow > permit.tmp; mv permit.tmp permit; sleep 10; done ) & refresher=$!
trap 'kill $refresher 2>/dev/null' EXIT
date -u +%FT%TZ > runs/$name/start
"$bin" --url https://transparent-pir.valargroup.dev --fixture fixture-natural.json \
  --rate "$rate" --seconds "$secs" --permit permit --mix recent=0.8,archive=0.2 "$@" \
  > runs/$name/rate.jsonl 2> runs/$name/stderr.log
echo "exit=$?" > runs/$name/exit
date -u +%FT%TZ > runs/$name/end
