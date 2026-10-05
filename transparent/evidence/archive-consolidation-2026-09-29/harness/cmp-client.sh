#!/bin/bash
# One rate-query client against the public URL with a refreshed permit.
# Usage: cmp-client.sh DIR BINARY MIX QPS SECONDS WORKERS
set -euo pipefail
G=$1; BIN=$2; MIX=$3; QPS=$4; S=$5; WORKERS=$6
mkdir -p $G; echo allow > $G/permit
( while true; do echo allow > $G/permit.tmp; mv $G/permit.tmp $G/permit; sleep 10; done ) & REFRESH=$!
trap 'kill $REFRESH 2>/dev/null || true' EXIT
MIXARG=(); [ "$MIX" = default ] || MIXARG=(--mix "$MIX")
date -u +%s > $G/client_start_unix
set +e
$BIN --url https://transparent-pir.valargroup.dev --fixture /root/gate-fixture.json "${MIXARG[@]}" \
  --qps $QPS --workers $WORKERS --seconds $S --permit $G/permit > $G/queries.jsonl 2> $G/stderr.log
echo $? > $G/exit_code
date -u +%s > $G/client_end_unix
