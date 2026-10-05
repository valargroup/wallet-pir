#!/bin/bash
# Run rate-query against the public URL for a fixed interval, refreshing its permit.
set -euo pipefail
G=$1; QPS=$2; SECONDS_TOTAL=$3; WORKERS=${4:-32}
mkdir -p $G; echo allow > $G/permit
( while true; do echo allow > $G/permit.tmp; mv $G/permit.tmp $G/permit; sleep 10; done ) & REFRESH=$!
trap 'kill $REFRESH 2>/dev/null || true' EXIT
date -u +%s > $G/client_start_unix
set +e
/root/track-a-client-target/release/examples/rate-query --url https://transparent-pir.valargroup.dev \
  --fixture /root/gate-fixture.json --qps $QPS --workers $WORKERS --seconds $SECONDS_TOTAL --permit $G/permit \
  > $G/queries.jsonl 2> $G/stderr.log
echo $? > $G/exit_code
date -u +%s > $G/client_end_unix
