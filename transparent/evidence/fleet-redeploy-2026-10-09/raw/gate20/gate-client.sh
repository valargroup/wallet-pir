#!/bin/bash
# Run rate-query against the public URL for a fixed interval, refreshing its permit.
# Same as /root/gate-client.sh (recent-floor 2026-09-29) with the v11 canonical-load rate-query and fixture.
set -euo pipefail
G=$1; QPS=$2; SECONDS_TOTAL=$3; WORKERS=${4:-32}
mkdir -p $G; echo allow > $G/permit
( while true; do echo allow > $G/permit.tmp; mv $G/permit.tmp $G/permit; sleep 10; done ) & REFRESH=$!
trap "kill $REFRESH 2>/dev/null || true" EXIT
date -u +%s > $G/client_start_unix
set +e
/root/gate-v11-20261009/rate-query --url https://transparent-pir.valargroup.dev \
  --fixture /root/gate-v11-20261009/fixture.json --qps $QPS --workers $WORKERS --seconds $SECONDS_TOTAL --permit $G/permit \
  > $G/queries.jsonl 2> $G/stderr.log
echo $? > $G/exit_code
date -u +%s > $G/client_end_unix
