# Phase 7: archive-03 onto the new binary from its disk runtime cache (install_worker under the lock).
. /root/deploy-06a972db/env.sh
systemctl is-active transparent-5qps-continuous && systemctl stop transparent-5qps-continuous
grep -E 'caddy_http_request_duration_seconds_count\{code="5' /var/lib/pir-apm/edge.prom > $DEP/router-5xx-before-archive.prom || true
date -u +%FT%TZ | tee $DEP/archive-start
s=$(date +%s)
flock -n -E 75 $LOCK timeout 2700 python3 -B $DEP/install-worker.py $S $FLEET transparent-pir-archive-03 $R/binaries $OUT/transparent-pir-archive-03 install 2>&1 | tee $DEP/archive-03.log
rc=${PIPESTATUS[0]}; date -u +%FT%TZ | tee $DEP/archive-end; echo "archive-03 install rc=$rc wall=$(( $(date +%s) - s ))s"; exit $rc
