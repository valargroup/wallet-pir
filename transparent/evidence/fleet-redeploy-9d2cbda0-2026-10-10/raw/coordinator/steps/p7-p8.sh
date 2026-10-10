# archive-03 onto 9d2cbda0 from its disk runtime cache, then re-pin it and restart the 5 QPS load.
. /root/deploy-9d2cbda0/env.sh
bash $DEP/steps/p7-archive.sh; rc=$?
bash $DEP/steps/health.sh
[ $rc = 0 ] || { echo "archive install failed rc=$rc; load left stopped"; exit $rc; }
python3 $DEP/pin.py $NEWW transparent-pir-archive-03
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/load-start-archive; systemctl is-active transparent-5qps-continuous
grep -E 'caddy_http_request_duration_seconds_count\{code="5' /var/lib/pir-apm/edge.prom > $DEP/router-5xx-after-archive.prom || true
