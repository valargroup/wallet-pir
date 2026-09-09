#!/bin/sh
set -eu
cd /opt/transparent-sync-bench-client
exec python3 compare.py \
  --scenario scenario.json --fresh-sample fresh-sample.json \
  --binary ./transparent-loadtest-linear \
  --block-url https://transparent-sync-bench.valargroup.dev \
  --out-dir /opt/transparent-sync-bench-client/results/full-20260909-private-apm \
  --metadata conditions-private-apm.json \
  --block-metrics-url http://127.0.0.1:19097/metrics \
  --pir-metrics filter=http://127.0.0.1:19100/metrics \
  --pir-metrics recent-01=http://10.142.0.10:8093/metrics \
  --pir-metrics recent-02=http://10.142.0.8:8093/metrics \
  --pir-metrics recent-03=http://10.142.0.7:8093/metrics \
  --pir-metrics recent-04=http://10.142.0.12:8093/metrics \
  --pir-metrics archive-01=http://10.142.0.6:8093/metrics \
  --pir-metrics archive-02=http://10.142.0.9:8093/metrics
