. /root/deploy-a317455e/env.sh
bash $DEP/steps/worker-checks.sh $A03 transparent-pir-archive-03 ""
curl -s http://$A03:8093/v1/ready > $DEP/archive-ready-after.json; python3 -c "import json;r=json.load(open('$DEP/archive-ready-after.json'));print({k:r.get(k) for k in ('ready','mode','warm_runtimes','target_runtimes','prewarm_failed','runtime_cache','map_sha256','started_unix')})"
curl -s 127.0.0.1:8094/v1/status | python3 -c "import json,sys;s=json.load(sys.stdin);print('controller',s['phase'],s['public_height'],s['node_height'],s['freshness_seconds'])"
$W root@$A03 "grep -E '^(MemTotal|MemAvailable)' /proc/meminfo; systemctl show transparent-shard-server -p MemoryCurrent -p MemoryPeak; df -h /"
grep -E 'caddy_http_request_duration_seconds_count\{code="5' /var/lib/pir-apm/edge.prom > $DEP/router-5xx-after-archive.prom || true
diff $DEP/router-5xx-before-archive.prom $DEP/router-5xx-after-archive.prom || true
