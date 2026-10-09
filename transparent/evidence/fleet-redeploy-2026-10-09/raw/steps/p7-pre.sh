. /root/deploy-a317455e/env.sh
python3 $DEP/steps/membership.py
python3 -c "import json;m=json.load(open('/opt/transparent-publisher/v11/state/membership.json'));print('archive member', json.dumps(m['members']['transparent-pir-archive-03'])[:600])"
curl -s http://$A03:8093/v1/ready | python3 -c "import json,sys;r=json.load(sys.stdin);print('archive ready',r['ready'],r['mode'],r['warm_runtimes'],r['target_runtimes'],r['binary_sha256'][:12],r['runtime_cache'],r.get('prewarm_failed'))"
$W root@$A03 "grep -E '^(MemTotal|MemAvailable)' /proc/meminfo; du -sh /srv/transparent-pir/v11/runtime-cache; df -h / /srv/transparent-pir 2>/dev/null; systemctl show transparent-shard-server -p MemoryCurrent -p NRestarts -p MainPID -p ActiveEnterTimestamp"
systemctl is-active transparent-5qps-continuous || true
lslocks | grep wallet-pir-production || echo lock-free
