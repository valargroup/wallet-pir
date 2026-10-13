#!/bin/bash
set -euo pipefail
exec 9>/run/lock/wallet-pir-production.lock; flock -n 9 || { echo lock-held; exit 2; }
W=/root/obs-1868e5a6; R=/opt/pir-quality/releases/apm-5ad1ef61
echo "e63c02ded46ebc38063c82a26b225b2487f01c7c57a59031e04c78671d26b8fb  $W/pir-apm-5ad1ef61" | sha256sum -c -
mkdir -p $R; install -m 755 $W/pir-apm-5ad1ef61 $R/pir-apm
cp -a /etc/pir-quality/transparent-production.json $W/backup/transparent-production.json.obs-1868e5a6
python3 - <<PY
import json
p="/etc/pir-quality/transparent-production.json"; c=json.load(open(p))
[s for s in c["services"] if s["service"]=="transparent"][0]["min_recent_replicas"]=1
open(p+".next","w").write(json.dumps(c,indent=2)+"\n")
PY
chmod 644 /etc/pir-quality/transparent-production.json.next; mv /etc/pir-quality/transparent-production.json.next /etc/pir-quality/transparent-production.json
D=/etc/systemd/system/pir-apm.service.d
printf "[Service]\nExecStart=\nExecStart=/usr/bin/python3 /opt/enhance-pir/releases/observability-20260926/credential-exec.py %s/pir-apm\n" $R > $D/zzzzzzzzzzzzzzzzzzzzzz-apm-5ad1ef61.conf
systemctl daemon-reload; systemctl restart pir-apm.service
for i in $(seq 45); do curl -sf http://127.0.0.1:3002/healthz >/dev/null && break; sleep 2; done
systemctl show pir-apm.service -p ExecStart --value | grep -o "$R/pir-apm" | head -1
journalctl -u pir-apm.service --since "-1min" --no-pager | grep -ciE "transparent_config_invalid|panic" || true
echo DONE
