#!/bin/bash
# Switch pir-apm and the host sampler on the coordinator to main 1868e5a6 (v12 history + txid site, shadow).
set -euo pipefail
exec 9>/run/lock/wallet-pir-production.lock
flock -n 9 || { echo "production lock held"; exit 2; }
W=/root/obs-1868e5a6; R=/opt/pir-quality/releases/obs-1868e5a6; B=$W/backup
mkdir -p $B $R
cp -a /etc/pir-quality/transparent-production.json /etc/pir-quality/hosts.json /etc/pir-quality/apm.env $B/
cp -a /etc/systemd/system/pir-apm.service.d $B/pir-apm.service.d
[ -d /etc/systemd/system/pir-quality-hosts.service.d ] && cp -a /etc/systemd/system/pir-quality-hosts.service.d $B/ || true
echo "a4ab946f8be2812e167ba586759706ec03945c093666c81a77eaf2c278672bc6  $W/pir-apm" | sha256sum -c -
install -m 755 $W/pir-apm $R/pir-apm
install -m 755 $W/quality-host-sampler.py $R/quality-host-sampler.py
rollback() {
  echo "ROLLBACK"; rm -f /etc/systemd/system/pir-apm.service.d/zzzzzzzzzzzzzzzzzzzzz-apm-obs-1868e5a6.conf /etc/systemd/system/pir-quality-hosts.service.d/zz-obs-1868e5a6.conf
  cp -a $B/transparent-production.json $B/hosts.json /etc/pir-quality/
  systemctl daemon-reload; systemctl restart pir-quality-hosts.service pir-apm.service
}
# Sampler first: its output is additive, so the running APM keeps reading it.
install -m 600 $W/hosts.json /etc/pir-quality/hosts.json
mkdir -p /etc/systemd/system/pir-quality-hosts.service.d
printf '[Service]\nExecStart=\nExecStart=/usr/bin/python3 %s/quality-host-sampler.py --config /etc/pir-quality/hosts.json\n' $R > /etc/systemd/system/pir-quality-hosts.service.d/zz-obs-1868e5a6.conf
systemctl daemon-reload; systemctl restart pir-quality-hosts.service
for i in $(seq 30); do python3 -c "import json,sys;d=json.load(open('/var/lib/pir-apm/hosts.json'));sys.exit(0 if 'transparent-pir-txid-k8-01' in d and d['transparent-pir-txid-k8-01'].get('at') else 1)" && break; sleep 2; done
python3 -c "import json;d=json.load(open('/var/lib/pir-apm/hosts.json'));print('sampler sources', sorted(k for k,v in d.items() if v.get('at')))"
# APM: new config and binary together.
install -m 644 $W/transparent-production.json /etc/pir-quality/transparent-production.json
printf '[Service]\nExecStart=\nExecStart=/usr/bin/python3 /opt/enhance-pir/releases/observability-20260926/credential-exec.py %s/pir-apm\n' $R > /etc/systemd/system/pir-apm.service.d/zzzzzzzzzzzzzzzzzzzzz-apm-obs-1868e5a6.conf
systemctl daemon-reload; systemctl restart pir-apm.service
ok=0; for i in $(seq 45); do curl -sf http://127.0.0.1:3002/healthz >/dev/null && { ok=1; break; }; sleep 2; done
[ $ok = 1 ] || { rollback; exit 1; }
systemctl show pir-apm.service -p ExecStart --value | grep -o "$R/pir-apm" | head -1
journalctl -u pir-apm.service --since "-2min" --no-pager | grep -iE "transparent_config_invalid|error|panic" | tail -5 || true
echo SWITCH_DONE
