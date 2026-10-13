#!/bin/bash
# v1 sunset, steps 1-2: stop v11, hand the journal writer to v12, install the router shell.
set -euo pipefail
B=/root/v1-sunset-20261013
P=/opt/transparent-publisher
J=/srv/transparent-activity/full-v3/journal
R=/opt/transparent-public-router/c9262e84
RSSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -i $P/credentials/deploy-ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$P/credentials/known_hosts root@10.142.0.11"
V11="transparent-5qps-continuous.service transparent-fleet-actuator.timer transparent-fleet-scaler.service transparent-replica-reconciler.service transparent-control-sessions.service transparent-publish-controller.service transparent-txid-display-controller.service"
log(){ echo "$(date -u +%FT%TZ) $*"; }
exec 9>/run/lock/wallet-pir-production.lock; flock -n 9 || { echo "production lock held"; exit 1; }
mkdir -p $B
cp -a $P/v11/controller.json $P/v11/fleet.json $B/; cp -a $P/v12/controller.json $B/v12-controller.json
$RSSH 'cat /etc/caddy/Caddyfile' > $B/router-Caddyfile
for u in $V11 transparent-v12-publish-controller.service; do echo "$u $(systemctl is-active $u || true) $(systemctl is-enabled $u || true)"; done > $B/units-before.txt
curl -s -m5 http://10.142.0.3:8096/v1/status > $B/v12-status-before.json
log "backups in $B"
mkdir -p $R && tar xzf /root/router-c9262e84.tgz -C $R
cat > /opt/transparent-public-router/router.json <<J
{"public_host":"transparent-pir.valargroup.dev","internal_listen":"10.142.0.11:8080","router_host":"10.142.0.11","ssh_key":"$P/credentials/deploy-ssh","known_hosts":"$P/credentials/known_hosts"}
J
python3 -B $R/transparent/ops/scripts/transparent-public-router.py render --config /opt/transparent-public-router/router.json > $B/router-shell.caddy
log "router shell rendered"
for u in $V11; do systemctl stop $u; systemctl disable -q $u 2>/dev/null || true; log "stopped+disabled $u ($(systemctl is-active $u || true))"; done
for i in $(seq 1 30); do fuser -s $J/writer.lock || break; sleep 1; done
if fuser -s $J/writer.lock; then log "journal writer lock still held"; exit 1; fi
log "journal writer lock released"
python3 - <<'PY'
import json,os
p='/opt/transparent-publisher/v12/controller.json';c=json.load(open(p));c['follow_journal']=False
open(p+'.next','w').write(json.dumps(c,indent=1)+'\n');os.replace(p+'.next',p)
PY
systemctl restart transparent-v12-publish-controller.service
log "v12 controller restarted as journal writer"
python3 -B $R/transparent/ops/scripts/transparent-public-router.py apply --config /opt/transparent-public-router/router.json
log "router shell applied"
for i in $(seq 1 90); do
  s=$(curl -s -m5 http://10.142.0.3:8096/v1/status || true)
  if echo "$s" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("phase")=="serving" and d.get("node_height",0)-d.get("journal_height",0)<=1 and not d.get("ingest_error") else 1)' 2>/dev/null; then log "v12 serving at node: $(echo "$s" | head -c 220)"; break; fi
  sleep 5
done
fuser -v $J/writer.lock 2>&1 | tail -1
log "done"
