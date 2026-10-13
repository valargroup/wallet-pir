#!/bin/bash
# Rollback of v1-sunset.sh: v11 back as journal writer and router owner.
set -euo pipefail
B=/root/v1-sunset-20261013; P=/opt/transparent-publisher
RSSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -i $P/credentials/deploy-ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$P/credentials/known_hosts root@10.142.0.11"
exec 9>/run/lock/wallet-pir-production.lock; flock -n 9 || { echo "production lock held"; exit 1; }
cp -a $B/v12-controller.json $P/v12/controller.json
systemctl stop transparent-v12-publish-controller.service
$RSSH 'set -e; cat > /etc/caddy/Caddyfile.rollback-next; caddy validate --adapter caddyfile --config /etc/caddy/Caddyfile.rollback-next >&2; mv /etc/caddy/Caddyfile.rollback-next /etc/caddy/Caddyfile; systemctl reload caddy' < $B/router-Caddyfile
for u in transparent-publish-controller.service transparent-control-sessions.service transparent-replica-reconciler.service transparent-fleet-scaler.service transparent-txid-display-controller.service transparent-fleet-actuator.timer transparent-5qps-continuous.service; do
  systemctl unmask -q $u 2>/dev/null || true
  if grep -q "^$u [a-z]* enabled" $B/units-before.txt; then systemctl enable -q $u; fi
  if grep -q "^$u active" $B/units-before.txt; then systemctl start $u; fi
  echo "restored $u"
done
sleep 5; systemctl start transparent-v12-publish-controller.service
echo rolled back
