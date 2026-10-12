#!/bin/bash
# Serve wallet API v2 on transparent-pir.valargroup.dev: the v1 fleet's router
# imports /etc/caddy/api-v2/*.caddy beside the txid display routes. The v1
# routes are unchanged; the next v11 publication renders the import.
#
#   prod-v2-route.sh apply|rollback
set -euo pipefail
MODE=$1
F=/opt/transparent-publisher/v11/fleet.json
D=/opt/transparent-publisher/v11/api-v2-route
GLOB=/etc/caddy/api-v2/*.caddy
R="ssh -i /opt/transparent-publisher/credentials/deploy-ssh -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -o BatchMode=yes root@10.142.0.11"
exec 9>/run/lock/wallet-pir-production.lock
flock -w 120 9 || { echo LOCK_BUSY; exit 1; }
echo "lock held $(date -u +%FT%TZ)"
case "$MODE" in
apply)
  test -f "$D/api-v2-routes.caddy"
  [ -f "$D/fleet.before.json" ] || cp "$F" "$D/fleet.before.json"
  $R "mkdir -p /etc/caddy/api-v2 && cat > /etc/caddy/api-v2/v2.caddy" < "$D/api-v2-routes.caddy"
  # Validate the exact router the adapter will render before opting in, so a
  # bad route file can never fail a v1 publication's route step.
  $R "set -e; sed 's#^\timport /etc/caddy/txid-display/\*\.caddy\$#&\n\timport /etc/caddy/api-v2/*.caddy#' /etc/caddy/Caddyfile > /tmp/Caddyfile.v2check
      echo imports=\$(grep -c 'import /etc/caddy/api-v2' /tmp/Caddyfile.v2check)
      caddy validate --config /tmp/Caddyfile.v2check --adapter caddyfile 2>&1 | tail -1"
  python3 - "$F" "$GLOB" <<'EOF'
import json, sys
path, glob = sys.argv[1:]
fleet = json.load(open(path))
imports = fleet.get("route_imports") or []
if glob not in imports:
    fleet["route_imports"] = imports + [glob]
open(path + ".next", "w").write(json.dumps(fleet, indent=1))
EOF
  mv "$F.next" "$F"
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["route_imports"])' "$F"
  ;;
rollback)
  cp "$D/fleet.before.json" "$F"
  echo "route_imports restored; the next publication renders the router without /v2"
  ;;
esac
echo "done $(date -u +%FT%TZ)"
