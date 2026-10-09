#!/bin/zsh
# Deploy the controller with --ship-runtimes on the coordinator over SSH.
# usage: deploy-controller.sh <source-sha>
# Adds one drop-in, zz-ship.conf: the new binary, every current argument, plus
# --ship-runtimes, and TRANSPARENT_BUILD_THREADS=2 for the 200% CPU quota.
# Rollback: rollback-controller.sh (removes the drop-in and restarts).
set -eu
SHA=$1
SP=${0:A:h}
CFG=$SP/ssh_config
BIN=$SP/release/txid-display-controller
WANT=$(shasum -a 256 $BIN | cut -d' ' -f1)
REL=/opt/transparent-txid-display/releases/$SHA-ship
UNIT=transparent-txid-display-controller.service
DIR=/etc/systemd/system/$UNIT.d

ssh -F $CFG coordinator "mkdir -p $REL"
scp -F $CFG -q $BIN coordinator:$REL/txid-display-controller
ssh -F $CFG coordinator "chmod 0755 $REL/txid-display-controller && echo '$WANT  $REL/txid-display-controller' | sha256sum -c -"

# Keep every argument of the current ExecStart; replace the binary, add the flag.
ARGS=$(ssh -F $CFG coordinator "systemctl show -p ExecStart --value $UNIT" | sed -n 's/.*argv\[\]=[^ ]* \([^;]*\) ;.*/\1/p')
[[ -n $ARGS && $ARGS == run\ --root\ /srv/zakura/txid-display-poc/root* && $ARGS == *--mode\ replay-then-live* && $ARGS != *--ship-runtimes* ]] || { echo "unexpected ExecStart args: $ARGS"; exit 1; }

DROPIN=$(cat <<EOT
# Shipped recent runtimes ($SHA): the controller builds the recent revision's
# runtimes into each candidate; recent-01 loads and self-checks them.
# Two build threads for the unit's CPUQuota=200%.
# Rollback: rm this file; systemctl daemon-reload; systemctl restart $UNIT
[Service]
ExecStart=
ExecStart=$REL/txid-display-controller $ARGS --ship-runtimes
Environment=TRANSPARENT_BUILD_THREADS=2
EOT
)

# Restart right after an activation, between cycles.
ssh -F $CFG coordinator 'python3 - <<"PY"
import json, time, urllib.request
def status():
    return json.load(urllib.request.urlopen("http://127.0.0.1:8099/"))
def last():
    return status()["last_cycle"]["activated_ms"]
a = last()
while last() == a:
    time.sleep(0.5)
# Right after an activation active.json is written; a queued seal would be
# lost to the restart, so wait for the next activation while one is queued.
while status()["queue"]:
    print("seal queued; waiting for the next activation")
    a = last()
    while last() == a:
        time.sleep(0.5)
print("activation observed, seal queue empty")
PY'
ssh -F $CFG coordinator "mkdir -p $DIR && cat > $DIR/zz-ship.conf" <<<"$DROPIN"
ssh -F $CFG coordinator "systemctl daemon-reload && systemctl restart $UNIT && sleep 3 && systemctl is-active $UNIT && systemctl show -p ExecStart -p CPUQuotaPerSecUSec -p CPUWeight -p Environment $UNIT | cut -c1-400"
