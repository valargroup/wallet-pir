#!/bin/zsh
# Deploy the shipped-runtimes recent worker binary to recent-01 over SSH.
# usage: deploy-recent.sh <source-sha>
# Keeps zz-latency.conf's arguments, environment and limits; replaces only the
# binary path. Rollback: put the previous release path back into the drop-in,
# daemon-reload, restart.
set -eu
SHA=$1
SP=${0:A:h}
CFG=$SP/ssh_config
BIN=$SP/release/transparent-txid-server
WANT=$(shasum -a 256 $BIN | cut -d' ' -f1)
REL=/opt/transparent-txid-display/releases/$SHA-ship
UNIT=transparent-txid-display-worker.service
DROPIN=/etc/systemd/system/$UNIT.d/zz-latency.conf
PREV=/opt/transparent-txid-display/releases/07f906753cb0-latency/transparent-txid-server

ssh -F $CFG recent-01 "mkdir -p $REL"
scp -F $CFG -q $BIN recent-01:$REL/transparent-txid-server
ssh -F $CFG recent-01 "chmod 0755 $REL/transparent-txid-server && echo '$WANT  $REL/transparent-txid-server' | sha256sum -c -"

# The drop-in must still name the previous binary exactly once.
ssh -F $CFG recent-01 "grep -c '^ExecStart=$PREV ' $DROPIN" | grep -qx 1
ssh -F $CFG recent-01 "cp -n $DROPIN $DROPIN.before-$SHA && sed -i 's#^ExecStart=$PREV #ExecStart=$REL/transparent-txid-server #; s#^\# Display latency change (07f906753cb0)#\# Shipped runtimes ($SHA) on the display latency change (07f906753cb0)#' $DROPIN && grep -c '^ExecStart=$REL/transparent-txid-server --listen 10.142.0.10:8095 --role recent-replica ' $DROPIN" | grep -qx 1

# Restart right after an activation, so the restart does not interrupt a prepare.
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
ssh -F $CFG recent-01 "systemctl daemon-reload && systemctl restart $UNIT && sleep 2 && systemctl is-active $UNIT && systemctl show -p ExecStart -p CPUQuotaPerSecUSec -p CPUWeight -p Environment $UNIT | cut -c1-240"
