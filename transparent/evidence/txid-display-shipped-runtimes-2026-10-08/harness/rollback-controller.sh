#!/bin/zsh
# Remove the --ship-runtimes drop-in and restart the controller between cycles.
set -eu
SP=${0:A:h}
CFG=$SP/ssh_config
UNIT=transparent-txid-display-controller.service
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
ssh -F $CFG coordinator "rm -f /etc/systemd/system/$UNIT.d/zz-ship.conf && systemctl daemon-reload && systemctl restart $UNIT && sleep 3 && systemctl is-active $UNIT && systemctl show -p ExecStart $UNIT | cut -c1-300"
