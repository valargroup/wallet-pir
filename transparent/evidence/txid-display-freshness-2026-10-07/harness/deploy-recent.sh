#!/bin/zsh
# Deploy the display recent worker latency change to recent-01 over SSH.
# usage: deploy-recent.sh <source-sha>
set -eu
SHA=$1
SP=${0:A:h}
CFG=$SP/ssh_config
BIN=$SP/release/transparent-txid-server
WANT=$(shasum -a 256 $BIN | cut -d' ' -f1)
REL=/opt/transparent-txid-display/releases/$SHA-latency
UNIT=transparent-txid-display-worker.service
OLD=/opt/transparent-txid-display/releases/d191f86b2873e4250cee3ae6952f830c0d1e7a55/transparent-txid-server

ssh -F $CFG recent-01 "mkdir -p $REL"
scp -F $CFG -q $BIN recent-01:$REL/transparent-txid-server
ssh -F $CFG recent-01 "chmod 0755 $REL/transparent-txid-server && echo '$WANT  $REL/transparent-txid-server' | sha256sum -c -"

# Keep every argument of the current ExecStart; replace only the binary path.
ARGS=$(ssh -F $CFG recent-01 "systemctl show -p ExecStart --value $UNIT" | sed -n 's/.*argv\[\]=[^ ]* \([^;]*\) ;.*/\1/p')
[[ -n $ARGS && $ARGS == *--role\ recent-replica* ]] || { echo "could not parse ExecStart args: $ARGS"; exit 1; }

DROPIN=$(cat <<EOF
# Display latency change ($SHA): batched hint for txid-2k, two build threads on
# up to two CPUs. Display still loses to history: weight 50 < 100, nice 10.
# Rollback: rm this file; systemctl daemon-reload; systemctl restart $UNIT
[Service]
ExecStart=
ExecStart=$REL/transparent-txid-server $ARGS
Environment=TRANSPARENT_BUILD_THREADS=2
CPUQuota=200%
CPUWeight=50
EOF
)

# Wait for the controller to finish an activation, so the restart does not
# interrupt a prepare.
ssh -F $CFG coordinator 'python3 - <<"PY"
import json, time, urllib.request
def last():
    return json.load(urllib.request.urlopen("http://127.0.0.1:8099/"))["last_cycle"]["activated_ms"]
a = last()
while last() == a:
    time.sleep(0.5)
print("activation observed")
PY'

ssh -F $CFG recent-01 "mkdir -p /etc/systemd/system/$UNIT.d && cat > /etc/systemd/system/$UNIT.d/zz-latency.conf" <<<"$DROPIN"
ssh -F $CFG recent-01 "systemctl daemon-reload && systemctl restart $UNIT && sleep 2 && systemctl is-active $UNIT && systemctl show -p ExecStart -p CPUQuotaPerSecUSec -p CPUWeight -p Environment $UNIT | cut -c1-220"
