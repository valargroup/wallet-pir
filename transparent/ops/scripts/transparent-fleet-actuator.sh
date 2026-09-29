#!/usr/bin/env bash
# Run a transparent fleet actuator command on the coordinator with the Wallet
# PIR runtime credential, exactly as its timer does. Examples:
#   transparent-fleet-actuator.sh status
#   transparent-fleet-actuator.sh scale-out --count 1
#   transparent-fleet-actuator.sh scale-in --member transparent-pir-recent-05
set -euo pipefail
release="${TRANSPARENT_RELEASE:-/opt/transparent-publisher/releases/current}"
exec systemd-run --quiet --wait --pipe --collect --property=UMask=0077 \
  --property=LoadCredentialEncrypted=runtime:/etc/credstore.encrypted/enhance-pir-runtime \
  /usr/bin/python3 /opt/enhance-pir/ops/wallet-pir-runtime.py /usr/bin/python3 \
  "$release/repo/transparent/ops/scripts/transparent-fleet-actuator.py" \
  --config /opt/transparent-publisher/scaler/actuator.json "$@"
