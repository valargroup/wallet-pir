#!/bin/zsh
# Run one txid display deploy phase, then end any orphaned lock session from this Mac.
phase=$1
~/.config/wallet-pir-deploy/txid-display/dp-v3.sh txid-display-deploy --expect-plan-sha256 67f42ea740762c3cb058174828d3ea96fdd8c8abcb007918f1cd99a24c50a7f4 --phase $phase > /dev/null 2>&1; rc=$?
echo "phase=$phase exit=$rc $(date -u +%T)"
L=$(ls -t ~/.config/wallet-pir-deploy/txid-display/log/*-v3-txid-display-deploy.log | head -1); grep -vE '^\s|^[{}]' $L | tail -8 | cut -c1-250
sleep 3
ssh -o BatchMode=yes root@167.99.42.60 'flock -n /run/lock/wallet-pir-production.lock true && echo LOCK_FREE || { for p in $(pgrep -f "flock -n /run/lock/wallet-pir-production.lock"); do pp=$(ps -o ppid= -p $p | tr -d " "); ss -tnp | grep -q "pid=$pp,.*181.91.84.80" && { echo "ending orphan lock session $pp"; kill $pp; }; done; sleep 2; flock -n /run/lock/wallet-pir-production.lock true && echo LOCK_FREE || echo LOCK_HELD; }'
exit $rc
