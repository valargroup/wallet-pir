#!/bin/zsh
# Run one txid display deploy phase, then end any orphaned lock session from this Mac.
phase=$1
~/.config/wallet-pir-deploy/txid-display/dp-v4.sh txid-display-deploy --expect-plan-sha256 d6a5e1b4be769abb354a1c949f9edea1341afff970d485091b68b9f9417e8838 --phase $phase "${@:2}" > /dev/null 2>&1; rc=$?
echo "phase=$phase exit=$rc $(date -u +%T)"
L=$(ls -t ~/.config/wallet-pir-deploy/txid-display/log/*-v4-txid-display-deploy.log | head -1); grep -vE '^\s|^[{}]' $L | tail -8 | cut -c1-250
sleep 3
ssh -o BatchMode=yes root@167.99.42.60 'bash /root/deploy-9d2cbda0/lock-clean.sh 181.91.84.80'
exit $rc
