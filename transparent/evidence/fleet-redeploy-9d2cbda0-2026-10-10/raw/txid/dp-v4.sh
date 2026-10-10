#!/bin/zsh
# usage: dp-v2.sh <txid-display-command> [args...]; v4 request (9d2cbda0); logs to log/<utc>-v2-<command>.log
H=$HOME/.config/wallet-pir-deploy/txid-display
cd /Users/roman/projects/wallet-pir/.claude/worktrees/deploy-v2-6c200028 || exit 1
cmd=$1; shift
log=$H/log/$(date -u +%Y%m%dT%H%M%SZ)-v4-$cmd.log
WALLET_PIR_DEPLOY_INVENTORY=$HOME/.config/wallet-pir-deploy/inventory.json \
  python3 ops/scripts/wallet-pir-deploy.py --state-dir $HOME/.config/wallet-pir-deploy/state "$cmd" \
  --request $H/request-v4.json --request-sha256 f5d6996a90d79657a12c3f43abab4b7c5de53df36fe3bcda8afaaae2370855b5 "$@" 2>&1 | tee $log
exit ${pipestatus[1]}
