#!/bin/zsh
# usage: dp-v2.sh <txid-display-command> [args...]; v3 request (06a972db); logs to log/<utc>-v2-<command>.log
H=$HOME/.config/wallet-pir-deploy/txid-display
cd /Users/roman/projects/wallet-pir/.claude/worktrees/deploy-v2-6c200028 || exit 1
cmd=$1; shift
log=$H/log/$(date -u +%Y%m%dT%H%M%SZ)-v3-$cmd.log
WALLET_PIR_DEPLOY_INVENTORY=$HOME/.config/wallet-pir-deploy/inventory.json \
  python3 ops/scripts/wallet-pir-deploy.py --state-dir $HOME/.config/wallet-pir-deploy/state "$cmd" \
  --request $H/request-v3.json --request-sha256 505e6e205ad44b06c5180734e9398b33bc19189b36d8f8b69d670b57e0465f74 "$@" 2>&1 | tee $log
exit ${pipestatus[1]}
