#!/bin/zsh
# txid-4k8 cutover on the k8 site: stop, workers (replacing the live 2k8 map), route, controller.
set -u; setopt pipefail
H=$HOME/.config/wallet-pir-deploy/txid-display
S=/private/tmp/claude-501/-Users-roman-projects-wallet-pir--claude-worktrees-pir-bit-encoding-choices-cdf426/a2792897-e89d-4597-bcae-e80bd698f5ef/scratchpad/ssh_config
P=$(cat $H/plan-k8-4k8.sha256)
NEW=$(ssh -F $S coord 'journalctl -u transparent-txid-display-k8-bootstrap-4k8-snap -o cat --no-pager | grep "^{" | tail -1' | python3 -c 'import json,sys; print(json.load(sys.stdin)["map_sha256"])') || exit 1
echo "new map $NEW"
date -u +%FT%TZ; $H/dp-k8-4k8.sh txid-display-stop >/dev/null || { echo STOP_FAILED; exit 1; }
OLD=$(ssh -F $S k8 'cat /opt/transparent-txid-display/worker/active.json' | python3 -c 'import json,sys; print(json.load(sys.stdin)["map_sha256"])')
echo "replacing $OLD"; date -u +%FT%TZ
$H/dp-k8-4k8.sh txid-display-deploy --expect-plan-sha256 $P --phase workers --publication-map-sha256 $NEW --replace-active $OLD | grep -v '^ ' | tail -15 || { echo WORKERS_FAILED; exit 1; }
date -u +%FT%TZ
$H/dp-k8-4k8.sh txid-display-deploy --expect-plan-sha256 $P --phase route | grep -v '^ ' | tail -5 || { echo ROUTE_FAILED; exit 1; }
$H/dp-k8-4k8.sh txid-display-deploy --expect-plan-sha256 $P --phase controller | grep -v '^ ' | tail -5 || { echo CONTROLLER_FAILED; exit 1; }
date -u +%FT%TZ; echo CUTOVER_DONE
