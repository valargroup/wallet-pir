#!/usr/bin/env bash
# Collect the raw state of a schema-8 rollout step into this directory.
# Read-only against production; run from the repository root.
set -euo pipefail
OUT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STAGE="${1:?usage: collect.sh <stage-name>}"
COORD=root@167.99.42.60
WORKERS=(10.142.0.15 10.142.0.16)
PUBLIC=https://enhance-pir.valargroup.dev

ssh -o BatchMode=yes "$COORD" 'date -u +%Y-%m-%dT%H:%M:%SZ' > "$OUT/$STAGE.utc"
curl -fsS "$PUBLIC/v1/enhance/init" > "$OUT/$STAGE-init.json" || echo '{}' > "$OUT/$STAGE-init.json"
curl -fsS "$PUBLIC/v1/health" > "$OUT/$STAGE-health.json" || echo '{}' > "$OUT/$STAGE-health.json"
ssh -o BatchMode=yes "$COORD" 'systemctl show -p ExecStart,ActiveState,MemoryCurrent --value enhance-pir-server' \
  > "$OUT/$STAGE-coordinator-unit.txt"
for w in "${WORKERS[@]}"; do
  ssh -o BatchMode=yes -J "$COORD" "root@$w" '
    echo "unit:"; systemctl show -p ExecStart,ActiveState --value enhance-pir-worker
    echo "memory.current:"; cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.current
    echo "memory.peak:"; cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.peak
    echo "memory.events:"; cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.events
    echo "memory.swap.current:"; cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.swap.current
    echo "disk:"; df -h / | tail -1
  ' > "$OUT/$STAGE-worker-${w##*.}.txt"
done
echo "collected $STAGE into $OUT"
