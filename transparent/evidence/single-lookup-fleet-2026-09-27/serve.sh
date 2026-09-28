#!/usr/bin/env bash
# Serve one variant of the bench set on the VPC address, replacing any other.
set -euo pipefail
mode=$1
ip=$(hostname -I | tr " " "\n" | grep "^10\.110\." | head -1)
systemctl stop sl-shard 2>/dev/null || true
systemctl reset-failed sl-shard 2>/dev/null || true
systemd-run --unit sl-shard --property=MemoryMax=7G --property=MemorySwapMax=0 --property=LimitNOFILE=1048576 --setenv=RUST_LOG=info \
  /usr/local/bin/transparent-shard-server --listen $ip:8093 --shard-dir /root/sl-shards-$mode \
  --cache-bytes 5368709120 --build-slots 1 --query-slots 2 --pilot-cold
for _ in $(seq 1 60); do curl -sf http://$ip:8093/v1/ready > /dev/null && { echo "$(hostname) serving $mode"; exit 0; }; sleep 1; done
echo "$(hostname) not ready" >&2; exit 1
