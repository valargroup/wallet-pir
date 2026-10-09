#!/bin/bash
# Every 10 s: worker host MemAvailable, service MemoryCurrent and any staged verify process RSS.
# Usage: mem-sampler.sh <worker-ip> <out>
. /root/deploy-a317455e/env.sh
h=$1; out=$2
while true; do
  line=$($W root@$h 'grep -E "^MemAvailable" /proc/meminfo | tr -s " "; systemctl show transparent-shard-server -p MemoryCurrent -p ActiveState; ps -eo rss,etime,args | grep "[s]taged/transparent-shard-server" | cut -c1-80' 2>&1 | tr '\n' ' ')
  echo "$(date -u +%FT%TZ) $line" >> "$out"
  sleep 10
done
