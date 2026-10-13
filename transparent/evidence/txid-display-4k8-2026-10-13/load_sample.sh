#!/usr/bin/env bash
# Samples the txid worker and host every 15 s, read-only, until killed.
while true; do
  printf '{"unix":%s,"cpu_ns":%s,"mem":%s,"load":"%s","cpu":"%s"}\n' "$(date +%s)" \
    "$(systemctl show transparent-txid-display-worker -p CPUUsageNSec --value)" \
    "$(systemctl show transparent-txid-display-worker -p MemoryCurrent --value)" \
    "$(cut -d' ' -f1-3 /proc/loadavg)" "$(head -1 /proc/stat)"
  sleep 15
done
