#!/bin/bash
# Every 5 s: epoch, controller phase, public_height, node_height, freshness_seconds, cycle_seconds, ingest_error. Output: $1
out=$1
while true; do
  t=$(date +%s.%N)
  s=$(curl -s -m 3 127.0.0.1:8094/v1/status | python3 -c "import json,sys
try:
 s=json.load(sys.stdin);print(s.get('phase'),s.get('public_height'),s.get('node_height'),s.get('freshness_seconds'),s.get('cycle_seconds'),(s.get('ingest_error') or '-').replace(' ','_'))
except Exception:print('unavailable')")
  echo "$t $s" >> "$out"
  sleep 5
done
