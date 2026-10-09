#!/bin/bash
# Every second: UTC epoch, HTTP code of both public map origins, controller phase/public_height. Output: $1
out=$1
while true; do
  t=$(date +%s.%N)
  a=$(curl -s -o /dev/null -m 3 -w "%{http_code}" https://transparent-pir.valargroup.dev/v1/shards)
  b=$(curl -s -o /dev/null -m 3 -w "%{http_code}" https://enhance-pir.valargroup.dev/v1/filters/shards)
  s=$(curl -s -m 2 127.0.0.1:8094/v1/status | python3 -c "import json,sys
try:
 s=json.load(sys.stdin);print(s.get(\"phase\"),s.get(\"public_height\"),s.get(\"node_height\"),s.get(\"freshness_seconds\"))
except Exception as e:print(\"status-unavailable\")" 2>/dev/null)
  echo "$t $a $b $s" >> $out
  sleep 1
done
