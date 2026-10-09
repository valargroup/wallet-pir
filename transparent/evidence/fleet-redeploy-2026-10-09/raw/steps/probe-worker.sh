#!/bin/bash
# Every second: UTC epoch and a worker /v1/ready summary. Usage: probe-worker.sh <host> <out>
h=$1; out=$2
while true; do
  t=$(date +%s.%N)
  r=$(curl -s -m 2 http://$h:8093/v1/ready | python3 -c "import json,sys
try:
 r=json.load(sys.stdin);print(r.get(\"ready\"),r.get(\"mode\"),r.get(\"warm_runtimes\"),r.get(\"target_runtimes\"),(r.get(\"binary_sha256\") or \"\")[:12],json.dumps(r.get(\"runtime_cache\"),separators=(\",\",\":\")))
except Exception as e:print(\"unreachable\")" 2>/dev/null)
  echo "$t $r" >> $out
  sleep 1
done
