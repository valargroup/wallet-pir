#!/bin/bash
# Every 15 s append a one-line summary of the 5 QPS supervisor status to $1.
# Fields: utc mode exact errors missed p50 p99 latched freshness public_height node_height reasons
out=$1
while true; do
  python3 -W ignore - >> "$out" 2>&1 <<'EOF'
import json, os, datetime
L = '/srv/transparent-activity/canonical-load/v11'
try:
    s = json.load(open(L + '/status.json'))
    t = s['trailing_60s']; p = s.get('publisher') or {}
    print(datetime.datetime.utcnow().strftime('%FT%TZ'), s['mode'], t['exact'], t['errors'], t['missed_slots'],
          t['http_p50_seconds'], t['http_p99_seconds'], 'latched' if os.path.exists(L + '/latched.json') else 'nolatch',
          p.get('freshness_seconds'), p.get('public_height'), p.get('node_height'), '|'.join(s.get('reasons') or []), s['utc'])
except Exception as e:
    print(datetime.datetime.utcnow().strftime('%FT%TZ'), 'sample-error', e)
EOF
  sleep 15
done
