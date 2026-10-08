#!/bin/zsh
# Print the controller's last cycle whenever it changes: block-to-serving fields
# and the recent worker's shipped counts. usage: watch-cycles.sh [count]
set -eu
SP=${0:A:h}
CFG=$SP/ssh_config
N=${1:-5}
ssh -F $CFG coordinator "python3 - $N <<'PY'
import json, sys, time, urllib.request
n = int(sys.argv[1]); seen = None
while n > 0:
    d = json.load(urllib.request.urlopen('http://127.0.0.1:8099/'))
    lc = d.get('last_cycle') or {}
    if lc.get('cycle') != seen:
        seen = lc.get('cycle'); n -= 1
        w = [x for x in lc.get('workers', []) if x.get('role') == 'recent-replica']
        print(json.dumps({k: lc.get(k) for k in ('cycle','tip','prebuild_ms','shipped_bytes','prebuild_failures','ship_ms','prepare_ms','activate_ms','cycle_ms','freshness_ms')} | {'recent': [{k: x.get(k) for k in ('shipped','shipped_fallbacks','self_check_ms','built','reused','seconds')} for x in w]}), flush=True)
    time.sleep(1)
PY"
