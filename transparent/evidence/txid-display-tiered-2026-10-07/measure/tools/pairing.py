#!/usr/bin/env python3
"""Router Caddy reloads per history activation (reconciler worker_membership event).

  pairing.py MEMBERSHIP_LOG RELOAD_LOG SPLIT_ISO
Each reload is assigned to the nearest membership event within 15 s of it.
"""
import sys, json
from datetime import datetime
from collections import Counter

def ts(line):
    return datetime.fromisoformat(line.split()[0]).timestamp()

mem = [(ts(l), json.loads(l[l.index('{'):])) for l in open(sys.argv[1]) if 'worker_membership' in l]
rel = [ts(l) for l in open(sys.argv[2]) if 'Reloading caddy' in l]
split = datetime.fromisoformat(sys.argv[3]).timestamp()
per = Counter(); unmatched = 0
for r in rel:
    near = [m for m in mem if abs(r - m[0]) <= 15]
    if not near:
        unmatched += 1; continue
    m = min(near, key=lambda m: abs(m[0] - r)); per[m[0]] += 1
out = {}
for name, sel in (('before', lambda t: t < split), ('after', lambda t: t >= split)):
    ms = [m for m in mem if sel(m[0])]
    dist = Counter(per.get(m[0], 0) for m in ms)
    out[name] = {'activations': len(ms), 'reloads': sum(per.get(m[0], 0) for m in ms),
                 'reloads_per_activation': dict(sorted(dist.items())),
                 'reload_timestamps_total': sum(1 for r in rel if sel(r))}
out['unmatched_reloads'] = unmatched
print(json.dumps(out, indent=1))
