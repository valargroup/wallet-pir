#!/usr/bin/env python3
"""Per-class outcome and measured bill of a sample recovery (wallets.ndjson).

A wallet's bill is every byte its sync moved: the map, filters, manifests
(with their choice tables), setups and private queries both ways, for the
directory and the page shards. MB are 10^6 bytes, as the census tables use.
Usage: summarize.py wallets.ndjson
"""
import collections
import json
import sys


def percentile(values, p):
    values = sorted(values)
    if not values:
        return None
    k = (len(values) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def total(w):
    t = w['map_bytes'] + w['filter_bytes'] + w['manifest_bytes']
    for table in ('directory', 'pages'):
        c = w[table]
        t += c['setup_bytes'] + c['query_upload'] + c['query_download']
    return t


rows = [json.loads(line) for line in open(sys.argv[1])]
by = collections.defaultdict(list)
for w in rows:
    by[w['class']].append(w)
out = {'wallets': len(rows), 'exact': sum(w['exact'] for w in rows),
       'errors': sum('error' in w for w in rows), 'classes': {}}
for name, ws in sorted(by.items()):
    ok = [w for w in ws if 'error' not in w]
    bills = [total(w) for w in ok]
    out['classes'][name] = {
        'wallets': len(ws),
        'exact': sum(w['exact'] for w in ws),
        'completion': dict(collections.Counter(w.get('completion', 'error').split(' ')[0] for w in ws)),
        'bill_mb_p50_p95_p99': [round(percentile(bills, p) / 1e6, 2) for p in (50, 95, 99)],
        'directory_queries_p50_max': [percentile([w['directory']['queries'] for w in ok], 50),
                                      max(w['directory']['queries'] for w in ok)],
        'page_queries_p50_p99_max': [percentile([w['pages']['queries'] for w in ok], 50),
                                     percentile([w['pages']['queries'] for w in ok], 99),
                                     max(w['pages']['queries'] for w in ok)],
        'seconds_p50_p99': [round(percentile([w['seconds'] for w in ok], 50), 2),
                            round(percentile([w['seconds'] for w in ok], 99), 2)],
    }
print(json.dumps(out, indent=1))
