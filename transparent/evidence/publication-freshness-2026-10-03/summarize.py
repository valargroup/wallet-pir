#!/usr/bin/env python3
"""Summarize alternating baseline/final publication_bench records.

Reads cmp/<fill>-<bin>-r<round>.json and compares the published trees of
cmp/<fill>-base and cmp/<fill>-final byte for byte."""
import filecmp, hashlib, json, os, statistics, sys

root = sys.argv[1] if len(sys.argv) > 1 else 'cmp'
fills = ['10.2', '13.5', '15.9']

def tree(path):
    out = {}
    for base, _, files in os.walk(path):
        for name in files:
            full = os.path.join(base, name)
            rel = os.path.relpath(full, path)
            with open(full, 'rb') as f:
                out[rel] = hashlib.sha256(f.read()).hexdigest()
    return out

summary = {'schema': 'transparent-publication-bench-summary-v1', 'fills': {}}
for fill in fills:
    entry = {}
    for b in ['base', 'final']:
        publish, runtime, maps, params, tails = [], {}, [], {}, []
        for r in (1, 2):
            rec = json.load(open(f'{root}/{fill}-{b}-r{r}.json'))
            for c in rec['cycles']:
                publish.append(c['publish_seconds'])
                maps.append((c['height'], c['map_sha256']))
                tails.append(c['tail'])
            for x in rec['runtime']:
                runtime.setdefault(x['table'], []).append(x['seconds'])
                params.setdefault(x['table'], set()).add(x['public_params_sha256'])
        entry[b] = {
            'publish_seconds': publish,
            'publish_median': statistics.median(publish),
            'runtime_seconds': runtime,
            'runtime_median': {t: statistics.median(v) for t, v in runtime.items()},
            'runtime_median_sum': sum(statistics.median(v) for v in runtime.values()),
            'map_sha256_by_height': sorted(set(maps)),
            'public_params_sha256': {t: sorted(v) for t, v in params.items()},
            'tail': tails[-1],
        }
    a, z = tree(f'{root}/{fill}-base'), tree(f'{root}/{fill}-final')
    published = lambda t: {k: v for k, v in t.items() if not k.startswith('journal/')}
    entry['byte_comparison'] = {
        'files_compared': len(published(a)),
        'published_bytes_identical': published(a) == published(z),
        'journal_identical': {k: v for k, v in a.items() if k.startswith('journal/') and not k.endswith('writer.lock')}
            == {k: v for k, v in z.items() if k.startswith('journal/') and not k.endswith('writer.lock')},
        'differing_files': sorted(k for k in set(a) | set(z) if a.get(k) != z.get(k) and not k.endswith('writer.lock')),
    }
    entry['map_digests_identical'] = entry['base']['map_sha256_by_height'] == entry['final']['map_sha256_by_height']
    entry['public_params_identical'] = entry['base']['public_params_sha256'] == entry['final']['public_params_sha256']
    entry['publish_median_reduction'] = 1 - entry['final']['publish_median'] / entry['base']['publish_median']
    summary['fills'][fill] = entry
json.dump(summary, sys.stdout, indent=1, default=list)
