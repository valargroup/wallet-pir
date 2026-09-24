#!/usr/bin/env python3
"""Rebuild the compact result table from immutable per-trial output."""
import gzip
import json
import pathlib

root = pathlib.Path(__file__).resolve().parent
results = []
for path in sorted((root / 'raw').glob('*.cgroup.json*')):
    name = path.name.split('.cgroup.json')[0]
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as f:
        run = json.load(f)
    events = [json.loads(line) for line in (root / 'raw' / (name + '.jsonl')).read_text().splitlines()]
    baseline = next((e for e in events if e['stage'] == 'baseline'), None)
    complete = next((e for e in events if e['stage'] in ['complete', 'fixture_complete']), None)
    resident = [e for e in events if e['stage'] == 'resident']
    results.append({
        'name': name,
        'result': run['properties']['Result'],
        'memory_peak_bytes': int(run['properties']['MemoryPeak']),
        'memory_peak_gib': round(int(run['properties']['MemoryPeak']) / 2**30, 6),
        'baseline': baseline,
        'last_resident': resident[-1] if resident else None,
        'complete': complete,
        'elapsed': run['elapsed'],
        'command': run['command'],
    })
(root / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
for r in results:
    print(r['name'], r['result'], r['memory_peak_gib'],
          (r['complete'] or {}).get('detail', {}).get('responses_verified', '-'))
