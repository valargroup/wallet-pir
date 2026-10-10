#!/usr/bin/env python3
"""Check and tabulate the served-segment certificates of one run.

Every planned segment (inputs/plan.tsv) must have a 49-bit nearest and a 44-bit
dithered report whose rows binding equals the manifest's segment hash, and a
certificate meeting its floor (archive-wide pages 83, otherwise 128). The
dithered certificate's 49-bit nearest screen must equal the separate nearest
result. Prints results.json; exits 1 on any violation.
"""
import collections
import json
import sys
from pathlib import Path

run = Path(sys.argv[1])
FLOORS = {('archive-wide', 'pages'): 83}
WIDTH = {'nearest': (49, 'nearest'), 'dithered': (44, 'dithered')}
problems, rows, setups = [], [], collections.defaultdict(set)
for line in (run / 'inputs/plan.tsv').read_text().splitlines():
    prod, shard, geometry, table, index, digest, _ = line.split('\t')
    manifest_hashes = {}
    floor = FLOORS.get((geometry, table), 128)
    results = {}
    for rounding, (bits, mode) in WIDTH.items():
        name = f'{prod}-s{shard}-{table}.{index}-{digest[:12]}-{rounding}'
        report_path = run / 'reports' / f'{name}.report.json'
        cert_path = run / 'reports' / f'{name}.certificate.json'
        if not report_path.exists() or not cert_path.exists() or cert_path.stat().st_size == 0:
            problems.append(f'{name}: missing report or certificate')
            continue
        report = json.loads(report_path.read_text())
        cert = json.loads(cert_path.read_text())
        actual = cert['actual_profile']
        if (actual['query_bits'], actual['query_rounding']) != (bits, mode):
            problems.append(f'{name}: width {actual["query_bits"]}/{actual["query_rounding"]}')
        if actual['certified_failure_bits'] < floor:
            problems.append(f'{name}: {actual["certified_failure_bits"]} bits below floor {floor}')
        setups[(geometry, table)].add(report['setup_id'])
        results[rounding] = {'bits': actual['certified_failure_bits'], 'database': report['database_sha256'],
                             'rows': report['rows'], 'screen': cert.get('query_screen') or []}
    if len(results) < 2:
        continue
    if results['nearest']['database'] != results['dithered']['database']:
        problems.append(f'{prod} s{shard} {table}: widths certified different bytes')
    screen = next((v['certified_failure_bits'] for v in results['dithered']['screen']
                   if v['query_bits'] == 49 and v['query_rounding'] == 'nearest'), None)
    if screen != results['nearest']['bits']:
        problems.append(f'{prod} s{shard} {table}: dithered screen 49-nearest {screen} != {results["nearest"]["bits"]}')
    rows.append({'product': prod, 'shard': int(shard), 'geometry': geometry, 'table': table,
                 'segment': int(index), 'manifest_digest': digest, 'rows': results['nearest']['rows'],
                 'database': results['nearest']['database'], 'floor': floor,
                 'nearest_49_bits': results['nearest']['bits'], 'dithered_44_bits': results['dithered']['bits']})
for key, ids in setups.items():
    if len(ids) != 1:
        problems.append(f'{key}: {len(ids)} setup ids')
groups = collections.defaultdict(list)
for row in rows:
    groups[(row['product'], row['geometry'], row['table'])].append(row)
summary = [{'product': p, 'geometry': g, 'table': t, 'segments': len(v), 'floor': v[0]['floor'],
            'min_nearest_49_bits': min(r['nearest_49_bits'] for r in v),
            'min_dithered_44_bits': min(r['dithered_44_bits'] for r in v)}
           for (p, g, t), v in sorted(groups.items())]
json.dump({'segments': len(rows), 'problems': problems, 'summary': summary, 'rows': rows}, sys.stdout, indent=1)
print()
sys.exit(1 if problems else 0)
