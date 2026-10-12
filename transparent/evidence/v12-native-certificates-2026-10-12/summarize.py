#!/usr/bin/env python3
"""Tabulates this directory's certificates as JSON: one row per report.

For each report: the table, its rows, the fill, the query rounding and the
certified failure bits of the profile it describes. A dithered report's
certificate also screens other query transports on the same weights; its
49-bit nearest and 44-bit dithered entries are copied, so a row of each
rounding can be checked against the other's own report.
"""
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
rows = []
for report_path in sorted(out.glob('*.report.json')):
    name = report_path.name.removesuffix('.report.json')
    report = json.loads(report_path.read_text())
    certificate = json.loads((out / f'{name}.certificate.json').read_text())
    actual = certificate['actual_profile']
    row = {
        'name': name,
        'database': report['database_sha256'],
        'rows': report['rows'],
        'cols': report['cols'],
        'setup_id': report['setup_id'],
        'worst_case_query': report.get('worst_case_query'),
        'query_bits': actual['query_bits'],
        'query_rounding': actual['query_rounding'],
        'query_bytes': actual['query_bytes'],
        'certified_failure_bits': actual['certified_failure_bits'],
        'meets_128': actual['meets_128'],
        'max_deterministic_error': actual['max_deterministic_error'],
    }
    screen = certificate.get('query_screen') or []
    for bits, rounding in [(49, 'nearest'), (44, 'dithered')]:
        entry = next((v for v in screen
                      if v['query_bits'] == bits and v['query_rounding'] == rounding), None)
        if entry is not None:
            row[f'screen_{bits}_{rounding}_bits'] = entry['certified_failure_bits']
    rows.append(row)
json.dump(rows, sys.stdout, indent=1)
print()
