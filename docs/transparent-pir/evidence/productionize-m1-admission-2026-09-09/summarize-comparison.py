"""Derive query retry and completion-gap evidence without excluding failed attempts."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
rows = []
for row in json.loads((root / 'qualification/summary.json').read_text())['runs']:
    report = json.loads((root / 'qualification' / row['report']).read_text())
    queries = report['queries']
    gaps = []
    for client in range(2):
        completed = sorted(q['finished_seconds'] for q in queries
                           if q['client'] == client and q.get('query', {}).get('exact'))
        gaps.append(max((b - a for a, b in zip(completed, completed[1:])), default=None))
    rows.append({**row, 'cold_prewarm_seconds': report['cold_prewarm_seconds'],
                 'retry_count': sum(q.get('retry', False) for q in queries),
                 'max_success_gap_by_client_seconds': gaps})
print(json.dumps({'scope': 'Worker-stage screening; no fleet acceptance. Successful-query latency excludes overload/retry delay; completion gaps and retry counts retain it.',
                  'runs': rows}, indent=2))
