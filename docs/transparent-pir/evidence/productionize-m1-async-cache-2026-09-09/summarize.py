#!/usr/bin/env python3
"""Rebuild the stage and query-gap summary from this experiment's raw reports."""
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
rows = []
previous = ROOT.parent / 'productionize-m1-construction-2026-09-09'
variants = [('baseline', previous / 'baseline'), ('portable', ROOT.parent / 'productionize-m1-target-cpu-2026-09-09/portable'), ('focused', ROOT / 'focused'), ('qualification', ROOT / 'qualification')]
for variant, directory in variants:
    if not (directory / 'summary.json').exists():
        continue
    for result in json.loads((directory / 'summary.json').read_text())['runs']:
        report = json.loads((directory / result['report']).read_text())
        log = (directory / (result['name'] + '.log')).read_text()
        log = re.sub(r'\x1b\[[0-9;]*m', '', log)
        stages = {}
        for line in log.splitlines():
            match = re.search(r'seconds=([0-9.e+-]+).*stage="([^"]+)"', line)
            if match:
                stages.setdefault(match[2], []).append(float(match[1]))
        # Complete startup builds 28 runtimes; the two changed tables in each
        # of two publications account for the final four construction samples.
        measured = {k: v[-4:] for k, v in stages.items() if k in
                    ('encode_database', 'public_setup', 'hint_columns',
                     'pack_preprocessing', 'publish_parameters', 'runtime_build', 'disk_save')}
        client_logs = sorted((directory / (result['name'] + '-clients')).glob('client-*.jsonl'))
        queries = [json.loads(line) for path in client_logs for line in path.read_text().splitlines()] if client_logs else report['queries']
        result = dict(result)
        result['reported_exact_query_count'] = result['exact_query_count']
        result['exact_query_count'] = sum(bool(q.get('query', {}).get('exact')) for q in queries)
        result['fatal_client_errors'] = sum(bool(q.get('error')) and not q.get('retry', False) for q in queries)
        gaps = []
        for client in range(report['query_clients']):
            finished = sorted(q['finished_seconds'] for q in queries
                              if q['client'] == client and q.get('query', {}).get('exact'))
            gaps.extend(b-a for a, b in zip(finished, finished[1:]))
        retries = sum(bool(q.get('retry')) for q in queries)
        memory = dict(line.split() for line in report['kernel_memory']['memory.stat'].splitlines())
        rows.append(dict(variant=variant, **result,
                         cold_seconds=report['cold_prewarm_seconds'], final_anon_bytes=int(memory['anon']), final_file_bytes=int(memory['file']),
                         retries=retries, retry_attempt_fraction=retries / (retries + result['exact_query_count']),
                         max_completion_gap_seconds=max(gaps, default=None),
                         stages_seconds=measured,
                         mean_stage_seconds={k: statistics.mean(v) for k, v in measured.items()}))
baseline = next(r for r in rows if r['variant'] == 'baseline')
for r in rows:
    r['query_availability_screen_passed'] = (
        r['retry_attempt_fraction'] <= baseline['retry_attempt_fraction'] and
        r['max_completion_gap_seconds'] <= baseline['max_completion_gap_seconds'] and r['fatal_client_errors'] == 0)
    r['combined_screen_passed'] = r['exit_code'] == 0 and r.get('client_exit_code') == 0 and r.get('client_shutdown_complete', False) and all(r.get(k) for k in (
        'worker_budget_passed', 'query_availability_screen_passed',
        'memory_qualification_passed', 'cold_warm', 'exact_queries', 'client_isolation_passed',
        'completed', 'load_overlapped_burst', 'cold_persistence_complete', 'persistence_complete'))
(ROOT / 'comparison.json').write_text(json.dumps(rows, indent=2) + '\n')
for r in rows:
    print(r['variant'], r['name'],
          'visibility', round(r['max_worker_visibility_seconds'], 3),
          'exact', r['exact_query_count'], 'retries', r['retries'],
          'gap', round(r['max_completion_gap_seconds'], 3),
          'pack', round(r['mean_stage_seconds']['pack_preprocessing'], 3),
          'save', round(r['mean_stage_seconds']['disk_save'], 3))
