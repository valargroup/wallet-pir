#!/usr/bin/env python3
"""Summarize one local end-to-end run directory as JSON.

Reads `rate.jsonl[.gz]`, `timeline.jsonl[.gz]`, `verify.json`, `audit.json`,
`census.json` and `bandwidth.json` from RUN_DIR. Percentiles are nearest-rank
lower (`sorted[int((n - 1) * p)]`), as `analyze-gate.py`. Failed lookups stay
in the denominators; torn last lines are skipped.

    analyze.py RUN_DIR [--out summary.json]
"""
import argparse
import collections
import gzip
import json
from pathlib import Path


def lines(path):
    path = Path(path)
    if not path.exists():
        path = path.with_name(path.name + '.gz')
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as f:
        for line in f:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def pct(values, p):
    values = sorted(values)
    return values[int((len(values) - 1) * p)] if values else None


def dist(values):
    values = list(values)
    return {'n': len(values), 'p50': pct(values, 0.5), 'p99': pct(values, 0.99), 'max': max(values, default=None)}


def load(run):
    events = list(lines(run / 'rate.jsonl'))
    counts = collections.Counter(e['event'] for e in events)
    lookups = [e for e in events if e['event'] == 'lookup']
    queries = [e for e in events if e['event'] == 'query']
    errors = [e for e in events if e['event'] == 'error']
    by = collections.defaultdict(list)
    for e in lookups:
        tier = 'archive' if e.get('sealed') else 'recent'
        by[(tier, 'cold' if e.get('cold') else 'warm', e.get('class') or e.get('fixture_class'))].append(
            e['total_seconds'])
    span = (events[-1]['unix'] - events[0]['unix']) if events else 0
    return {
        'events': dict(counts),
        'span_seconds': round(span, 1),
        'lookups_per_second': round(len(lookups) / span, 2) if span else None,
        'exact': sum(1 for e in lookups if e.get('exact')),
        'not_exact': sum(1 for e in lookups if not e.get('exact')),
        'outcomes': dict(collections.Counter(e['outcome'] for e in lookups)),
        'errors': dict(collections.Counter(str(e.get('status') or e.get('error'))[:80] for e in errors)),
        'missed_slots': dict(collections.Counter(e.get('reason') for e in events if e['event'] == 'missed_slot')),
        'stale_retries': sum(e.get('stale_retries', 0) for e in lookups),
        'lookup_seconds': dist(e['total_seconds'] for e in lookups),
        'lookup_seconds_warm': dist(e['total_seconds'] for e in lookups if not e.get('cold')),
        'lookup_seconds_cold': dist(e['total_seconds'] for e in lookups if e.get('cold')),
        'query_http_seconds': dist(e['http_seconds'] for e in queries),
        'lookup_seconds_by_tier_state_class': {'/'.join(k): dist(v) for k, v in sorted(by.items())},
    }


def timeline(run):
    events = list(lines(run / 'timeline.jsonl'))
    kinds = collections.Counter(e['kind'] for e in events)
    cycles = [e for e in events if e['kind'] == 'cycle']
    plain = [c for c in cycles if not c.get('sealed_published') and not c.get('dropped')]
    sealing = [c for c in cycles if c.get('sealed_published')]
    return {
        'kinds': dict(kinds),
        'cycle_ms': dist(c['cycle_ms'] for c in plain),
        'cycle_ms_with_seal': dist(c['cycle_ms'] for c in sealing),
        'prepare_ms': dist(c['prepare_ms'] for c in plain),
        'publish_s': dist(round(c['build_s'] + c['verify_s'] + c['digest_s'] + c['write_s'], 3) for c in plain),
        'recent_records': dist(c['recent']['records'] for c in cycles),
        'recent_dir_segments': sorted({tuple(c['recent']['dir_segments']) for c in cycles}),
        'recent_page_segments': sorted({c['recent']['page_segments'] for c in cycles}),
        'seals': [{k: e.get(k) for k in ('shard_id', 'start', 'end', 'digest', 'bucket_counts', 'bootstrap',
                                         'decided_tip')} for e in events if e['kind'] == 'seal'],
        'drops': [{k: e.get(k) for k in ('shard_id', 'start', 'end', 'digest', 'cycle')}
                  for e in events if e['kind'] == 'drop'],
        'errors': [e for e in events if e['kind'] == 'error'],
    }


def census(run):
    data = json.load(open(run / 'census.json'))
    summary = data['summary']
    out = {k: v for k, v in summary.items() if k != 'classes_below_floor'}
    below = summary.get('classes_below_floor', [])
    out['classes_below_floor'] = len(below)
    out['smallest_class'] = min(below, key=lambda c: c['txids'], default=None)
    out['inline_if'] = data.get('payload', {}).get('inline_if')
    return out


def bandwidth(run):
    data = json.load(open(run / 'bandwidth.json'))
    worst = collections.defaultdict(int)
    for row in data['rows']:
        key = '%s/%s' % (row['state'], row['class'])
        worst[key] = max(worst[key], row['measured_total'])
    return {
        'all_measured_equal_computed': all(r['equal'] for r in data['rows']),
        'plain_http': data.get('plain_http'),
        'max_measured_total_by_state_class': dict(sorted(worst.items(), key=lambda kv: (
            kv[0].split('/')[0], int(kv[0].split('-')[-1]) if '-' in kv[0] else 0))),
        'summary': data.get('summary'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run', type=Path)
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    run = args.run
    summary = {
        'verify': json.load(open(run / 'verify.json')),
        'audit': {k: v for k, v in json.load(open(run / 'audit.json')).items() if k in ('violations', 'maps')},
        'census': census(run),
        'load': load(run),
        'timeline': timeline(run),
        'bandwidth': bandwidth(run),
    }
    summary['audit']['violations'] = len(summary['audit'].get('violations') or [])
    text = json.dumps(summary, indent=1, sort_keys=True)
    if args.out:
        args.out.write_text(text + '\n')
    print(text)


if __name__ == '__main__':
    main()
