#!/usr/bin/env python3
"""Live block-to-serving and controller events from the display timeline.

  python3 harness/live_freshness.py live/timeline.jsonl.gz > summary.json

A cycle is live when its tip is above LIVE_AFTER_TIP, the height at which the
deploy session recorded that replay had reached live. Per cycle the statistic is
max(freshness_ms): the oldest block that cycle made servable. The window ends at
WINDOW_END_UTC, before the 2026-10-07 load windows and the recent-01 drop-in.
Percentiles use the same index rule as ../txid-display-freshness-2026-10-07.
"""
import datetime
import gzip
import json
import sys

LIVE_AFTER_TIP = 3508457
WINDOW_END_UTC = datetime.datetime(2026, 10, 7, 13, 40, tzinfo=datetime.timezone.utc)
OVER_MS = 20000


def q(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p * (len(values) - 1))))]


def utc(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'


def dist(values):
    return {'n': len(values), 'p50_ms': q(values, 0.5), 'p95_ms': q(values, 0.95), 'max_ms': max(values),
            f'over_{OVER_MS}_ms': sum(1 for v in values if v > OVER_MS)}


def main(path):
    rows = [json.loads(line) for line in gzip.open(path, 'rt')]
    end_ms = WINDOW_END_UTC.timestamp() * 1000
    cycles = [r for r in rows if r['kind'] == 'cycle']
    live = [r for r in cycles if r['tip'] > LIVE_AFTER_TIP]
    live_start = live[0]['unix_ms']
    window = [r for r in live if r['activated_ms'] <= end_ms]
    per_cycle = [max(r['freshness_ms']) for r in window]
    per_block = [v for r in window for v in r['freshness_ms']]
    worst = max(window, key=lambda r: max(r['freshness_ms']))

    def events(kind, lo=None, hi=None):
        out = []
        for r in rows:
            if r['kind'] != kind:
                continue
            if lo is not None and r['unix_ms'] < lo:
                continue
            if hi is not None and r['unix_ms'] > hi:
                continue
            out.append(r)
        return out

    def describe(r):
        keep = ('shard_id', 'start', 'end', 'bootstrap', 'decided_tip', 'depth', 'ancestor', 'old_tip', 'stage', 'error')
        return dict({'utc': utc(r['unix_ms'])}, **{k: r[k] for k in keep if k in r})

    summary = {
        'source': path,
        'timeline': {'records': len(rows), 'first_utc': utc(rows[0]['unix_ms']), 'last_utc': utc(rows[-1]['unix_ms']),
                     'cycles': len(cycles), 'blocks': sum(1 for r in rows if r['kind'] == 'block')},
        'definition': {'live': f'cycle tip > {LIVE_AFTER_TIP}', 'window_end_utc': WINDOW_END_UTC.isoformat(),
                       'per_cycle': 'max(freshness_ms)', 'percentile': 'sorted[round(p*(n-1))]'},
        'live_window': {'first_cycle': window[0]['cycle'], 'last_cycle': window[-1]['cycle'],
                        'first_activated_utc': utc(window[0]['activated_ms']),
                        'last_activated_utc': utc(window[-1]['activated_ms']),
                        'per_cycle': dist(per_cycle), 'per_block': dist(per_block),
                        'worst_cycle': {'cycle': worst['cycle'], 'activated_utc': utc(worst['activated_ms']),
                                        'tip': worst['tip'], 'freshness_ms': worst['freshness_ms'],
                                        'prepare_ms': worst['prepare_ms']}},
        'live_cycles_to_capture_end': len(live),
        'seals': {'total': len(events('seal')), 'bootstrap': sum(1 for r in events('seal') if r.get('bootstrap')),
                  'live': len(events('seal', lo=live_start)), 'records': [describe(r) for r in events('seal')]},
        'reorgs': {'total': len(events('reorg')), 'live': len(events('reorg', lo=live_start)),
                   'live_window': len(events('reorg', lo=live_start, hi=end_ms)),
                   'records': [describe(r) for r in events('reorg')]},
        'errors': {'total': len(events('error')), 'live_window': len(events('error', lo=live_start, hi=end_ms)),
                   'records': [describe(r) for r in events('error')]},
        'lag': {'total': len(events('lag')), 'live': len(events('lag', lo=live_start))},
        'dropped_archives': sum(len(r['dropped']) for r in cycles),
        'last_cycle': {'cycle': cycles[-1]['cycle'], 'activated_utc': utc(cycles[-1]['activated_ms']),
                       'tip': cycles[-1]['tip'], 'archives': cycles[-1]['archives'],
                       'recent_records': cycles[-1]['recent']['records']},
    }
    json.dump(summary, sys.stdout, indent=1)
    sys.stdout.write('\n')


if __name__ == '__main__':
    main(sys.argv[1])
