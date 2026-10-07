#!/usr/bin/env python3
"""Size a txid display backfill below height 3,407,001 from published maps.

Inputs, all read-only:
- inputs/history-shards.json, inputs/txid-shards.json and inputs/txid-manifest-*.json,
  captured by capture.py from the public routes;
- the v9 served map, the v10 pre-cutover map and the v3 full-chain publication
  occupancy already retained in this repository;
- the 2026-09-08 census's events-per-block density (4,096-block buckets).

Every history shard records the distinct txids of its journal events, which is
every transaction with a non-OP_RETURN transparent output or a non-coinbase
transparent input. That is the display set minus transactions whose only
transparent part is a coinbase input or an OP_RETURN/empty output, so it is a
lower bound for display records; calibration against the live display map
over the heights both cover follows.

A height range rarely aligns with shard boundaries. Per map, the lower bound
counts shards inside the range, the upper bound every shard touching it, and
the estimate splits a straddling shard by the census's event density. The
maps have different boundaries, so the tightest bounds across them are kept.

Writes sizing.json; prints a summary. No network access.
"""
import json
import math
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EVIDENCE = HERE.parent

# Derived constants, each traced in the README.
RUNTIME_RESERVED = 75_545_144  # SharedParams::reserved_bytes for a txid-2k table
RUNTIME_MIN = 8_388_608 + 14_848 + 16 + 33_587_240  # four-byte compiled matrix
TABLE_BYTES = 2_048 * 4_096  # one segment of one table, on disk
ARCHIVE_TARGET = 40_000
RECENT_FLOOR = 10_000
INGEST_BLOCKS_PER_S = (3_500_738 - 324_000) / (1_790_825_427.6743314 - 1_790_806_460.4975498)
BENCH_PREPARE_S = (3.9, 6.6)  # two-table prepare p50, 2 CPU and 1 CPU (2026-10-05 bench)
BENCH_PUBLISH_S = 0.9  # publisher build+verify+digest+write per shard, upper (2026-10-05 bench)
COLD_INLINE_FORMULA = 119_700  # cold inline lookup, formula with a 25-entry map at 600 B
FORMULA_MAP_BYTES = 25 * 600
WARM_INLINE = 92_600

V3_EVENT_BYTES = 42_684_252_745  # full v3 journal committed event bytes through 3,500,738
DISPLAY_START = 3_407_001
CANDIDATES = [
    ('sapling', 419_200),
    ('nu5', 1_687_104),
    ('2.0M', 2_000_000),
    ('2.5M', 2_500_000),
    ('3.0M', 3_000_000),
]


def load_map(path):
    shards = json.loads(Path(path).read_bytes())['shards']
    return [(s['start_height'], s['end_height'], s['txids']) for s in shards]


def load_occupancy(path):
    """The v3 full-chain publication: shard blocks are contiguous from height 0."""
    data = json.loads(Path(path).read_bytes())
    out, start = [], 0
    for entry in data['allocation']['occupancy']:
        occupancy = entry['occupancy']
        end = start + occupancy['blocks'] - 1
        out.append((start, end, occupancy['txids'], occupancy['events']))
        start = end + 1
    return out


def load_density(path):
    """Events per height from the census density table, as cumulative sums."""
    buckets = []
    for line in Path(path).read_text().splitlines():
        match = re.fullmatch(r'\s*(\d+)-(\d+)\s+([\d.]+) events/block', line)
        if match:
            buckets.append((int(match[1]), int(match[2]), float(match[3])))
    return buckets


class Weights:
    """Cumulative event weight by height; uniform per block past the census."""

    def __init__(self, buckets):
        self.buckets = buckets
        self.end = buckets[-1][1]

    def mass(self, a, b):
        if b < a:
            return 0.0
        total = 0.0
        for lo, hi, rate in self.buckets:
            lo2, hi2 = max(lo, a), min(hi, b)
            if lo2 <= hi2:
                total += (hi2 - lo2 + 1) * rate
        if b > self.end:
            # Past the census: use the last bucket's rate, i.e. uniform blocks.
            total += (b - max(a, self.end + 1) + 1) * self.buckets[-1][2]
        return total


def bracket(shards, a, b, weights):
    lower = upper = 0
    estimate = 0.0
    for start, end, txids, *_ in shards:
        if end < a or start > b:
            continue
        upper += txids
        if a <= start and end <= b:
            lower += txids
            estimate += txids
        else:
            whole = weights.mass(start, end)
            part = weights.mass(max(a, start), min(b, end))
            estimate += txids * (part / whole if whole else (min(b, end) - max(a, start) + 1) / (end - start + 1))
    covered = shards[0][0] <= a and shards[-1][1] >= b
    return {'lower': lower, 'upper': upper, 'estimate': round(estimate), 'covers': covered}


def combine(maps, a, b, weights):
    per = {name: bracket(shards, a, b, weights) for name, shards in maps.items()}
    usable = [r for r in per.values() if r['covers']]
    lower = max(r['lower'] for r in usable)
    upper = min(r['upper'] for r in usable)
    estimates = sorted(min(max(r['estimate'], lower), upper) for r in usable)
    estimate = estimates[len(estimates) // 2]
    return {'from': a, 'through': b, 'blocks': b - a + 1, 'history_txids_lower': lower,
            'history_txids_upper': upper, 'history_txids_estimate': estimate,
            'per_block': round(estimate / (b - a + 1), 2), 'per_map': per}


def main():
    display_map = json.loads((HERE / 'inputs/txid-shards.json').read_bytes())
    tip = display_map['shards'][-1]['end_height']
    manifests = [json.loads(p.read_bytes()) for p in sorted((HERE / 'inputs').glob('txid-manifest-*.json'))]
    history_live = load_map(HERE / 'inputs/history-shards.json')
    assert history_live[-1][1] == tip, 'history and display maps were captured at different tips'
    maps = {
        'live-v11-2026-10-07': history_live,
        'v9-served-2026-09-28': load_map(EVIDENCE / 'v9-cutover-2026-09-28/served-map.json'),
        'v10-before-2026-09-28': load_map(EVIDENCE / 'v10-cutover-2026-09-28/load/before-map.json'),
        'v3-full-publication-2026-09-30': load_occupancy(
            EVIDENCE / 'activity-metadata-2026-09-30/full-publication-terminal.json'),
    }
    weights = Weights(load_density(EVIDENCE / 'census-2026-09-08/census.txt'))

    # Display records: exact over the live display map.
    display_records = sum(s['records'] for s in display_map['shards'])
    sealed = [s for s in display_map['shards'] if s['sealed']]
    recent = display_map['shards'][-1]
    calibration = combine(maps, DISPLAY_START, tip, weights)
    ratio = display_records / calibration['history_txids_estimate']
    payload = sum(m['payload_bytes'] for m in manifests)
    records = sum(m['records'] for m in manifests)
    map_raw = (HERE / 'inputs/txid-shards.json').stat().st_size
    map_entry_bytes = map_raw / len(display_map['shards'])

    bands = []
    edges = [h for _, h in CANDIDATES] + [DISPLAY_START]
    for lo, hi in zip(edges, edges[1:]):
        bands.append(combine(maps, lo, hi - 1, weights))
    bands.append({'from': DISPLAY_START, 'through': tip, 'blocks': tip - DISPLAY_START + 1,
                  'display_records_exact': display_records,
                  'history_txids_estimate': calibration['history_txids_estimate'],
                  'per_block': round(display_records / (tip - DISPLAY_START + 1), 2)})

    mean_archive = sum(s['records'] for s in sealed) / len(sealed)
    archive_days = (sealed[-1]['end_height'] - DISPLAY_START + 1) / len(sealed) * 75 / 86_400
    occupancy = maps['v3-full-publication-2026-09-30']
    v3_events = sum(o[3] for o in occupancy)
    bytes_per_event = V3_EVENT_BYTES / v3_events

    def journal_events(start):
        total = 0.0
        for lo, hi, _, events in occupancy:
            if hi < start:
                continue
            whole, part = weights.mass(lo, hi), weights.mass(max(lo, start), hi)
            total += events * (part / whole)
        last = occupancy[-1]
        total += (tip - last[1]) * last[3] / (last[1] - last[0] + 1)
        return total

    def floor_for(archives):
        """The lowest height whose range to the tip holds `archives` sealed archives
        plus a recent shard of the current size, by the history estimate."""
        want = archives * mean_archive + recent['records']
        lo, hi = 1, tip
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if combine(maps, mid, tip, weights)['history_txids_estimate'] * ratio >= want:
                lo = mid
            else:
                hi = mid - 1
        return lo

    per_archive_held = 2 * RUNTIME_RESERVED
    budgets = []
    for gib in (4, 8, 12, 16, 20):
        cache = gib * 2**30
        # The cache holds the published archives and one staged seal, with the
        # 10% margin that turns today's 4 GiB into its 24-archive window. Work
        # memory (one two-table build at 4x a table, two queries at 2x each) is
        # admitted against 0.9 x MemoryMax on top of the cache.
        window = int(0.9 * cache // per_archive_held) - 1
        work = 4 * RUNTIME_RESERVED + 2 * 2 * RUNTIME_RESERVED
        budgets.append({'cache_gib': gib, 'max_archive_shards': window,
                        'memory_max_gib': math.ceil((cache + work) / 0.9 / 2**30),
                        'floor_today': floor_for(window),
                        'coverage_days': round(window * archive_days)})
    options = []
    for name, start in CANDIDATES:
        backfill = combine(maps, start, DISPLAY_START - 1, weights)
        lo, est, hi = (backfill['history_txids_lower'], backfill['history_txids_estimate'],
                       backfill['history_txids_upper'])
        total = est + display_records
        # Sealed archives at the cutover tip: everything but a recent shard of
        # between the floor and about one target.
        archives = round((total - (RECENT_FLOOR + ARCHIVE_TARGET) / 2) / mean_archive)
        archives_range = [math.floor((lo + display_records - ARCHIVE_TARGET - RECENT_FLOOR) / mean_archive),
                          math.ceil((min(hi, est * 1.05) + display_records - RECENT_FLOOR) / mean_archive)]
        blocks = tip - start + 2
        reserved = archives * 2 * RUNTIME_RESERVED
        options.append({
            'name': name, 'start_height': start, 'journal_start_height': start - 1,
            'backfill_blocks': DISPLAY_START - start,
            'backfill_history_txids': {'lower': lo, 'estimate': est, 'upper': hi},
            'total_display_records_estimate': total,
            'sealed_archives_at_cutover': archives,
            'sealed_archives_range': archives_range,
            'archive_runtimes': 2 * archives,
            'reserved_bytes': reserved,
            'reserved_gib': round(reserved / 2**30, 2),
            'min_resident_bytes': archives * 2 * RUNTIME_MIN,
            'archive_disk_bytes': archives * (2 * TABLE_BYTES + 2 * RUNTIME_RESERVED),
            'publication_root_bytes': archives * 2 * TABLE_BYTES,
            'journal_event_bytes': round(journal_events(start) * bytes_per_event),
            'journal_blocks': blocks,
            'sidecar_payload_bytes': round(total * payload / records),
            'sidecar_min_allocation_bytes': blocks * 4096,
            'ingest_hours_at_history_rate': round(blocks / INGEST_BLOCKS_PER_S / 3600, 2),
            'bootstrap_publish_minutes': round(archives * BENCH_PUBLISH_S / 60, 1),
            'worker_prepare_minutes': [round(archives * s / 60, 1) for s in BENCH_PREPARE_S],
            'map_bytes_raw': round((archives + 1) * map_entry_bytes),
            'cold_inline_lookup_bytes': round(COLD_INLINE_FORMULA - FORMULA_MAP_BYTES
                                              + (archives + 1) * map_entry_bytes),
        })

    result = {
        'tip': tip,
        'display': {
            'start_height': display_map['start_height'], 'sealed_archives': len(sealed),
            'records': display_records, 'recent_records': recent['records'],
            'mean_archive_records': round(mean_archive, 1),
            'payload_bytes_per_record': round(payload / records, 1),
            'max_page_rows_used': max(m['page_rows_used'] for m in manifests),
            'page_rows_per_segment': 2048,
            'directory_segments': sorted({len(m['buckets'][0]['directory_segments']) for m in manifests}),
            'page_segments': sorted({len(m['page_segments']) for m in manifests}),
            'map_bytes': map_raw, 'map_entry_bytes': round(map_entry_bytes),
            'days_per_archive_at_75s_blocks': round(archive_days, 2),
        },
        'calibration': {
            'range': [DISPLAY_START, tip], 'display_records_exact': display_records,
            'history_txids': {k: calibration[k] for k in
                              ('history_txids_lower', 'history_txids_estimate', 'history_txids_upper')},
            'display_over_history_estimate': round(ratio, 4),
        },
        'bands': bands,
        'budgets': budgets,
        'journal': {'v3_events_through_3500738': v3_events, 'v3_event_bytes': V3_EVENT_BYTES,
                    'bytes_per_event': round(bytes_per_event, 1)},
        'options': options,
        'constants': {
            'runtime_reserved_bytes': RUNTIME_RESERVED, 'runtime_min_bytes': RUNTIME_MIN,
            'table_segment_bytes': TABLE_BYTES, 'ingest_blocks_per_s': round(INGEST_BLOCKS_PER_S, 1),
            'bench_prepare_s': BENCH_PREPARE_S, 'bench_publish_s': BENCH_PUBLISH_S,
            'cold_inline_formula_bytes': COLD_INLINE_FORMULA, 'formula_map_bytes': FORMULA_MAP_BYTES,
            'warm_inline_bytes': WARM_INLINE,
        },
    }
    (HERE / 'sizing.json').write_text(json.dumps(result, indent=1) + '\n')

    print('tip %d; display %d records in %d archives + recent %d; display/history %.4f'
          % (tip, display_records, len(sealed), recent['records'], ratio))
    for band in bands:
        if 'display_records_exact' in band:
            print('%9d-%9d %8d blocks  display %9d exact (%.2f/block)' % (
                band['from'], band['through'], band['blocks'], band['display_records_exact'], band['per_block']))
        else:
            print('%9d-%9d %8d blocks  history txids %9d [%d, %d] (%.2f/block)' % (
                band['from'], band['through'], band['blocks'], band['history_txids_estimate'],
                band['history_txids_lower'], band['history_txids_upper'], band['per_block']))
    for o in options:
        print('%-7s start %9d backfill %9d [%d, %d]; archives %d %s; reserved %.2f GiB; '
              'disk %.1f GB; journal events %.1f GB; ingest %.1f h; map %d B; cold inline %d B' % (
                  o['name'], o['start_height'], o['backfill_history_txids']['estimate'],
                  o['backfill_history_txids']['lower'], o['backfill_history_txids']['upper'],
                  o['sealed_archives_at_cutover'], o['sealed_archives_range'], o['reserved_gib'],
                  o['archive_disk_bytes'] / 1e9, o['journal_event_bytes'] / 1e9,
                  o['ingest_hours_at_history_rate'], o['map_bytes_raw'], o['cold_inline_lookup_bytes']))
    for b in budgets:
        print('cache %2d GiB (MemoryMax %2dG): window %3d archives, floor today %d, about %d days of coverage'
              % (b['cache_gib'], b['memory_max_gib'], b['max_archive_shards'], b['floor_today'],
                 b['coverage_days']))
    return 0


if __name__ == '__main__':
    sys.exit(main())
