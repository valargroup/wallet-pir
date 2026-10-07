#!/usr/bin/env python3
"""Size a txid display publication from genesis, with host and map options.

Extends sizing.py (whose sizing.json stays as committed) after Roman chose a
genesis floor. Reuses its inputs and bracketing, adds:

- a start at height 1 (the display start must lie above the journal start,
  whose block is its parent, so height 0 is the parent, not a record);
- per-runtime memory measured by `shard-residency` (residency/*.txt) beside
  the 75.5 MB reservation;
- geometry variants for the archives below 3,407,001;
- host fit, map bytes under three serving designs, and ingest needs.

Writes genesis.json; prints a summary. No network access.
"""
import gzip
import json
import math
import random
import sys
from pathlib import Path

import sizing as s

HERE = Path(__file__).resolve().parent
GENESIS_START = 1
BLOSSOM = 653_600  # block time 150 s -> 75 s; the pre-2020 era boundary used here
DISPLAY_START = s.DISPLAY_START

# Runtime bytes per table segment. Reserved: SharedParams::reserved_bytes.
# Floor: the same formula with the compiled matrix at four-byte words, which the
# residency runs reproduce (steady-state increments, see the README).
RUNTIME = {
    'txid-2k': {'reserved': 75_545_144, 'floor': 41_990_712, 'rows': 2_048},
    'txid-4k': {'reserved': 83_933_752, 'floor': 50_379_320, 'rows': 4_096},
}
QUERY_UP = {'txid-2k': 40_200, 'txid-4k': 52_744}  # 8 + request_len(rows)
QUERY_DOWN = 5_648  # 16 + response_len(2,048 columns), one segment
HEADERS = 400  # per request, both directions, as the bandwidth formula
YEAR_ARCHIVES = 365 / 6.25  # seals per year at the live rate
HOSTS = [
    # (name, GiB of RAM, SSD GB, USD per month: DigitalOcean list, verify before approval)
    ('archive-03 (shared with history)', None, None, 0),
    ('m-4vcpu-32gb', 32, 100, 168),
    ('m-8vcpu-64gb', 64, 200, 336),
    ('m-16vcpu-128gb', 128, 400, 672),
]
ARCHIVE03_DISPLAY_MEMORY_MAX_GIB = 14  # largest that keeps the 20% floor (deployment.md)


def lookup_bytes(geometry, pages):
    queries = 2 + pages
    return queries * (QUERY_UP[geometry] + QUERY_DOWN + HEADERS)


def cache_for(window, per_archive):
    """sizing.py's window rule inverted: cache that holds `window` archives."""
    return (window + 1) * per_archive / 0.9


def memory_max_for(cache, runtime):
    return (cache + 4 * runtime + 2 * 2 * runtime) / 0.9


def host_usable(gib):
    """Display MemoryMax a dedicated host allows: 20% floor, 1.5 GiB for the OS."""
    return gib * 0.8 - 1.5


def synthetic_map(entries, template):
    """A map of `entries` archives in the live map's shape, random hashes."""
    rng = random.Random(7)
    shards = []
    for shard_id in range(entries):
        entry = dict(template)
        entry['shard_id'] = shard_id
        entry['start_height'] = 1 + shard_id * 8_300
        entry['end_height'] = entry['start_height'] + 8_299
        for key in ('parent_block_hash', 'terminal_block_hash', 'manifest_digest'):
            entry[key] = '%064x' % rng.getrandbits(256)
        entry['records'] = entry['min_bucket_records'] = 40_000 + rng.randrange(20)
        shards.append(entry)
    return shards


def main():
    display_map = json.loads((HERE / 'inputs/txid-shards.json').read_bytes())
    tip = display_map['shards'][-1]['end_height']
    maps = {
        'live-v11-2026-10-07': s.load_map(HERE / 'inputs/history-shards.json'),
        'v9-served-2026-09-28': s.load_map(s.EVIDENCE / 'v9-cutover-2026-09-28/served-map.json'),
        'v10-before-2026-09-28': s.load_map(s.EVIDENCE / 'v10-cutover-2026-09-28/load/before-map.json'),
        'v3-full-publication-2026-09-30': s.load_occupancy(
            s.EVIDENCE / 'activity-metadata-2026-09-30/full-publication-terminal.json'),
    }
    weights = s.Weights(s.load_density(s.EVIDENCE / 'census-2026-09-08/census.txt'))
    display_records = sum(x['records'] for x in display_map['shards'])
    sealed = [x for x in display_map['shards'] if x['sealed']]
    mean_archive = sum(x['records'] for x in sealed) / len(sealed)
    manifests = [json.loads(p.read_bytes()) for p in sorted((HERE / 'inputs').glob('txid-manifest-*.json'))]
    payload_per_record = sum(m['payload_bytes'] for m in manifests) / sum(m['records'] for m in manifests)

    band_pre_sapling = s.combine(maps, GENESIS_START, 419_199, weights)
    pre_blossom = s.combine(maps, GENESIS_START, BLOSSOM - 1, weights)
    backfill = s.combine(maps, GENESIS_START, DISPLAY_START - 1, weights)
    whole_history = s.combine(maps, 0, tip, weights)
    est = backfill['history_txids_estimate']
    lo, hi = backfill['history_txids_lower'], backfill['history_txids_upper']
    total = est + display_records
    live_era = display_records  # stays txid-2k at T = 40,000 in every variant

    # Archives at cutover per variant: (geometry below DISPLAY_START, its target,
    # the boundary below which that geometry applies).
    variants = {
        'A txid-2k everywhere': ('txid-2k', 40_000, DISPLAY_START),
        'B txid-4k at 80,000 below 3,407,001': ('txid-4k', 80_000, DISPLAY_START),
        'C txid-4k at 80,000 below Blossom only': ('txid-4k', 80_000, BLOSSOM),
    }
    rows = []
    for name, (geometry, target, boundary) in variants.items():
        old = s.combine(maps, GENESIS_START, boundary - 1, weights)['history_txids_estimate'] if boundary > 1 else 0
        mid = total - old - live_era  # txid-2k records between the boundary and 3,407,001
        old_archives = round(old / (target + mean_archive - 40_000))
        new_archives = round((mid + live_era - (s.RECENT_FLOOR + s.ARCHIVE_TARGET) / 2) / mean_archive)
        for accounting in ('reserved', 'floor'):
            per_old = 2 * RUNTIME[geometry][accounting]
            per_new = 2 * RUNTIME['txid-2k'][accounting]
            held = old_archives * per_old + new_archives * per_new
            growth = YEAR_ARCHIVES * per_new
            window = old_archives + new_archives + math.ceil(YEAR_ARCHIVES)
            cache = (held + growth + per_new) / 0.9
            memory_max = memory_max_for(cache, RUNTIME['txid-2k'][accounting])
            fits = []
            for host, gib, _, _ in HOSTS:
                usable = ARCHIVE03_DISPLAY_MEMORY_MAX_GIB if gib is None else host_usable(gib)
                fits.append({'host': host, 'usable_memory_max_gib': round(usable, 1),
                             'fits': memory_max / 2**30 <= usable})
            rows.append({
                'variant': name, 'accounting': accounting,
                'old_geometry': geometry, 'old_target': target, 'old_boundary': boundary,
                'old_archives': old_archives, 'txid_2k_archives': new_archives,
                'runtimes': 2 * (old_archives + new_archives),
                'held_gib': round(held / 2**30, 1),
                'growth_gib_per_year': round(growth / 2**30, 1),
                'window_with_one_year': window,
                'cache_gib_one_year': round(cache / 2**30, 1),
                'memory_max_gib_one_year': math.ceil(memory_max / 2**30),
                'disk_gb': round((old_archives * 2 * RUNTIME[geometry]['rows'] * 4096
                                  + new_archives * 2 * 2048 * 4096
                                  + old_archives * 2 * RUNTIME[geometry][accounting]
                                  + new_archives * 2 * RUNTIME['txid-2k'][accounting]) / 1e9, 1),
                'old_archive_lookup_bytes': {'inline': lookup_bytes(geometry, 0),
                                             'pages_3': lookup_bytes(geometry, 3),
                                             'pages_4': lookup_bytes(geometry, 4)},
                'host_fit': fits,
            })

    # Map bytes: the live map's entry shape at the variant A count.
    entries = rows[0]['old_archives'] + rows[0]['txid_2k_archives'] + 1
    template = dict(display_map['shards'][0])
    full = dict(display_map)
    full['shards'] = synthetic_map(entries, template)
    raw = json.dumps(full, indent=2).encode()
    live_raw = (HERE / 'inputs/txid-shards.json').read_bytes()
    chunk = dict(full)
    chunk['shards'] = full['shards'][:32]
    chunk_raw = json.dumps(chunk, indent=2).encode()
    recent_only = dict(full)
    recent_only['shards'] = full['shards'][-1:]
    recent_raw = json.dumps(recent_only, indent=2).encode() + b' ' * 200  # plus an index pointer
    gz = lambda b: len(gzip.compress(b, 6))
    formula_base = s.COLD_INLINE_FORMULA - s.FORMULA_MAP_BYTES
    cold_4p = 325_400 - s.FORMULA_MAP_BYTES
    map_designs = {
        'one map, uncompressed (today)': {'cold_map_bytes': len(raw), 'refetch_bytes': len(raw)},
        'one map, gzip at the router': {'cold_map_bytes': gz(raw), 'refetch_bytes': gz(raw)},
        'recent map + immutable 32-entry index chunks, gzip': {
            'cold_map_bytes': gz(recent_raw) + gz(chunk_raw), 'refetch_bytes': gz(recent_raw)},
    }
    for design in map_designs.values():
        design['cold_inline_lookup'] = formula_base + design['cold_map_bytes']
        design['cold_4_page_lookup'] = cold_4p + design['cold_map_bytes']

    blocks = tip + 1  # heights 0..tip: the journal starts at genesis, the display at 1
    occupancy_events = sum(o[3] for o in maps['v3-full-publication-2026-09-30'])
    last = maps['v3-full-publication-2026-09-30'][-1]
    events = occupancy_events + (tip - last[1]) * last[3] / (last[1] - last[0] + 1)
    ingest = {
        'journal_start_height': 0, 'display_start_height': GENESIS_START, 'blocks': blocks,
        'event_bytes': round(events * s.V3_EVENT_BYTES / occupancy_events),
        'sidecar_files': blocks, 'sidecar_min_allocation_bytes': blocks * 4096,
        'sidecar_payload_bytes': round(total * payload_per_record),
        'fsyncs': 3 * blocks,
        'hours_at_history_rate': round(blocks / s.INGEST_BLOCKS_PER_S / 3600, 2),
        'publication_root_bytes': (rows[0]['old_archives'] + rows[0]['txid_2k_archives']) * 2 * s.TABLE_BYTES,
    }

    result = {
        'tip': tip,
        'counts': {
            'pre_sapling_1_419199': {k: band_pre_sapling[k] for k in
                                     ('history_txids_lower', 'history_txids_estimate', 'history_txids_upper')},
            'pre_blossom_1_653599': pre_blossom['history_txids_estimate'],
            'backfill_1_3407000': {'lower': lo, 'estimate': est, 'upper': hi},
            'display_3407001_tip_exact': display_records,
            'total_display_records_estimate': total,
            'history_txids_0_tip_estimate': whole_history['history_txids_estimate'],
        },
        'variants': rows,
        'map': {'entries': entries, 'live_bytes': len(live_raw), 'live_gzip': gz(live_raw),
                'full_raw': len(raw), 'full_gzip': gz(raw), 'chunk_32_raw': len(chunk_raw),
                'chunk_32_gzip': gz(chunk_raw), 'recent_map_raw': len(recent_raw),
                'recent_map_gzip': gz(recent_raw), 'designs': map_designs},
        'ingest': ingest,
        'constants': {'runtime': RUNTIME, 'query_up': QUERY_UP, 'query_down': QUERY_DOWN,
                      'headers': HEADERS, 'year_archives': round(YEAR_ARCHIVES, 1),
                      'hosts': HOSTS, 'archive03_display_memory_max_gib': ARCHIVE03_DISPLAY_MEMORY_MAX_GIB},
    }
    (HERE / 'genesis.json').write_text(json.dumps(result, indent=1) + '\n')

    c = result['counts']
    print('pre-Sapling 1-419,199: %d [%d, %d]; pre-Blossom %d; backfill 1-3,407,000: %d [%d, %d]; total %d'
          % (c['pre_sapling_1_419199']['history_txids_estimate'], c['pre_sapling_1_419199']['history_txids_lower'],
             c['pre_sapling_1_419199']['history_txids_upper'], c['pre_blossom_1_653599'], est, lo, hi, total))
    for r in rows:
        print('%-40s %-8s old %3d + 2k %3d archives; held %5.1f GiB (+%.1f/yr); one-year cache %5.1f GiB, '
              'MemoryMax %3dG; disk %5.1f GB; old inline %d B, 4 pages %d B; fits %s' % (
                  r['variant'], r['accounting'], r['old_archives'], r['txid_2k_archives'], r['held_gib'],
                  r['growth_gib_per_year'], r['cache_gib_one_year'], r['memory_max_gib_one_year'], r['disk_gb'],
                  r['old_archive_lookup_bytes']['inline'], r['old_archive_lookup_bytes']['pages_4'],
                  [f['host'] for f in r['host_fit'] if f['fits']]))
    m = result['map']
    print('map %d entries: raw %d, gzip %d; chunk raw %d gzip %d; recent raw %d gzip %d'
          % (m['entries'], m['full_raw'], m['full_gzip'], m['chunk_32_raw'], m['chunk_32_gzip'],
             m['recent_map_raw'], m['recent_map_gzip']))
    for name, d in m['designs'].items():
        print('  %-52s cold map %6d, refetch %6d, cold inline %6d, cold 4 pages %6d'
              % (name, d['cold_map_bytes'], d['refetch_bytes'], d['cold_inline_lookup'], d['cold_4_page_lookup']))
    print('ingest', ingest)
    return 0


if __name__ == '__main__':
    sys.exit(main())
