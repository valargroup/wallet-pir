#!/usr/bin/env python3
"""Price txid display layouts for genesis coverage from the journal census.

Reads census.json.gz (read-only) and writes layout.json; prints a summary.
Python 3 standard library only, no network.

Inputs taken from the census, all at the 128-byte inline cutoff (the codec's
INLINE_BYTES) and the `stored` size population (the census found no unknown
fees, so stored sizes are the exact sizes the builder would pack):

- per-archive page-row demand, as the `pages.demand` histograms per era: the
  multiset of 4,096-byte page rows each 40,000-record archive needs under the
  v1 packer, with archives that cross an activation height labelled
  `mixed-era`;
- the census's own shared-page allocations (`pages.layout_options`), exact
  for groups of k = 1, 4, 16 and 64 adjacent archives at 1,024 and 4,096 rows;
- record sizes per era (`domains.*.sizes.stored`) for page counts per lookup.

Derived arithmetic, not measurement. Runtime sizes follow
`SharedParams::held_bytes` (built, four-byte words) and `reserved_bytes`.
Lookup bytes follow `txid-bandwidth formula`. Host fit follows the genesis
sizing (`../txid-display-backfill-sizing-2026-10-07/genesis.py`).
"""
import gzip
import json
import math
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

ROW = 4_096
FRAGMENT = ROW - 4 - 2 - 40  # census FRAGMENT: payload bytes per page fragment
D = 2_048  # native row quantum: a table's rows are a multiple of this
# shared/pir-native: BLOCK_PUBLIC 14,848 + header 16 + prepared block.
HINT_HELD = 14_848 + 16 + 32 + 2 * D * 8 + 8 + D * D * 2 * 4  # 33,602,104
HINT_RESERVED = 14_848 + 16 + 32 + 2 * D * 8 + 8 + D * D * 2 * 8  # 67,156,536
KEY = 27_648  # uploaded packing key
DOWN = 16 + 5_632  # one segment's answer to one query
SETUP = 19_800 + 300  # one segment's setup document, base64 plus JSON
HEADERS = 400  # per request, both directions (formula default)
MANIFEST = 1_500
SPLIT_MAP_COLD = 4_817  # recent map + one chunk, gzip, 426 entries (status, split map)
MIB, GIB = 2**20, 2**30

ERAS = ['Sprout', 'Overwinter', 'Sapling', 'Blossom', 'Heartwood', 'Canopy', 'NU5',
        'NU6', 'NU6.1', 'NU6.2', 'NU6.3']
PRE_NU6 = ERAS[:7]
# Seal rate: NU6.3 records over its 80,530 blocks at 75 s (census), per year.
HOSTS = [('m-4vcpu-32gb', 32, 168), ('m-8vcpu-64gb', 64, 336), ('m-16vcpu-128gb', 128, 672)]


def held(rows):
    return rows * ROW + HINT_HELD


def reserved(rows):
    return rows * ROW + HINT_RESERVED


def up(rows):
    return 8 + KEY + math.ceil(rows * 49 / 8)


def segs(demand, rows):
    return max(1, math.ceil(demand / rows))


def hist_values(h):
    out = []
    for value, count in h['histogram']:
        out += [value] * count
    return out


def load():
    census = json.loads(gzip.decompress((HERE / 'census.json.gz').read_bytes()))
    demand = {}
    for x in census['pages']['demand']:
        if x['threshold'] == 128 and x['size_population'] == 'stored':
            demand[x['era']] = hist_values(x['page_rows'])
    return census, demand


def page_mix(census):
    """Share of records by page count (0 = inline) per era, from stored sizes."""
    mix = {}
    for era in ERAS + ['all']:
        counts = {}
        for size, n in census['domains'][era]['sizes']['stored']['histogram']:
            p = 0 if size <= 128 else math.ceil(size / FRAGMENT)
            counts[p] = counts.get(p, 0) + n
        total = sum(counts.values())
        mix[era] = {p: n / total for p, n in sorted(counts.items())}
    return mix


def lookup(dir_rows, dir_segs, page_rows, page_segs, pages):
    """Warm and cold bytes of one lookup: two directory queries, then one per page."""
    warm = 2 * (up(dir_rows) + dir_segs * DOWN + HEADERS)
    setups = dir_segs
    if pages:
        warm += pages * (up(page_rows) + page_segs * DOWN + HEADERS)
        setups += page_segs
    cold = warm + setups * (SETUP + HEADERS) + MANIFEST + HEADERS + SPLIT_MAP_COLD + 2 * HEADERS
    return warm, cold


class Layout:
    """Per-archive tables: (era, dir_rows, dir_segs, page_rows, page_segs, page_share).

    `page_share` is the fraction of a shared page table charged to this archive
    (1 when it owns its table); memory sums page segments times share.
    """

    def __init__(self, name, archives, note, target=40_000):
        self.name, self.archives, self.note, self.target = name, archives, note, target

    def memory(self, size):
        total = 0.0
        for a in self.archives:
            total += a['dir_segs'] * size(a['dir_rows'])
            total += a['page_segs'] * a['share'] * size(a['page_rows'])
        return total

    def runtimes(self):
        return round(sum(a['dir_segs'] + a['page_segs'] * a['share'] for a in self.archives))

    def disk(self):
        return sum(a['dir_segs'] * (a['dir_rows'] * ROW + held(a['dir_rows']))
                   + a['page_segs'] * a['share'] * (a['page_rows'] * ROW + held(a['page_rows']))
                   for a in self.archives)

    def largest_runtime(self):
        return max(max(held(a['dir_rows']), held(a['page_rows'])) for a in self.archives)


def archive(era, demand, dir_rows, page_rows, target=40_000):
    return {'era': era, 'demand': demand, 'dir_rows': dir_rows, 'dir_segs': 1,
            'page_rows': page_rows, 'page_segs': segs(demand, page_rows), 'share': 1.0,
            'records': target}


def min_memory_rows(demand, choices):
    """The page geometry with the least held memory for this demand; ties take fewer rows."""
    return min(choices, key=lambda g: (segs(demand, g) * held(g), g))


def eras_of(demand):
    out = []
    for era in ERAS + ['mixed-era']:
        out += [(era, r) for r in demand.get(era, [])]
    return out


def is_old(era):
    # A mixed-era archive starts in the older era; price it as old (conservative
    # for every rule below, whose old geometry is the larger one).
    return era in PRE_NU6 or era == 'mixed-era'


def build_layouts(demand):
    rows = eras_of(demand)
    L = []
    L.append(Layout('txid-2k (today)', [archive(e, r, 2048, 2048) for e, r in rows],
                    '2,048-row directory and pages; pages split into 2,048-row segments'))
    L.append(Layout('txid-4k', [archive(e, r, 4096, 4096) for e, r in rows],
                    '4,096-row directory and pages'))
    L.append(Layout('2k directory, 4k pages', [archive(e, r, 2048, 4096) for e, r in rows],
                    'new geometry: 2,048-row directory, 4,096-row pages, every archive'))
    L.append(Layout('2k directory, 8k pages', [archive(e, r, 2048, 8192) for e, r in rows],
                    'new geometry: 2,048-row directory, 8,192-row pages, every archive'))
    L.append(Layout('2k directory, 8k pages before NU6',
                    [archive(e, r, 2048, 8192 if is_old(e) else 2048) for e, r in rows],
                    'pages 8,192 rows for archives starting before NU6, txid-2k after'))
    L.append(Layout('2k directory, pages sized per archive {2k,4k,8k}',
                    [archive(e, r, 2048, min_memory_rows(r, (2048, 4096, 8192))) for e, r in rows],
                    'seal picks the page geometry with least held memory for the archive'))
    L.append(Layout('2k directory, pages sized per archive {2k,4k,8k,16k}',
                    [archive(e, r, 2048, min_memory_rows(r, (2048, 4096, 8192, 16384)))
                     for e, r in rows],
                    'as above with a 16,384-row option'))
    return L


def shared_recent(demand, k, page_choices=(2048, 4096, 8192)):
    """Per-archive page geometry before NU6; NU6-onward archives share 2,048-row pages in k-groups.

    Era order inside NU6..NU6.3 is unknown to the census, so groups are formed
    in a seeded random order; the result is reported as min/max over 200 orders.
    """
    old = [(e, r) for e, r in eras_of(demand) if is_old(e)]
    new = [(e, r) for e, r in eras_of(demand) if not is_old(e)]
    results = []
    for seed in range(200):
        rng = random.Random(seed)
        order = new[:]
        rng.shuffle(order)
        archives = [archive(e, r, 2048, min_memory_rows(r, page_choices)) for e, r in old]
        for i in range(0, len(order), k):
            group = order[i:i + k]
            total = sum(r for _, r in group)
            s = segs(total, 2048)
            for e, r in group:
                a = archive(e, r, 2048, 2048)
                a['page_segs'], a['share'], a['group_segs'] = s, 1 / len(group), s
                archives.append(a)
        results.append(Layout(f'per-archive pages before NU6, NU6+ pages shared k={k}', archives,
                              f'NU6 onward: {k} adjacent archives share one 2,048-row page table'))
    return results


def paired_old(demand, name, dir_rows, page_choices):
    """archive_target 80,000 before NU6: adjacent 40k archives paired; txid-2k at 40,000 after.

    Pairing order within an era is unknown; seeded random pairings, min/max over
    200. The pair's page demand is the sum of its halves (concatenated packing,
    within one row per half of the real 80k packing). Mixed-era archives are
    paired with each other, a simplification for 10 of 426 archives.
    """
    results = []
    for seed in range(200):
        rng = random.Random(seed)
        archives = []
        for era in PRE_NU6 + ['mixed-era']:
            rs = demand.get(era, [])[:]
            rng.shuffle(rs)
            for i in range(0, len(rs), 2):
                pair = rs[i:i + 2]
                archives.append(archive(era, sum(pair), dir_rows,
                                        min_memory_rows(sum(pair), page_choices),
                                        target=40_000 * len(pair)))
        for era in ERAS[7:]:
            archives += [archive(era, r, 2048, 2048) for r in demand.get(era, [])]
        results.append(Layout(name, archives, 'archive_target 80,000 before NU6 (2,726,400)'))
    return results


def census_shared(census, demand):
    """The census's exact shared allocations, re-priced, for txid-2k and txid-4k."""
    opts = {}
    for o in census['pages']['layout_options']:
        if o['threshold'] == 128 and o['size_population'] == 'stored':
            opts[(o['archives_per_pages_table'], o['page_geometry_rows'])] = o
    out = []
    archives = census['pages']['archives']
    for k in (1, 4, 16, 64):
        groups = math.ceil(archives / k)
        r1024 = opts[(k, 1024)]['encoded_database_bytes'] // ROW
        r4096 = opts[(k, 4096)]['encoded_database_bytes'] // ROW
        # 2,048-row allocation: each group's demand rounds up to 2,048, which is
        # between its 1,024 and 4,096 roundings. Exact at k = 1 from the histogram.
        if k == 1:
            lo2 = hi2 = sum(segs(r, 2048) * 2048 for r in demand['all'])
        else:
            lo2, hi2 = r1024, min(r1024 + groups * 1024, r4096)
        for geometry, page_rows_bounds in (('txid-2k', (lo2, hi2)), ('txid-4k', (r4096, r4096))):
            dir_rows = 2048 if geometry == 'txid-2k' else 4096
            row_g = dir_rows
            res = []
            for rows in page_rows_bounds:
                s = math.ceil(rows / row_g)
                dirs = archives  # census: one coarse directory segment per archive
                res.append({
                    'page_segments': s,
                    'held_gib': (dirs * held(dir_rows) + s * held(row_g)) / GIB,
                    'reserved_gib': (dirs * reserved(dir_rows) + s * reserved(row_g)) / GIB,
                })
            out.append({'geometry': geometry, 'archives_per_pages_table': k, 'groups': groups,
                        'page_segments': [res[0]['page_segments'], res[-1]['page_segments']],
                        'held_gib': [round(res[0]['held_gib'], 1), round(res[-1]['held_gib'], 1)],
                        'reserved_gib': [round(res[0]['reserved_gib'], 1), round(res[-1]['reserved_gib'], 1)]})
    return out


def era_lookups(layout, mix):
    """Per era: page geometry and segments at the median and largest archive, lookup bytes."""
    out = {}
    for era in ERAS:
        arcs = sorted((a for a in layout.archives if a['era'] == era), key=lambda a: a['demand'])
        if not arcs:
            continue
        rows = {}
        # The census's quantile: the ceil(n/2)-th smallest archive.
        for label, a in (('p50', arcs[math.ceil(len(arcs) / 2) - 1]), ('max', arcs[-1])):
            ps = a.get('group_segs', a['page_segs'])
            rows[label] = {'page_rows': a['page_rows'], 'page_segments': ps,
                           'demand_rows': a['demand']}
            for p in (0, 1, 4):
                w, c = lookup(a['dir_rows'], a['dir_segs'], a['page_rows'], ps, p)
                rows[label][f'warm_{p}'] = w
                rows[label][f'cold_{p}'] = c
        out[era] = rows
    return out


def mean_lookup(layout, mix):
    """Mean warm bytes per lookup with every display record equally likely."""
    total, n = 0.0, 0
    for a in layout.archives:
        m = mix.get(a['era'], mix['all'])
        ps = a.get('group_segs', a['page_segs'])
        for p, share in m.items():
            total += a['records'] * share * lookup(a['dir_rows'], a['dir_segs'], a['page_rows'], ps, p)[0]
        n += a['records']
    return total / n


def per_new_archive(layout):
    """Memory of one archive sealed after cutover: the layout's mean over its NU6.3 archives.

    NU6.3 page demand (p50 116, max 620 rows) fits one txid-2k pair, or a share
    of a shared page table where the layout shares NU6-onward pages.
    """
    arcs = [a for a in layout.archives if a['era'] == 'NU6.3']
    cost = lambda a, size: a['dir_segs'] * size(a['dir_rows']) + a['page_segs'] * a['share'] * size(a['page_rows'])
    return (sum(cost(a, held) for a in arcs) / len(arcs),
            sum(cost(a, reserved) for a in arcs) / len(arcs))


def host_fit(held_cutover, growth, per_new, largest):
    cache = (held_cutover + growth + per_new) / 0.9
    memory_max = (cache + 8 * largest) / 0.9
    fits = [h for h, gib, _ in HOSTS if memory_max / GIB <= gib * 0.8 - 1.5]
    return cache, memory_max, fits


def summarize(layout, mix, year_archives):
    h = layout.memory(held)
    r = layout.memory(reserved)
    new_h, new_r = per_new_archive(layout)
    out = {'name': layout.name, 'note': layout.note, 'archives': len(layout.archives),
           'runtimes': layout.runtimes(),
           'page_segments': round(sum(a['page_segs'] * a['share'] for a in layout.archives)),
           'held_gib': round(h / GIB, 2), 'reserved_gib': round(r / GIB, 2),
           'growth_gib_per_year': round(year_archives * new_h / GIB, 2),
           'disk_gb': round(layout.disk() / 1e9, 1),
           'mean_warm_lookup_bytes': round(mean_lookup(layout, mix))}
    for label, total, new in (('held', h, new_h), ('reserved', r, new_r)):
        size = held if label == 'held' else reserved
        largest = max(max(size(a['dir_rows']), size(a['page_rows'])) for a in layout.archives)
        cache, mm, fits = host_fit(total, year_archives * new, new, largest)
        out[f'{label}_one_year_cache_gib'] = round(cache / GIB, 1)
        out[f'{label}_memory_max_gib'] = math.ceil(mm / GIB)
        out[f'{label}_fits'] = fits
    out['eras'] = era_lookups(layout, mix)
    return out


def main():
    census, demand = load()
    mix = page_mix(census)
    nu63 = census['domains']['NU6.3']['records']
    nu63_days = (3_508_673 - 3_428_143 + 1) * 75 / 86_400
    year_archives = nu63 / nu63_days * 365 / 40_000
    layouts = [summarize(l, mix, year_archives) for l in build_layouts(demand)]

    def spread(runs):
        sums = [summarize(l, mix, year_archives) for l in runs]
        lo = min(sums, key=lambda s: s['held_gib'])
        hi = max(sums, key=lambda s: s['held_gib'])
        hi['held_gib_range'] = [lo['held_gib'], hi['held_gib']]
        hi['orders'] = len(runs)
        return hi

    for k in (4, 16, 64):
        layouts.append(spread(shared_recent(demand, k)))
    layouts.append(spread(paired_old(demand, '80k before NU6 as txid-4k, txid-2k after', 4096, (4096,))))
    layouts.append(spread(paired_old(demand, '80k before NU6, 4k directory, 8k pages, txid-2k after',
                                     4096, (8192,))))
    layouts.append(spread(paired_old(demand, '80k before NU6, 4k directory, pages {4k,8k}, txid-2k after',
                                     4096, (4096, 8192))))
    layouts.append(spread(paired_old(demand, '80k before NU6, 4k directory, pages {4k,8k,16k}, txid-2k after',
                                     4096, (4096, 8192, 16384))))

    dir_load = {}
    for era in ERAS:
        dom = census['domains'][era]
        per = dom['thresholds']['128']['stored']['directory_entry_bytes'] / dom['records']
        dir_load[era] = round(per * 40_000 / (2048 * (ROW - 4)), 3)

    result = {
        'schema': 'txid-display-genesis-layout-v1',
        'source': {'census_json_sha256': 'ce0f3fff30d963b99cd6431634a66f50e781c63c4675e4c8c0dc7fef9b29c86b',
                   'threshold': 128, 'size_population': 'stored'},
        'constants': {'hint_held_bytes': HINT_HELD, 'hint_reserved_bytes': HINT_RESERVED,
                      'segment_answer_bytes': DOWN, 'setup_bytes': SETUP, 'headers': HEADERS,
                      'manifest_bytes': MANIFEST, 'split_map_cold_bytes': SPLIT_MAP_COLD,
                      'year_archives': round(year_archives, 1),
                      'hosts': HOSTS, 'usable': 'GiB x 0.8 - 1.5'},
        'directory_load_at_2048_rows_40k': dir_load,
        'page_mix': {e: {str(p): round(s, 6) for p, s in m.items()} for e, m in mix.items()},
        'census_shared_repriced': census_shared(census, demand),
        'layouts': layouts,
        'qualification': 'Derived from census per-archive demand; one directory segment per archive '
                         'is assumed from 45-54% directory load, not replayed at 2,048 rows. Era '
                         'order within an era is unknown, so shared and paired options report a '
                         'range over seeded orders. Not native RSS, latency or a build.',
    }
    (HERE / 'layout.json').write_text(json.dumps(result, indent=1) + '\n')

    print('year archives %.1f; directory load at 2,048 rows: %s' % (year_archives, dir_load))
    print('\ncensus shared allocations re-priced (GiB held [lo, hi]; reserved):')
    for c in result['census_shared_repriced']:
        print('  %-8s k=%-2d page segments %-12s held %-14s reserved %s' % (
            c['geometry'], c['archives_per_pages_table'], c['page_segments'], c['held_gib'], c['reserved_gib']))
    print()
    for s in layouts:
        print('%-58s runtimes %4d pages %4d held %5.1f GiB%s reserved %5.1f; MemoryMax %3dG fits %s; '
              'reserved MemoryMax %3dG fits %s; disk %5.1f GB; mean warm %6d B' % (
                  s['name'], s['runtimes'], s['page_segments'], s['held_gib'],
                  ' [%.1f-%.1f]' % tuple(s['held_gib_range']) if 'held_gib_range' in s else '',
                  s['reserved_gib'], s['held_memory_max_gib'], s['held_fits'][:1],
                  s['reserved_memory_max_gib'], s['reserved_fits'][:1], s['disk_gb'],
                  s['mean_warm_lookup_bytes']))
        for era, v in s['eras'].items():
            print('    %-10s p50 %5d rows -> %5d x%-2d warm 0/1/4 %6d %6d %6d cold %6d %6d %6d | '
                  'max %5d -> %5d x%-2d warm 1/4 %6d %6d' % (
                      era, v['p50']['demand_rows'], v['p50']['page_rows'], v['p50']['page_segments'],
                      v['p50']['warm_0'], v['p50']['warm_1'], v['p50']['warm_4'],
                      v['p50']['cold_0'], v['p50']['cold_1'], v['p50']['cold_4'],
                      v['max']['demand_rows'], v['max']['page_rows'], v['max']['page_segments'],
                      v['max']['warm_1'], v['max']['warm_4']))
    return 0


if __name__ == '__main__':
    sys.exit(main())
