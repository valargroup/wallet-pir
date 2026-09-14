#!/usr/bin/env python3
"""Select parent-filter finalists and run paired, isolated HTTP waves.

Offline rankings are provisional. A recommendation requires HTTP correctness and
latency evidence; this tool never deploys or changes a publication.
"""
import argparse
import html
import itertools
import json
import random
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

RECENT = ('catch-up-1d', 'catch-up-7d', 'catch-up-30d')
WEIGHTS = {'catch-up-1d': 2, 'catch-up-7d': 2, 'catch-up-30d': 2,
           'unused': 2, 'small-active': 4, 'restore-6m': 3,
           'restore-old': 2, 'multi-script': 2, 'reused-tail': 1}


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


def summarize(rows, split):
    profiles = {}
    for profile in WEIGHTS:
        selected = [r for r in rows if r['profile'] == profile and r['split'] == split]
        if not selected:
            raise ValueError(f'missing {split} profile {profile}')
        profiles[profile] = statistics.mean(r['child_bytes'] + r['parent_bytes'] + r['metadata_bytes'] for r in selected)
    return {'profiles': profiles,
            'recent_mean': statistics.mean(profiles[p] for p in RECENT),
            'mixed_mean': sum(profiles[p] * w / 20 for p, w in WEIGHTS.items())}


def eligible(score, baseline):
    return all(score['profiles'][p] <= baseline['profiles'][p] for p in RECENT)


def ranked(candidates):
    """Predeclared 1% primary-score equivalence band, then mixed cost."""
    if not candidates:
        return []
    best = min(c['tuning']['recent_mean'] for c in candidates)
    band = [c for c in candidates if c['tuning']['recent_mean'] <= best * 1.01]
    return sorted(band, key=lambda c: (c['tuning']['mixed_mean'], c['requests'], c['id']))


def select(directory):
    directory = Path(directory)
    report = read(directory / 'report.json')
    base = read(directory / 'baseline-wallets.json')
    base_scores = {s: summarize(base, s) for s in ('tuning', 'held-out')}
    by_tier = {tier: [None] + [c for c in report['candidates'] if c['tier'] == tier]
               for tier in ('recent-8k', 'archive-wide')}
    loaded = {c['id']: read(directory / f"{c['id']}-wallets.json") for c in report['candidates']}
    combined = []
    for recent, archive in itertools.product(*by_tier.values()):
        picked = [c for c in (recent, archive) if c]
        rows = [dict(r) for r in base]
        for candidate in picked:
            for i, r in enumerate(loaded[candidate['id']]):
                if r['index'] != base[i]['index']:
                    raise ValueError('wallet ordering changed')
                for key in ('child_bytes', 'parent_bytes', 'metadata_bytes', 'requests'):
                    rows[i][key] += r[key] - base[i][key]
        item = {'id': '+'.join(c['id'] for c in picked) or 'baseline',
                'manifests': {c['tier']: c['id'] + '.json' for c in picked},
                'tuning': summarize(rows, 'tuning'), 'held-out': summarize(rows, 'held-out'),
                'requests': sum(r['requests'] for r in rows if r['split'] == 'tuning')}
        item['eligible_tuning'] = eligible(item['tuning'], base_scores['tuning'])
        item['eligible_held_out'] = eligible(item['held-out'], base_scores['held-out'])
        combined.append(item)
    order = ranked([c for c in combined if c['eligible_tuning']])
    # Freeze finalists using tuning only. Do not replace failed holdouts by
    # looking down the held-out ranking.
    finalists = [c for c in order if c['id'] != 'baseline'][:3]
    result = {'schema': 'transparent-parent-selection-v1', 'status': 'provisional_requires_http',
              'baseline': base_scores, 'finalists': finalists, 'combined': combined,
              'objective': 'recent equally weighted first; mixed-20 second within 1%',
              'limits': 'Filter+metadata payload only; PIR byte changes require HTTP measurement.'}
    write(directory / 'selection.json', result)
    render(directory, result)
    return result


def render(directory, result, decision=None):
    rows = []
    baseline = result['baseline']['held-out']
    for c in result['finalists']:
        s = c['held-out']
        rows.append('<tr>' + ''.join(f'<td>{html.escape(str(v))}</td>' for v in
                    [c['id'], round(s['recent_mean'] / 1024, 2),
                     round(100 * (1 - s['recent_mean'] / baseline['recent_mean']), 2),
                     round(s['mixed_mean'] / 1048576, 3), c['eligible_held_out']]) + '</tr>')
    document = '''<!doctype html><meta charset="utf-8"><title>Parent filter evaluation</title>
<style>body{font:16px/1.5 system-ui;max-width:1200px;margin:40px auto;padding:20px;color:#182b3b}table{border-collapse:collapse;width:100%}td,th{padding:12px;text-align:left;border-bottom:1px solid #ddd}pre{white-space:pre-wrap;overflow-wrap:anywhere}.notice{background:#fff3d6;padding:16px}</style>
<h1>Parent filter evaluation</h1><p class="notice">Offline finalists; production parameters require the HTTP correctness and latency gates. Synthetic public-script wallets are not population weights.</p>
<table><tr><th>Candidate</th><th>Recent KiB</th><th>Recent saving %</th><th>Mixed MiB/wallet</th><th>Held-out guardrail</th></tr>'''
    document += ''.join(rows) + '</table>'
    if decision is not None:
        document += '<h2>HTTP validation</h2><p class="notice">' + html.escape(
            f"Status: {decision['status']}. Recommendation: {decision['recommended'] or 'none; validation incomplete'}.") + '</p>'
        document += '<details><summary>HTTP decision and missing evidence</summary><pre>' + html.escape(json.dumps(decision, indent=2)) + '</pre></details>'
    document += '<details><summary>Selection and provenance</summary><pre>' + html.escape(json.dumps(result, indent=2)) + '</pre></details>'
    (Path(directory) / 'report.html').write_text(document)


def held_out_sample(sample_path, evaluation, destination):
    sample = read(sample_path)
    splits = read(Path(evaluation) / 'split.json')
    if len(sample['clients']) > len(splits):
        raise ValueError('sample and split length mismatch')
    sample['clients'] = [w for i,w in enumerate(sample['clients']) if splits[i] == 'held-out']
    sample['classes'] = {p: sum(w['class']==p for w in sample['clients']) for p in WEIGHTS}
    if any(n != 40 for n in sample['classes'].values()):
        raise ValueError('expected exactly 40 held-out wallets per profile')
    sample['per_class'] = 40
    sample['provenance'] = 'Frozen held-out subset for parent-filter evaluation; original expectations preserved.'
    write(destination, sample)


def waves(args):
    selection = read(Path(args.evaluation) / 'selection.json')
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    deadline = time.monotonic()+1800
    while True:
        try:
            with urllib.request.urlopen(args.shard_url.rstrip('/')+'/v1/ready',timeout=10) as response:
                readiness=json.load(response)
            with urllib.request.urlopen(args.shard_url.rstrip('/')+'/v1/parent-evaluation',timeout=10) as response:
                experiment=json.load(response)
            if readiness.get('ready') and experiment == {'warmup':'paired-baseline','live_activation':False}:
                write(output/'server-readiness.json',readiness); break
        except OSError:
            pass
        if time.monotonic()>deadline: raise RuntimeError('isolated workload-warmup server did not become ready')
        time.sleep(2)
    scenario = read(args.scenario)
    held_out = output / 'held-out-sample.json'
    held_out_sample(args.sample, args.evaluation, held_out)
    scenario.update({'sample': str(held_out.resolve()), 'shard_url': args.shard_url,
                     'filter_url': args.filter_url, 'allow_advancing_publication': False,
                     'preparation_cache': 'reuse', 'profiles': WEIGHTS,
                     'metrics_targets': {'evaluation': args.shard_url.rstrip('/')+'/metrics'}})
    if args.journal_seeds:
        scenario['experimental_journal_seeds'] = str(Path(args.journal_seeds).resolve())
    variants = [{'id': 'baseline', 'manifests': {}}] + selection['finalists']
    write(output / 'variants.json', variants)
    order = []
    for seed in range(1, 11):
        ids = list(range(len(variants)))
        random.Random(f'parent-filter-paired-v1:{seed}').shuffle(ids)
        order.extend((seed, i) for i in ids)
    write(output / 'order.json', order)
    warmed = set()
    for seed, i in order:
        if seed not in warmed:
            warm = output / f'warmup-{seed:02d}.json'
            write(warm, dict(scenario, seed=seed, experimental_parent_manifests={}))
            with (output / f'warmup-{seed:02d}.log').open('wb') as log:
                result = subprocess.run([args.binary, '--scenario', str(warm), '--out-dir', str(output / warm.stem)], stdout=log, stderr=subprocess.STDOUT, timeout=args.wave_deadline)
            if result.returncode:
                write(output / 'failure.json', {'phase': 'baseline-warmup', 'seed': seed,
                      'candidate': 'baseline', 'returncode': result.returncode})
                raise RuntimeError(f'baseline warmup failed: {warm}')
            warmed.add(seed)
        variant = variants[i]
        config = dict(scenario, seed=seed, experimental_parent_manifests={
            tier: args.parent_origin.rstrip('/') + '/' + name for tier, name in variant['manifests'].items()})
        path = output / f'wave-{seed:02d}-variant-{i}.json'
        write(path, config)
        run = output / path.stem
        with (output / (path.stem + '.log')).open('wb') as log:
            result = subprocess.run([args.binary, '--scenario', str(path), '--out-dir', str(run)],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=args.wave_deadline)
        if result.returncode:
            write(output / 'failure.json', {'seed': seed, 'candidate': variant['id'], 'returncode': result.returncode})
            raise RuntimeError(f'failed wave {run}; evidence preserved')
    write(output / 'complete.json', {'waves': len(order), 'variants': variants})


def http_summary(directory, evaluation):
    directory, evaluation = Path(directory), Path(evaluation)
    variants = read(directory / 'variants.json')
    order = read(directory / 'order.json')
    all_users = [[] for _ in variants]
    failures = []
    if not (directory / 'complete.json').exists():
        failures.append('Missing complete.json: paired experiment did not finish')
    if (directory / 'failure.json').exists():
        failures.append(read(directory / 'failure.json'))
    for seed, index in order:
        path = directory / f'wave-{seed:02d}-variant-{index}' / 'report.json'
        if not path.exists():
            failures.append(str(path)); continue
        report = read(path)
        if not report.get('success') or any(u.get('outcome') != 'exact' for u in report['users']):
            failures.append(str(path))
        all_users[index].extend(report['users'])
    results = []
    for variant, users in zip(variants, all_users):
        profiles = {}
        for p in WEIGHTS:
            rows = [u for u in users if u['profile']==p]
            if not rows: continue
            profiles[p] = {'n': len(rows), 'mean_bytes': statistics.mean(u['http_totals']['bytes_down'] for u in rows),
                           'median_seconds': statistics.median(u['seconds'] for u in rows),
                           'max_seconds': max(u['seconds'] for u in rows),
                           'mean_requests': statistics.mean(u['http_totals']['calls'] for u in rows)}
        results.append({'id':variant['id'], 'profiles':profiles})
    base=results[0]['profiles']
    choices=[]
    for index,r in enumerate(results[1:],1):
        profiles=r['profiles']
        valid=not failures and all(p in profiles and p in base for p in WEIGHTS)
        r['correctness_gate']=valid
        r['recent_bytes_gate']=valid and all(profiles[p]['mean_bytes']<=base[p]['mean_bytes'] for p in RECENT)
        r['latency_gate']=valid and all(profiles[p]['median_seconds']<=base[p]['median_seconds']*1.10 for p in RECENT)
        if valid:
            r['recent_mean']=statistics.mean(profiles[p]['mean_bytes'] for p in RECENT)
            r['mixed_mean']=sum(profiles[p]['mean_bytes']*w/20 for p,w in WEIGHTS.items())
            recent_base=statistics.mean(base[p]['mean_bytes'] for p in RECENT)
            mixed_base=sum(base[p]['mean_bytes']*w/20 for p,w in WEIGHTS.items())
            r['recent_saving']=1-r['recent_mean']/recent_base
            r['mixed_saving']=1-r['mixed_mean']/mixed_base
            r['adoption_gate']=r['recent_saving']>=0.05 or (r['recent_saving']>=-0.01 and r['mixed_saving']>=0.10)
            r['offline_holdout_gate']=variants[index]['eligible_held_out']
            if all(r[k] for k in ('recent_bytes_gate','latency_gate','adoption_gate','offline_holdout_gate')): choices.append(r)
    best=min((c['recent_mean'] for c in choices),default=0)
    choices=[c for c in choices if c['recent_mean']<=best*1.01]
    choices.sort(key=lambda c:(c['mixed_mean'],sum(p['mean_requests'] for p in c['profiles'].values()),c['id']))
    decision={'status':'incomplete' if failures else 'http_validated', 'failures':failures,
              'recommended':None if failures else (choices[0]['id'] if choices else 'baseline'), 'results':results,
              'limits':'Synthetic mixed workload; median latency only. Final production adoption additionally requires retained-cache and revision tests.'}
    write(directory/'decision.json',decision)
    if (evaluation/'selection.json').exists():
        render(evaluation, read(evaluation/'selection.json'), decision)
    return decision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('select'); p.add_argument('evaluation')
    p = sub.add_parser('summarize-http'); p.add_argument('directory'); p.add_argument('evaluation')
    p = sub.add_parser('held-out'); p.add_argument('sample'); p.add_argument('evaluation'); p.add_argument('out')
    p = sub.add_parser('waves')
    for name in ('evaluation', 'scenario', 'sample', 'shard-url', 'filter-url', 'parent-origin', 'out'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--journal-seeds')
    p.add_argument('--binary', default='target/release-fast/transparent-loadtest')
    p.add_argument('--wave-deadline', type=int, default=7200)
    args = parser.parse_args()
    if args.command == 'held-out':
        held_out_sample(args.sample, args.evaluation, args.out)
        return
    if args.command == 'select': select(args.evaluation)
    elif args.command == 'summarize-http': http_summary(args.directory,args.evaluation)
    else: waves(args)


if __name__ == '__main__':
    main()
