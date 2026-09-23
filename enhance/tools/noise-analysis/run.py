#!/usr/bin/env python3
"""Run/resume the exact matrix. Each case is bound to the executable and config."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
from verify import analyze


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--queries', type=int, default=128)
    p.add_argument('--shards', default='0,1,2')
    p.add_argument('--patterns', default='zero,max,alternating,impulse,random,records')
    p.add_argument('--edge', choices=['first', 'last', 'both'], default='both')
    p.add_argument('--jobs', type=int, default=4)
    p.add_argument('--reverse', action='store_true', help='Evaluate larger configurations first')
    p.add_argument('--source-revision')
    args = p.parse_args()
    binary = args.binary.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    matrix = json.loads(subprocess.check_output([str(binary), '--matrix']))
    config = dict(binary_sha256=digest, matrix=matrix, queries=args.queries,
                  shards=args.shards, patterns=args.patterns, edge=args.edge,
                  rayon_threads=os.environ.get('RAYON_NUM_THREADS'),
                  source_revision=args.source_revision or subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
    manifest = args.output / 'manifest.json'
    if manifest.exists() and json.loads(manifest.read_text()) != config:
        raise SystemExit('resume manifest mismatch; use a new output directory')
    manifest.write_text(json.dumps(config, indent=2) + '\n')
    edges = ['first', 'last'] if args.edge == 'both' else [args.edge]
    summary = []
    def case(spec):
        shape, edge, pattern, shard = spec
        used = shape[f'{edge}_used_rows']
        name = f'r{shape["domain_rows"]}-u{used}-{pattern}-s{shard}'
        path = args.output / f'{name}.json'
        if not path.exists():
            with (args.output / f'{name}.log').open('w') as log:
                command = [str(binary), '--used-rows', str(used), '--pattern', pattern,
                           '--shard', shard, '--queries', str(args.queries), '--output', str(path)]
                completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
                if completed.returncode and not path.exists():
                    raise SystemExit(f'{name}: extraction failed; see log')
        x = json.loads(path.read_text())
        if x['binary_sha256'] != digest or x['used_rows'] != used or x['pattern'] != pattern or x['shard'] != int(shard) or len(x['queries']) != args.queries:
            raise SystemExit(f'{name}: stale/mismatched case')
        row = dict(case=name, sha256=hashlib.sha256(path.read_bytes()).hexdigest(), **analyze(x))
        return row
    specs = list(itertools.product(matrix, edges, args.patterns.split(','), args.shards.split(',')))
    if args.reverse:
        specs.reverse()
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        pending = [pool.submit(case, spec) for spec in specs]
        for future in as_completed(pending):
            row = future.result()
            summary.append(row)
            summary.sort(key=lambda r: r['case'])
            (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
            print(f'{len(summary)}: {row["case"]}: {row["reason"]}; empirical={row["threshold_utilization"]:.4%}', flush=True)
    (args.output / 'complete.json').write_text(json.dumps(dict(cases=len(summary), clear=False,
        reason='independent review and deployed snapshot coverage required'), indent=2) + '\n')


if __name__ == '__main__':
    main()
