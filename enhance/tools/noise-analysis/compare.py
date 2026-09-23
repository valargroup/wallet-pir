#!/usr/bin/env python3
"""Cross-check deterministic certificate data across two platform runs."""
import argparse
import json
from pathlib import Path


def compare(left, right):
    fields = ('implementation', 'schema', 'rows', 'used_rows', 'records', 'units', 'params',
              'setup_seed', 'database_sha256', 'public_c1_sha256', 'column_sums',
              'deterministic_bounds', 'threshold', 'cdf_table', 'cdf_max_val')
    for field in fields:
        if left[field] != right[field]:
            raise ValueError(f'platform mismatch: {field}')
    if (left['blocks'] is None) != (right['blocks'] is None):
        raise ValueError('platform mismatch: weight coverage')
    if len(left['blocks'] or []) != len(right['blocks'] or []):
        raise ValueError('platform mismatch: block count')
    for a, b in zip(left['blocks'] or [], right['blocks'] or []):
        for field in ('block', 'sum_squares', 'sum_absolute', 'sum_signed', 'maximum_weight'):
            if a[field] != b[field]:
                raise ValueError(f'platform mismatch: block {a["block"]}/{field}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('left', type=Path)
    parser.add_argument('right', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--expected-cases', type=int, help='Fail unless both directories contain exactly this many matching cases')
    args = parser.parse_args()
    if args.expected_cases is not None and args.expected_cases <= 0:
        parser.error('expected case count must be positive')
    left = {p.name: p for p in args.left.glob('r*-u*-*-s*.json')}
    right = {p.name: p for p in args.right.glob('r*-u*-*-s*.json')}
    shared = sorted(left.keys() & right.keys())
    for name in shared:
        compare(json.loads(left[name].read_text()), json.loads(right[name].read_text()))
    result = dict(matching_cases=len(shared), cases=shared,
                  left_only=sorted(left.keys()-right.keys()), right_only=sorted(right.keys()-left.keys()),
                  scope='deterministic inputs, public packing material, and grouped moments; fresh-query noise samples differ')
    text = json.dumps(result, indent=2)+'\n'
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end='')
    if args.expected_cases is not None and (len(shared) != args.expected_cases or result['left_only'] or result['right_only']):
        raise SystemExit('incomplete cross-platform campaign')


if __name__ == '__main__':
    main()
