#!/usr/bin/env python3
"""Execute selected helper suites; full qualification restores exhaustive tiers."""
import argparse
import json
from fast import HELPERS, run


def targets(selected, full=False):
    if not isinstance(selected, list) or not all(isinstance(name, str) for name in selected):
        raise ValueError('selected helpers must be a list of target names')
    unknown = set(selected) - HELPERS
    if unknown:
        raise ValueError(f'unknown helper checks: {sorted(unknown)}')
    return sorted({('check-ops-scaler' if full and name == 'check-ops-scaler-fast' else name)
                   for name in selected})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets', required=True, help='JSON array from the shared selector')
    parser.add_argument('--full', action='store_true')
    args = parser.parse_args()
    selected = targets(json.loads(args.targets), args.full)
    print('Selected helper checks: ' + ', '.join(selected), flush=True)
    if selected:
        run(['make', '-j4', *selected], stage='full-helpers' if args.full else 'helpers')


if __name__ == '__main__':
    main()
