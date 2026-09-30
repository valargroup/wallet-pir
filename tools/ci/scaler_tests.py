#!/usr/bin/env python3
"""Classify scaler feedback separately from exhaustive simulation/model checks."""
import argparse
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / 'transparent/ops/tests'


def validate(registry):
    if set(registry) != {'fast', 'full'}:
        raise ValueError('scaler registry requires fast and full tiers')
    assigned = [name for names in registry.values() for name in names]
    discovered = {p.stem for p in TESTS.glob('test_scaler_*.py')} | {'test_membership_model'}
    if len(assigned) != len(set(assigned)) or set(assigned) != discovered:
        raise ValueError(f'scaler classification mismatch: discovered={sorted(discovered)}, assigned={assigned}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tier', choices=['fast', 'full'])
    args = parser.parse_args()
    registry = json.loads((ROOT / 'tools/ci/scaler-tests.json').read_text())
    validate(registry)
    print('Scaler classification is complete', flush=True)
    if args.tier:
        sys.path.insert(0, str(TESTS))
        names = registry['fast'] + (registry['full'] if args.tier == 'full' else [])
        tests = unittest.defaultTestLoader.loadTestsFromNames(names)
        if not tests.countTestCases():
            raise ValueError('selected scaler tier contains no tests')
        result = unittest.TextTestRunner(verbosity=1).run(tests)
        raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    main()
