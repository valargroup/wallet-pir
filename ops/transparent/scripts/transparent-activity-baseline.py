#!/usr/bin/env python3
"""Snapshot, verify or restore a reviewed v10 host baseline for schema phases.

Invoke as a checksum-bound dependency of wallet-pir-deploy.py's phase programs.
This only operates files; the phase owner quiesces writers before capture or
restore and verifies canonical recovery before reopening either public origin.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
from wallet_pir_ops import inherited_lock  # noqa: E402

SPEC = importlib.util.spec_from_file_location('activity_baseline', HERE.parent/'lib/activity_schema_baseline.py')
BASELINE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASELINE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['capture', 'verify', 'restore'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--include', nargs='+')
    args = parser.parse_args()
    if args.action != 'verify':
        inherited_lock.descriptors(required=True, path='/run/lock/wallet-pir-production.lock')
        if os.geteuid() != 0:
            raise ValueError('baseline mutation requires the pinned root wrapper phase')
    if args.action == 'capture':
        if args.plan is None or args.include:
            raise ValueError('capture requires a reviewed plan and no restore selection')
        record = BASELINE.capture(args.root, json.loads(args.plan.read_text()))
    elif args.action == 'restore':
        record = BASELINE.restore(args.root, args.include)
    else:
        if args.plan or args.include:
            raise ValueError('verify only accepts the retained root')
        record = BASELINE.verify(args.root)
    # Only public identities/counts; never print configs, units or credentials.
    print(json.dumps({'action': args.action, 'plan_sha256': record['plan_sha256'],
                      'targets': len(record['files']), 'retained_namespaces': len(record['retained'])}))


if __name__ == '__main__':
    main()
