#!/usr/bin/env python3
"""Product host phase dependency; invoke only from the locked schema recipe."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('activity_host', HERE.parent/'lib/activity_schema_host.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['preflight', 'capture', 'stage', 'activate', 'restore', 'verify-worker', 'verify-rollback-worker'])
    parser.add_argument('--plan', type=Path, required=True)
    args = parser.parse_args()
    host = M.Host(M.load(args.plan))
    if Path(__file__).resolve().parents[3] != Path('/srv/transparent-activity/ops/sources')/host.plan['source_sha']:
        raise ValueError('host phase must run from its pinned immutable operations source')
    host.identity(mutation=args.action in ('capture', 'stage', 'activate', 'restore'))
    if args.action.startswith('verify-'):
        result = host.verify_worker(rollback=args.action == 'verify-rollback-worker')
    else:
        result = getattr(host, args.action)()
    # Never emit private plan/unit/config bytes.
    print(json.dumps({'action': args.action, 'result': result}))


if __name__ == '__main__':
    main()
