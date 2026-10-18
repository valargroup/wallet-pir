#!/usr/bin/env python3
"""Guarded routing dependency of the coordinator's reviewed schema recipe."""
import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('activity_routing', HERE.parent/'lib/activity_schema_routing.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['withdraw', 'route-private', 'verify', 'reopen', 'verify-public'])
    parser.add_argument('--kind', required=True, choices=['v10', 'v11'])
    parser.add_argument('--plan', type=Path, required=True)
    args = parser.parse_args()
    routing = M.Routing(M.H.load(args.plan))
    routing.identity(mutate=True)
    action = {'route-private':'route_private', 'verify-public':'public'}.get(args.action, args.action)
    result = asyncio.run(getattr(routing, action)(args.kind))
    print(json.dumps({'action':args.action, 'kind':args.kind, 'result':result}))


if __name__ == '__main__':
    main()
