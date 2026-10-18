#!/usr/bin/env python3
"""Upgrade recent replicas one at a time while the others keep serving.

No maintenance window: each replica is replaced only while at least two other
recent replicas are routed, and the next one starts only after the reconciler
has routed the upgraded replica again on the current publication. Archive
owners are never touched; they still need the maintenance procedure.

Stop observers that latch on worker restarts (the continuous load supervisor)
before running this, and re-qualify their expected binaries afterwards.
"""
import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
import time

SCRIPT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('publisher_deploy', SCRIPT/'deploy-transparent-publisher.py')
D = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(D)
L = D.LIVE

MEMBERSHIP_MAX_AGE = 10
MIN_OTHER_SERVING = 2


def membership(fleet):
    """The reconciler's fresh membership record, or an error."""
    value = json.loads((fleet.root/'membership.json').read_text())
    if not 0 <= time.time() - value['updated_unix'] <= MEMBERSHIP_MAX_AGE:
        raise RuntimeError('membership record is stale; is the reconciler running?')
    return value


def serving_recent(fleet, value):
    """Recent replicas the router sends traffic to: serving and rendered.
    A draining replica still attests but takes no traffic, so it does not
    count toward the replicas that keep the tier up during a restart."""
    roles = {w['id']: w['role'] for w in fleet.roster}
    return {worker for worker, observed in value['members'].items()
            if roles.get(worker) == 'recent-replica' and observed['state'] == 'serving'
            and observed.get('rendered', True)}


def check_others_serving(fleet, worker_id):
    others = serving_recent(fleet, membership(fleet)) - {worker_id}
    if len(others) < MIN_OTHER_SERVING:
        raise RuntimeError(f'only {len(others)} other recent replicas serving; refusing to restart {worker_id}')
    return others


async def wait_serving(fleet, worker_id, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if worker_id in serving_recent(fleet, membership(fleet)):
                return
        except (OSError, ValueError, KeyError, RuntimeError):
            pass
        await asyncio.sleep(2)
    raise RuntimeError(worker_id + ' was not routed again after its upgrade')


async def roll(args):
    fleet = L.Fleet(json.loads(args.fleet_config.read_text()))
    if not fleet.c.get('manage_all_workers'):
        raise RuntimeError('rolling upgrades need the reconciler to manage every member')
    replicas = [w for w in fleet.roster if w['role'] == 'recent-replica'
                and (not args.workers or w['id'] in args.workers)]
    if args.workers and {w['id'] for w in replicas} != set(args.workers):
        raise ValueError('--workers must name recent replicas in the roster')
    args.out.mkdir(parents=True, exist_ok=True)
    # Refuse before touching anything if the fleet could not absorb one restart.
    for worker in replicas:
        check_others_serving(fleet, worker['id'])
    for worker in replicas:
        others = check_others_serving(fleet, worker['id'])
        print(json.dumps({'event': 'roll_start', 'worker': worker['id'], 'others_serving': sorted(others)}), flush=True)
        started = time.monotonic()
        await D.install_worker(fleet, worker, args.artifacts, str(args.out/worker['id']), warm_seconds=args.warm_seconds)
        await wait_serving(fleet, worker['id'], args.route_seconds)
        print(json.dumps({'event': 'roll_done', 'worker': worker['id'],
                          'seconds': round(time.monotonic() - started, 1)}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fleet-config', type=Path, default=Path('/opt/transparent-publisher/fleet.json'))
    parser.add_argument('--artifacts', type=Path, required=True,
                        help='directory holding transparent-shard-server and shard-control')
    parser.add_argument('--out', type=Path, required=True, help='per-worker rollback material')
    parser.add_argument('--workers', nargs='*', default=[])
    parser.add_argument('--warm-seconds', type=int, default=900)
    parser.add_argument('--route-seconds', type=int, default=300)
    asyncio.run(roll(parser.parse_args()))


if __name__ == '__main__':
    main()
