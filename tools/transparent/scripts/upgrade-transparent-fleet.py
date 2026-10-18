#!/usr/bin/env python3
"""Upgrade a canary or the whole fleet under explicit transparent maintenance.

Run on the coordinator. Full-fleet mode requires a successful, matching six-hour
canary result. Failure leaves both public origins guarded until canonical, warm
service has been verified; a binary rollback never rewinds publication records.
"""
import argparse
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import time
import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('deploy', HERE/'deploy-transparent-publisher.py')
D = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(D)
L = D.LIVE

# Archive workers without a populated disk cache need a full runtime build.
# Keep installation bounded separately from post-start warm-up; the previous
# shared fifteen-minute bound also consumed staging and shard validation time.
INSTALL_SECONDS = 2400
WARM_SECONDS = 1800
ROLLBACK_SECONDS = 1800


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_gate(result, binary, script, config, roster, headless_helper=None, storage_helper=None):
    expected = dict(binary_sha256=binary, fleet_script_sha256=script, fleet_config_sha256=config, roster_sha256=roster)
    if headless_helper is not None:
        expected['headless_helper_sha256'] = headless_helper
    if storage_helper is not None:
        expected['storage_helper_sha256'] = storage_helper
    if result.get('passed') is not True or any(result.get(k) != v for k, v in expected.items()):
        raise ValueError('canary result is unsuccessful or does not match this deployment')
    if (result.get('worker') != 'transparent-pir-recent-01' or result.get('seconds', 0) < 21600
            or min(result.get('blocks', 0), result.get('replica_blocks', 0)) < 300):
        raise ValueError('canary duration/block gate is incomplete')
    counts = result.get('exact_queries', [])
    if len(counts) < 2 or min(counts) < 1000:
        raise ValueError('canary lacks sustained exact-query evidence')
    for key, maximum in [('public_budget_seconds', 30), ('replica_budget_seconds', 60),
                         ('maximum_visibility_seconds', 30), ('maximum_canary_visibility_seconds', 60)]:
        if not isinstance(result.get(key), (int, float)) or not 0 <= result[key] <= maximum:
            raise ValueError('canary freshness gate failed: '+key)


def guard_coordinator(text):
    """Replace only the known transparent handlers, preserving other services."""
    required = ['handle @transparent_publication {', 'handle @legacy_transparent_filters {']
    optional = ['handle_path /v1/filters/parents/* {', 'handle @transparent_queries {']
    for marker in required + optional:
        count = text.count(marker)
        if count == 0 and marker in optional:
            continue
        if count != 1:
            raise ValueError('cannot identify unique transparent handler: '+marker)
        start = text.index(marker)+len(marker)
        depth, end = 1, start
        while end < len(text) and depth:
            depth += (text[end] == '{') - (text[end] == '}')
            end += 1
        if depth:
            raise ValueError('unbalanced coordinator configuration')
        text = text[:start]+'\n\t\theader Retry-After 1\n\t\trespond "transparent fleet maintenance" 503\n\t'+text[end-1:]
    return text


def apply_coordinator(data):
    candidate = Path('/etc/caddy/Caddyfile.maintenance-next')
    D.atomic_bytes(candidate, data)
    D.execute(['caddy', 'validate', '--config', candidate, '--adapter', 'caddyfile'])
    D.atomic_bytes(Path('/etc/caddy/Caddyfile'), data)
    D.execute(['systemctl', 'reload', 'caddy'])


def service(action, *names):
    D.execute(['systemctl', action, *names])


async def start_authority():
    service('start', 'transparent-replica-reconciler')
    service('start', 'transparent-publish-controller')
    deadline = time.monotonic()+60
    while True:
        try:
            await asyncio.to_thread(D.read_json, 'http://127.0.0.1:8094/v1/shards')
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise RuntimeError('publication authority did not become available under maintenance')
            await asyncio.sleep(0.5)


async def verify_public(fleet):
    first = fleet.c['authority_upstream']+'/v1/filters/shards'
    second = 'https://'+fleet.c['public_host']+'/v1/shards'
    a = await asyncio.to_thread(D.read_json, first)
    b = await asyncio.to_thread(D.read_json, second)
    if a != b and await asyncio.to_thread(D.read_json, first) != b:
        raise RuntimeError('public origins disagree after maintenance')
    tail = b['shards'][-1]
    fleet.canonical.clear()
    if await fleet.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
        raise RuntimeError('public authority is not canonical after maintenance')


async def maintenance(fleet, saved):
    D.atomic_bytes(saved/'Caddyfile.coordinator', Path('/etc/caddy/Caddyfile').read_bytes())
    guarded = guard_coordinator((saved/'Caddyfile.coordinator').read_text()).encode()
    D.atomic_bytes(saved/'Caddyfile.guarded', guarded)
    # The persistent router guard also applies to future controller route calls.
    async with fleet.lock('routing'):
        L.atomic_json(fleet.root/'maintenance.json', {'enabled': True})
        await fleet.route([])
        apply_coordinator(guarded)
    for url in ['https://'+fleet.c['public_host']+'/v1/shards', fleet.c['authority_upstream']+'/v1/filters/shards']:
        try:
            await asyncio.to_thread(D.read_json, url)
        except urllib.error.HTTPError as error:
            if error.code == 503:
                continue
            raise
        raise RuntimeError('maintenance did not withdraw '+url)


async def reopen(fleet, saved):
    async with fleet.lock('routing'):
        target = fleet.reconciliation_target()
        if target is None or not fleet.quorum(target[0]['workers']):
            raise RuntimeError('cannot reopen without a canonical publication quorum')
        active, request = target
        tail = json.loads((Path(request['directory'])/'shards.json').read_text())['shards'][-1]
        fleet.canonical.clear()
        if await fleet.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
            raise RuntimeError('publication changed chain before reopening')
        routed = [w for w in fleet.roster if w['id'] in active['workers']]
        statuses = await asyncio.gather(*(fleet.control(w, {'operation':'status'}) for w in routed))
        if not all(fleet.attests(status, active['map_sha256']) for status in statuses):
            raise RuntimeError('an advertised worker lost its current warm publication')
        # Do not overwrite an unrelated Caddy update made during maintenance.
        if Path('/etc/caddy/Caddyfile').read_bytes() != (saved/'Caddyfile.guarded').read_bytes():
            raise RuntimeError('coordinator configuration changed during maintenance')
        apply_coordinator((saved/'Caddyfile.coordinator').read_bytes())
        L.atomic_json(fleet.root/'maintenance.json', {'enabled': False})
        try:
            await fleet.route([w for w in fleet.roster if w['id'] in active['workers']], json.loads(Path(active['assignment']).read_text()))
        except BaseException:
            L.atomic_json(fleet.root/'maintenance.json', {'enabled': True})
            apply_coordinator((saved/'Caddyfile.guarded').read_bytes())
            raise


async def verify_workers(fleet, workers, binary, query_binary, publications):
    target = fleet.reconciliation_target()
    if target is None:
        raise RuntimeError('publication is withdrawn')
    active, request = target
    tail = json.loads((Path(request['directory'])/'shards.json').read_text())['shards'][-1]
    fleet.canonical.clear()
    if await fleet.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
        raise RuntimeError('publication is no longer canonical')
    if tail['end_height'] < await fleet.node_height():
        raise RuntimeError('publication has not caught up to the node')
    assignment = json.loads(Path(active['assignment']).read_text())
    assigned = {w['id']: w['shards'] for w in assignment['workers']}
    async def check(worker):
        status = await fleet.control(worker, {'operation':'status'})
        ready = await asyncio.to_thread(D.read_json, 'http://'+worker['upstream']+'/v1/ready')
        expected = binary[worker['id']] if isinstance(binary, dict) else binary
        if not fleet.attests(status, active['map_sha256']) or not ready.get('ready') or ready.get('binary_sha256') != expected:
            raise RuntimeError(worker['id']+' does not attest the selected warm binary/publication')
        output = await L.run([query_binary, '--url', 'http://'+worker['upstream'], '--publications', publications,
                              '--shard', str(assigned[worker['id']][-1]), '--seconds', '5'], timeout=60)
        rows = [json.loads(line) for line in output.splitlines() if line.startswith(b'{')]
        if {row.get('table') for row in rows if row.get('exact') is True} != {'directory', 'pages'}:
            raise RuntimeError(worker['id']+' lacks an exact private-query result')
        return {'worker':worker['id'], 'ready':ready, 'exact_queries':sum(row.get('exact') is True for row in rows)}
    results = await asyncio.gather(*(check(w) for w in workers))
    fleet.canonical.clear()
    if await fleet.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
        raise RuntimeError('chain changed during private-query verification')
    return results


async def ready_to_freeze(fleet, workers):
    target = fleet.reconciliation_target()
    if target is None:
        raise RuntimeError('waiting for publication')
    tail = json.loads((Path(target[1]['directory'])/'shards.json').read_text())['shards'][-1]
    if tail['end_height'] < await fleet.node_height():
        raise RuntimeError('waiting for the authority to catch up before freezing verification')
    statuses = await asyncio.gather(*(fleet.control(w, {'operation':'status'}) for w in workers))
    if not all(fleet.attests(s, target[0]['map_sha256']) for s in statuses):
        raise RuntimeError('waiting for workers to join the current publication')


async def upgrade(args):
    fleet = L.Fleet(json.loads(args.fleet_config.read_text()))
    binary = sha(args.artifacts/'transparent-shard-server')
    workers = [w for w in fleet.roster if not args.worker or w['id'] == args.worker]
    if not workers or (args.worker and (len(workers) != 1 or workers[0]['role'] != 'recent-replica')):
        raise ValueError('canary must name one existing recent replica')
    if not args.worker:
        if args.canary_result is None:
            raise ValueError('full fleet upgrade requires --canary-result')
        validate_gate(json.loads(args.canary_result.read_text()), binary,
                      sha(HERE/'transparent-live-fleet.py'), sha(args.fleet_config), sha(fleet.c['roster']),
                      sha(HERE/'transparent-headless-console.py') if fleet.c.get('headless_console', False) else None,
                      sha(HERE/'transparent-storage-policy.py') if fleet.c.get('storage_nodiscard', False) else None)
    args.out.mkdir(parents=True, exist_ok=False)
    D.atomic_bytes(args.out/'fleet.json', args.fleet_config.read_bytes())
    before = {w['id']: (await asyncio.to_thread(D.read_json, 'http://'+w['upstream']+'/v1/ready'))['binary_sha256'] for w in workers}
    L.atomic_json(args.out/'deployment.json', {'binary_sha256':binary, 'source_sha':args.source_sha, 'workers':[w['id'] for w in workers], 'before':before})
    # Verify all selected hosts before withdrawing either public origin.
    await asyncio.gather(*(D.install_worker(fleet, w, args.artifacts, str(args.out), stage_only=True) for w in workers))
    await maintenance(fleet, args.out)
    services = ('transparent-publish-controller', 'transparent-replica-reconciler')
    try:
        service('stop', *services)
        await asyncio.wait_for(asyncio.gather(*(D.install_worker(fleet, w, args.artifacts, str(args.out), warm_seconds=WARM_SECONDS) for w in workers)), INSTALL_SECONDS)
        config = {**fleet.c, 'reconcile_workers': [],
                  'managed_recent_workers': sorted({*fleet.managed_ids(), *(w['id'] for w in workers if w['role'] == 'recent-replica')})}
        L.atomic_json(args.fleet_config, config)
        fleet = L.Fleet(config)
        service('start', 'transparent-replica-reconciler')
        await asyncio.sleep(2)
        service('start', 'transparent-publish-controller')
        deadline = time.monotonic()+900
        while True:
            try:
                # Freeze preparation/activation only after all target workers
                # attest the authority, then verify private rows on that snapshot.
                await ready_to_freeze(fleet, workers)
                service('stop', *services)
                evidence = await verify_workers(fleet, workers, binary, args.query_binary, args.publications)
                break
            except Exception as error:
                print(str(error), flush=True)
                service('start', 'transparent-replica-reconciler')
                service('start', 'transparent-publish-controller')
                if time.monotonic() >= deadline:
                    raise RuntimeError('fleet verification exceeded fifteen minutes') from error
                await asyncio.sleep(2)
        L.atomic_json(args.out/'verified.json', evidence)
        await start_authority()
        await reopen(fleet, args.out)
        await verify_public(fleet)
        L.atomic_json(args.out/'result.json', {'passed':True, 'binary_sha256':binary, 'workers':[w['id'] for w in workers]})
    except BaseException as error:
        # Record the original cause before a potentially lengthy rollback.
        # TimeoutError has an empty string representation.
        failure = {'error_type':type(error).__name__,
                   'error':str(error) or type(error).__name__,
                   'install_seconds':INSTALL_SECONDS, 'warm_seconds':WARM_SECONDS}
        L.atomic_json(args.out/'failure.json', failure)
        # Stop retries before reverting binaries/configuration. Preserve the
        # current active records and revocations, including any new reorg state.
        service('stop', *services)
        L.atomic_json(fleet.root/'maintenance.json', {'enabled':True})
        await fleet.route([])
        apply_coordinator((args.out/'Caddyfile.guarded').read_bytes())
        async def restore(worker):
            backup = shlex.quote(str(args.out))
            await fleet.ssh(worker['ssh_host'], f'''set -eu
if [ -f {backup}/worker.service ]; then
 install -m755 {backup}/transparent-shard-server /usr/local/bin/transparent-shard-server.rollback
 mv /usr/local/bin/transparent-shard-server.rollback /usr/local/bin/transparent-shard-server
 install -m755 {backup}/shard-control /usr/local/bin/shard-control
 if [ -f {backup}/headless-console.py ]; then install -Dm755 {backup}/headless-console.py /usr/local/lib/transparent-pir/headless-console.py; fi
 if [ -f {backup}/storage-mount-options ]; then python3 {backup}/restore-storage-policy.py --restore-options-file {backup}/storage-mount-options; fi
 if [ -f {backup}/storage-policy.py ]; then install -Dm755 {backup}/storage-policy.py /usr/local/lib/transparent-pir/storage-policy.py; fi
 install -m644 {backup}/worker.service /etc/systemd/system/transparent-shard-server.service
 systemctl daemon-reload
 systemctl restart transparent-shard-server
fi''', timeout=60)
        restored = await asyncio.gather(*(restore(w) for w in workers), return_exceptions=True)
        D.atomic_bytes(args.fleet_config, (args.out/'fleet.json').read_bytes())
        fleet = L.Fleet(json.loads(args.fleet_config.read_text()))
        service('start', 'transparent-replica-reconciler')
        service('start', 'transparent-publish-controller')
        rollback_errors = [str(r) for r in restored if isinstance(r, BaseException)]
        reopened = False
        deadline = time.monotonic()+ROLLBACK_SECONDS
        while not rollback_errors and time.monotonic() < deadline:
            try:
                await ready_to_freeze(fleet, workers)
                service('stop', *services)
                evidence = await verify_workers(fleet, workers, before, args.query_binary, args.publications)
                L.atomic_json(args.out/'rollback-verified.json', evidence)
                await start_authority()
                await reopen(fleet, args.out)
                await verify_public(fleet)
                reopened = True
                break
            except Exception as recovery_error:
                print('rollback verification: '+str(recovery_error), flush=True)
            finally:
                service('start', 'transparent-replica-reconciler')
                service('start', 'transparent-publish-controller')
            await asyncio.sleep(5)
        L.atomic_json(args.out/'result.json', {'passed':False, **failure, 'rollback_errors':rollback_errors, 'public_maintenance':not reopened})
        raise RuntimeError('upgrade failed; rollback '+('verified and reopened' if reopened else 'requires recovery with public maintenance retained')) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fleet-config', type=Path, default=Path('/opt/transparent-publisher/fleet.json'))
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--worker', help='One recent canary; omit for the gated whole-fleet batch')
    parser.add_argument('--canary-result', type=Path)
    parser.add_argument('--query-binary', type=Path, required=True)
    parser.add_argument('--publications', type=Path, default=Path('/srv/zakura/transparent-publications'))
    parser.add_argument('--out', type=Path, required=True)
    asyncio.run(upgrade(parser.parse_args()))


if __name__ == '__main__':
    main()
