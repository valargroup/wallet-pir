#!/usr/bin/env python3
"""Read-only rollout gate. Run on the coordinator beside sustained query jobs.

Samples publication once per second and worker counters every 30 seconds.
A failed gate writes evidence and exits nonzero; it never advances a rollout.
"""
import argparse
import asyncio
import datetime
import importlib.util
import json
from pathlib import Path
import time
import subprocess
import urllib.request


def fetch(url):
    with urllib.request.urlopen(url, timeout=8) as response:
        return response.read()


def facts(raw):
    out = {}
    for line in raw.splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            out[key] = int(value) if value.isdigit() else value
        elif line.startswith(('oom ', 'oom_kill ', 'MemTotal:')):
            words = line.split()
            out[words[0].rstrip(':')] = int(words[1])
    return out


def worker_failure(baseline, current):
    for key in ('NRestarts', 'ExecMainStartTimestampMonotonic', 'oom', 'oom_kill'):
        if current.get(key) != baseline.get(key):
            return key + ' changed'
    if current['MemoryCurrent'] > current['MemTotal'] * 1024 * 0.8:
        return 'less than twenty percent host memory headroom'
    return None


def completed_blocks(seen, visible, end_height, now, budget):
    completed = {}
    for height, observed in seen.items():
        if height <= end_height and height not in visible:
            latency = now-observed
            if latency > budget:
                raise RuntimeError('publication arrived late at block '+str(height))
            completed[height] = latency
    return completed


async def observe(args):
    spec = importlib.util.spec_from_file_location('fleet', args.fleet_script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fleet = module.Fleet(json.loads(Path(args.fleet_config).read_text()))
    worker = next(w for w in fleet.roster if w['id'] == args.worker)
    began = time.monotonic()
    baseline = None
    seen = {}
    visible = {}
    canary_visible = {}
    last_node = None
    last_hash = None
    next_worker = 0
    samples = 0
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    stream = (output/'samples.ndjson').open('w', buffering=1)
    queries = []
    query_logs = []
    if args.query_binary:
        for index in range(args.query_workers):
            log = (output/f'query-{index}.ndjson').open('w', buffering=1)
            query_logs.append(log)
            queries.append(subprocess.Popen([args.query_binary, '--url', 'http://'+worker['upstream'],
                '--publications', args.publications, '--shard', str(args.shard), '--seconds', '43200'], stdout=log, stderr=log))

    def emit(event, **value):
        record = dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), event=event, **value)
        stream.write(json.dumps(record)+'\n')
        return record

    try:
        while True:
            now = time.monotonic()
            if any(process.poll() is not None for process in queries):
                raise RuntimeError('a sustained private-query process exited before the gate finished')
            status = json.loads(await asyncio.to_thread(fetch, 'http://127.0.0.1:8094/v1/status'))
            node = status['node_height']
            if last_node is None:
                last_node = node
                emit('start', node_height=node, worker=args.worker, minimum_seconds=args.seconds, minimum_blocks=args.blocks)
            fleet.canonical.clear()
            if last_hash is not None and (node < last_node or await fleet.canonical_hash(last_node) != last_hash):
                # Restart the block count conservatively, including same-height
                # replacements. Orphaned samples never satisfy the rollout gate.
                emit('chain_reorganized', old=last_node, new=node)
                seen.clear()
                visible.clear()
                canary_visible.clear()
                last_node = node
            for height in range(last_node+1, node+1):
                seen[height] = now
            last_node = node
            last_hash = await fleet.canonical_hash(node)
            # Re-read A to distinguish a normal activation between two HTTP
            # reads from origins that disagree while publication is stable.
            a = await asyncio.to_thread(fetch, args.filter_origin+'/v1/filters/shards')
            b = await asyncio.to_thread(fetch, args.shard_origin+'/v1/shards')
            if a != b:
                again = await asyncio.to_thread(fetch, args.filter_origin+'/v1/filters/shards')
                if b != again:
                    raise RuntimeError('public origins disagree across a stable re-read')
                emit('publication_changed_between_reads')
            tail = json.loads(b)['shards'][-1]
            fleet.canonical.clear()
            canonical = await fleet.canonical_hash(tail['end_height'])
            if canonical != tail['terminal_block_hash']:
                raise RuntimeError('public endpoint is not canonical')
            completed = completed_blocks(seen, visible, tail['end_height'], time.monotonic(), args.freshness_seconds)
            visible.update(completed)
            for height, latency in completed.items():
                emit('block_visible', height=height, seconds=latency, public_height=tail['end_height'])
            canary_map = json.loads(await asyncio.to_thread(fetch, 'http://'+worker['upstream']+'/v1/shards'))
            canary_tail = canary_map['shards'][-1]
            if await fleet.canonical_hash(canary_tail['end_height']) != canary_tail['terminal_block_hash']:
                raise RuntimeError('canary endpoint is not canonical')
            caught_up = completed_blocks(seen, canary_visible, canary_tail['end_height'], time.monotonic(), args.freshness_seconds)
            canary_visible.update(caught_up)
            for height, latency in caught_up.items():
                emit('canary_block_visible', height=height, seconds=latency)
            # A burst can coalesce; the oldest uncovered block must still
            # become visible within the ordinary-block freshness budget.
            overdue = [h for h,t in seen.items() if (h not in visible or h not in canary_visible) and time.monotonic()-t > args.freshness_seconds]
            if overdue:
                raise RuntimeError('publication freshness exceeded at block '+str(min(overdue)))
            if now >= next_worker:
                raw = await fleet.ssh(worker['ssh_host'], 'systemctl show transparent-shard-server -p NRestarts -p ExecMainStartTimestampMonotonic -p MemoryCurrent -p MemoryPeak; cat /sys/fs/cgroup/system.slice/transparent-shard-server.service/memory.events; head -1 /proc/meminfo')
                current = facts(raw.decode())
                ready = json.loads(await asyncio.to_thread(fetch, 'http://'+worker['upstream']+'/v1/ready'))
                control = await fleet.control(worker, {'operation':'status'})
                if ready.get('binary_sha256') != args.binary_sha256 or not ready.get('ready'):
                    raise RuntimeError('canary is not warm on the selected binary')
                if baseline is None:
                    baseline = current
                reason = worker_failure(baseline, current)
                emit('worker', facts=current, ready=ready, control=control)
                if reason:
                    raise RuntimeError(reason)
                samples += 1
                next_worker = now+30
            if now-began >= args.seconds and min(len(visible), len(canary_visible)) >= args.blocks:
                counts = []
                for index in range(len(queries)):
                    with (output/f'query-{index}.ndjson').open() as log:
                        counts.append(sum(1 for line in log if '"exact":true' in line))
                if queries and any(count < args.minimum_queries for count in counts):
                    raise RuntimeError('insufficient exact private-query samples')
                result = emit('result', passed=True, seconds=now-began, blocks=len(visible), worker_samples=samples, exact_queries=counts,
                              maximum_visibility_seconds=max(visible.values(), default=None),
                              maximum_canary_visibility_seconds=max(canary_visible.values(), default=None))
                (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
                return
            await asyncio.sleep(1)
    except BaseException as error:
        result = emit('result', passed=False, seconds=time.monotonic()-began, blocks=len(visible), error=str(error))
        (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        raise
    finally:
        for process in queries:
            if process.poll() is None:
                process.terminate()
        for process in queries:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in query_logs:
            log.close()
        stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fleet-script', default='/opt/transparent-publisher/transparent-live-fleet.py')
    parser.add_argument('--fleet-config', default='/opt/transparent-publisher/fleet.json')
    parser.add_argument('--query-binary')
    parser.add_argument('--query-workers', type=int, default=2)
    parser.add_argument('--minimum-queries', type=int, default=1000)
    parser.add_argument('--publications', default='/srv/zakura/transparent-publications')
    parser.add_argument('--shard', type=int, default=173)
    parser.add_argument('--worker', required=True)
    parser.add_argument('--binary-sha256', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--seconds', type=int, default=21600)
    parser.add_argument('--blocks', type=int, default=300)
    parser.add_argument('--freshness-seconds', type=float, default=30)
    parser.add_argument('--filter-origin', default='https://enhance-pir.valargroup.dev')
    parser.add_argument('--shard-origin', default='https://transparent-pir.valargroup.dev')
    asyncio.run(observe(parser.parse_args()))

if __name__ == '__main__':
    main()
