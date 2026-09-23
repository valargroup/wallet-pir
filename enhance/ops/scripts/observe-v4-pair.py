#!/usr/bin/env python3
"""Record off-host hardware evidence for an isolated bootstrapped v4 pair.

This is a measurement tool, not a qualification verifier or load generator.
Every scheduled slot is retained, including SSH failures and missed deadlines.
"""
import argparse
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import time

spec = importlib.util.spec_from_file_location('v4_bootstrap_pair', Path(__file__).with_name('v4-bootstrap-pair.py'))
pair_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pair_module)


def observed_targets(operation, config, bundle):
    if operation['phase'] != 'bootstrapped' or operation['bootstrap']['config_digest'] != pair_module.journal_module.digest(config):
        raise ValueError('observation requires the matching bootstrapped pair')
    identity = pair_module.installer.verify_bundle(bundle, config['revision'], config['manifest_sha256'])
    targets = []
    for index in range(operation['before'] * 2, operation['target_groups'] * 2):
        address = f'digitalocean_droplet.worker[{index}]'
        receipt = operation['bootstrap']['receipts'][address]
        target = {'address': address, 'resource_id': operation['resources'][address],
                  'name': receipt['worker_name'], 'private_ipv4': receipt['private_ipv4']}
        pair_module.validate_receipt(receipt, target, config, identity)
        targets.append(target)
    if len(targets) != 2 or pair_module.journal_module.digest(targets) != operation['bootstrap']['targets_digest']:
        raise ValueError('observation targets differ from the bootstrap binding')
    for target in targets:
        receipt = operation['bootstrap']['receipts'][target['address']]
        target['expected'] = {'binary_sha256': identity['binary_sha256'], 'source_revision': config['revision'],
                              'bootstrap_manifest_sha256': config['manifest_sha256'],
                              'boot_id': receipt['host']['boot_id'], 'limits': config['limits'][target['name']],
                              'memory_total_bytes': receipt['host']['memory_total_bytes']}
    return targets


def bind_sample(value, target):
    if value['error'] is not None:
        return
    expected = target['expected']
    if (any(value.get(key) != expected[key] for key in ('binary_sha256', 'source_revision', 'bootstrap_manifest_sha256', 'boot_id'))
            or value.get('hostname') != target['name'] or value.get('worker_private_ipv4') != target['private_ipv4']
            or value['host_memory_bytes']['MemTotal'] != expected['memory_total_bytes']
            or any(value['cgroup_bytes'][metric] != expected['limits'][key] for metric, key in
                   [('memory.high', 'memory_high_bytes'), ('memory.max', 'memory_max_bytes'), ('memory.swap.max', 'memory_swap_max_bytes')])):
        value['error'] = 'sample_differs_from_bootstrap_binding'


def reject_constant(_value):
    raise ValueError('nonfinite sample value')


def sample(remote, script, started, scheduled):
    sent = time.monotonic_ns()
    try:
        output = remote.command(['python3', script], timeout=20)
        if len(output) > 1024 * 1024:
            raise ValueError('oversized sample')
        value = json.loads(output, parse_constant=reject_constant)
        if value.get('kind') != 'enhance-v4-hardware-sample' or value.get('version') != 2 or 'error' not in value:
            raise ValueError('invalid hardware sample')
        required = {'boot_id', 'host_id_sha256', 'binary_sha256', 'main_pid', 'main_start_ticks',
                    'process_memory', 'cgroup_bytes', 'memory.events', 'service', 'worker', 'runtime_metrics', 'worker_after_metrics'}
        if value['error'] is None and not required <= value.keys():
            raise ValueError('incomplete hardware sample')
        if value['error'] is None:
            runtime = value['runtime_metrics']
            if (type(runtime.get('state_consistent')) is not bool
                    or type(runtime.get('values')) is not dict
                    or type(runtime['values'].get('memory_sample_available')) is not int
                    or runtime['values']['memory_sample_available'] not in (0, 1)):
                raise ValueError('invalid runtime observation')
    except Exception:
        value = {'kind': 'enhance-v4-hardware-sample', 'version': 2, 'error': 'transport_or_decode_failed'}
    return {'scheduled_offset_ns': scheduled, 'sent_offset_ns': sent - started,
            'received_offset_ns': time.monotonic_ns() - started, 'sample': value}


def observe(targets, config, remotes, out, seconds, interval):
    if type(seconds) is not int or seconds <= 0 or not math.isfinite(interval) or not 1 <= interval <= 10:
        raise ValueError('use a positive duration and a one-to-ten second interval')
    if len(targets) != 2 or len(remotes) != 2 or len({t['resource_id'] for t in targets}) != 2:
        raise ValueError('observe exactly two distinct worker resources')
    out.mkdir(parents=True, exist_ok=False, mode=0o700)
    host_id = Path('/etc/machine-id')
    manifest = {'kind': 'enhance-v4-hardware-observation', 'qualification': 'unqualified', 'status': 'recording',
                'sample_version': 2, 'seconds_requested': seconds, 'interval_seconds': interval, 'targets': targets,
                'bootstrap_config_digest': pair_module.journal_module.digest(config),
                'controller_platform': platform.platform(),
                'controller_host_id_sha256': hashlib.sha256(host_id.read_bytes()).hexdigest() if host_id.exists() else None,
                'wall_time_ns': time.time_ns()}
    pair_module.journal_module.atomic(out / 'manifest.json', manifest)
    script = '/opt/enhance-pir-v4/bootstrap/' + config['manifest_sha256'] + '/bundle/sample-v4-worker.py'
    start = time.monotonic_ns()
    errors = 0
    records = 0
    unavailable_memory = 0
    inconsistent_runtime = 0
    trace_path = out / 'hardware.jsonl'
    with trace_path.open('x') as trace, concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        trace_path.chmod(0o600)
        for index in range(math.ceil(seconds / interval) + 1):
            offset = min(round(index * interval * 1e9), seconds * 1_000_000_000)
            now = time.monotonic_ns()
            if now < start + offset:
                time.sleep((start + offset - now) / 1e9)
            if time.monotonic_ns() > start + offset + round(interval * 1e9):
                values = [{'scheduled_offset_ns': offset, 'sample': {'kind': 'enhance-v4-hardware-sample',
                           'version': 2, 'error': 'missed_sampling_deadline'}} for _ in targets]
            else:
                pending = [pool.submit(sample, remote, script, start, offset) for remote in remotes]
                values = [future.result() for future in pending]
            for target, value in zip(targets, values):
                value.update(worker=target['name'], resource_id=target['resource_id'])
                try:
                    bind_sample(value['sample'], target)
                except (KeyError, TypeError):
                    value['sample']['error'] = 'sample_differs_from_bootstrap_binding'
                if (manifest['controller_host_id_sha256'] is not None
                        and value['sample'].get('host_id_sha256') == manifest['controller_host_id_sha256']):
                    value['sample']['error'] = 'controller_is_worker_host'
                errors += value['sample']['error'] is not None
                runtime = value['sample'].get('runtime_metrics', {})
                if value['sample']['error'] is None:
                    unavailable_memory += runtime['values']['memory_sample_available'] == 0
                    inconsistent_runtime += not runtime['state_consistent']
                records += 1
                trace.write(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')
            trace.flush()
            os.fsync(trace.fileno())
    manifest.update(status='recorded', elapsed_ns=time.monotonic_ns() - start,
                    records=records, error_samples=errors, unavailable_memory_samples=unavailable_memory,
                    inconsistent_runtime_samples=inconsistent_runtime,
                    trace_sha256=hashlib.sha256(trace_path.read_bytes()).hexdigest())
    pair_module.journal_module.atomic(out / 'manifest.json', manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('state-dir', 'bootstrap-policy', 'bundle', 'ssh-key', 'known-hosts', 'out'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=21600)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        config = json.loads(args.bootstrap_policy.read_text())
        with pair_module.journal_module.Journal(args.state_dir) as journal:
            operation = copy.deepcopy(journal.state['operation'])
        targets = observed_targets(operation, config, args.bundle)
        remotes = [pair_module.Remote(target, args.ssh_key, args.known_hosts, config['known_hosts_sha256']) for target in targets]
        result = observe(targets, config, remotes, args.out, args.seconds, args.interval)
        print(json.dumps({key: result[key] for key in ('status', 'qualification', 'records', 'error_samples')}))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError):
        raise SystemExit('v4 observation stopped; retain the partial trace and recording manifest') from None


if __name__ == '__main__':
    main()
