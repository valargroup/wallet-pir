#!/usr/bin/env python3
"""Bounded public exact-answer validation with live CUDA and systemd evidence."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/lib'))
from wallet_pir_ops.deploy.descriptors import load_inventory, targets, load_descriptors
from wallet_pir_ops.deploy.remote import SSHExecutor

PROTOCOL = 'ironwood-enhance-pir-v9-native-two-mask-m29'
COUNTER = 'enhance_worker_matvec_duration_seconds_count'


def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


def counter(text):
    values = [float(line.split()[-1]) for line in text.splitlines()
              if line.startswith(COUNTER + ' ') or line.startswith(COUNTER + '{')]
    if not values or any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError('CUDA evaluation counter missing')
    return sum(values)


def validate_report(report):
    if (report.get('protocol') != PROTOCOL or report.get('exact_answer_oracle') is not True
            or report.get('succeeded', 0) <= 0 or report.get('completed') != report.get('succeeded')
            or report.get('incorrect_answers', 1) != 0 or any(report.get('errors', {}).values())
            or report.get('unstarted_arrivals', 1) != 0
            or report.get('warmup_incorrect_answers', 1) != 0
            or any(report.get('warmup_errors', {}).values())):
        raise ValueError('exact-answer report failed acceptance')


def worker_snapshot(executor, target):
    state = executor.probe_unit(target.host, target.unit)
    code, output = executor.run(target.host, ['python3', '-c',
        "import json,pathlib,subprocess,sys; u=sys.argv[1]; "
        "p=subprocess.check_output(['systemctl','show',u,'-p','ControlGroup','-p','NRestarts'],text=True); "
        "d=dict(x.split('=',1) for x in p.splitlines()); "
        "f=pathlib.Path('/sys/fs/cgroup'+d['ControlGroup']+'/memory.events'); "
        "e=dict(x.split() for x in f.read_text().splitlines()); "
        "print(json.dumps({'oom':int(e['oom']),'oom_kill':int(e['oom_kill']),'oom_group_kill':int(e.get('oom_group_kill',0)),'restarts':int(d['NRestarts'])}))",
        target.unit], 15)
    if code:
        raise RuntimeError('cannot inspect worker OOM/restart evidence')
    return {**json.loads(output), 'exe_sha256': state['exe_sha256'],
            'main_pid': state['main_pid'], 'active_state': state['active_state']}


def validate_health(worker, router):
    """The GPU worker and the packing router both serve an active, ready publication.

    Health carries no per-worker routing state; the CUDA evaluation counter
    proves the GPU answered measured queries.
    """
    if (worker.get('protocol') != PROTOCOL or worker.get('matvec') !=
            {'matvec_backend': 'cuda', 'cuda_device': 0} or worker.get('ready') is not True
            or not worker.get('active')):
        raise ValueError('worker is not ready on native CUDA device 0')
    if router.get('ready') is not True or not router.get('active'):
        raise ValueError('router is not ready')


def wait_for_readiness(worker_url, config, identity, baseline, budget, *, recover=False,
                       fetcher=fetch, clock=time.monotonic, sleep=time.sleep, samples=None):
    """Require a ready worker and router, recovering within the shared budget only when asked.

    Process, OOM and restart drift remain immediate failures.
    """
    started = clock()
    while True:
        worker = json.loads(fetcher(worker_url + '/internal/health'))
        router = json.loads(fetcher(config['router_url'] + '/internal/health'))
        if identity() != baseline:
            raise ValueError('worker identity, OOM or restart evidence changed during recovery')
        if samples is not None:
            samples.append({'at': time.time(), 'worker_active': worker.get('active'),
                            'router_active': router.get('active')})
        try:
            validate_health(worker, router)
            return worker, router, max(0, budget - (clock() - started))
        except ValueError as error:
            if not recover:
                raise
            problem = str(error)
        remaining = budget - (clock() - started)
        if remaining <= 0:
            raise ValueError(problem + ' within the shared readiness budget')
        sleep(min(2, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    inventory = load_inventory(args.inventory)
    service = load_descriptors(Path(__file__).resolve().parents[1] / 'deploy/deploy.toml')['enhance']
    chosen = targets(service, inventory)
    if len(chosen) != 1 or chosen[0].key != 'worker@enhance-pir-gpu-01':
        raise ValueError('CUDA verification requires the GPU-only inventory')
    target = chosen[0]
    config = inventory.services['enhance']['cuda_validation']
    executor = SSHExecutor(inventory)
    worker_url = 'http://' + target.vars['listen']
    args.out.mkdir(parents=True, exist_ok=False)
    evidence = {'passed': False, 'binary_sha256': args.sha256,
                'client_sha256': hashlib.sha256(args.client.read_bytes()).hexdigest(),
                'started_at': time.time(), 'samples': []}
    process = None
    try:
        baseline = worker_snapshot(executor, target)
        evidence['before'] = baseline
        if baseline['active_state'] != 'active' or baseline['exe_sha256'] != args.sha256:
            raise ValueError('running binary does not match CUDA artifact')
        # Wait for readiness before measurement, then fail on any readiness loss.
        evidence['readiness_samples'] = []
        _, _, readiness_budget = wait_for_readiness(
            worker_url, config, lambda:worker_snapshot(executor,target), baseline, 60,
            recover=True, samples=evidence['readiness_samples'])
        before = counter(fetch(worker_url + '/internal/metrics'))
        evidence['evaluations_before'] = before
        command = [str(args.client), '--server', config['origin'], '--oracle', config['oracle'],
                   '--parallelism', '1', '--rate', '1', '--warmup', '10s', '--duration', '60s',
                   '--max-error-rate', '0', '--fail-fast', '--phase-file', str(args.out / 'phase.json'),
                   '--json-out', str(args.out / 'load.json')]
        with (args.out / 'load.log').open('w') as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            deadline = time.monotonic() + 240
            measured_before = None
            while True:
                worker = json.loads(fetch(worker_url + '/internal/health'))
                router = json.loads(fetch(config['router_url'] + '/internal/health'))
                validate_health(worker, router)
                current = worker_snapshot(executor, target)
                if current != baseline:
                    raise ValueError('worker identity, readiness, OOM or restart evidence changed')
                evidence['samples'].append({'at': time.time(), 'worker': worker, 'router': router,
                                            'systemd': current})
                if measured_before is None and (args.out / 'phase.json').exists():
                    phase = json.loads((args.out / 'phase.json').read_text())
                    if phase.get('phase') != 'measured' or phase.get('protocol') != PROTOCOL:
                        raise ValueError('invalid load-client measured phase marker')
                    measured_before = counter(fetch(worker_url + '/internal/metrics'))
                    evidence['measured_phase'] = phase
                    evidence['measured_evaluations_before'] = measured_before
                if process.poll() is not None:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('exact-answer validation exceeded deadline')
                time.sleep(2)
        if process.returncode:
            raise ValueError('exact-answer client failed; see load.log')
        validate_report(json.loads((args.out / 'load.json').read_text()))
        after = counter(fetch(worker_url + '/internal/metrics'))
        evidence['evaluations_after'] = after
        if measured_before is None or after <= measured_before:
            raise ValueError('no CUDA evaluations observed during the measured exact-query phase')
        worker, router, readiness_budget = wait_for_readiness(
            worker_url, config, lambda:worker_snapshot(executor,target), baseline, readiness_budget,
            samples=evidence['readiness_samples'])
        evidence['readiness_wait_remaining_seconds'] = readiness_budget
        evidence['final_router'] = router
        evidence['final_worker'] = worker
        evidence['passed'] = True
    except BaseException as error:
        evidence['error'] = str(error)
        raise
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        evidence['finished_at'] = time.time()
        (args.out / 'verification.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'passed': True, 'report': str(args.out / 'verification.json')}))


if __name__ == '__main__':
    main()
