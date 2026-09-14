#!/usr/bin/env python3
"""Run the agreed bounded public-origin baseline from the repository root."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import time
import urllib.request

OUT = pathlib.Path(__file__).resolve().parent
ROOT = OUT.parents[2]
URL = 'https://enhance-pir.valargroup.dev'
BIN = ROOT / 'target/release/enhance-pir-load-test'

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def fetch(path, name):
    started = now()
    try:
        with urllib.request.urlopen(URL + path, timeout=4) as response:
            raw = response.read()
            status = response.status
        value = json.loads(raw)
        (OUT / name).write_bytes(raw)
        return {'time': started, 'status': status, 'file': name}, value
    except Exception as error:
        return {'time': started, 'error': str(error)}, None

manifest = {
    'run_id': OUT.name, 'started_utc': now(), 'origin': URL,
    'source_commit': command('git', 'rev-parse', 'HEAD'),
    'build_command': 'cargo build --locked --release -p enhance-pir-load-test -p enhance-pir --features enhance-pir/cli',
    'tool_sha256': hashlib.sha256(BIN.read_bytes()).hexdigest(),
    'cargo_lock_sha256': hashlib.sha256((ROOT / 'Cargo.lock').read_bytes()).hexdigest(),
    'rustc': command('rustc', '-Vv'), 'client_os': command('sw_vers'),
    'client_cpu': command('sysctl', '-n', 'machdep.cpu.brand_string'),
    'client_ram_bytes': int(command('sysctl', '-n', 'hw.memsize')),
    'client_logical_cpus': int(command('sysctl', '-n', 'hw.logicalcpu')),
    'client_network': 'Developer machine to public HTTPS origin; access link, route, and server region not independently measured.',
    'server_revision': None, 'server_hardware': None,
    'workload': '1024 seeded random positions, closed-loop queries, one initialized generation per step, fresh query randomness',
    'seed': 42, 'warmup_seconds': 10, 'measured_seconds_per_step': 60,
    'repetitions_per_step': 1, 'steps': [],
    'limits': 'No query errors; p99 <= 5000 ms; abort after two consecutive failed health probes. No deployment changes.',
    'unmeasured': ['server memory/CPU/disk', 'server SKU and region', 'transport overhead', 'exact canonical answer comparison', 'whole-wallet recovery', 'full-capacity qualification'],
}

def save():
    (OUT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

save()
for parallelism in (1, 2, 4, 8):
    prefix = f'c{parallelism}'
    health, health_body = fetch('/v1/health', prefix + '-health-before.json')
    init, init_body = fetch('/v1/enhance/init', prefix + '-init-before.json')
    step = {'parallelism': parallelism, 'started_utc': now(), 'before': [health, init]}
    manifest['steps'].append(step)
    if not health_body or not init_body or health_body.get('phase', {}).get('phase') != 'serving':
        step['result'] = 'blocked: pre-step health/init unavailable or not serving'
        save()
        break
    generation = init_body['generation']
    if generation['schema_version'] != 7 or generation['protocol_revision'] != 'ironwood-enhance-pir-v2':
        step['result'] = 'blocked: incompatible schema/protocol'
        save()
        break
    args = [str(BIN), '--server', URL, '--parallelism', str(parallelism), '--duration', '60s', '--warmup', '10s', '--seed', '42', '--max-error-rate', '0', '--slo-p99-ms', '5000', '--json-out', str(OUT / (prefix + '-summary.json'))]
    step['command'] = args
    started = time.monotonic()
    with (OUT / (prefix + '.log')).open('w') as log, (OUT / (prefix + '-health-probes.jsonl')).open('w') as probes:
        proc = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        consecutive = 0
        probe_number = 0
        while proc.poll() is None:
            probe, body = fetch('/v1/health', prefix + f'-probe-{probe_number:03d}.json')
            probe_number += 1
            good = body is not None and body.get('phase', {}).get('phase') in ('serving', 'building')
            consecutive = 0 if good else consecutive + 1
            probe['healthy'] = good
            probes.write(json.dumps(probe) + '\n')
            probes.flush()
            if consecutive >= 2:
                step['interrupted'] = 'two consecutive failed health probes'
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                break
            time.sleep(5)
        step['exit_code'] = proc.wait()
    step['elapsed_wall_seconds'] = time.monotonic() - started
    step['ended_utc'] = now()
    after, _ = fetch('/v1/health', prefix + '-health-after.json')
    after_init, _ = fetch('/v1/enhance/init', prefix + '-init-after.json')
    step['after'] = [after, after_init]
    summary = OUT / (prefix + '-summary.json')
    passed = False
    if summary.exists():
        result = json.loads(summary.read_text())
        passed = result['completed'] > 0 and result['errors'] == 0 and result['stages'][0]['p99_ms'] <= 5000
    step['result'] = 'passed' if passed and step['exit_code'] == 0 else 'failed or incomplete; no escalation'
    save()
    print(prefix + ': ' + step['result'], flush=True)
    if step['result'] != 'passed':
        break
    if parallelism != 8:
        time.sleep(30)
manifest['ended_utc'] = now()
save()
