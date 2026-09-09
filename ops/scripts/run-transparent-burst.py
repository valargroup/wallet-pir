#!/usr/bin/env python3
"""Run isolated worker burst comparisons in fresh processes, preserving failures."""
import argparse
import hashlib
import json
import os
import math
from pathlib import Path
import platform
import signal
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]


def memory_qualification(kernel, isolated, overhead):
    """Use the kernel high-water mark, including startup, never sampled RSS."""
    peak = int(kernel.get('memory.peak', 0))
    events = dict(line.split() for line in kernel.get('memory.events', '').splitlines())
    headroom = ((8 << 30) - overhead - peak) / (8 << 30)
    return dict(kernel_peak_bytes=peak, modeled_host_headroom_fraction=headroom,
                memory_qualification_passed=bool(isolated and peak > 0 and headroom >= .20
                    and int(events.get('oom', -1)) == 0 and int(events.get('oom_kill', -1)) == 0
                    and kernel.get('memory.max', '').strip() == '7516192768'
                    and kernel.get('memory.high', '').strip() == '5905580032'
                    and kernel.get('memory.swap.max', '').strip() == '0'
                    and kernel.get('cpuset.cpus.effective', '').strip() == '0-3'),
                kernel_memory_events=events)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--fixture', type=Path, help='Frozen three-publication recent assignment')
    parser.add_argument('--systemd', action='store_true', help='Fresh 4-CPU, MemoryHigh=5.5 GiB, MemoryMax=7 GiB cgroup per run (Linux prebuilt only)')
    parser.add_argument('--host-overhead-bytes', type=int, default=512 << 20,
                        help='Reserved non-worker memory in the modeled 8 GiB headroom check')
    parser.add_argument('--test-binary', type=Path, help='Prebuilt revisions_and_cache test executable')
    parser.add_argument('--external-clients', action='store_true', help='Separate client process; Linux systemd runs pin clients to CPUs 4-7')
    parser.add_argument('--source-sha', help='Source revision for a prebuilt executable')
    parser.add_argument('--worker-budget-seconds', type=float, default=30.,
                        help='Stricter worker-stage screening budget, at most the 30s public ceiling')
    parser.add_argument('--build-slots', type=int, choices=[1, 2], nargs='+', default=[1, 2], help='Configurations to compare (default: 1 2)')
    parser.add_argument('--repetitions', type=int, default=2)
    args = parser.parse_args()
    if len(set(args.build_slots)) != len(args.build_slots):
        parser.error("--build-slots must not repeat a configuration")
    if args.systemd and (not args.test_binary or platform.system() != 'Linux'):
        parser.error('--systemd requires Linux and --test-binary')
    if args.external_clients and not args.test_binary:
        parser.error('--external-clients requires --test-binary')
    if args.fixture and not args.fixture.is_file():
        parser.error('--fixture must exist')
    if not 0 <= args.host_overhead_bytes < (8 << 30):
        parser.error('--host-overhead-bytes must be within the modeled 8 GiB host')
    if args.test_binary and not args.source_sha:
        parser.error('--test-binary requires --source-sha')
    if not 0 < args.worker_budget_seconds <= 30:
        parser.error('--worker-budget-seconds must be positive and at most 30')
    if args.repetitions < 1:
        parser.error('--repetitions must be positive')
    output = args.out.resolve() if args.out else Path(tempfile.mkdtemp(prefix='transparent-burst-'))
    output.mkdir(parents=True, exist_ok=True)
    # Refuse to mix runs or overwrite prior evidence.
    with (output / 'manifest.json').open('x') as stream:
        source = args.source_sha or subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        paths = [ROOT / 'ops/scripts/run-transparent-burst.py']
        paths += [ROOT / 'Cargo.toml', ROOT / 'Cargo.lock']
        paths += sorted((ROOT / 'pir').glob('*/Cargo.toml'))
        paths += sorted((ROOT / 'server').glob('*/Cargo.toml'))
        paths += sorted((ROOT / 'server/transparent-shard-server').rglob('*.rs'))
        json.dump({'source_sha': source, 'machine': platform.platform(),
                   'external_clients': args.external_clients, 'client_cpus': '4-7' if args.systemd and args.external_clients else None,
                   'worker_budget_seconds': args.worker_budget_seconds, 'build_slots': args.build_slots, 'repetitions': args.repetitions, 'isolated_cgroup': args.systemd,
                   'fixture_sha256': hashlib.sha256(args.fixture.read_bytes()).hexdigest() if args.fixture else None,
                   'modeled_host_bytes': 8 << 30, 'host_overhead_bytes': args.host_overhead_bytes,
                   'test_binary_sha256': hashlib.sha256(args.test_binary.read_bytes()).hexdigest() if args.test_binary else None,
                   'source_files_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if not p.name.startswith('._')}}, stream, indent=2)
    print(f'Reports: {output}', flush=True)
    command = ['cargo', 'test', '--release', '-p', 'transparent-shard-server',
               '--test', 'revisions_and_cache', 'burst::publication_burst_under_exact_load',
               '--', '--ignored', '--exact', '--nocapture']
    if args.test_binary:
        command = [str(args.test_binary.resolve()), 'burst::publication_burst_under_exact_load',
                   '--ignored', '--exact', '--nocapture']
    runs = []
    for repetition in range(args.repetitions):
        for slots in (args.build_slots if repetition % 2 == 0 else list(reversed(args.build_slots))):
            name = f'repeat-{repetition + 1}-slots-{slots}'
            report = output / f'{name}.json'
            env = dict({k: v for k, v in os.environ.items() if not k.startswith("TRANSPARENT_BURST_")}, TRANSPARENT_BURST_BUILD_SLOTS=str(slots),
                       TRANSPARENT_BURST_REPORT=str(report), TRANSPARENT_BURST_SOURCE_SHA=source)
            if args.fixture:
                env['TRANSPARENT_BURST_FIXTURE'] = str(args.fixture.resolve())
            client_process = None
            client_log = None
            if args.external_clients:
                client_dir = output / (name + '-clients')
                client_dir.mkdir()
                env['TRANSPARENT_BURST_CLIENT_DIR'] = str(client_dir)
                client_command = [str(args.test_binary.resolve()), 'burst::external_query_clients', '--ignored', '--exact', '--nocapture']
                if args.systemd:
                    client_command = ['taskset', '-c', '4-7'] + client_command
                client_log = (client_dir / 'process.log').open('x')
                client_process = subprocess.Popen(client_command, env=env, stdout=client_log, stderr=subprocess.STDOUT, start_new_session=True)
            unit = 'transparent-full-burst-' + uuid.uuid4().hex if args.systemd else None
            run_command = command
            if unit:
                run_command = ['systemd-run', '--wait', '--pipe', '--collect', '--unit', unit,
                               '--property=CPUAffinity=0 1 2 3', '--property=AllowedCPUs=0-3', '--property=MemoryHigh=5905580032',
                               '--property=MemoryMax=7516192768', '--property=MemorySwapMax=0']
                run_command += ['--setenv=' + k + '=' + v for k, v in env.items() if k.startswith('TRANSPARENT_BURST_')]
                run_command += command
            print(f'Running {name}', flush=True)
            with (output / f'{name}.log').open('x') as log:
                process = subprocess.Popen(run_command, cwd=ROOT, env=env, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = process.wait(timeout=900)
                except subprocess.TimeoutExpired:
                    if unit:
                        subprocess.run(['systemctl', 'stop', unit], timeout=30, check=False)
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    code = 124
                except BaseException:
                    if unit:
                        subprocess.run(['systemctl', 'stop', unit], timeout=30, check=False)
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    raise
                finally:
                    if client_process:
                        (client_dir / 'stop').touch()
                        try:
                            client_code = client_process.wait(timeout=70)
                        except subprocess.TimeoutExpired:
                            os.killpg(client_process.pid, signal.SIGKILL)
                            client_process.wait()
                            client_code = 124
                        client_log.close()
            row = {'name': name, 'build_slots': slots, 'exit_code': code, 'report': report.name}
            if client_process:
                row['client_exit_code'] = client_code
                if client_code != 0:
                    row['exit_code'] = client_code
            if report.exists() and report.stat().st_size:
                try:
                    value = json.loads(report.read_text())
                    if args.external_clients:
                        row['external_clients'] = value.get('external_clients', False)
                        row['client_shutdown_complete'] = value.get('client_shutdown_complete', False)
                        if args.systemd:
                            host = json.loads((client_dir / 'host.json').read_text())
                            client_cpus = dict(line.split(':', 1) for line in host['proc_status'].splitlines())['Cpus_allowed_list'].strip()
                            worker_group = value.get('kernel_memory', {}).get('path')
                            client_group = host.get('kernel', {}).get('path')
                            row['client_isolation_passed'] = bool(client_cpus == '4-7' and worker_group and client_group and worker_group != client_group)
                    row['load_overlapped_burst'] = value.get('load_overlapped_burst', False)
                    row['cold_warm'] = value.get('cold_warm', False)
                    row['assigned_shards'] = value.get('assigned_shards', 1)
                    if args.fixture:
                        row.update(memory_qualification(value.get('kernel_memory', {}), args.systemd, args.host_overhead_bytes))
                    latencies = sorted(q['query']['seconds'] for q in value.get('queries', []) if 'query' in q)
                    row.update(exact_query_count=len(latencies),
                               query_p50_seconds=latencies[math.ceil(len(latencies) * .50) - 1] if latencies else None,
                               query_p95_seconds=latencies[math.ceil(len(latencies) * .95) - 1] if latencies else None,
                               completed=value['completed'], exact_queries=value['exact_queries'],
                               worker_30s_budget_passed=value['worker_30s_budget_passed'],
                               max_worker_visibility_seconds=max((a['worker_visibility_seconds'] for a in value['activations']), default=None),
                               peak_process_rss_bytes=max((s['process_rss_bytes'] or 0 for s in value['memory_samples']), default=None))
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                    row['report_error'] = str(error)
                    row['worker_30s_budget_passed'] = False
            row['worker_budget_seconds'] = args.worker_budget_seconds
            row['worker_budget_passed'] = bool(row.get('completed') and row.get('max_worker_visibility_seconds') is not None and row['max_worker_visibility_seconds'] <= args.worker_budget_seconds)
            runs.append(row)
            (output / 'summary.json').write_text(json.dumps({'scope': 'isolated worker; modeled headroom is not live-host acceptance', 'runs': runs}, indent=2) + '\n')
            print(json.dumps(row), flush=True)
    return 0 if all(r['exit_code'] == 0 and r.get('completed') and r.get('exact_queries')
                    and r.get('worker_30s_budget_passed') and r.get('worker_budget_passed')
                    and (not args.external_clients or (r.get('external_clients') and r.get('client_shutdown_complete')))
                    and (not (args.external_clients and args.systemd) or r.get('client_isolation_passed'))
                    and (not args.fixture or (r.get('cold_warm') and r.get('assigned_shards') == 14 and r.get('load_overlapped_burst') and r.get('memory_qualification_passed'))) for r in runs) else 1


if __name__ == '__main__':
    raise SystemExit(main())
