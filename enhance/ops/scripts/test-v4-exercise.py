#!/usr/bin/env python3
"""Run the small v4 exercise profile using separate disposable worker processes.

This validates the workload driver, not full-size assignments or hardware limits.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

spec = importlib.util.spec_from_file_location('v4_local', Path(__file__).with_name('test-v4-local.py'))
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, default=Path('target/release-fast'))
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--sealed-shards', type=int, choices=(6, 7), default=6)
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--min-publications', type=int, default=6)
    args = parser.parse_args()
    if args.seconds <= 0 or args.min_publications < 6:
        parser.error('positive duration and at least six publications required')
    args.out.mkdir(parents=True, exist_ok=False)
    binary = args.bin_dir.resolve() / 'enhance-pir-v4'
    manifest = {'kind': 'v4-workload-driver-smoke', 'qualification': 'unqualified',
                **local.provenance(args.bin_dir.resolve()), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    processes, logs = [], []
    with tempfile.TemporaryDirectory(prefix='v4-exercise-') as directory:
        root = Path(directory)
        try:
            replicas = []
            for index in range(2):
                port = local.free_port()
                replica = {'name': f'worker-{index}', 'url': f'http://127.0.0.1:{port}'}
                replicas.append(replica)
                log = (args.out / f'worker-{index}.log').open('w')
                logs.append(log)
                processes.append(subprocess.Popen([str(binary), 'worker', '--sealed-shards', str(args.sealed_shards), '--listen', f'127.0.0.1:{port}',
                                                   '--data-dir', str(root / f'worker-{index}')], stdout=log, stderr=log))
            deadline = time.monotonic() + 30
            for replica in replicas:
                while True:
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError('worker stopped during startup')
                    try:
                        local.fetch(replica['url'] + '/internal/v4/health')
                        break
                    except Exception:
                        if time.monotonic() > deadline:
                            raise RuntimeError('worker startup timed out') from None
                        time.sleep(0.1)
            inventory = root / 'workers.json'
            inventory.write_text(json.dumps({'groups': [{'name': 'g0', 'replicas': replicas}]}))
            command = [str(binary), 'exercise', '--sealed-shards', str(args.sealed_shards), '--isolated-workers', '--profile', 'smoke',
                       '--data-dir', str(root / 'exercise'), '--worker-config', str(inventory), '--listen', '127.0.0.1:0',
                       '--seconds', str(args.seconds), '--min-publications', str(args.min_publications),
                       '--publication-interval', '1', '--concurrency', '2']
            (args.out / 'command.json').write_text(json.dumps(command, indent=2))
            with (args.out / 'exercise.log').open('w') as output:
                process = subprocess.Popen(command, stdout=output, stderr=output)
                processes.append(process)
                status = process.wait(timeout=args.seconds + args.min_publications * 15 + 180)
            for name in ('exercise.json', 'publications.jsonl', 'v4-source-mode'):
                source = root / 'exercise' / name
                if source.exists():
                    shutil.copyfile(source, args.out / name)
            if (root / 'exercise' / 'metrics').exists():
                shutil.copytree(root / 'exercise' / 'metrics', args.out / 'metrics')
            if status:
                raise RuntimeError('workload process failed; retained its report and trace')
            report = json.loads((args.out / 'exercise.json').read_text())
            if (report['qualification'] != 'unqualified' or report['status'] != 'recorded'
                    or report['profile'] != 'smoke' or report['publications'] < args.min_publications
                    or report['expired_refreshes'] < 2 or report['background_correct_answers'] <= 0
                    or report['background_errors'] != 0 or report['six_hour_publication_gate']):
                raise RuntimeError('workload smoke expectations failed')
            if len(report['final_metrics']) != 3:
                raise RuntimeError('missing final metric snapshots')
            for scrape in report['final_metrics']:
                text = (args.out / 'metrics' / scrape['file']).read_text()
                if hashlib.sha256(text.encode()).hexdigest() != scrape['sha256']:
                    raise RuntimeError('metric snapshot digest mismatch')
                if scrape['target']['role'] == 'worker':
                    for sample in ('enhance_v4_worker_memory_sample_available 1',
                                   'enhance_v4_worker_model_within_limit 1',
                                   'enhance_v4_worker_candidate_present 0'):
                        if sample + '\n' not in text:
                            raise RuntimeError('unexpected final worker metric state')
                elif 'enhance_v4_publication_target_current 1\n' not in text:
                    raise RuntimeError('publication metrics did not reach the final target')
            manifest['report_sha256'] = hashlib.sha256((args.out / 'exercise.json').read_bytes()).hexdigest()
            manifest['exit_code'] = status
            print(json.dumps({key: report[key] for key in ('publications', 'background_correct_answers', 'background_errors', 'background_p99_ms', 'expired_refreshes')}))
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for log in logs:
                log.close()
            (args.out / 'run.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
