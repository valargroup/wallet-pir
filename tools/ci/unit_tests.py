#!/usr/bin/env python3
"""Run exact Cargo-reported test programs with a bounded working set.

Cases stay in one process per program so immutable public fixtures can be reused.
Cargo compilation has already finished; no target directory writer runs here.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from fast_build import available_cpus
from stage import run


def execute(programs, jobs, *, budget=False, test_threads=1):
    library_directory = subprocess.check_output(['rustc', '--print', 'target-libdir'], text=True).strip()

    def execute_program(program):
        directory = program['directory']
        binary = program['executable']
        env = dict(os.environ, RUST_TEST_THREADS=str(test_threads), CARGO_MANIFEST_DIR=directory,
                   CARGO_MANIFEST_PATH=str(Path(directory) / 'Cargo.toml'),
                   CARGO_PKG_NAME=program['package'], CARGO_PKG_VERSION=program['version'])
        if program['package'] == 'transparent-publisher' and not program.get('integration'):
            # Publisher correctness builds full archive runtimes. Serving keeps
            # half the CPUs for queries; serial unit cases have no query load.
            # Bound this process's build pool without changing serving defaults
            # or an explicitly selected qualification environment.
            cpus = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else (os.cpu_count() or 1)
            env.setdefault('TRANSPARENT_BUILD_THREADS', str(min(cpus, 4)))
        key = 'DYLD_FALLBACK_LIBRARY_PATH' if sys.platform == 'darwin' else 'LD_LIBRARY_PATH'
        env[key] = os.pathsep.join([str(Path(binary).parent), library_directory, env.get(key, '')])
        if budget:
            from test_budget import execute as timed_execute
            # Keep Unix socket paths below macOS's 104-byte address limit even
            # when the caller's TMPDIR is a long per-user or runner path.
            with tempfile.TemporaryDirectory(prefix='wp-', dir='/tmp') as scratch:
                env.update(TMPDIR=scratch, TMP=scratch, TEMP=scratch)
                count = timed_execute([binary, *program['args']], cwd=directory, env=env,
                                      report=env.get('WALLET_PIR_CASE_LOG'),
                                      enforce=env.get('WALLET_PIR_ENFORCE_BUDGET') != '0')
            if program.get('integration') and not count:
                raise ValueError(f'{binary}: integration executable contains no tests')
        else:
            run([binary, *program['args']], cwd=directory, env=env, stage='tests:' + program['package'])

    def invoke(program):
        started = time.monotonic()
        passed = False
        try:
            execute_program(program)
            passed = True
        finally:
            print('CI_TEST_PROGRAM ' + json.dumps(dict(
                package=program['package'], executable=Path(program['executable']).name,
                seconds=round(time.monotonic() - started, 3), passed=passed)), flush=True)

    def run_concurrent(selected, workers):
        if not selected:
            return
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(invoke, program) for program in selected]
            failures = []
            try:
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as error:
                        if budget and os.environ.get('WALLET_PIR_ENFORCE_BUDGET') == '0':
                            failures.append(error)
                        else:
                            raise
            except BaseException:
                if budget:
                    from test_budget import stop
                    stop()
                for future in futures:
                    future.cancel()
                raise
            if failures:
                raise RuntimeError(f'{len(failures)} diagnostic test programs failed') from failures[0]

    def beside_status(program):
        # The Enhance publication library is a few seconds. On eight cores it
        # fits beside the two Status prepares and still finishes before the pool.
        return (program.get('exclusive') and program['package'] == 'enhance-publisher'
                and not program.get('integration'))

    # Encrypted HTTP fixtures retain their own concurrency. Keep their working
    # sets apart from other Rust programs before overlapping ordinary units.
    # The two Status groups overlap each other on eight cores and still finish
    # before the pool. Below eight cores they stay serial: one prepare already
    # uses the whole machine.
    serial = [program for program in programs
              if program.get('exclusive') and program.get('exclusive_group') != 'status'
              and not beside_status(program)]
    status = [program for program in programs if program.get('exclusive_group') == 'status']
    publisher = [program for program in programs if beside_status(program)]
    ordinary = [program for program in programs if not program.get('exclusive')]
    for program in serial:
        invoke(program)
    overlap_status = len(status) > 1 and available_cpus() >= 8
    if overlap_status:
        wave = status + publisher
        run_concurrent(wave, min(3, len(wave)))
    else:
        for program in publisher:
            invoke(program)
        run_concurrent(status, 1)
    run_concurrent(ordinary, jobs)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--jobs', type=int, choices=(1, 2, 3), required=True)
    parser.add_argument('--budget', action='store_true', help='time every case and enforce the correctness budget')
    parser.add_argument('--test-threads', type=int, choices=(1, 2), default=1)
    args = parser.parse_args()
    if args.budget:
        from test_budget import install_cancellation
        install_cancellation()
    execute(json.loads(args.plan.read_text()), args.jobs, budget=args.budget, test_threads=args.test_threads)


if __name__ == '__main__':
    main()
