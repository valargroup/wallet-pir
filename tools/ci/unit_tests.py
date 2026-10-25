#!/usr/bin/env python3
"""Run exact Cargo-reported test programs with a bounded working set.

Cases stay in one process per program so immutable public fixtures can be reused.
Cargo compilation has already finished; no target directory writer runs here.
"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from stage import run


def run_pool(programs, workers, heavy_limit, invoke, *, tolerate=lambda error: False, cancel=lambda: None):
    """Run in queue order, starting a memory-heavy program only while a heavy slot is free.

    A later light program may start ahead of a waiting heavy one, so the pool
    stays busy without overlapping more heavy fixtures than fit. A free heavy
    slot is taken before any light program: lights earlier in the queue must
    not hold the critical path. That slot goes to the waiting heavy program
    with the most cases, so the longest starts first.
    Returns the errors `tolerate` accepted; any other error cancels the rest
    and is raised.
    """
    pending = list(programs)
    running = {}
    failures = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            while pending or running:
                heavy = sum(running.values())
                while len(running) < workers:
                    waiting = [i for i, program in enumerate(pending) if program.get('heavy')]
                    largest = max(waiting, key=lambda i: pending[i].get('case_count', 0), default=None)
                    if largest is not None and heavy < heavy_limit:
                        index = largest
                    else:
                        index = next((i for i, program in enumerate(pending)
                                      if not program.get('heavy')), None)
                    if index is None:
                        break
                    program = pending.pop(index)
                    running[pool.submit(invoke, program)] = bool(program.get('heavy'))
                    heavy += bool(program.get('heavy'))
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    del running[future]
                    try:
                        future.result()
                    except Exception as error:
                        if not tolerate(error):
                            raise
                        failures.append(error)
        except BaseException:
            cancel()
            for future in running:
                future.cancel()
            raise
    return failures


def execute(programs, jobs, *, budget=False, test_threads=1, heavy_slots=None):
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
                cases=program.get('case_count'), heavy=bool(program.get('heavy')),
                seconds=round(time.monotonic() - started, 3), passed=passed)), flush=True)

    def run_concurrent(selected, workers):
        def tolerate(error):
            return budget and os.environ.get('WALLET_PIR_ENFORCE_BUDGET') == '0'

        def cancel():
            if budget:
                from test_budget import stop
                stop()

        failures = run_pool(selected, workers, workers if heavy_slots is None else heavy_slots,
                            invoke, tolerate=tolerate, cancel=cancel)
        if failures:
            raise RuntimeError(f'{len(failures)} diagnostic test programs failed') from failures[0]

    # Encrypted HTTP fixtures retain their own concurrency. Keep their working
    # sets apart from other Rust programs before overlapping ordinary units.
    serial = [program for program in programs if program.get('exclusive')]
    ordinary = [program for program in programs if not program.get('exclusive')]
    for program in serial:
        invoke(program)
    run_concurrent(ordinary, jobs)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--jobs', type=int, choices=(1, 2, 3), required=True)
    parser.add_argument('--budget', action='store_true', help='time every case and enforce the correctness budget')
    parser.add_argument('--heavy-slots', type=int, help='memory-heavy programs that may run at once (default: --jobs)')
    parser.add_argument('--test-threads', type=int, choices=(1, 2), default=1)
    args = parser.parse_args()
    if args.budget:
        from test_budget import install_cancellation
        install_cancellation()
    execute(json.loads(args.plan.read_text()), args.jobs, budget=args.budget, test_threads=args.test_threads,
            heavy_slots=args.heavy_slots)


if __name__ == '__main__':
    main()
