"""Run discovered unittest modules on bounded, separate worker processes.

Discovery remains authoritative, including imported or generated test cases.
Each child receives exactly the discovered case IDs for its module, preserving
module/class fixtures while isolating patches and mutable globals from peers.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import os
import signal
import sys
import unittest

from stage import run, install_cancellation, stop


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def discover(directory, split_modules=()):
    loader = unittest.TestLoader()
    suite = loader.discover(str(directory), pattern='test_*.py')
    if loader.errors:
        raise ValueError('\n'.join(loader.errors))
    requested = set(split_modules)
    discovered = list(cases(suite))
    names = {type(case).__module__ for case in discovered}
    if requested - names:
        raise ValueError(f'unknown modules requested for class splitting: {sorted(requested - names)}')
    for name in requested:
        if any(hasattr(sys.modules[name], fixture) for fixture in ('setUpModule', 'tearDownModule')):
            raise ValueError(f'{name}: module fixtures require one process for the whole module')
    modules = {}
    for case in discovered:
        kind = type(case)
        group = kind.__module__
        if group in requested:
            group += '.' + kind.__qualname__
        modules.setdefault(group, []).append(case.id())
    if not modules:
        raise ValueError(f'no unittest cases discovered in {directory}')
    return modules


def execute(directory, jobs, split_modules=()):
    modules = discover(directory, split_modules)
    # Keep the caller's working directory, as unittest discovery did. Only its
    # import search path changes so child interpreters can resolve the case IDs.
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(directory), os.environ.get('PYTHONPATH', '')]))
    install_cancellation()
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(run, [sys.executable, '-m', 'unittest', *names],
                               cwd=Path.cwd(), env=env, stage='tests:' + module)
                   for module, names in modules.items()]
        try:
            for future in as_completed(futures):
                future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            stop(signal.SIGTERM)
            raise
    print(f'Python tests complete: {sum(map(len, modules.values()))} cases in '
          f'{len(modules)} groups with {jobs} workers', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--jobs', type=int, choices=(1, 2), default=2)
    parser.add_argument('--split-module', action='append', default=[],
                        help='split an independent module by test class; module fixtures are refused')
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_dir():
        parser.error(f'test directory does not exist: {directory}')
    execute(directory, args.jobs, args.split_module)


if __name__ == '__main__':
    main()
