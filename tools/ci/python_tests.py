"""Run discovered unittest modules on bounded, separate worker processes.

Discovery remains authoritative, including imported or generated test cases.
Each child receives exactly the discovered case IDs for its module, preserving
module/class fixtures while isolating patches and mutable globals from peers.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
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


def selected_modules(modules, selection):
    """Select exact discovered IDs; a stale or empty fast tier must fail closed."""
    if (not isinstance(selection, list) or not selection
            or not all(isinstance(name, str) for name in selection)
            or len(selection) != len(set(selection))):
        raise ValueError('test selection must be a nonempty list of unique case IDs')
    requested = set(selection)
    discovered = {name for names in modules.values() for name in names}
    if requested - discovered:
        raise ValueError(f'unknown selected cases: {sorted(requested - discovered)}')
    return {module: chosen for module, names in modules.items()
            if (chosen := [name for name in names if name in requested])}


def execute(directory, jobs, split_modules=(), selection=None, isolate_ci_output=False):
    modules = discover(directory, split_modules)
    if selection is not None:
        modules = selected_modules(modules, selection)
    # Keep the caller's working directory, as unittest discovery did. Only its
    # import search path changes so child interpreters can resolve the case IDs.
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(directory), os.environ.get('PYTHONPATH', '')]))
    if isolate_ci_output:
        # Tooling tests deliberately invoke and break the CI helpers. Keep
        # their fixture commands out of the enclosing job's output and receipts.
        for key in ('GITHUB_ENV', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY',
                    'WALLET_PIR_STAGE_LOG', 'WALLET_PIR_ARTIFACT_LOG',
                    'WALLET_PIR_STAGE_PARENT'):
            env.pop(key, None)
        # The correctness runner still times actual Python cases (including
        # setup/cleanup). Fixture-specific writers must isolate their own logs.
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
    parser.add_argument('--selection', type=Path, help='JSON array of exact discovered IDs for fast feedback')
    parser.add_argument('--isolate-ci-output', action='store_true',
                        help='keep CI-tool fixture commands out of the enclosing GitHub job and receipts')
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_dir():
        parser.error(f'test directory does not exist: {directory}')
    selection = json.loads(args.selection.read_text()) if args.selection else None
    if args.selection and selection is None:
        parser.error('test selection must be a nonempty list of unique case IDs')
    execute(directory, args.jobs, args.split_module, selection, args.isolate_ci_output)


if __name__ == '__main__':
    main()
