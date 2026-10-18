#!/usr/bin/env python3
"""Include changes from canceled or failed main runs in the next fast check."""
import argparse
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ops/lib'))
from wallet_pir_ops.github_api import api


def prepared_base(repo, head, previous):
    """Reuse only a completed full fast preparation on main's current ancestry.

    The preparation workflow always runs --all. A diagnostic CI dispatch may
    select only part of a change, so it can never establish this baseline.
    """
    runs = api(f'repos/{repo}/actions/workflows/ci-prepare.yml/runs?branch=main&event=workflow_dispatch&status=success&per_page=1',
               timeout=3, attempts=2)['workflow_runs']
    if not runs:
        return previous
    run = runs[0]
    sha = run.get('head_sha', '')
    if (run.get('status') != 'completed' or run.get('conclusion') != 'success'
            or run.get('event') != 'workflow_dispatch' or run.get('head_branch') != 'main'
            or run.get('path') != '.github/workflows/ci-prepare.yml'
            or run.get('head_repository', {}).get('full_name') != repo
            or not re.fullmatch('[0-9a-f]{40}', sha)):
        return previous
    if sha == previous:
        return previous
    compare = lambda start, end: api(f'repos/{repo}/compare/{start}...{end}', timeout=3, attempts=2)['status']
    if previous and compare(previous, sha) != 'ahead':
        return previous
    if compare(sha, head) not in ('ahead', 'identical'):
        return previous
    attempt = run.get('run_attempt')
    if type(attempt) is not int or attempt < 1:
        return previous
    jobs = api(f'repos/{repo}/actions/runs/{run["id"]}/attempts/{attempt}/jobs?per_page=100',
               timeout=3, attempts=2)['jobs']
    if len(jobs) != 1 or jobs[0].get('status') != 'completed' or jobs[0].get('conclusion') != 'success':
        return previous
    steps = {step['name']: step for step in jobs[0].get('steps', [])}
    for name in ('Compile fast units', 'Validate prepared units', 'Validate affected fast units'):
        step = steps.get(name, {})
        if step.get('status') != 'completed' or step.get('conclusion') != 'success':
            return previous
    return sha


def comparison(event, base, repo, head=''):
    if event != 'push':
        return base
    runs = api(f'repos/{repo}/actions/workflows/ci.yml/runs?branch=main&event=push&status=success&per_page=1',
               timeout=3, attempts=2)['workflow_runs']
    sha = ''  # Missing history selects all Rust and helpers.
    if runs:
        run = runs[0]
        sha = run['head_sha']
        if (run['conclusion'] != 'success' or run['event'] != 'push' or run['head_branch'] != 'main'
                or not re.fullmatch('[0-9a-f]{40}', sha)):
            raise ValueError('invalid successful-main comparison')
    return prepared_base(repo, head, sha) if re.fullmatch('[0-9a-f]{40}', head) else sha


def preparation_comparison(base, repo, head):
    """Warm an explicit comparison or the same main history routine CI uses."""
    if base:
        if not re.fullmatch('[0-9a-f]{40}', base):
            raise ValueError('preparation comparison must be a full commit SHA')
        return base
    return comparison('push', '', repo, head)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare', action='store_true',
                        help='use an explicit comparison or successful main history for cache preparation')
    args = parser.parse_args(argv)
    inputs = (os.environ.get('BASE', ''), os.environ['GITHUB_REPOSITORY'], os.environ.get('GITHUB_SHA', ''))
    base = (preparation_comparison(*inputs) if args.prepare
            else comparison(os.environ['GITHUB_EVENT_NAME'], *inputs))
    with open(os.environ['GITHUB_ENV'], 'a') as out:
        out.write('FAST_BASE=' + base + '\n')
    print(base)


if __name__ == '__main__':
    main()
