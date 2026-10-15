#!/usr/bin/env python3
"""Pre-push guard: refuse a push to main while main's latest finished CI full run failed.

Commits go straight to main and CI full runs after the push, so a red main is
only noticed by whoever watches it. On 2026-10-12 and 2026-10-14 main stayed
red for about eight hours each while further pushes landed on top. This guard
shows the failing jobs to the next pusher. Push the fix or revert with
WALLET_PIR_PUSH_ON_RED=1. When GitHub cannot be asked, it warns and allows.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = 'CI full'
OVERRIDE = 'WALLET_PIR_PUSH_ON_RED'
MARKER = '# wallet-pir red-main guard'
HOOK = f'''#!/bin/sh
{MARKER} (installed by make prepare-dev)
s="$(git rev-parse --show-toplevel)/tools/ci/main_health.py"
[ -f "$s" ] || exit 0
exec python3 "$s" --pre-push "$@"
'''


def gh(*args):
    result = subprocess.run(['gh', *args], cwd=ROOT, capture_output=True, text=True, timeout=30,
                            env=dict(os.environ, GH_PROMPT_DISABLED='1'))
    if result.returncode:
        raise RuntimeError(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else f'gh exited {result.returncode}')
    return json.loads(result.stdout)


def latest_finished(runs):
    """Newest run that passed or failed; cancelled and running ones say nothing about main."""
    return next((r for r in runs if r.get('status') == 'completed' and r.get('conclusion') in ('success', 'failure')), None)


def failing_steps(jobs):
    return [f"{job['name']}: {step['name']}" for job in jobs if job.get('conclusion') == 'failure'
            for step in job.get('steps', []) if step.get('conclusion') == 'failure'] \
        or [job['name'] for job in jobs if job.get('conclusion') == 'failure']


def pushes_main(lines):
    """True when a pre-push ref update creates or moves refs/heads/main (not a deletion)."""
    for line in lines:
        parts = line.split()
        if len(parts) == 4 and parts[2] == 'refs/heads/main' and set(parts[1]) != {'0'}:
            return True
    return False


def verdict(runs, jobs_for, override=False):
    """(allowed, message) for main's CI full history, newest first."""
    run = latest_finished(runs)
    if run is None or run['conclusion'] == 'success':
        return True, ''
    lines = [f"main is red: {WORKFLOW} failed at {run['headSha'][:8]} ({run.get('url', '')})"]
    lines += [f'  {step}' for step in failing_steps(jobs_for(run['databaseId']))]
    if override:
        return True, '\n'.join(lines + [f'{OVERRIDE}=1: pushing the fix or revert.'])
    return False, '\n'.join(lines + [
        'Fix or revert that failure first; pushing other work on top hides whether it is fixed.',
        f'If this push is the fix or revert, push again with {OVERRIDE}=1.'])


def check_main():
    runs = gh('run', 'list', '--workflow', WORKFLOW, '--branch', 'main', '--event', 'push', '--limit', '10',
              '--json', 'databaseId,status,conclusion,headSha,url')
    return verdict(runs, lambda run_id: gh('run', 'view', str(run_id), '--json', 'jobs')['jobs'],
                   override=os.environ.get(OVERRIDE) == '1')


def hook_path():
    hooks = subprocess.check_output(['git', 'rev-parse', '--git-path', 'hooks'], cwd=ROOT, text=True).strip()
    return (ROOT / hooks / 'pre-push').resolve()


def installed():
    path = hook_path()
    return path.is_file() and MARKER in path.read_text()


def install():
    path = hook_path()
    if path.exists() and not installed():
        print(f'{path} already exists and is not the red-main guard; add a call to tools/ci/main_health.py --pre-push to it',
              file=sys.stderr)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HOOK)
    path.chmod(0o755)
    print(f'Installed the red-main pre-push guard at {path}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--pre-push', nargs='*', metavar='ARG', help='run as the pre-push hook (ref updates on stdin)')
    action.add_argument('--install', action='store_true')
    action.add_argument('--installed', action='store_true', help='exit 0 when the hook is installed')
    args = parser.parse_args()
    if args.install:
        return install()
    if args.installed:
        raise SystemExit(0 if installed() else 1)
    if not pushes_main(sys.stdin.read().splitlines()):
        return
    try:
        allowed, message = check_main()
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f'red-main guard: could not read {WORKFLOW} status ({error}); allowing the push', file=sys.stderr)
        return
    if message:
        print(message, file=sys.stderr)
    if not allowed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
