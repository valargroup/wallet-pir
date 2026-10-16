#!/usr/bin/env python3
"""Include changes from canceled or failed main runs in the next fast check."""
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ops/lib'))
from wallet_pir_ops.github_api import api


def comparison(event, base, repo):
    if event != 'push':
        return base
    runs = api(f'repos/{repo}/actions/workflows/ci.yml/runs?branch=main&event=push&status=success&per_page=1',
               timeout=3, attempts=2)['workflow_runs']
    if not runs:
        return ''  # Missing history selects all Rust and helpers.
    run = runs[0]
    sha = run['head_sha']
    if (run['conclusion'] != 'success' or run['event'] != 'push' or run['head_branch'] != 'main'
            or not re.fullmatch('[0-9a-f]{40}', sha)):
        raise ValueError('invalid successful-main comparison')
    return sha


if __name__ == '__main__':
    base = comparison(os.environ['GITHUB_EVENT_NAME'], os.environ.get('BASE', ''),
                      os.environ['GITHUB_REPOSITORY'])
    with open(os.environ['GITHUB_ENV'], 'a') as out:
        out.write('FAST_BASE=' + base + '\n')
    print(base)
