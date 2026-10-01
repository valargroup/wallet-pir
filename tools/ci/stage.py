#!/usr/bin/env python3
"""Time one command without importing Cargo/selection dependencies.

When WALLET_PIR_STAGE_LOG is set (CI), each stage is also appended there with
its parent stage so job reports count nested wrappers once. When
WALLET_PIR_ARTIFACT_LOG is set (CI), Cargo build/check/clippy/test commands
also emit JSON messages: rendered diagnostics go to stderr, other output passes
through, and each compiler-artifact is logged with its `fresh` flag only.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'shared/dev'))
from target_lease import inherited_fds  # noqa: E402

PARENT = 'WALLET_PIR_STAGE_PARENT'
ARTIFACT_LOG = 'WALLET_PIR_ARTIFACT_LOG'
BUILDS = {'build', 'check', 'clippy', 'test'}


def with_messages(command):
    """The Cargo command with JSON messages added, or None if not a build."""
    if not command or Path(command[0]).name != 'cargo':
        return None
    subcommand = next((index for index, arg in enumerate(command[1:], 1) if not arg.startswith('-')), None)
    if subcommand is None or command[subcommand] not in BUILDS:
        return None
    end = command.index('--') if '--' in command else len(command)
    if any(arg.startswith('--message-format') for arg in command[:end]):
        return None
    return [*command[:subcommand + 1], '--message-format=json-diagnostic-rendered-ansi', *command[subcommand + 1:]]


def relay(process, log, stage):
    """Pass non-message output through; log sanitized compiler-artifact records."""
    sys.path.insert(0, str(ROOT / 'tools/ci'))
    from cargo_cache import artifact_unit
    command = uuid.uuid4().hex[:12]
    with open(log, 'a') as out:
        for line in process.stdout:
            message = None
            if line.startswith('{'):
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    pass
            if not isinstance(message, dict) or 'reason' not in message:
                sys.stdout.write(line)
                sys.stdout.flush()
            elif message['reason'] == 'compiler-message':
                sys.stderr.write((message.get('message') or {}).get('rendered') or '')
                sys.stderr.flush()
            elif (unit := artifact_unit(message)) is not None:
                out.write(json.dumps({**unit, 'stage': stage, 'command': command}) + '\n')


def run(command, *, env=None, stage='helper'):
    identifier = uuid.uuid4().hex[:12]
    child = dict(os.environ if env is None else env, **{PARENT: identifier})
    started = time.time()
    start = time.monotonic()
    print('+ ' + ' '.join(command), flush=True)
    log = os.environ.get(ARTIFACT_LOG)
    messages = with_messages(command) if log else None
    if messages:
        with subprocess.Popen(messages, cwd=ROOT, env=child, stdout=subprocess.PIPE, text=True,
                              errors='replace', **inherited_fds(child)) as process:
            relay(process, log, stage)
        result = subprocess.CompletedProcess(messages, process.returncode)
    else:
        result = subprocess.run(command, cwd=ROOT, env=child, **inherited_fds(child))
    elapsed = time.monotonic() - start
    record = {'stage': stage, 'seconds': round(elapsed, 3), 'exit': result.returncode}
    print('CHECK_STAGE ' + json.dumps(record), flush=True)
    log = os.environ.get('WALLET_PIR_STAGE_LOG')
    if log:
        with open(log, 'a') as out:
            out.write(json.dumps({**record, 'id': identifier, 'parent': os.environ.get(PARENT),
                                  'started': round(started, 3)}) + '\n')
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(f'| {stage} | {elapsed:.2f}s | {result.returncode} |\n')
    result.check_returncode()



if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[2] != '--':
        raise SystemExit('usage: stage.py <phase> -- <command>')
    run(sys.argv[3:], stage=sys.argv[1])
