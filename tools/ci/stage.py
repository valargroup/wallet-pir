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
import signal
import threading
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
COMMAND_GROUP = 'WALLET_PIR_COMMAND_GROUP'
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


_ACTIVE = {}
_PROCESS_LOCK = threading.RLock()
_CANCELLED = 0
_SPAWNING = False


def reap(groups):
    # Allow shell EXIT traps to remove owned fixtures before forcing any
    # uncooperative descendants to stop. This thread keeps cancellation bounded.
    deadline = time.monotonic() + 2
    remaining = set(groups)
    while remaining and time.monotonic() < deadline:
        for group in list(remaining):
            try:
                os.killpg(group, 0)
            except (ProcessLookupError, PermissionError):
                remaining.remove(group)
        if remaining:
            time.sleep(.05)
    for group in remaining:
        try:
            os.killpg(group, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def cancel(signum, _frame):
    """Stop owned command groups, including Cargo's compiler/test descendants."""
    global _CANCELLED
    with _PROCESS_LOCK:
        _CANCELLED = signum
        if _SPAWNING:
            return
        processes = list(_ACTIVE.values())
    # Do not re-enter Popen.wait here: the signal can interrupt its waitpid
    # lock. A separate reaper bounds cleanup after graceful signal propagation.
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    threading.Thread(target=reap, args=([p.pid for p in processes],), daemon=False).start()
    raise SystemExit(128 + signum)


def install_cancellation():
    if os.name == 'posix' and not os.environ.get(COMMAND_GROUP) and threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, cancel)


def run(command, *, env=None, stage='helper'):
    global _SPAWNING
    install_cancellation()
    identifier = uuid.uuid4().hex[:12]
    child = dict(os.environ if env is None else env, **{PARENT: identifier})
    owns_group = os.name == 'posix' and not os.environ.get(COMMAND_GROUP)
    child[COMMAND_GROUP] = '1'
    started = time.time()
    start = time.monotonic()
    print('+ ' + ' '.join(command), flush=True)
    log = os.environ.get(ARTIFACT_LOG)
    messages = with_messages(command) if log else None
    with _PROCESS_LOCK:
        if _CANCELLED:
            raise SystemExit(128 + _CANCELLED)
        # Defer a signal during main-thread Popen construction until the new
        # group has an owner record. Worker-thread construction holds the lock.
        _SPAWNING = True
        try:
            process = subprocess.Popen(messages or command, cwd=ROOT, env=child,
                                       stdout=subprocess.PIPE if messages else None,
                                       text=True, errors='replace', start_new_session=owns_group,
                                       **inherited_fds(child))
            _ACTIVE[process.pid] = process
        finally:
            _SPAWNING = False
        if _CANCELLED:
            cancel(_CANCELLED, None)
    try:
        with process:
            if messages:
                relay(process, log, stage)
            process.wait()
        result = subprocess.CompletedProcess(messages or command, process.returncode)
    finally:
        with _PROCESS_LOCK:
            _ACTIVE.pop(process.pid, None)
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
