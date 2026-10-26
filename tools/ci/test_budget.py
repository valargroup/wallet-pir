"""Time serial libtest cases while keeping immutable fixtures in one process.

Stable Rust prints each case name before running it. Read partial lines so a
stalled case has a deadline, rather than waiting for its final newline. Test
output remains captured by libtest; no nightly flags or compiler overrides.
"""
import json
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time

_processes = set()
_lock = threading.RLock()
_cancelled = False


def kill_group(process):
    """SIGKILL the session a test executable leads. A group that is already gone is not an error: Linux reports
    ESRCH, and macOS reports EPERM once only exited (zombie or reaped) members remain. EPERM while the executable
    itself still runs is a real failure to stop it and is raised."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        if process.poll() is None:
            raise


def stop():
    global _cancelled
    with _lock:
        _cancelled = True
        for process in _processes:
            kill_group(process)


def install_cancellation():
    def cancel(number, _frame):
        stop()
        raise SystemExit(128 + number)
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, cancel)


def record(path, entry):
    if path:
        data = (json.dumps(entry) + '\n').encode()
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, data)
        finally:
            os.close(descriptor)


def case_seconds(value):
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('case budget must be a finite positive number of seconds')
    return seconds


def execute(command, *, cwd, env, seconds=None, report=None, enforce=True):
    """Run one serial test executable and refuse missing or unfinished cases."""
    seconds = case_seconds(env.get("WALLET_PIR_CASE_SECONDS", 30) if seconds is None else seconds)
    options = [*command, '--test-threads=1', '--color=never', '--format=pretty']
    listed = subprocess.check_output([*options, '--list'], cwd=cwd, env=env, text=True)
    expected = {line[:-6] for line in listed.splitlines() if line.endswith(': test')}
    # Ignored tests are still listed by libtest. They must be accounted for as
    # ignored, but must not acquire a running-case timeout.
    with _lock:
        if _cancelled:
            raise RuntimeError('test execution cancelled')
        process = subprocess.Popen(options, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        _processes.add(process)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    pending = ''
    active = None
    completed = set()
    started = time.monotonic()
    previous = started
    failed = False
    timed_out = False

    def terminate(_signal=None, _frame=None):
        kill_group(process)
        if _signal is not None:
            raise SystemExit(128 + _signal)

    # The caller owns this executable; stop its descendants on cancellation too.
    signals = {}
    if threading.current_thread() is threading.main_thread():
        for number in (signal.SIGTERM, signal.SIGINT):
            signals[number] = signal.signal(number, terminate)
    try:
        while selector.get_map():
            now = time.monotonic()
            if enforce and now - (active[1] if active else previous) >= seconds:
                timed_out = True
                print(f'Case deadline exceeded: {active[0] if active else command[0]}', flush=True)
                terminate()
            for key, _ in selector.select(timeout=.05):
                data = os.read(key.fd, 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                sys.stdout.write(data.decode(errors='replace'))
                sys.stdout.flush()
                pending += data.decode(errors='replace')
                while True:
                    if active is None:
                        match = re.search(r'(?:^|\n)test ([^\n]+?) \.\.\. ', pending)
                        if match is None:
                            break
                        name = match[1].removesuffix(' - should panic')
                        if name not in expected or name in completed:
                            raise ValueError(f'unexpected or repeated test: {name}')
                        active = (name, time.monotonic())
                        pending = pending[match.end():]
                    match = re.search(r'(?:^|\n)(ok|FAILED|ignored)(?:[^\n]*)\n', pending)
                    if match is None:
                        break
                    now = time.monotonic()
                    name, began = active
                    elapsed = now - began
                    status = match[1]
                    record(report, dict(test=name, executable=os.path.basename(command[0]),
                                        seconds=round(elapsed, 6), status=status))
                    failed |= status == 'FAILED' or (enforce and status != 'ignored' and elapsed >= seconds)
                    completed.add(name)
                    pending = pending[match.end():]
                    active = None
                    previous = now
        code = process.wait()
        if timed_out or code or failed or active or completed != expected:
            missing = sorted(expected - completed)
            raise RuntimeError(f'test program failed: exit={code}, timeout={timed_out}, missing={missing}')
        return len(completed)
    finally:
        terminate()
        process.wait()
        process.stdout.close()
        selector.close()
        with _lock:
            _processes.discard(process)
        for number, handler in signals.items():
            signal.signal(number, handler)
