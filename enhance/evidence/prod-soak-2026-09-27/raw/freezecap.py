"""Capture per-thread kernel wait state of a Status role while it is frozen.

Every 100 ms, probe each role's lock-free /internal/metrics. If a probe is
still unanswered after 300 ms, dump comm/state/wchan (and kernel stack when
readable) for every thread of that role, once per freeze.
"""
import os
import subprocess
import sys
import threading
import time
import urllib.request

ROLES = {'worker': 8481, 'router': 8482}


def pid_of(unit):
    out = subprocess.run(['systemctl', 'show', unit, '-p', 'MainPID', '--value'], capture_output=True, text=True)
    return int(out.stdout.strip() or 0)


def dump(role, pid, waited_ms):
    lines = [f'=== {time.strftime("%H:%M:%S", time.gmtime())} {role} pid {pid} unanswered {waited_ms} ms']
    try:
        tids = sorted(os.listdir(f'/proc/{pid}/task'), key=int)
    except OSError:
        return
    for tid in tids:
        base = f'/proc/{pid}/task/{tid}'
        try:
            comm = open(f'{base}/comm').read().strip()
            state = open(f'{base}/stat').read().rsplit(')', 1)[1].split()[0]
            wchan = open(f'{base}/wchan').read().strip() or '-'
        except OSError:
            continue
        stack = ''
        try:
            frames = [l.split(' ', 1)[-1].split('+')[0] for l in open(f'{base}/stack').read().splitlines()[:6]]
            stack = ' <- '.join(frames)
        except OSError:
            pass
        if state != 'S' or wchan not in ('futex_wait_queue', 'ep_poll', 'do_epoll_wait', 'hrtimer_nanosleep', '-'):
            lines.append(f'  {tid} {comm} {state} {wchan} {stack}')
        else:
            lines.append(f'  {tid} {comm} {state} {wchan}')
    print('\n'.join(lines), flush=True)


def watch(role, port):
    unit = f'status-{role}'
    while True:
        done = threading.Event()
        started = time.perf_counter()

        def request():
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{port}/internal/metrics', timeout=10).read()
            except Exception:
                pass
            done.set()

        threading.Thread(target=request, daemon=True).start()
        if not done.wait(0.3):
            dump(role, pid_of(unit), int((time.perf_counter() - started) * 1000))
            done.wait(10)
            print(f'--- {role} answered after {int((time.perf_counter() - started) * 1000)} ms', flush=True)
        time.sleep(0.1)


for role, port in ROLES.items():
    threading.Thread(target=watch, args=(role, port), daemon=True).start()
time.sleep(float(sys.argv[1]))
