import ctypes, json, os, resource, select, signal, sys, time
gate, notify, report, address, cpu, grace = map(int, sys.argv[1:7])
wall = float(sys.argv[7])
argv = sys.argv[9:]
stop = []
signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
for name in ('SIGHUP', 'SIGINT', 'SIGPIPE'):
    signal.signal(getattr(signal, name), signal.SIG_IGN)
libc = ctypes.CDLL(None, use_errno=True)
guardian = os.getpid()
if libc.prctl(36, 1, 0, 0, 0) != 0:
    os._exit(126)
deadline = time.monotonic() + wall
native = os.fork()
if native == 0:
    try:
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != guardian:
            os._exit(125)
        for name in ('SIGTERM', 'SIGHUP', 'SIGINT', 'SIGPIPE'):
            signal.signal(getattr(signal, name), signal.SIG_DFL)
        os.close(notify)
        os.close(report)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_AS, (address, address))
        resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu + grace))
        received = b''
        while len(received) < 3:
            data = os.read(gate, 3 - len(received))
            if not data:
                break
            received += data
        os.close(gate)
        if received == b'go\n':
            os.execve(argv[0], argv, os.environ)
    finally:
        os._exit(125)
os.close(gate)
pidfd = os.pidfd_open(native)
state = {'status': None, 'ended': None, 'ended_monotonic': None}
def stat(pid):
    try:
        with open('/proc/%d/stat' % pid) as stream:
            fields = stream.read().rsplit(')', 1)[1].split()
    except (OSError, IndexError):
        return None
    return fields[0], int(fields[1]), int(fields[19])
def reap():
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return
        if pid == native:
            state['status'], state['ended'] = status, time.time()
            state['ended_monotonic'] = time.monotonic()
def subtree():
    children = {}
    for name in os.listdir('/proc'):
        if name.isdigit():
            observed = stat(int(name))
            if observed and observed[0] != 'Z':
                children.setdefault(observed[1], []).append((int(name), observed[2]))
    found, pending = [], [guardian]
    while pending and len(found) < 4096:
        for item in children.get(pending.pop(), []):
            found.append(item)
            pending.append(item[0])
    return found
try:
    os.write(notify, b'%d\n' % native)
except OSError:
    pass
os.close(notify)
expired = stopped = False
while state['status'] is None:
    if (stop or time.monotonic() >= deadline) and not (expired or stopped):
        expired, stopped = not stop, bool(stop)
        try:
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        except ProcessLookupError:
            pass
    select.select([pidfd], [], [], .2)
    reap()
os.close(pidfd)
killed, end = {}, time.monotonic() + 10
while time.monotonic() < end:
    members = subtree()
    if not members:
        break
    for pid, start in members:
        try:
            fd = os.pidfd_open(pid)
        except OSError:
            continue
        try:
            observed = stat(pid)
            if observed and observed[2] == start:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                killed[pid, start] = {'pid': pid, 'start_ticks': start}
        except OSError:
            pass
        finally:
            os.close(fd)
    time.sleep(.05)
    reap()
time.sleep(.05)
reap()
survivors = [{'pid': pid, 'start_ticks': start} for pid, start in subtree()]
value = {'native_pid': native, 'returncode': os.waitstatus_to_exitcode(state['status']),
         'ended_unix': state['ended'], 'ended_monotonic': state['ended_monotonic'],
         'expired': expired, 'stopped': stopped,
         'killed': list(killed.values())[:64], 'killed_count': len(killed), 'survivors': survivors[:64]}
os.write(report, json.dumps(value, sort_keys=True).encode() + b'\n')
os.fsync(report)
os.close(report)
os._exit(1 if survivors else 0)
