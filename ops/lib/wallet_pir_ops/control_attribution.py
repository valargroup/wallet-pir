"""Read-only cross-host attribution of the replica reconciler's worker controls.

The installed `transparent-replica-reconciler.service` (source 4c85b6c2) runs
`/usr/local/bin/shard-control /run/transparent-pir/control.sock` on workers
over SSH. On the worker that process lives in an SSH `session-N.scope`, never
in a baseline unit cgroup, so an owner survey cannot bind it locally. Its
stdin (the operation) is not observable, so it is never labelled read-only:
this module attributes ownership only, and only by two independent kernel
observations of one TCP connection:

- worker (`controls`): an exact `shard-control` argv whose parent is root's
  login shell running exactly `-c <fixed command>` as its own session leader,
  whose parent is an sshd session process holding exactly one established TCP
  connection, all in one root SSH session scope, with no other children;
- coordinator (`authority`): the reconciler unit active with its pinned
  fragment, no drop-ins, a main process whose argv is the fragment's exact
  `ExecStart`, whose script has the pinned bytes and was last changed before
  the process started, and direct `ssh` children with the exact direct (not
  multiplexed) control argv, each holding exactly one established TCP
  connection.

`attribute` admits a worker control only when a fresh coordinator snapshot,
taken immediately before or after that worker's survey, shows a verified
client holding the reverse of the control's connection, injectively. IP
addresses, keys, names or argv never admit anything alone. Commands sent over
a shared (multiplexed) master cannot be bound to their client and therefore
stay unattributed; so does any chain or identity that differs.

Stdlib only, no package imports, never writes or signals: source bootstrap
embeds its exact text beside `owner_survey`.
"""
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import socket
import stat
import subprocess
import time

KIND = 'reconciler-control-attribution-v1'
COORDINATOR = '6a16bce88fb2493681d327344090293a'
UNIT = 'transparent-replica-reconciler.service'
FRAGMENT = Path('/etc/systemd/system/'+UNIT)
# Root-verified installed bytes; the repository template differs.
FRAGMENT_SHA256 = '869e260606f0ad020824a14c906ea6f7ac235c5832739452e027033631ca4915'
SCRIPT = Path('/opt/transparent-publisher/transparent-live-fleet.py')
SCRIPT_SOURCE = '4c85b6c20ced1e2077245491e77d3afc98bfd644'
SCRIPT_SHA256 = 'f3df5c533dc6e6f346e42a7fd7ac77dda9f00e97899440c0df813b5d6e26a083'
CONTROL = ('/usr/local/bin/shard-control', '/run/transparent-pir/control.sock')
CONTROL_COMMAND = shlex.join(CONTROL)+' || [ "$?" -eq 1 ]'
CONTROL_SHA256 = 'c5827d6ffd4521742577b98784b6f9b557b1e5724e88146716af2ae3f917dad8'
SSH = '/usr/bin/ssh'
SSHD = ('/usr/sbin/sshd', '/usr/lib/openssh/sshd-session')
SESSION = re.compile(r'0::/user\.slice/user-0\.slice/session-[0-9]{1,10}\.scope')
HOSTNAME = re.compile(r'[A-Za-z0-9_.:-]{1,253}')
ESTABLISHED = '01'
SECONDS = 60
MAX_CANDIDATES = 64
MAX_DESCRIPTORS = 4096
MAX_TABLE = 8 << 20
MAX_FILE = 1 << 20
TOLERANCE_SECONDS = 2

if hashlib.sha256(b'\0'.join(c.encode() for c in CONTROL)+b'\0').hexdigest() != CONTROL_SHA256:
    raise ValueError('fixed control argv digest differs')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def kernel(pid, proc):
    """Kernel identity; None when absent. Never a kernel thread."""
    try:
        raw = (proc/str(pid)/'stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    head, tail = raw.rsplit(')', 1)
    fields = tail.split()
    return {'pid': pid, 'state': fields[0], 'ppid': int(fields[1]), 'pgid': int(fields[2]),
            'session': int(fields[3]), 'start_ticks': int(fields[19]),
            'kernel': bool(int(fields[6]) & 0x00200000)}


def uid(pid, proc):
    """Real, effective, saved and filesystem UIDs must agree."""
    for line in (proc/str(pid)/'status').read_text().splitlines():
        if line.startswith('Uid:'):
            values = set(line.split()[1:])
            require(len(values) == 1, 'control process UIDs differ')
            return int(values.pop())
    raise ValueError('control process UID unavailable')


def command(pid, proc):
    with (proc/str(pid)/'cmdline').open('rb') as stream:
        raw = stream.read(4097)
    require(len(raw) <= 4096, 'control process argv exceeds bound')
    return raw


def executable(pid, proc):
    return os.readlink(proc/str(pid)/'exe')


def group(pid, proc):
    return (proc/str(pid)/'cgroup').read_text().strip()


def children(pid, proc):
    """Every direct child of every thread, bounded."""
    found = []
    tasks = sorted(os.listdir(proc/str(pid)/'task'))
    require(len(tasks) <= MAX_DESCRIPTORS, 'control process thread count exceeds bound')
    for task in tasks:
        with (proc/str(pid)/'task'/task/'children').open('rb') as stream:
            raw = stream.read(65537)
        require(len(raw) <= 65536, 'control process child list exceeds bound')
        found.extend(int(x) for x in raw.split())
    require(len(found) <= MAX_DESCRIPTORS, 'control process child count exceeds bound')
    return sorted(set(found))


def address(text):
    """A /proc/net/tcp{,6} endpoint as [canonical IP text, port]."""
    host, port = text.split(':')
    raw = bytes.fromhex(host)
    require(len(raw) in (4, 16), 'socket address length invalid')
    # Each 32-bit word is in host byte order.
    raw = b''.join(raw[i:i+4][::-1] for i in range(0, len(raw), 4))
    ip = ipaddress.ip_address(socket.inet_ntop(socket.AF_INET if len(raw) == 4 else socket.AF_INET6, raw))
    if ip.version == 6 and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return [str(ip), int(port, 16)]


def connection(pid, proc):
    """The single established TCP connection a process holds, from its own namespace."""
    require(os.readlink(proc/str(pid)/'ns'/'net') == os.readlink(proc/'self'/'ns'/'net'),
            'control process uses another network namespace')
    inodes = set()
    for count, entry in enumerate(os.listdir(proc/str(pid)/'fd'), 1):
        require(count <= MAX_DESCRIPTORS, 'control process descriptor count exceeds bound')
        try:
            target = os.readlink(proc/str(pid)/'fd'/entry)
        except FileNotFoundError:
            continue
        if target.startswith('socket:[') and target.endswith(']'):
            inodes.add(target[8:-1])
    found = []
    for table in ('tcp', 'tcp6'):
        with (proc/str(pid)/'net'/table).open('rb') as source:
            raw = source.read(MAX_TABLE+1)
        require(len(raw) <= MAX_TABLE, 'socket table exceeds bound')
        for row in raw.decode().splitlines()[1:]:
            fields = row.split()
            if len(fields) > 9 and fields[9] in inodes and fields[3] == ESTABLISHED:
                found.append({'local': address(fields[1]), 'remote': address(fields[2])})
    require(len(found) == 1, 'control process does not hold exactly one established TCP connection')
    return found[0]


def root_shell(passwd=None):
    shell = passwd if passwd is not None else pwd.getpwuid(0).pw_shell
    require(shell.startswith('/') and '\n' not in shell, 'root login shell invalid')
    return shell


def chain(pid, proc, shell, sessions):
    """One exact reconciler-shaped worker control, or a ValueError naming the first mismatch."""
    p = kernel(pid, proc)
    require(p is not None and p['state'] != 'Z' and not p['kernel'], 'control process absent')
    raw = command(pid, proc)
    require(raw == b'\0'.join(c.encode() for c in CONTROL)+b'\0', 'control argv differs')
    require(executable(pid, proc) == CONTROL[0], 'control executable differs')
    require(uid(pid, proc) == 0, 'control process is not root')
    scope = group(pid, proc)
    require(SESSION.fullmatch(scope) is not None, 'control process is not in a root SSH session scope')
    require(children(pid, proc) == [], 'control process has children')
    q = kernel(p['ppid'], proc)
    require(q is not None and q['state'] != 'Z' and not q['kernel'], 'control shell absent')
    require(command(q['pid'], proc) == b'\0'.join([os.path.basename(shell).encode(), b'-c', CONTROL_COMMAND.encode()])+b'\0',
            'control shell command differs')
    require(executable(q['pid'], proc) == os.path.realpath(shell), 'control shell is not the root login shell')
    require(uid(q['pid'], proc) == 0 and group(q['pid'], proc) == scope, 'control shell identity differs')
    require(q['session'] == q['pgid'] == q['pid'] and p['session'] == p['pgid'] == q['pid'],
            'control process is not in its shell session')
    require(q['start_ticks'] <= p['start_ticks'] and children(q['pid'], proc) == [pid],
            'control shell lineage differs')
    r = kernel(q['ppid'], proc)
    require(r is not None and r['state'] != 'Z' and not r['kernel'], 'control sshd session absent')
    require(executable(r['pid'], proc) in SSHD and uid(r['pid'], proc) == 0 and group(r['pid'], proc) == scope,
            'control parent is not the root sshd session')
    require(r['start_ticks'] <= q['start_ticks'] and r['session'] != q['session'], 'control sshd lineage differs')
    require(r['pid'] not in sessions, 'one sshd connection carries more than one control')
    sessions.add(r['pid'])
    link = connection(r['pid'], proc)
    for item in (p, q, r):
        require(kernel(item['pid'], proc) == item, 'control lineage changed during observation')
    return {'pid': pid, 'start_ticks': p['start_ticks'], 'exe': CONTROL[0], 'command_sha256': CONTROL_SHA256,
            'cgroup': scope, 'shell': {'pid': q['pid'], 'start_ticks': q['start_ticks']},
            'sshd': {'pid': r['pid'], 'start_ticks': r['start_ticks']}, 'connection': link}


def controls(*, proc=Path('/proc'), tick=lambda: None, shell=None):
    """Worker side: (exact control chains, rejected candidates) for every live shard-control.

    A rejected candidate keeps only its PID and the mismatch; the owner survey
    still refuses it as unattributed.
    """
    shell = root_shell(shell)
    found, rejected, sessions, candidates = [], [], set(), 0
    for entry in sorted(os.listdir(proc)):
        if not entry.isdigit():
            continue
        tick()
        pid = int(entry)
        try:
            if executable(pid, proc).removesuffix(' (deleted)') != CONTROL[0]:
                continue
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        candidates += 1
        require(candidates <= MAX_CANDIDATES, 'worker control candidates exceed bound')
        try:
            found.append(chain(pid, proc, shell, sessions))
        except (OSError, ValueError) as error:
            rejected.append({'pid': pid, 'reason': (str(error) if isinstance(error, ValueError)
                                                    else type(error).__name__)[:120]})
    return found, rejected


def properties(unit):
    keys = ('Id', 'ActiveState', 'SubState', 'MainPID', 'ControlGroup', 'FragmentPath', 'DropInPaths',
            'NeedDaemonReload')
    result = subprocess.run(['systemctl', 'show', unit, '--property='+','.join(keys)], capture_output=True,
                            timeout=5)
    require(result.returncode == 0 and len(result.stdout) <= 65536, 'reconciler unit observation failed')
    return dict(line.split('=', 1) for line in result.stdout.decode().splitlines() if '=' in line)


def pinned_bytes(path, digest, tick):
    path = Path(path)
    require(all(not p.is_symlink() for p in (path, *path.parents)), 'reconciler path contains a link')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_size <= MAX_FILE, 'reconciler file is not bounded regular')
        raw = stream.read(MAX_FILE+1)
        tick()
        after = os.fstat(stream.fileno())
    require(len(raw) <= MAX_FILE and hashlib.sha256(raw).hexdigest() == digest and
            (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and
            os.stat(path, follow_symlinks=False).st_ino == before.st_ino,
            'reconciler file differs from pinned bytes')
    return raw, after


def exec_start(fragment):
    """The single literal ExecStart argv of the pinned fragment."""
    starts = []
    for line in fragment.decode().splitlines():
        key, separator, value = line.partition('=')
        if separator and key.strip() == 'ExecStart':
            starts.append(value.strip())
    require(len(starts) == 1 and starts[0] and not any(c in starts[0] for c in '%$\\"\'') and
            starts[0][0] not in '-@+!:|', 'reconciler ExecStart is not one literal command')
    argv = starts[0].split()
    require(argv[:2] == ['/usr/bin/python3', str(SCRIPT)], 'reconciler ExecStart does not run the pinned script')
    return argv


def booted(proc):
    for line in (proc/'stat').read_text().splitlines():
        if line.startswith('btime '):
            return int(line.split()[1])
    raise ValueError('kernel boot time unavailable')


def client(pid, main, scope, proc):
    """A direct, unmultiplexed reconciler control client and its connection; None for any other child."""
    current = kernel(pid, proc)
    if current is None or current['state'] == 'Z' or current['ppid'] != main['pid']:
        return None
    require(current['start_ticks'] >= main['start_ticks'], 'reconciler child predates its parent')
    if executable(pid, proc) != SSH or uid(pid, proc) != 0 or group(pid, proc) != scope:
        return None
    try:
        argv = [a.decode() for a in command(pid, proc).split(b'\0')[:-1]]
    except (ValueError, UnicodeError):
        return None
    if (len(argv) != 13 or argv[:5] != ['ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes', '-o']
            or not argv[5].startswith('UserKnownHostsFile=/') or argv[6] != '-i' or not argv[7].startswith('/')
            or argv[8:11] != ['-oControlMaster=no', '-oControlPersist=no', '-oControlPath=none']
            or not argv[11].startswith('root@') or HOSTNAME.fullmatch(argv[11][5:]) is None
            or argv[12] != CONTROL_COMMAND):
        return None
    try:
        link = connection(pid, proc)
    except ValueError:
        # Still connecting or closing: never a client, never a refusal by itself.
        return None
    require(kernel(pid, proc) == current, 'reconciler client changed during observation')
    return {'pid': pid, 'start_ticks': current['start_ticks'], 'destination': argv[11][5:], 'connection': link}


def authority(*, proc=Path('/proc'), tick=lambda: None, show=properties, machine_path=Path('/etc/machine-id')):
    """Coordinator side: one fresh verified reconciler snapshot, or a refused status; never raises."""
    deadline = time.monotonic()+SECONDS

    def check():
        tick()
        require(time.monotonic() < deadline, 'reconciler observation deadline exceeded')
    try:
        machine = machine_path.read_text().strip()
        require(machine == COORDINATOR and os.geteuid() == 0, 'reconciler attribution requires the pinned root coordinator')
        before = show(UNIT)
        check()
        pid = int(before.get('MainPID', '0'))
        require(before.get('Id') == UNIT and before.get('ActiveState') == 'active' and
                before.get('SubState') == 'running' and before.get('ControlGroup') == '/system.slice/'+UNIT and
                before.get('FragmentPath') == str(FRAGMENT) and before.get('DropInPaths') == '' and
                before.get('NeedDaemonReload') == 'no' and pid > 1, 'reconciler unit differs from pinned identity')
        fragment, _ = pinned_bytes(FRAGMENT, FRAGMENT_SHA256, check)
        argv = exec_start(fragment)
        main = kernel(pid, proc)
        require(main is not None and main['state'] != 'Z' and not main['kernel'], 'reconciler main process absent')
        scope = '0::/system.slice/'+UNIT
        require(command(pid, proc) == b'\0'.join(a.encode() for a in argv)+b'\0' and uid(pid, proc) == 0 and
                group(pid, proc) == scope and executable(pid, proc).startswith('/usr/bin/python3'),
                'reconciler main process differs from its unit')
        _, script = pinned_bytes(SCRIPT, SCRIPT_SHA256, check)
        started = booted(proc)+main['start_ticks']/os.sysconf('SC_CLK_TCK')
        # ctime cannot be set from user space: these bytes predate the interpreter.
        require(script.st_ctime < started, 'reconciler script changed after its process started')
        clients = []
        for child in children(pid, proc):
            check()
            try:
                found = client(child, main, scope, proc)
            except (FileNotFoundError, ProcessLookupError):
                continue
            if found is not None:
                clients.append(found)
        check()
        require(kernel(pid, proc) == main and show(UNIT) == before, 'reconciler identity changed during observation')
        boot = (proc/'sys/kernel/random/boot_id').read_text().strip()
        return {'kind': KIND, 'status': 'verified', 'machine_id': machine, 'boot_id': boot, 'unit': UNIT,
                'fragment_sha256': FRAGMENT_SHA256, 'script_sha256': SCRIPT_SHA256, 'script_source': SCRIPT_SOURCE,
                'main': {'pid': pid, 'start_ticks': main['start_ticks'],
                         'command_sha256': hashlib.sha256(b'\0'.join(a.encode() for a in argv)+b'\0').hexdigest()},
                'clients': clients, 'observed_unix': time.time(), 'monotonic': time.monotonic()}
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return {'kind': KIND, 'status': 'refused', 'observed_unix': time.time(), 'monotonic': time.monotonic(),
                'reason': (str(error) if isinstance(error, ValueError) else type(error).__name__)[:200]}


def attribute(found, snapshots, *, now=None):
    """Bind every worker control to one verified fresh reconciler client by its exact reverse connection.

    `snapshots` are coordinator `authority` results taken just before and
    after the worker survey. Raises for any control without exactly one
    client identity, or two controls on one client.
    """
    now = time.monotonic() if now is None else now
    usable = [s for s in snapshots if isinstance(s, dict) and s.get('kind') == KIND and s.get('status') == 'verified'
              and s.get('machine_id') == COORDINATOR and 0 <= now-s.get('monotonic', -1e18) <= SECONDS]
    claimed, result = set(), []
    for control in found:
        link = control['connection']
        owners = {(s['main']['pid'], s['main']['start_ticks'], c['pid'], c['start_ticks'])
                  for s in usable for c in s['clients']
                  if c['connection'] == {'local': link['remote'], 'remote': link['local']}}
        require(len(owners) == 1, 'worker control %d is not attributable to a verified reconciler client' % control['pid'])
        owner = owners.pop()
        require(owner[2:] not in claimed, 'two worker controls claim one reconciler client')
        claimed.add(owner[2:])
        result.append({'control': {'pid': control['pid'], 'start_ticks': control['start_ticks']},
                       'reconciler': {'pid': owner[0], 'start_ticks': owner[1]},
                       'client': {'pid': owner[2], 'start_ticks': owner[3]}, 'connection': link})
    return result


def verify_controls(found):
    """Shape of reported worker controls; values are compared by `attribute`."""
    require(isinstance(found, list) and len(found) <= MAX_CANDIDATES, 'worker controls invalid')
    for item in found:
        require(isinstance(item, dict) and set(item) == {'pid', 'start_ticks', 'exe', 'command_sha256', 'cgroup',
                                                          'shell', 'sshd', 'connection'} and
                type(item['pid']) is int and item['pid'] > 1 and type(item['start_ticks']) is int and
                item['exe'] == CONTROL[0] and item['command_sha256'] == CONTROL_SHA256 and
                isinstance(item['cgroup'], str) and SESSION.fullmatch(item['cgroup']) is not None and
                all(isinstance(item[k], dict) and set(item[k]) == {'pid', 'start_ticks'} and
                    type(item[k]['pid']) is int and type(item[k]['start_ticks']) is int for k in ('shell', 'sshd')) and
                isinstance(item['connection'], dict) and set(item['connection']) == {'local', 'remote'} and
                all(isinstance(v, list) and len(v) == 2 and isinstance(v[0], str) and type(v[1]) is int
                    for v in item['connection'].values()),
                'worker control evidence invalid')
    require(len({i['pid'] for i in found}) == len(found) and len({i['sshd']['pid'] for i in found}) == len(found),
            'worker control evidence repeats a process')
    return found
