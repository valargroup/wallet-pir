"""Immutable full-journal snapshot: lock protocol, quiescence, copy and recovery.

Real writer processes hold the existing `writer.lock` with `flock`, which the
Rust probe below shows is the native `File::try_lock` protocol. A file-backed
fake systemd starts and stops them, a fixture node answers canonical anchors and
a real flock stands in for the production lock. Owners run in-process or as real
detached processes. Nothing contacts a host or touches production paths.
"""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
from wallet_pir_ops import hostlock, inherited_lock  # noqa: E402
import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location('journal_snapshot_test', HERE.parent/'lib/activity_journal_snapshot.py')
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)
REAL_FENCE = S.schema_fence.local_schema_fence
MACHINE = 'c'*32
WORKER = 'd'*32
PYTHON = os.path.realpath(sys.executable)
UNIT = 'fixture-journal-writer.service'
THROUGH = 3
BLOCKS = 8

# Shared by the fixture builder and the real writer processes.
JOURNAL_CODE = r'''
import hashlib, os
from pathlib import Path

def internal(height, salt=b'block'):
    return hashlib.sha256(salt+str(height).encode()).digest()

def display(height, salt=b'block'):
    return internal(height, salt)[::-1].hex()

def entries(height):
    data = b''
    for i in range(height % 3):
        script = bytes([0x76, 0xa9, 0x14, height % 251, i])
        data += len(script).to_bytes(2, 'little')+script+(87).to_bytes(2, 'little')+bytes([i])*87
    return data

def sidecar(raw):
    body = b'TPIRTX01'+raw+(0).to_bytes(4, 'little')+b'\x00'
    return body+hashlib.sha256(body).digest()

def lengths(journal):
    path = Path(journal)/'checkpoint.bin'
    if not path.exists():
        return 0, 0
    data = path.read_bytes()
    return int.from_bytes(data[:8], 'little'), int.from_bytes(data[8:], 'little')

def commit(journal, events, blocks):
    tmp = Path(journal)/'checkpoint.bin.tmp'
    with open(tmp, 'wb') as out:
        out.write(events.to_bytes(8, 'little')+blocks.to_bytes(8, 'little')); out.flush(); os.fsync(out.fileno())
    os.replace(tmp, Path(journal)/'checkpoint.bin')

def append(journal, salt=b'block'):
    journal = Path(journal)
    events, blocks = lengths(journal)
    for name, length in (('events.bin', events), ('blocks.bin', blocks)):
        with open(journal/name, 'ab') as out:
            out.truncate(length)
    height = blocks//48
    raw = internal(height, salt)
    data = entries(height)
    with open(journal/'events.bin', 'ab') as out:
        out.write(data); out.flush(); os.fsync(out.fileno())
    with open(journal/'blocks.bin', 'ab') as out:
        out.write(raw+events.to_bytes(8, 'little')+(height % 3).to_bytes(8, 'little')); out.flush(); os.fsync(out.fileno())
    if height % 2 == 0:
        (journal/'display-v1').mkdir(exist_ok=True)
        (journal/'display-v1'/(raw[::-1].hex()+'.bin')).write_bytes(sidecar(raw))
    commit(journal, events+len(data), blocks+48)

def rollback(journal, keep):
    journal = Path(journal)
    events, blocks = lengths(journal)
    raw = (journal/'blocks.bin').read_bytes()
    offset = int.from_bytes(raw[keep*48+32:keep*48+40], 'little')
    commit(journal, offset, keep*48)
    for name, length in (('events.bin', offset), ('blocks.bin', keep*48)):
        with open(journal/name, 'ab') as out:
            out.truncate(length)
'''
exec(JOURNAL_CODE, globals())

WRITER = JOURNAL_CODE+r'''
import fcntl, signal, sys, time
journal, ready, mode = sys.argv[1:4]
stop = [False]
def term(*_):
    stop[0] = True
signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == 'stubborn' else term)
# EventStore::open: create(true), truncate(false), read, write, then try_lock.
fd = os.open(os.path.join(journal, 'writer.lock'), os.O_RDWR | os.O_CREAT, 0o600)
if mode != 'nolock':
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(2)
Path(ready).write_text(str(os.getpid()))
while not stop[0]:
    if mode == 'append':
        append(journal)
    time.sleep(.02)
if mode in ('reorg', 'reorg-longer'):
    _, blocks = lengths(journal)
    rollback(journal, blocks//48-3)
    for _ in range(1 if mode == 'reorg' else 4):
        append(journal, b'fork')
'''

LAUNCH = r'''
import json, os, subprocess, sys
argv, ready = json.loads(sys.argv[1]), sys.argv[2]
child = subprocess.Popen(argv, start_new_session=True, stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print(child.pid)
'''

CHILD = r'''
import json, sys
config = json.loads(open(sys.argv[1]).read())
sys.path.insert(0, config['tests'])
import test_activity_journal_snapshot as T
T.configure(config, setattr)
sys.exit(T.entry(sys.argv[2], sys.argv[3], config))
'''


def holder(path, mode='LOCK_EX'):
    """A real process holding `flock(mode)` on `path` until its stdin closes."""
    process = subprocess.Popen([PYTHON, '-c', 'import fcntl,os,sys;f=os.open(sys.argv[1],os.O_RDONLY);'
                                'fcntl.flock(f,fcntl.%s);print(1,flush=True);sys.stdin.read()' % mode, str(path)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    process.stdout.readline(); process.stdout.close()
    return process


def release(process):
    if process.poll() is None:
        process.stdin.close(); process.wait(5)
    elif not process.stdin.closed:
        process.stdin.close()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def alive(pid):
    try:
        state = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')', 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return False
    return state not in ('Z', 'X')


class FakeSystemd:
    """File-backed unit state; start/stop run and signal real writer processes."""

    def __init__(self, root):
        self.root = Path(root)
        self.file = self.root/'systemd.json'

    def state(self):
        return json.loads(self.file.read_text())

    def write(self, state):
        tmp = self.file.with_suffix('.tmp')
        tmp.write_text(json.dumps(state)); os.replace(tmp, self.file)

    def show(self, unit):
        state = self.state()
        assert unit == state['unit']
        if state['active'] == 'active' and not alive(state['pid']):
            state.update(active='failed', pid=0); self.write(state)
        return {'ActiveState':state['active'], 'SubState':'running' if state['active'] == 'active' else 'dead',
                'MainPID':str(state['pid']), 'NRestarts':'0', 'FragmentPath':state['fragment'],
                'DropInPaths':' '.join(state['drop_ins']), 'ControlGroup':'/fixture.slice/'+unit,
                'NeedDaemonReload':'no', **{k:'' for k in S.INTERPLAY}, **state.get('interplay', {})}

    def pids(self, group):
        state = self.state()
        return {state['pid']} if state['pid'] and alive(state['pid']) else set()

    def stop(self, unit, timeout):
        state = self.state()
        if state.get('stop_error'):
            raise subprocess.CalledProcessError(1, ['systemctl', 'stop', unit])
        if state['active'] == 'active':
            os.kill(state['pid'], signal.SIGTERM)
            deadline = time.monotonic()+min(timeout, 2)
            while alive(state['pid']) and time.monotonic() < deadline:
                time.sleep(.02)
            if alive(state['pid']):
                raise subprocess.TimeoutExpired(['systemctl', 'stop', unit], timeout)
            state.update(active='inactive', pid=0); self.write(state)

    def start(self, unit, timeout):
        state = self.state()
        if state.get('start_errors', 0) > 0:
            state['start_errors'] -= 1; self.write(state)
            raise subprocess.CalledProcessError(1, ['systemctl', 'start', unit])
        if state['active'] == 'active':
            return
        ready = self.root/('ready-%d' % time.monotonic_ns())
        argv = [PYTHON, '-B', str(self.root/'writer.py'), str(self.root/'journal'), str(ready), state['mode']]
        pid = int(subprocess.run([PYTHON, '-B', '-c', LAUNCH, json.dumps(argv), str(ready)],
                                 capture_output=True, check=True).stdout)
        deadline = time.monotonic()+5
        while not ready.exists() and alive(pid) and time.monotonic() < deadline:
            time.sleep(.01)
        state.update(active='active' if alive(pid) else 'failed', pid=pid if alive(pid) else 0)
        self.write(state)


class FakeExecutor:
    """Pinned SSH stand-in: every remote host is a private tree under hosts/<name>."""

    def __init__(self, root):
        self.root = Path(root)

    def call(self, host, op, deadline=120, **arguments):
        assert 0 < deadline
        failure = self.root/('hosts/%s.unreachable' % host)
        if failure.exists():
            raise S.RemoteError('%s: %s failed (exit 255): connection refused' % (host, op))
        assert op == 'ownership_probe' and set(arguments) == {'roots', 'lock_path', 'machine_id_path', 'classes'}
        base = self.root/'hosts'/host
        local = lambda path: str(base/str(path).lstrip('/'))
        probe = S.host_helper.ownership_probe([local(r) for r in arguments['roots']], local(arguments['lock_path']),
                                              local(arguments['machine_id_path']), arguments['classes'], proc=str(base/'proc'))
        # Names as the remote host reports them, in its own namespace.
        probe['records'] = {'/'+str(Path(k).relative_to(base)):v for k, v in probe['records'].items()}
        probe['files'] = [['/'+str(Path(k).relative_to(base)), size, kind] for k, size, kind in probe['files']]
        return json.loads(json.dumps(probe))


BOOT = int(time.time())-100000
TICKS = os.sysconf('SC_CLK_TCK')
BOOT_ID = '6f0c1d6e-0000-4000-8000-000000000001'
EARLIER_BOOT_ID = '6f0c1d6e-0000-4000-8000-000000000000'


def fake_proc(proc):
    """An empty `/proc` tree: boot time and lock table only."""
    proc.mkdir(parents=True, exist_ok=True)
    (proc/'stat').write_text('cpu 0 0 0 0\nbtime %d\n' % BOOT)
    (proc/'locks').touch()
    (proc/'sys/kernel/random').mkdir(parents=True, exist_ok=True)
    (proc/'sys/kernel/random/boot_id').write_text(BOOT_ID+'\n')


def fake_process(proc, pid, *, started, ppid=1, pgrp=None, session=None, state='S', lock=None,
                 inherited=False, wrapper=False, unreadable=False, exe='/usr/bin/sleep', argv=None,
                 cgroup='/user.slice/session-1.scope'):
    base = proc/str(pid)
    (base/'fd').mkdir(parents=True)
    fields = [state, str(ppid), str(pgrp or pid), str(session or pid), *['0']*15, str(int((started-BOOT)*TICKS)), *['0']*5]
    (base/'stat').write_text('%d (fake) %s\n' % (pid, ' '.join(fields)))
    if lock is not None:
        os.symlink(lock, base/'fd'/'3')
    (base/'environ').write_bytes(b'PATH=/usr/bin\0'+(b'WALLET_PIR_PRODUCTION_LOCK_FDS=3\0' if inherited else b''))
    argv = argv or ['/usr/bin/python3', '/srv/s/ops/scripts/wallet-pir-deploy.py' if wrapper else 'native']
    (base/'cmdline').write_bytes(b''.join(a.encode()+b'\0' for a in argv))
    os.symlink(exe, base/'exe')
    (base/'cgroup').write_text('0::%s\n' % cgroup)
    if unreadable:
        (base/'fd').chmod(0)
    return base


class FakeNode:
    def __init__(self, root):
        self.file = Path(root)/'node.json'

    def block_hash(self, height, budget):
        assert budget.remaining() > 0
        overrides = json.loads(self.file.read_text()) if self.file.exists() else {}
        return overrides.get(str(height), display(height))


def configure(config, assign):
    """Point the module at the fixture; `assign` is setattr or a patcher."""
    root = Path(config['root'])
    for name, value in (('JOURNAL', root/'journal'), ('SNAPSHOTS', root/'snapshots/journal'),
                        ('OWNERS', root/'owners'), ('MAP', root/'publication/shards.json'),
                        ('RESULT', root/'publication/result.json'), ('CUTOFF', root/'publication/cutoff.json'),
                        ('MEMINFO', root/'meminfo'), ('OWNER', os.getuid()), ('statvfs', disk(root)),
                        ('SOURCE', root/'ops/sources'), ('MACHINE_ID', root/'machine-id'),
                        ('SSHExecutor', lambda inventory: FakeExecutor(root)), ('FLEET_PROC', root/'proc')):
        assign(S, name, value)
    assign(S.P, 'THROUGH', THROUGH)
    assign(S.schema_fence, 'INPUT_STAGING', root/'owners')
    assign(S.schema_fence, 'HOST_ACTIONS', root/'fence/host-actions')
    assign(S.schema_fence, 'local_schema_fence', lambda **options: REAL_FENCE(root/'fence/schema', **options))
    lock = type('FixtureLock', (hostlock.PinnedHostLock,),
                {'PATH':root/'production.lock', 'MACHINE_ID':root/'machine-id', 'ROOT_UID':os.getuid()})
    assign(S, 'ProductionLock', lock)
    assign(S.Snapshot, 'identity', lambda self: None)
    config_path = root/'config.json'
    assign(S.Snapshot, 'owner_command', lambda self: [PYTHON, '-B', str(root/'child.py'), str(config_path), 'owner', self.identifier])


def disk(root):
    """statvfs from `disk.json` when present; a fail-after counter models a floor crossing."""
    calls = [0]

    def statvfs(path):
        file = Path(root)/'disk.json'
        value = json.loads(file.read_text()) if file.exists() else {}
        calls[0] += 1
        bavail = value.get('bavail', 900)
        if 'fail_after' in value and calls[0] > value['fail_after']:
            bavail = 1
        return SimpleNamespace(f_bavail=bavail, f_blocks=value.get('blocks', 1000), f_frsize=1 << 20)
    return statvfs


def delayed(config):
    delay = config.get('delay')
    if delay:
        real = S.copy_file

        def copy_file(*args):
            time.sleep(delay)
            return real(*args)
        S.copy_file = copy_file


def entry(action, expected, config):
    root = Path(config['root'])
    delayed(config)
    systemd, node = FakeSystemd(root), FakeNode(root)
    try:
        if action == 'owner':
            S.owner_main(expected, systemd=systemd, node=node)
        else:
            request = json.loads((root/'request.json').read_text())
            inventory = S.descriptors.load_inventory(root/'inventory.json')
            snapshot = S.Snapshot(inventory, request, expected, systemd=systemd, node=node)
            snapshot.stage(config['plan'])
    except S.Interrupted:
        return 75
    except BaseException as error:
        print(type(error).__name__, error, flush=True)
        return 1
    return 0


class Fixture(unittest.TestCase):
    bounds = {'precopy_seconds':30, 'stop_seconds':5, 'copy_seconds':30, 'restart_seconds':5, 'restart_attempts':2,
              'total_seconds':200}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.root.chmod(0o700)
        self.config = {'root':str(self.root), 'tests':str(HERE)}

        def patcher(thing, name, value):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        configure(self.config, patcher)
        fake_proc(self.root/'proc')
        fake_proc(self.root/'hosts/worker-01/proc')
        (self.root/'config.json').write_text(json.dumps(self.config))
        (self.root/'child.py').write_text(CHILD)
        (self.root/'writer.py').write_text(WRITER)
        (self.root/'machine-id').write_text(MACHINE+'\n')
        (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 500 kB\n')
        self.journal = self.root/'journal'; self.journal.mkdir()
        (self.journal/'meta.json').write_text(json.dumps({'version':3, 'genesis_hash':display(0), 'start_height':0}, indent=2))
        (self.journal/'writer.lock').touch(mode=0o600)
        for _ in range(BLOCKS):
            append(self.journal)
        # An uncommitted append past the checkpoint, with its unreachable sidecar.
        events, blocks = lengths(self.journal)
        tail = internal(BLOCKS, b'tail')
        with open(self.journal/'events.bin', 'ab') as out: out.write(b'\xff'*200)
        with open(self.journal/'blocks.bin', 'ab') as out: out.write(tail+events.to_bytes(8, 'little')+(2).to_bytes(8, 'little'))
        (self.journal/'display-v1'/(tail[::-1].hex()+'.bin')).write_bytes(sidecar(tail))
        publication = self.root/'publication'; publication.mkdir()
        (publication/'shards.json').write_bytes(b'{"shards":[]}')
        (publication/'result.json').write_text(json.dumps({'status':'passed', 'map_sha256':sha(publication/'shards.json')}))
        (publication/'cutoff.json').write_text(json.dumps({'anchor':{'height':THROUGH, 'hash':display(THROUGH)}}))
        units = self.root/'units'; (units/'writer.service.d').mkdir(parents=True)
        self.fragment = units/'writer.service'
        self.fragment.write_text('[Service]\nExecStart=%s -B %s %s\n' % (PYTHON, self.root/'writer.py', self.journal))
        self.dropin = units/'writer.service.d/10-nice.conf'
        self.dropin.write_text('[Service]\nNice=10\n')
        self.systemd = FakeSystemd(self.root)
        self.systemd.write({'unit':UNIT, 'active':'inactive', 'pid':0, 'fragment':str(self.fragment),
                            'drop_ins':[str(self.dropin)], 'mode':'idle'})
        self.addCleanup(self.kill_writer)
        self.node = FakeNode(self.root)
        self.start_writer()
        self.worker = self.root/'hosts/worker-01'
        self.remote(S.MACHINE_ID).parent.mkdir(parents=True)
        self.remote(S.MACHINE_ID).write_text(WORKER+'\n')
        self.inventory = S.descriptors.load_inventory(self.write_inventory())

    def write_inventory(self, **hosts):
        path = self.root/'inventory.json'
        path.write_text(json.dumps({'hosts':{'coordinator':{'machine_id':MACHINE}, 'worker-01':{'machine_id':WORKER}, **hosts},
                                    'ssh':{'mode':'config'}, 'lock':{'type':'pinned_host', 'machine_id':MACHINE},
                                    'services':{}}))
        return path

    def remote(self, path):
        """The worker's copy of an absolute production-shaped path."""
        return self.worker/str(path).lstrip('/')

    def kill_writer(self):
        pid = self.systemd.state()['pid']
        if pid and alive(pid):
            os.kill(pid, signal.SIGKILL)
            deadline = time.monotonic()+5
            while alive(pid) and time.monotonic() < deadline:
                time.sleep(.01)

    def start_writer(self, mode='idle'):
        if self.systemd.state()['active'] == 'active':
            self.systemd.stop(UNIT, 5)
        state = self.systemd.state(); state['mode'] = mode; self.systemd.write(state)
        self.systemd.start(UNIT, 5)
        state = self.systemd.state()
        self.assertEqual(state['active'], 'active')
        return state['pid']

    def request(self, **changes):
        pid = self.systemd.state()['pid']
        request = {'version':1, 'kind':S.KIND, 'source_sha':'e'*40, 'attempt':1, 'machine_id':MACHINE,
                   'candidate':{'source_sha':S.C.SOURCE_SHA, 'identity':S.C.identity()},
                   'publication':{'map_sha256':sha(self.root/'publication/shards.json'), 'anchor_height':THROUGH,
                                  'anchor_hash':display(THROUGH)},
                   'journal':{'version':3, 'genesis_hash':display(0), 'start_height':0},
                   'sidecars':{'policy':'mirror-source', 'coverage_heights':[0, 2]},
                   'writer':{'unit':UNIT, 'fragment_path':str(self.fragment), 'fragment_sha256':sha(self.fragment),
                             'drop_ins':[{'path':str(self.dropin), 'sha256':sha(self.dropin)}],
                             'binary_path':PYTHON, 'binary_sha256':sha(PYTHON), 'main_pid':pid,
                             'process_start':S.process_start(pid)},
                   'bounds':dict(self.bounds)}
        for key, value in changes.items():
            if isinstance(value, dict) and isinstance(request.get(key), dict):
                request[key] = {**request[key], **value}
            else:
                request[key] = value
        return request

    def snapshot(self, request=None):
        request = request or self.request()
        (self.root/'request.json').write_text(json.dumps(request))
        return S.Snapshot(self.inventory, request, S.digest(request), systemd=self.systemd, node=self.node)

    def plan(self, snapshot):
        return S.digest(snapshot.stable_plan())

    def inprocess(self, snapshot, before_owner=None):
        """Real stage intent, then the owner in this process under an inherited lock."""
        with patch.object(S.Snapshot, 'owner_command', lambda self: [PYTHON, '-c', 'pass']):
            with self.assertRaisesRegex(ValueError, 'ended running'):
                snapshot.stage(self.plan(snapshot))
        if before_owner:
            before_owner()
        with S.ProductionLock({'type':'pinned_host', 'machine_id':MACHINE}) as lock:
            with patch.dict(os.environ, {inherited_lock.VARIABLE:str(lock.fd)}):
                return snapshot.run_owner()

    def record(self, snapshot):
        return json.loads(snapshot.owner.read_text())

    def reconcile(self, snapshot):
        """In-process owners ran in this test process, which has since left them."""
        real = S.process_active
        with patch.object(S, 'process_active', lambda pid, start: pid != os.getpid() and real(pid, start)):
            return snapshot.reconcile()

    def fenced(self):
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            S.schema_fence.local_schema_fence()

    def writer_proof(self, snapshot, pid=None):
        state = self.systemd.state()
        self.assertEqual(state['active'], 'active')
        if pid is not None:
            self.assertEqual(state['pid'], pid)
        return snapshot.running(state['pid'], S.process_start(state['pid']))

    def wait(self, predicate, seconds=30):
        deadline = time.monotonic()+seconds
        while not predicate():
            self.assertLess(time.monotonic(), deadline, 'fixture condition timed out')
            time.sleep(.05)

    def spawn_parent(self, delay=None, plan=None, snapshot=None):
        snapshot = snapshot or self.snapshot()
        self.config.update(plan=plan or self.plan(snapshot), delay=delay)
        (self.root/'config.json').write_text(json.dumps(self.config))
        with open(self.root/'parent.log', 'ab') as log:
            parent = subprocess.Popen([PYTHON, '-B', str(self.root/'child.py'), str(self.root/'config.json'), 'parent',
                                       snapshot.identifier], stdout=log, stderr=subprocess.STDOUT)
        self.addCleanup(lambda: parent.poll() is None and (parent.kill(), parent.wait()))
        return snapshot, parent

    def phase(self, snapshot):
        try:
            return self.record(snapshot)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class LockProtocol(Fixture):
    def test_rust_try_lock_is_the_flock_this_module_proves_and_takes(self):
        rustc = shutil.which('rustc')
        if rustc is None:
            self.skipTest('rustc unavailable; the Python writer uses the same flock call')
        source = self.root/'probe.rs'
        source.write_text('''use std::io::Read;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let file = std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(&path).unwrap();
    if file.try_lock().is_err() { println!("refused"); std::process::exit(3); }
    println!("locked");
    let mut byte = [0u8; 1];
    let _ = std::io::stdin().read(&mut byte);
}
''')
        subprocess.run([rustc, '-O', str(source), '-o', str(self.root/'probe')], check=True, capture_output=True)
        self.kill_writer()
        self.wait(lambda: not S.flock_holders((self.journal/'writer.lock').stat()))
        probe = subprocess.Popen([str(self.root/'probe'), str(self.journal/'writer.lock')], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, text=True)
        self.addCleanup(probe.stdout.close)
        self.assertEqual(probe.stdout.readline().strip(), 'locked')
        fd, info = S.open_writer_lock()
        try:
            proof = S.lock_proof(fd, info, probe.pid)
            self.assertEqual((proof['class'], proof['access'], proof['contended']), ('FLOCK', 'WRITE', True))
            with self.assertRaisesRegex(ValueError, 'not held exclusively by the reviewed writer'):
                S.lock_proof(fd, info, os.getpid())
            with self.assertRaises(S.Foreign):
                S.acquire_writer_lock(fd, info)
            probe.stdin.close(); probe.wait(5)
            S.acquire_writer_lock(fd, info)
            refused = subprocess.run([str(self.root/'probe'), str(self.journal/'writer.lock')], stdin=subprocess.DEVNULL,
                                     capture_output=True, text=True)
            self.assertEqual((refused.returncode, refused.stdout.strip()), (3, 'refused'))
        finally:
            os.close(fd)

    def test_writer_lock_is_never_created_followed_or_shared(self):
        pid = self.systemd.state()['pid']
        fd, info = S.open_writer_lock()
        try:
            S.lock_proof(fd, info, pid)
        finally:
            os.close(fd)
        self.kill_writer(); self.wait(lambda: not alive(pid))
        (self.journal/'writer.lock').unlink()
        with self.assertRaises(FileNotFoundError):
            S.open_writer_lock()
        self.assertFalse((self.journal/'writer.lock').exists())
        (self.root/'elsewhere.lock').touch()
        (self.journal/'writer.lock').symlink_to(self.root/'elsewhere.lock')
        with self.assertRaises(OSError):
            S.open_writer_lock()
        (self.journal/'writer.lock').unlink()
        (self.journal/'writer.lock').touch(mode=0o600)
        os.link(self.journal/'writer.lock', self.root/'alias.lock')
        with self.assertRaisesRegex(ValueError, 'single-link'):
            S.open_writer_lock()

    def test_shared_unlisted_or_foreign_holders_refuse_without_taking_the_lock(self):
        self.kill_writer()
        self.wait(lambda: not S.flock_holders((self.journal/'writer.lock').stat()))
        fd, info = S.open_writer_lock()
        try:
            with self.assertRaisesRegex(ValueError, '0 holder'):
                S.lock_proof(fd, info, 12345)
            # Refusal never leaves this process holding the lock.
            self.assertEqual(S.flock_holders(info), [])
            shared = holder(self.journal/'writer.lock', 'LOCK_SH')
            with self.assertRaisesRegex(ValueError, 'not held exclusively'):
                S.lock_proof(fd, info, shared.pid)
            release(shared)
        finally:
            os.close(fd)


    def test_a_listed_holder_that_does_not_contend_refuses(self):
        # A POSIX record lock (fcntl) is invisible to flock: if anything ever
        # listed the writer while flock still succeeded, the protocols differ.
        self.kill_writer()
        posix = subprocess.Popen([PYTHON, '-c', 'import fcntl,os,sys;f=os.open(sys.argv[1],os.O_RDWR);'
                                  'fcntl.lockf(f,fcntl.LOCK_EX);print(1,flush=True);sys.stdin.read()',
                                  str(self.journal/'writer.lock')], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        posix.stdout.readline(); posix.stdout.close()
        self.addCleanup(release, posix)
        fd, info = S.open_writer_lock()
        try:
            self.assertEqual([h[0] for h in S.flock_holders(info)], ['POSIX'])
            with self.assertRaisesRegex(ValueError, 'not held exclusively'):
                S.lock_proof(fd, info, posix.pid)
            listed = [('FLOCK', 'ADVISORY', 'WRITE', posix.pid)]
            with patch.object(S, 'flock_holders', lambda _: listed), patch.object(S, 'holds_open', lambda *_: True):
                with self.assertRaisesRegex(ValueError, 'was not contended'):
                    S.lock_proof(fd, info, posix.pid)
            self.assertEqual([h[0] for h in S.flock_holders(info)], ['POSIX'])  # Released again.
        finally:
            os.close(fd)


class Request(Fixture):
    def test_request_is_closed_and_root_supplies_every_bound(self):
        request = self.request()
        self.assertEqual(S.validate(request), request)
        bad = []
        for key in S.BOUNDS:
            value = self.request(); del value['bounds'][key]; bad.append(value)
            value = self.request(); value['bounds'][key] = 0; bad.append(value)
            value = self.request(); value['bounds'][key] = 1.5; bad.append(value)
        bad += [self.request(extra=1), self.request(bounds={'total_seconds':10}),
                self.request(candidate={'identity':'0'*64}), self.request(publication={'anchor_height':THROUGH+1}),
                self.request(journal={'version':2}), self.request(journal={'start_height':1}),
                self.request(writer={'binary_path':'/usr/../bin/x'}), self.request(writer={'unit':'../x.service'}),
                self.request(writer={'main_pid':True}), self.request(sidecars={'policy':'skip'}),
                self.request(sidecars={'coverage_heights':[2, 0]}), self.request(sidecars={'coverage_heights':[1, 1]}),
                self.request(sidecars={'coverage_heights':[-1]}), self.request(sidecars={'extra':1}),
                self.request(writer={'drop_ins':[{'path':'/b/20.conf', 'sha256':'0'*64}, {'path':'/a/10.conf', 'sha256':'0'*64}]})]
        for value in bad:
            with self.assertRaises(ValueError):
                S.validate(value)
        with self.assertRaisesRegex(ValueError, 'reviewed digest'):
            S.Snapshot(self.inventory, request, '0'*64)


class Format(Fixture):
    def committed(self):
        return S.committed(self.journal, self.request())

    def test_committed_prefix_ignores_the_uncommitted_tail(self):
        summary = self.committed()
        self.assertEqual((summary['blocks'], summary['tip_height'], summary['tip_hash']), (BLOCKS, BLOCKS-1, display(BLOCKS-1)))
        self.assertGreater(summary['file_bytes']['events.bin'], summary['events_bytes'])
        self.assertEqual(summary['file_bytes']['blocks.bin'], (BLOCKS+1)*48)
        with self.assertRaisesRegex(ValueError, 'differs from its checkpoint'):
            S.committed(self.journal, self.request(), exact=True)

    def mutate(self, name, data, message):
        original = (self.journal/name).read_bytes()
        (self.journal/name).write_bytes(data(original))
        try:
            with self.assertRaisesRegex(ValueError, message):
                self.committed()
        finally:
            (self.journal/name).write_bytes(original)

    def test_malformed_metadata_and_checkpoints_refuse(self):
        meta = lambda **v: json.dumps({'version':3, 'genesis_hash':display(0), 'start_height':0, **v}).encode()
        self.mutate('meta.json', lambda _: meta(version=2), 'format, genesis or start height')
        self.mutate('meta.json', lambda _: meta(genesis_hash='0'*64), 'format, genesis or start height')
        self.mutate('meta.json', lambda _: meta(start_height=1), 'format, genesis or start height')
        self.mutate('meta.json', lambda _: meta(extra=1), 'format, genesis or start height')
        self.mutate('meta.json', lambda _: b' '*5000, 'bounded single-link')
        self.mutate('checkpoint.bin', lambda raw: raw[:15], 'not 16 bytes')
        self.mutate('checkpoint.bin', lambda raw: raw+b'\x00', 'bounded single-link')
        self.mutate('checkpoint.bin', lambda raw: raw[:8]+(BLOCKS*48-1).to_bytes(8, 'little'), 'complete 48-byte')
        self.mutate('checkpoint.bin', lambda raw: raw[:8]+(0).to_bytes(8, 'little'), 'complete 48-byte')
        self.mutate('checkpoint.bin', lambda raw: (10**9).to_bytes(8, 'little')+raw[8:], 'events.bin is shorter')
        self.mutate('checkpoint.bin', lambda raw: raw[:8]+(48*50).to_bytes(8, 'little'), 'blocks.bin is shorter')
        self.mutate('blocks.bin', lambda raw: raw[:(BLOCKS-1)*48+40], 'blocks.bin is shorter')
        last = (BLOCKS-1)*48
        self.mutate('blocks.bin', lambda raw: raw[:last+32]+(10**9).to_bytes(8, 'little')+raw[last+40:], 'past the committed')

    def records(self, raw, events):
        records = S.Records(events)
        for at in range(0, len(raw), 48):
            records.feed(raw[at:at+48])
        return records.finish()

    def test_record_boundaries(self):
        events, blocks = lengths(self.journal)
        raw = (self.journal/'blocks.bin').read_bytes()[:blocks]
        result = self.records(raw, events)
        self.assertEqual((result['blocks'], result['events']), (BLOCKS, sum(h % 3 for h in range(BLOCKS))))

        def edit(index, offset=None, count=None):
            data = bytearray(raw)
            at = index*48
            if offset is not None: data[at+32:at+40] = offset.to_bytes(8, 'little')
            if count is not None: data[at+40:at+48] = count.to_bytes(8, 'little')
            return bytes(data)
        for data, message in ((edit(0, offset=1), 'first block record'),
                              (edit(3, offset=0), 'decrease|boundary'),
                              (edit(1, count=0), 'boundary'),      # events without a count
                              (edit(0, count=1), 'boundary'),      # a count without events
                              (edit(2, count=50), 'boundary'),     # count beyond the native entry bound
                              (raw[:-1], 'incomplete')):
            with self.assertRaisesRegex(ValueError, message):
                self.records(data, events)
        records = S.Records(events); records.want(5, '0'*64)
        for at in range(0, len(raw), 48):
            records.feed(raw[at:at+48])
        with self.assertRaisesRegex(ValueError, 'reorganized or differs at height 5'):
            records.finish()

    def test_sidecars_are_bound_to_their_block(self):
        raw = internal(0)
        S.check_sidecar(sidecar(raw), raw)
        for data in (sidecar(internal(1)), sidecar(raw)[:-1]+b'\x00', b'TPIRTX02'+sidecar(raw)[8:], sidecar(raw)[:70]):
            with self.assertRaises(ValueError):
                S.check_sidecar(data, raw)


class Preflight(Fixture):
    def test_plan_is_read_only_and_reports_drift(self):
        snapshot = self.snapshot(self.request(writer={'main_pid':self.systemd.state()['pid']+1}))
        result = snapshot.plan()
        self.assertEqual(result['plan_sha256'], S.digest(result['plan']))
        self.assertIn('PID/start drift', result['observation']['drift'])
        self.assertEqual(result['observation']['sidecars'], (BLOCKS+1)//2)
        self.assertFalse(S.OWNERS.exists() or S.SNAPSHOTS.exists())
        self.assertEqual(self.snapshot().plan()['observation']['drift'], None)
        # The plan digest depends only on the reviewed request.
        self.assertEqual(self.snapshot().plan()['plan_sha256'], self.snapshot().plan()['plan_sha256'])

    def test_preflight_passes_and_proves_the_lock(self):
        result = self.snapshot().preflight()
        self.assertEqual(result['writer']['lock']['contended'], True)
        self.assertEqual(result['journal']['blocks'], BLOCKS)
        self.assertEqual([a['height'] for a in result['anchors']['anchors']], [0, THROUGH, BLOCKS-1])
        self.assertFalse(S.OWNERS.exists() or S.SNAPSHOTS.exists())

    def test_identity_drift_refuses(self):
        pid = self.systemd.state()['pid']
        start = S.process_start(pid)
        cases = [(self.request(writer={'main_pid':pid+1}), 'PID/start drift'),
                 (self.request(writer={'process_start':start+1}), 'PID/start drift'),
                 (self.request(writer={'fragment_sha256':'0'*64}), 'fragment bytes'),
                 (self.request(writer={'fragment_path':str(self.root/'other.service')}), 'fragment path'),
                 (self.request(writer={'drop_ins':[]}), 'drop-in set'),
                 (self.request(writer={'drop_ins':[{'path':str(self.dropin), 'sha256':'0'*64}]}), 'drop-in bytes'),
                 (self.request(writer={'binary_sha256':'0'*64}), 'installed bytes'),
                 (self.request(writer={'unit':'other.service'}), '')]
        for request, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex((ValueError, AssertionError), message):
                self.snapshot(request).preflight()
        # The ExecStart binary and the running executable are both bound.
        other = self.root/'other-binary'; shutil.copy(PYTHON, other)
        with self.assertRaisesRegex(ValueError, 'ExecStart binary'):
            self.snapshot(self.request(writer={'binary_path':str(other), 'binary_sha256':sha(other)})).preflight()
        request = self.request()
        self.fragment.write_text(self.fragment.read_text()+'# changed\n')
        with self.assertRaisesRegex(ValueError, 'fragment bytes'):
            self.snapshot(request).preflight()

    def test_cgroup_and_running_executable_are_bound(self):
        with patch.object(FakeSystemd, 'pids', lambda *_: set()):
            with self.assertRaisesRegex(ValueError, 'outside the unit cgroup'):
                self.snapshot().preflight()
        # The installed ExecStart binary is not the executable that is running.
        other = self.root/'installed-binary'
        other.write_bytes(Path(PYTHON).read_bytes()+b'changed')
        self.fragment.write_text('[Service]\nExecStart=%s -B %s %s\n' % (other, self.root/'writer.py', self.journal))
        request = self.request(writer={'binary_path':str(other), 'binary_sha256':sha(other)})
        with self.assertRaisesRegex(ValueError, 'running executable'):
            self.snapshot(request).preflight()

    def test_stop_propagating_or_restarting_dependents_refuse(self):
        for name in S.INTERPLAY:
            state = self.systemd.state(); state['interplay'] = {name:'other.unit'}; self.systemd.write(state)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, name+' would propagate'):
                self.snapshot().preflight()
        state = self.systemd.state(); state['interplay'] = {}; self.systemd.write(state)
        self.snapshot().preflight()

    def test_inactive_or_foreign_writers_refuse(self):
        request = self.request()
        pid = self.systemd.state()['pid']
        self.systemd.stop(UNIT, 5)
        with self.assertRaisesRegex(ValueError, 'not running'):
            self.snapshot(request).preflight()
        # A foreign process holding writer.lock while the reviewed unit restarts.
        foreign = holder(self.journal/'writer.lock')
        self.addCleanup(release, foreign)
        state = self.systemd.state(); state['mode'] = 'nolock'; self.systemd.write(state)
        self.systemd.start(UNIT, 5)
        with self.assertRaisesRegex(ValueError, 'not held exclusively by the reviewed writer'):
            self.snapshot(self.request()).preflight()
        self.assertNotEqual(self.systemd.state()['pid'], pid)

    def test_anchors_resources_and_publication(self):
        (self.root/'node.json').write_text(json.dumps({str(BLOCKS-1):'0'*64}))
        with self.assertRaisesRegex(ValueError, 'canonical anchor differs at height %d' % (BLOCKS-1)):
            self.snapshot().preflight()
        (self.root/'node.json').write_text(json.dumps({'0':'0'*64}))
        with self.assertRaisesRegex(ValueError, 'height 0'):
            self.snapshot().preflight()
        (self.root/'node.json').unlink()
        (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 199 kB\n')
        with self.assertRaisesRegex(ValueError, 'memory headroom'):
            self.snapshot().preflight()
        (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 200 kB\n')
        self.snapshot().preflight()
        (self.root/'disk.json').write_text(json.dumps({'bavail':200, 'blocks':1000}))
        with self.assertRaisesRegex(ValueError, 'disk headroom'):  # The copy's bytes are reserved first.
            self.snapshot().preflight()
        (self.root/'disk.json').write_text(json.dumps({'bavail':201, 'blocks':1000}))
        self.snapshot().preflight()
        (self.root/'publication/cutoff.json').write_text(json.dumps({'anchor':{'height':THROUGH, 'hash':'0'*64}}))
        with self.assertRaisesRegex(ValueError, 'publication identity'):
            self.snapshot().preflight()

    def test_namespace_misuse_refuses(self):
        S.SNAPSHOTS.parent.mkdir(parents=True)
        (self.root/'elsewhere').mkdir(mode=0o700)
        S.SNAPSHOTS.symlink_to(self.root/'elsewhere')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.snapshot().preflight()
        S.SNAPSHOTS.unlink(); S.SNAPSHOTS.mkdir(mode=0o750)
        with self.assertRaisesRegex(ValueError, 'private root-owned'):
            self.snapshot().preflight()
        S.SNAPSHOTS.chmod(0o700)
        snapshot = self.snapshot()
        for path in (snapshot.partial, snapshot.target):
            path.mkdir()
            with self.assertRaisesRegex(ValueError, 'already owned'):
                snapshot.preflight()
            path.rmdir()
        snapshot.preflight()

    def test_other_unfinished_owners_fence_the_snapshot(self):
        S.OWNERS.mkdir()
        (S.OWNERS/'latest.json').write_text(json.dumps({'request_sha256':'a'*64}))
        (S.OWNERS/('a'*64+'.json')).write_text(json.dumps({'request_sha256':'a'*64, 'status':'receiving'}))
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            self.snapshot().preflight()


class Stage(Fixture):
    def staged(self, snapshot, record, original):
        self.assertEqual(record['status'], 'staged')
        self.assertEqual(record['qualification'], S.NOT_QUALIFICATION)
        restored = record['restoration']['writer']
        self.assertNotEqual(restored['pid'], original)
        self.writer_proof(snapshot, restored['pid'])
        copy = snapshot.target/'journal'
        events, blocks = lengths(copy)
        self.assertEqual((copy/'events.bin').read_bytes(), (self.journal/'events.bin').read_bytes()[:events])
        self.assertEqual((copy/'blocks.bin').read_bytes(), (self.journal/'blocks.bin').read_bytes()[:blocks])
        self.assertEqual((copy/'meta.json').read_bytes(), (self.journal/'meta.json').read_bytes())
        names = {p.name for p in (copy/'display-v1').iterdir()}
        expected = {display(h)+'.bin' for h in range(blocks//48) if h % 2 == 0}
        self.assertEqual(names, expected)  # Unreachable uncommitted sidecars stay behind.
        for path in snapshot.target.rglob('*'):
            info = path.lstat()
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o700 if path.is_dir() else 0o400)
            self.assertTrue(path.is_dir() or info.st_nlink == 1)
        manifest = S.verify_tree(snapshot.target, record['manifest'], request=snapshot.request, budget=S.Budget(time.monotonic()+60, 'test'))
        self.assertEqual(manifest['qualification'], S.NOT_QUALIFICATION)
        self.assertEqual(snapshot.status()['status'], 'staged')
        S.schema_fence.local_schema_fence()  # A staged owner no longer fences.
        return manifest

    def test_inprocess_stage_copies_only_the_committed_prefix_and_restores(self):
        original = self.systemd.state()['pid']
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        manifest = self.staged(snapshot, record, original)
        self.assertEqual(manifest['journal']['blocks'], BLOCKS)
        self.assertEqual([e['phase'] for e in record['events']],
                         ['launching', 'adopted', 'precopying', 'quiescing', 'quiesced', 'copying', 'copied', 'restoring', 'restored', 'retained'])
        with S.ProductionLock({'type':'pinned_host', 'machine_id':MACHINE}) as lock:
            with patch.dict(os.environ, {inherited_lock.VARIABLE:str(lock.fd)}):
                with self.assertRaisesRegex(ValueError, 'already adopted'):
                    S.owner_main(snapshot.identifier, systemd=self.systemd, node=self.node)
        with self.assertRaisesRegex(ValueError, 'does not need reconciliation'):
            self.reconcile(snapshot)

    def test_detached_owner_survives_transport_loss_and_keeps_the_lock(self):
        original = self.start_writer('append')
        snapshot, parent = self.spawn_parent(delay=.6, snapshot=self.snapshot(self.request()))
        self.wait(lambda: self.phase(snapshot).get('phase') in ('quiescing', 'quiesced', 'copying'))
        parent.kill(); parent.wait(5)  # The SSH session and wrapper parent are gone.
        with self.assertRaises(BlockingIOError):
            with S.ProductionLock({'type':'pinned_host', 'machine_id':MACHINE}):
                pass
        self.fenced()
        self.wait(lambda: self.phase(snapshot).get('status') != 'running')
        record = self.record(snapshot)
        self.staged(snapshot, record, original)
        self.assertGreaterEqual(record['snapshot']['blocks'], record['before']['blocks'])
        with S.ProductionLock({'type':'pinned_host', 'machine_id':MACHINE}):
            pass

    def test_unknown_owner_outcome_is_not_inferred(self):
        snapshot = self.snapshot(self.request(bounds={'precopy_seconds':1, 'stop_seconds':1, 'copy_seconds':1,
                                                      'restart_seconds':1, 'restart_attempts':1, 'total_seconds':4}))
        with patch.object(S.Snapshot, 'owner_command', lambda self: ['sleep', '5']):
            with self.assertRaisesRegex(S.Unknown, 'still running'):
                snapshot.stage(self.plan(snapshot))
        self.assertEqual(self.record(snapshot)['status'], 'running')
        self.fenced()
        with self.assertRaises(BlockingIOError):
            snapshot.reconcile()  # The surviving owner process still holds the lock.
        self.wait(lambda: self.lock_free())
        record = self.reconcile(snapshot)  # Launcher and owner are gone; nothing was stopped.
        self.assertFalse(record['reconciliation']['writer']['started'])

    def lock_free(self):
        try:
            with S.ProductionLock({'type':'pinned_host', 'machine_id':MACHINE}):
                return True
        except BlockingIOError:
            return False

    def test_stage_requires_the_reviewed_plan(self):
        snapshot = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'plan changed'):
            snapshot.stage('0'*64)
        self.assertFalse(snapshot.owner.exists())


class Failures(Fixture):
    def failed(self, snapshot, record, status, writer=True):
        self.assertEqual(record['status'], status)
        self.assertFalse(snapshot.target.exists())
        self.fenced()
        if writer:
            self.assertEqual(record['restoration']['status'], 'proven')
            self.writer_proof(snapshot, record['restoration']['writer']['pid'])

    def reconciled(self, snapshot):
        partial = snapshot.partial.exists()
        record = self.reconcile(snapshot)
        self.assertEqual(record['status'], 'reconciled')
        self.assertFalse(snapshot.partial.exists() or snapshot.target.exists())
        if partial:
            self.assertTrue(Path(record['reconciliation']['retained'][0]).is_dir())
        self.writer_proof(snapshot, record['reconciliation']['writer']['writer']['pid'])
        S.schema_fence.local_schema_fence()
        return record

    def owner_failure(self, snapshot, exception, message, before_owner=None):
        with self.assertRaisesRegex(exception, message):
            self.inprocess(snapshot, before_owner)
        return self.record(snapshot)

    def test_stop_failures_keep_the_original_writer(self):
        state = self.systemd.state(); state['stop_error'] = True; self.systemd.write(state)
        original = state['pid']
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, subprocess.CalledProcessError, '')
        self.failed(snapshot, record, 'failed')
        self.assertEqual((record['restoration']['started'], record['restoration']['writer']['pid']), (False, original))
        state = self.systemd.state(); state['stop_error'] = False; self.systemd.write(state)
        self.reconciled(snapshot)

    def test_stop_timeout_interrupts_and_keeps_the_writer(self):
        original = self.start_writer('stubborn')
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, subprocess.TimeoutExpired, '')
        self.failed(snapshot, record, 'interrupted')
        self.assertEqual(record['restoration']['writer']['pid'], original)
        self.reconciled(snapshot)

    def test_foreign_writer_lock_holder_after_stop(self):
        snapshot = self.snapshot()
        holders = []

        def hold():
            # A foreign holder blocks both this owner and the restarted writer.
            stop = FakeSystemd.stop

            def stop_and_hold(fake, unit, timeout):
                stop(fake, unit, timeout)
                holders.append(holder(self.journal/'writer.lock'))
                self.addCleanup(release, holders[-1])
            p = patch.object(FakeSystemd, 'stop', stop_and_hold); p.start(); self.addCleanup(p.stop)
        record = self.owner_failure(snapshot, S.Foreign, 'still holds writer.lock', hold)
        self.assertEqual(record['status'], 'restore-failed')
        self.assertEqual(len(record['restore_attempts']), self.bounds['restart_attempts'])
        self.fenced()
        with self.assertRaisesRegex(ValueError, 'not restored within'):
            self.reconcile(snapshot)
        release(holders[0])
        self.reconciled(snapshot)

    def copy_failure(self, hook, exception, message, status='failed', request=None):
        snapshot = self.snapshot(request)
        real = S.copy_file
        calls = [0]

        def copy_file(source, destination, length, budget):
            calls[0] += 1
            hook(Path(source).name, calls[0])
            return real(source, destination, length, budget)
        with patch.object(S, 'copy_file', copy_file):
            record = self.owner_failure(snapshot, exception, message)
        self.failed(snapshot, record, status)
        self.assertTrue(snapshot.partial.is_dir())  # Partial bytes are retained.
        self.assertTrue(snapshot.health.exists())
        return snapshot, record

    def test_source_truncated_during_copy(self):
        def hook(name, _):
            if name == 'events.bin':
                with open(self.journal/'events.bin', 'ab') as out: out.truncate(100)
        snapshot, _ = self.copy_failure(hook, ValueError, 'shortened|shorter')
        self.reconciled(snapshot)

    def test_copy_deadline_interrupts(self):
        snapshot, _ = self.copy_failure(lambda *_: time.sleep(1.2), subprocess.TimeoutExpired, '', 'interrupted',
                                        self.request(bounds={'copy_seconds':1}))
        self.reconciled(snapshot)

    def test_disk_floor_crossed_during_copy(self):
        def hook(name, _):
            if name == 'blocks.bin':
                (self.root/'disk.json').write_text(json.dumps({'bavail':900, 'blocks':1000, 'fail_after':0}))
            time.sleep(S.SAMPLE_SECONDS)
        real = S.Snapshot.restore

        def restore(self_, *args, **options):
            (self.root/'disk.json').unlink()  # Headroom recovers once the copy stops.
            return real(self_, *args, **options)
        with patch.object(S.Snapshot, 'restore', restore):
            snapshot, record = self.copy_failure(hook, ValueError, 'disk headroom')
        self.assertEqual(record['restoration']['status'], 'proven')
        self.reconciled(snapshot)

    def test_foreign_restart_during_copy_refuses_restoration(self):
        def hook(name, _):
            if name == 'blocks.bin':
                self.start_writer('nolock')  # Someone else restarted the unit.
            time.sleep(S.SAMPLE_SECONDS)
        snapshot = self.snapshot()
        real = S.copy_file

        def copy_file(source, destination, length, budget):
            hook(Path(source).name, 0)
            return real(source, destination, length, budget)
        with patch.object(S, 'copy_file', copy_file):
            record = self.owner_failure(snapshot, S.Foreign, 'restarted during the snapshot')
        self.assertEqual(record['status'], 'restore-failed')
        self.assertIn('unknown owner', record['restore_error'])
        self.fenced()
        with self.assertRaisesRegex(ValueError, 'unknown owner'):
            self.reconcile(snapshot)
        self.assertEqual(self.record(snapshot)['status'], 'restore-failed')

    def test_corrupt_linked_or_hardlinked_sources_refuse(self):
        name = display(2)+'.bin'
        (self.journal/'display-v1'/name).write_bytes(b'TPIRTX01'+bytes(70))
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, ValueError, 'display sidecar')
        self.failed(snapshot, record, 'failed')
        self.reconciled(snapshot)
        (self.journal/'display-v1'/name).unlink()
        (self.journal/'display-v1'/name).symlink_to(self.root/'elsewhere')
        snapshot = self.snapshot(self.request(attempt=2))
        record = self.owner_failure(snapshot, OSError, '')
        self.failed(snapshot, record, 'failed')
        self.reconciled(snapshot)
        (self.journal/'display-v1'/name).unlink()
        (self.journal/'display-v1'/name).write_bytes(sidecar(internal(2)))
        os.link(self.journal/'events.bin', self.root/'events-alias')
        with self.assertRaisesRegex(ValueError, 'single-link'):
            self.snapshot(self.request(attempt=3)).preflight()

    def test_journal_reorganized_before_quiescence(self):
        self.start_writer('reorg')  # Rolls back three blocks and appends a fork on stop.
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, ValueError, 'reorganized below the pre-quiesce prefix')
        self.failed(snapshot, record, 'failed')
        self.reconciled(snapshot)

    def test_longer_fork_differs_from_the_recorded_prefix(self):
        # A longer fork passes the length check but not the recorded prefix.
        self.start_writer('reorg-longer')
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, ValueError, 'differs from its pre-quiesce prefix at height %d' % (BLOCKS-3))
        self.failed(snapshot, record, 'failed')
        self.reconciled(snapshot)

    def test_canonical_anchor_moves_after_copy(self):
        snapshot = self.snapshot()
        real = S.Snapshot.restore

        def restore(self_, *args, **options):
            (self.root/'node.json').write_text(json.dumps({str(BLOCKS-1):'0'*64}))
            return real(self_, *args, **options)
        with patch.object(S.Snapshot, 'restore', restore):
            record = self.owner_failure(snapshot, ValueError, 'canonical anchor differs')
        self.failed(snapshot, record, 'failed')
        self.assertTrue((snapshot.partial/'journal/events.bin').exists())
        (self.root/'node.json').unlink()
        self.reconciled(snapshot)

    def test_start_failures_exhaust_attempts_then_reconcile_restores(self):
        snapshot = self.snapshot()

        def fail_starts():
            state = self.systemd.state(); state['start_errors'] = self.bounds['restart_attempts']; self.systemd.write(state)
        record = self.owner_failure(snapshot, ValueError, 'not restored within', fail_starts)
        self.assertEqual(record['status'], 'restore-failed')
        self.assertEqual([a.get('start_error') for a in record['restore_attempts']], ['CalledProcessError']*2)
        self.assertEqual(self.systemd.state()['active'], 'inactive')
        self.fenced()
        self.reconciled(snapshot)

    def test_unproven_restarted_writer_is_left_for_root(self):
        snapshot = self.snapshot(self.request(bounds={'restart_seconds':1}))

        def nolock():
            state = self.systemd.state(); state['mode'] = 'nolock'; self.systemd.write(state)
        record = self.owner_failure(snapshot, S.Foreign, 'unproven', nolock)
        self.assertEqual(record['status'], 'restore-failed')
        self.assertEqual(self.systemd.state()['pid'], record['restore_attempts'][0]['pid'])
        # The started writer is owned, but reconciliation still needs its lock proof.
        with self.assertRaisesRegex(ValueError, 'not held exclusively'):
            self.reconcile(snapshot)
        self.kill_writer()
        state = self.systemd.state(); state['mode'] = 'idle'; self.systemd.write(state)
        self.reconciled(snapshot)

    def test_binary_or_unit_change_before_restore_refuses_start(self):
        snapshot = self.snapshot()
        real = S.Snapshot.restore

        def restore(self_, *args, **options):
            self.dropin.write_text('[Service]\nNice=5\n')
            return real(self_, *args, **options)
        with patch.object(S.Snapshot, 'restore', restore):
            record = self.owner_failure(snapshot, ValueError, 'drop-in bytes')
        self.assertEqual(record['status'], 'restore-failed')
        self.assertEqual(self.systemd.state()['active'], 'inactive')
        with self.assertRaisesRegex(ValueError, 'drop-in bytes'):
            self.reconcile(snapshot)
        self.dropin.write_text('[Service]\nNice=10\n')
        self.reconciled(snapshot)

    def test_restored_resources_must_pass_before_completion(self):
        snapshot = self.snapshot()
        real = S.Snapshot.restore

        def restore(self_, *args, **options):
            (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 100 kB\n')
            return real(self_, *args, **options)
        with patch.object(S.Snapshot, 'restore', restore):
            record = self.owner_failure(snapshot, ValueError, 'memory headroom')
        self.assertEqual(record['status'], 'restore-failed')
        (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 500 kB\n')
        # The restarted writer is this owner's recorded process.
        record = self.reconciled(snapshot)
        self.assertFalse(record['reconciliation']['writer']['started'])

    def test_interrupt_restores_the_writer_and_requires_reconcile(self):
        snapshot, parent = self.spawn_parent(delay=.8)
        self.wait(lambda: self.phase(snapshot).get('phase') == 'copying')
        os.kill(self.record(snapshot)['owner']['pid'], signal.SIGTERM)
        self.assertEqual(parent.wait(30), 1)
        record = self.record(snapshot)
        self.failed(snapshot, record, 'interrupted')
        self.assertEqual(record['error_type'], 'Interrupted')
        self.reconciled(snapshot)

    def test_killed_owner_leaves_a_fenced_stopped_writer_for_reconcile(self):
        snapshot, parent = self.spawn_parent(delay=.8)
        self.wait(lambda: self.phase(snapshot).get('phase') == 'copying')
        owner = self.record(snapshot)['owner']['pid']
        with self.assertRaises(BlockingIOError):
            snapshot.reconcile()  # The live owner still holds the production lock.
        os.kill(owner, signal.SIGKILL)
        self.assertEqual(parent.wait(30), 1)
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['phase']), ('running', 'copying'))
        self.assertEqual(self.systemd.state()['active'], 'inactive')
        self.fenced()
        self.reconciled(snapshot)

    def test_reconcile_refuses_a_live_recorded_launcher(self):
        snapshot = self.snapshot()
        record = self.owner_failure(snapshot, subprocess.CalledProcessError, '', self.stop_error)
        survivor = subprocess.Popen(['sleep', '30'])
        self.addCleanup(lambda: (survivor.kill(), survivor.wait()))
        record['launcher'] = {'pid':survivor.pid, 'process_start':S.process_start(survivor.pid)}
        S.durable.atomic_json(snapshot.owner, record)
        with self.assertRaisesRegex(ValueError, 'launcher process is still active'):
            self.reconcile(snapshot)
        survivor.kill(); survivor.wait()
        self.reconciled(snapshot)

    def stop_error(self):
        state = self.systemd.state(); state['stop_error'] = True; self.systemd.write(state)

    def test_reconcile_refuses_unknown_restarts_and_foreign_stops(self):
        snapshot, parent = self.spawn_parent(delay=.8)
        self.wait(lambda: self.phase(snapshot).get('phase') == 'copying')
        os.kill(self.record(snapshot)['owner']['pid'], signal.SIGKILL)
        parent.wait(30)
        self.start_writer()  # Not this owner's process.
        with self.assertRaisesRegex(ValueError, 'unknown owner'):
            snapshot.reconcile()
        self.assertEqual(self.record(snapshot)['status'], 'running')
        self.fenced()
        # A writer stopped before this owner's quiescence was someone else's.
        other = self.snapshot(self.request(attempt=2))
        S.OWNERS.mkdir(exist_ok=True)
        durable = S.durable
        durable.atomic_json(other.retained, other.request)
        inventory = other.pinned(self.inventory)
        durable.atomic_json(other.retained_inventory, inventory)
        durable.atomic_json(other.owner, {'kind':S.KIND, 'request_sha256':other.identifier, 'status':'failed',
                                          'phase':'adopted', 'events':[], 'inventory_sha256':durable.digest(inventory)})
        durable.atomic_json(S.OWNERS/'latest.json', {'request_sha256':other.identifier})
        self.systemd.stop(UNIT, 5)
        with self.assertRaisesRegex(ValueError, 'stopped outside this owner'):
            other.reconcile()


class Sidecars(Fixture):
    """Immutable sidecars are pre-copied live and reconciled under quiescence."""

    def every_height(self):
        for height in range(BLOCKS):
            path = self.journal/'display-v1'/(display(height)+'.bin')
            if not path.exists():
                path.write_bytes(sidecar(internal(height)))

    def after_precopy(self, change):
        real = S.Snapshot.precopy

        def precopy(self_, *args):
            result = real(self_, *args)
            change()
            return result
        p = patch.object(S.Snapshot, 'precopy', precopy); p.start(); self.addCleanup(p.stop)

    def failure(self, message):
        snapshot = self.snapshot()
        with self.assertRaisesRegex(ValueError, message):
            self.inprocess(snapshot)
        record = self.record(snapshot)
        self.assertEqual(record['status'], 'failed')
        self.assertTrue(snapshot.partial.is_dir() and snapshot.sidecar_inventory.exists())
        self.fenced()
        return snapshot, record

    def test_policy_every_committed_block(self):
        request = self.request(sidecars={'policy':'every-committed-block'})
        with self.assertRaisesRegex(ValueError, 'committed blocks have no display sidecar: %d' % (BLOCKS//2)):
            self.snapshot(request).preflight()
        self.every_height()
        snapshot = self.snapshot(request)
        record = self.inprocess(snapshot)
        summary = record['snapshot']['display']
        self.assertEqual((summary['sidecars'], summary['sidecars_absent_in_source']), (BLOCKS, 0))
        self.assertEqual(len(list((snapshot.target/'journal/display-v1').iterdir())), BLOCKS)

    def test_mirror_source_records_absence_without_inferring_it(self):
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        summary = record['snapshot']['display']
        self.assertEqual((summary['policy'], summary['sidecars'], summary['sidecars_absent_in_source']),
                         ('mirror-source', (BLOCKS+1)//2, BLOCKS//2))
        manifest = S.verify_tree(snapshot.target, record['manifest'], request=snapshot.request, budget=S.Budget(time.monotonic()+60, 'test'))
        self.assertEqual([c['height'] for c in manifest['journal']['display']['coverage']], [0, 2])
        self.assertEqual(manifest['journal']['display']['coverage'][1]['hash'], display(2))
        self.assertEqual(manifest['contents']['sidecars'], (BLOCKS+1)//2)

    def test_coverage_heights_need_their_sidecars(self):
        with self.assertRaisesRegex(ValueError, 'coverage heights have no display sidecar'):
            self.snapshot(self.request(sidecars={'coverage_heights':[0, THROUGH]})).preflight()
        with self.assertRaisesRegex(ValueError, 'beyond the committed journal'):
            self.snapshot(self.request(sidecars={'coverage_heights':[BLOCKS+10]})).preflight()

    def test_sidecar_replaced_after_precopy(self):
        def replace():
            path = self.journal/'display-v1'/(display(2)+'.bin')
            data = path.read_bytes(); path.unlink(); path.write_bytes(data)
        self.after_precopy(replace)
        self.failure('changed or vanished at height 2')

    def test_sidecar_removed_after_precopy(self):
        self.after_precopy(lambda: (self.journal/'display-v1'/(display(4)+'.bin')).unlink())
        self.failure('changed or vanished at height 4')

    def test_sidecar_written_after_precopy_is_copied_while_quiesced(self):
        self.after_precopy(lambda: (self.journal/'display-v1'/(display(3)+'.bin')).write_bytes(sidecar(internal(3))))
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        summary = record['snapshot']['display']
        self.assertEqual((summary['sidecars'], summary['copied_while_quiesced']), ((BLOCKS+1)//2+1, 1))
        self.assertTrue((snapshot.target/'journal/display-v1'/display_file(3)).exists())

    def test_newer_block_without_sidecar_refuses_every_committed_block(self):
        self.every_height()

        def extend():
            append(self.journal); append(self.journal)  # Height 9 has no sidecar.
            (self.journal/'display-v1'/display_file(BLOCKS)).write_bytes(sidecar(internal(BLOCKS)))
        self.after_precopy(extend)
        snapshot = self.snapshot(self.request(sidecars={'policy':'every-committed-block'}))
        with self.assertRaisesRegex(ValueError, 'committed block %d has no display sidecar' % (BLOCKS+1)):
            self.inprocess(snapshot)
        self.assertEqual(self.record(snapshot)['restoration']['status'], 'proven')

    def test_copy_changed_after_the_writer_restarts(self):
        snapshot = self.snapshot()
        real = S.Snapshot.restore

        def restore(self_, *args, **options):
            result = real(self_, *args, **options)
            events = snapshot.partial/'journal/events.bin'
            events.chmod(0o600)
            with open(events, 'r+b') as stream:
                stream.seek(10); byte = stream.read(1); stream.seek(10); stream.write(bytes([byte[0] ^ 1]))
            events.chmod(0o400)
            return result
        with patch.object(S.Snapshot, 'restore', restore):
            with self.assertRaisesRegex(ValueError, 'differ from the bytes copied'):
                self.inprocess(snapshot)
        self.assertFalse(snapshot.target.exists())

    def test_a_block_below_the_tip_changed_after_precopy(self):
        def rewrite():
            with open(self.journal/'blocks.bin', 'r+b') as stream:
                stream.seek(5*48); stream.write(internal(5, b'other'))
        self.after_precopy(rewrite)
        self.failure('differs from its pre-quiesce prefix at height 5')

    def test_precopy_deadline_never_stops_the_writer(self):
        original = self.systemd.state()['pid']
        real = S.copy_sidecar

        def slow(*args):
            time.sleep(.6)
            return real(*args)
        snapshot = self.snapshot(self.request(bounds={'precopy_seconds':1}))
        with patch.object(S, 'copy_sidecar', slow):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.inprocess(snapshot)
        events = {e['phase']:e['unix'] for e in self.record(snapshot)['events']}
        self.assertLess(self.record(snapshot)['finished_unix']-events['precopying'], 2.4)  # Stopped mid-pass.
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['phase']), ('interrupted', 'precopying'))
        self.assertEqual((record['restoration']['started'], record['restoration']['writer']['pid']), (False, original))
        self.reconcile(snapshot)

    def reconcile(self, snapshot):
        real = S.process_active
        with patch.object(S, 'process_active', lambda pid, start: pid != os.getpid() and real(pid, start)):
            record = snapshot.reconcile()
        self.assertEqual(record['status'], 'reconciled')
        self.assertTrue(Path(record['reconciliation']['retained'][0]).is_dir())


def display_file(height):
    return display(height)+'.bin'


class Fleet(Fixture):
    """Fresh exact owner-namespace and process-ownership evidence of every pinned host."""

    def setUp(self):
        super().setUp()
        self.proc = self.root/'hosts/worker-01/proc'
        self.worker_lock = self.remote(S.ProductionLock.PATH)
        self.worker_lock.parent.mkdir(parents=True, exist_ok=True)
        self.worker_lock.touch()
        self.addCleanup(lambda: [d.chmod(0o700) for d in self.proc.glob('*/fd')])

    def worker_record(self, path, value):
        target = self.remote(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(value) if not isinstance(value, str) else value)

    def worker_input(self, record, identifier='b'*64, latest=True):
        if latest:
            self.worker_record(S.schema_fence.INPUT_STAGING/'latest.json', {'request_sha256':identifier})
        self.worker_record(S.schema_fence.INPUT_STAGING/(identifier+'.json'), {'request_sha256':identifier, **record})

    def receipts(self, snapshot, label):
        runs = sorted(snapshot.fleet_receipts.glob(label+'-*'))
        self.assertTrue(runs)
        return {p.stem:json.loads(p.read_text()) for p in runs[-1].iterdir()}

    def refuses(self, message):
        with self.assertRaisesRegex(ValueError, message):
            self.snapshot().preflight()

    def test_the_host_inventory_stays_pinned_to_the_coordinator(self):
        snapshot = self.snapshot()
        self.assertIs(snapshot.inventory, self.inventory)  # Never shadowed by a private path.
        self.assertEqual(snapshot.pinned(self.inventory)['lock'], {'type':'pinned_host', 'machine_id':MACHINE})
        for lock in ({'type':'remote', 'host':'worker-01'}, {'type':'pinned_host', 'machine_id':WORKER}):
            with self.assertRaisesRegex(ValueError, 'pinned coordinator inventory'):
                snapshot.pinned(SimpleNamespace(hosts={}, ssh={}, lock=lock, services={}))
        with self.assertRaisesRegex(ValueError, 'pinned coordinator inventory'):
            snapshot.pinned(None)

    def test_every_pinned_host_is_read_and_receipts_are_retained(self):
        self.worker_input({'status':'staged', 'finished_unix':time.time()-60})
        fake_process(self.proc, 4100, started=time.time()-30)  # An unrelated live process.
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        self.assertEqual([h['host'] for h in record['hosts']], ['coordinator', 'worker-01'])
        for label in ('stage', 'owner'):
            receipts = self.receipts(snapshot, label)
            self.assertEqual({k:v['result'] for k, v in receipts.items()}, {'coordinator':'passed', 'worker-01':'passed'})
            probe = receipts['worker-01']['probe']
            self.assertEqual(probe['machine_id'].strip(), WORKER)
            self.assertIn(str(S.schema_fence.INPUT_STAGING/('b'*64+'.json')), probe['records'])
            self.assertEqual([(p['pid'], p['lock_fd'], p['inherited'], p['wrapper']) for p in probe['processes']],
                             [(4100, False, False, False)])  # Markers only; never environment or command text.
            self.assertFalse({'environ', 'cmdline'} & set(probe['processes'][0]))
        self.assertEqual(S.manifest_of(snapshot.target, record['manifest'])['hosts'], record['hosts'])

    def test_pins_and_reachability_fail_closed_with_retained_receipts(self):
        self.remote(S.MACHINE_ID).write_text('e'*32+'\n')
        snapshot = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'worker-01 machine identity differs'):
            snapshot.stage(self.plan(snapshot))
        self.assertFalse(snapshot.owner.exists())
        self.assertEqual(self.receipts(snapshot, 'stage')['worker-01']['result'], 'refused')
        self.remote(S.MACHINE_ID).write_text(WORKER+'\n')
        (self.root/'hosts/worker-01.unreachable').touch()
        with self.assertRaisesRegex(S.RemoteError, 'connection refused'):
            self.snapshot().preflight()
        (self.root/'hosts/worker-01.unreachable').unlink()
        self.inventory = S.descriptors.load_inventory(self.write_inventory(router={}))
        self.refuses('router has no machine pin')

    def test_unfinished_owners_and_source_staging_refuse(self):
        self.worker_input({'status':'receiving', 'started_unix':time.time()})
        self.refuses('unfinished input staging owner')
        self.worker_input({'status':'staged', 'finished_unix':time.time()-60})
        self.worker_record(S.schema_fence.HOST_ACTIONS/'latest.json', {'transaction':'transparent-schema-x', 'request_id':'r'})
        self.worker_record(S.schema_fence.HOST_ACTIONS/'transparent-schema-x/r.json', {'status':'running'})
        self.refuses('unfinished remote host owner')
        self.worker_record(S.schema_fence.HOST_ACTIONS/'transparent-schema-x/r.json', {'status':'passed'})
        self.worker_record(S.SOURCE.parent/'staging'/('a'*40+'.json'), {'version':1, 'status':'receiving', 'pid':1})
        self.refuses('unfinished source staging')
        self.worker_record(S.SOURCE.parent/'staging'/('a'*40+'.json'), {'version':1, 'status':'failed'})
        self.snapshot().preflight()
        self.worker_record(S.schema_fence.INPUT_STAGING/'latest.json', {'request_sha256':'c'*64})
        self.refuses('missing input staging owner')

    def test_dead_recorded_parent_with_a_live_orphaned_native_descendant_refuses(self):
        # The exact negative case: the recorded owner has exited and its native
        # child, reparented to init, carries no marker except its session.
        self.worker_input({'status':'staged', 'pid':4200, 'finished_unix':time.time()-60})
        fake_process(self.proc, 4201, started=time.time()-120, ppid=1, pgrp=4200, session=4200)
        self.refuses('live descendant PID 4201 of recorded owner PID 4200')
        # Grouped but not session-linked, and from an older owner no pointer names.
        self.worker_input({'status':'staged', 'pid':4200, 'finished_unix':time.time()-60})
        self.worker_input({'status':'failed', 'pid':4300, 'finished_unix':time.time()-7200}, identifier='9'*64, latest=False)
        (self.proc/'4201/stat').write_text((self.proc/'4201/stat').read_text().replace(' 4200 4200 ', ' 4300 77 '))
        self.refuses('live descendant PID 4201 of recorded owner PID 4300')

    def test_unrecorded_live_wrapper_processes_and_lock_holders_refuse(self):
        for number, options in enumerate(({'lock':self.worker_lock}, {'inherited':True}, {'wrapper':True})):
            base = fake_process(self.proc, 4400+number, started=time.time()-5, **options)
            with self.subTest(options=sorted(options)):
                self.refuses('live wrapper process or descendant: PID %d' % (4400+number))
            shutil.rmtree(base)
        fake_process(self.proc, 4500, started=time.time()-5, unreadable=True)
        self.refuses('process 4500 ownership is unreadable')
        (self.proc/'4500/fd').chmod(0o700); shutil.rmtree(self.proc/'4500')
        info = self.worker_lock.stat()
        (self.proc/'locks').write_text('1: FLOCK  ADVISORY  WRITE 4600 %02x:%02x:%d 0 EOF\n'
                                       % (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino))
        self.refuses('worker-01 production lock is held')
        (self.proc/'locks').write_text('')
        self.snapshot().preflight()

    def test_live_recorded_owners_refuse_and_reused_pids_do_not(self):
        fake_process(self.proc, 4700, started=time.time()-600)
        self.worker_input({'status':'staged', 'child_pid':4700, 'finished_unix':time.time()-60})
        self.refuses('live recorded owner PID 4700')
        # Finished before this process started: a reused PID, with its own children.
        self.worker_input({'status':'staged', 'child_pid':4700, 'finished_unix':time.time()-3600})
        fake_process(self.proc, 4701, started=time.time()-300, session=4700)
        self.snapshot().preflight()
        fake_process(self.proc, 4702, started=time.time()-900, session=4700)  # Older than the new leader.
        self.refuses('live descendant PID 4702 of recorded owner PID 4700')
        shutil.rmtree(self.proc/'4702')
        start = int((self.proc/'4700/stat').read_text().rsplit(')', 1)[1].split()[19])
        self.worker_input({'status':'reconciled', 'owner':{'pid':4700, 'process_start':start}})
        self.refuses('live recorded owner PID 4700')
        self.worker_input({'status':'reconciled', 'owner':{'pid':4700, 'process_start':start+1}})
        self.snapshot().preflight()
        self.worker_input({'status':'staged', 'child_pid':4700})  # No record time fails closed.
        self.refuses('live recorded owner PID 4700')

    def test_owner_namespaces_fail_closed_on_links_size_and_truncation(self):
        namespace = self.remote(S.schema_fence.INPUT_STAGING)
        namespace.mkdir(parents=True, exist_ok=True)
        (namespace/'old.json').symlink_to(self.root/'elsewhere.json')
        self.refuses('contains a link')
        (namespace/'old.json').unlink()
        (namespace/'big.json').write_text(' '*((1 << 20)+1))
        self.refuses('exceeds read bound')
        (namespace/'big.json').unlink()
        (namespace/'x.request.json').write_text(' '*((1 << 20)+1))  # Requests carry no owners.
        os.mkfifo(namespace/'pipe')
        self.refuses('special file')
        (namespace/'pipe').unlink()
        deep = namespace
        for level in range(9):
            deep = deep/('d%d' % level)
        deep.mkdir(parents=True)
        self.refuses('exceeds the depth bound')
        shutil.rmtree(namespace/'d0')
        self.snapshot().preflight()

    def test_every_namespace_entry_is_enumerated_within_bounds(self):
        # A schema transaction's phase logs sit two levels down; owners deeper still are read.
        schema = self.remote(S.schema_fence.SCHEMA_STATE)
        (schema/'transparent-schema-x/forward').mkdir(parents=True)
        (schema/'transparent-schema-x/forward/00-stage-1.log').write_text('log')
        self.worker_record(S.schema_fence.SCHEMA_STATE/'transparent-schema-x/forward/deep.owner.json',
                           {'status':'passed', 'child':{'pid':4900, 'start_ticks':5}})
        receipts = self.remote(S.schema_fence.INPUT_STAGING/('a'*64+'.snapshot-fleet/owner-1'))
        receipts.mkdir(parents=True)
        (receipts/'worker-01.json').write_text('not parsed: receipts hold copies')
        snapshot = self.snapshot()
        snapshot.preflight()
        probe = S.fleet_host('worker-01', WORKER, S.RemoteHost(S.SSHExecutor(self.inventory), 'worker-01'),
                             S.Budget(time.monotonic()+30, 'test'), S.ProductionLock.PATH)['probe']
        kinds = {Path(p).name:kind for p, _, kind in probe['files']}
        self.assertEqual((kinds['00-stage-1.log'], kinds['deep.owner.json'], kinds['worker-01.json']),
                         ('other', 'record', 'receipt'))
        fake_process(self.proc, 4900, started=BOOT+5.5/TICKS)  # Kernel start tick 5.
        self.refuses('live recorded owner PID 4900')
        for limit, message in ((2, 'entry bound'),):
            with self.assertRaisesRegex(ValueError, message):
                S.host_helper.owner_records([str(schema)], limit, 1 << 20)
        with self.assertRaisesRegex(ValueError, 'byte bound'):
            S.host_helper.owner_records([str(schema)], 100, 10)

    def test_owned_child_and_children_identities_count_and_observations_do_not(self):
        record = {'status':'staged', 'finished_unix':time.time()-60,
                  'child':{'pid':101, 'start_ticks':100},
                  'children':[{'identity':{'pid':102, 'start_ticks':101}}],
                  'launch':{'owner':{'pid':103, 'process_start':102, 'boot_id':BOOT_ID}},
                  'restoration':{'writer':{'pid':104, 'process_start':103}}, 'writer_before':{'pid':105}}
        found = {(pid, start) for pid, start, _, _ in S.owner_processes(record)}
        self.assertEqual(found, {(101, 100), (102, 101), (103, 102)})
        for pid, ticks in ((101, 100), (102, 101), (103, 102)):
            base = fake_process(self.proc, pid, started=BOOT+(ticks+.5)/TICKS)
            self.worker_input(record)
            with self.subTest(pid=pid):
                self.refuses('live recorded owner PID %d' % pid)
            shutil.rmtree(base)
        fake_process(self.proc, 104, started=time.time()-600)  # Observed writer only.
        fake_process(self.proc, 105, started=time.time()-600)
        self.snapshot().preflight()
        # An identity from an earlier boot names no live process.
        fake_process(self.proc, 103, started=BOOT+102.5/TICKS)
        self.worker_input(dict(record, launch={'owner':{'pid':103, 'process_start':102, 'boot_id':EARLIER_BOOT_ID}}))
        self.snapshot().preflight()
        self.worker_input(record)
        self.refuses('live recorded owner PID 103')
        # Null, malformed or contradictory boot evidence proves no other boot.
        for boot in ({'boot_id':None}, {'boot_id':'earlier-boot'}, {'boot_id':BOOT_ID.upper()}, {'boot_id':7},
                     {'boot_unix':None}, {'boot_unix':'0'}, {'boot_unix':float('nan')},
                     {'boot_id':BOOT_ID, 'boot_unix':BOOT-3600}, {'boot_id':None, 'boot_unix':BOOT-3600},
                     {'boot_id':EARLIER_BOOT_ID, 'boot_unix':None}):
            owner = dict({'pid':103, 'process_start':102}, **boot)
            self.worker_input(dict(record, launch={'owner':owner}))
            with self.subTest(boot=boot):
                self.refuses('live recorded owner PID 103')
        self.worker_input(dict(record, launch={'owner':{'pid':103, 'process_start':102, 'boot_unix':BOOT-3600}}))
        self.snapshot().preflight()
        # Without readable live boot evidence a recorded boot ID proves nothing.
        (self.proc/'sys/kernel/random/boot_id').unlink()
        self.worker_input(dict(record, launch={'owner':{'pid':103, 'process_start':102, 'boot_id':EARLIER_BOOT_ID}}))
        self.refuses('live recorded owner PID 103')
        (self.proc/'sys/kernel/random/boot_id').write_text(BOOT_ID+'\n')

    def test_guardian_child_native_identities_are_owned(self):
        # The candidate execution record's child is its guardian's identity,
        # with the forked native process nested under `native` in the same boot.
        record = {'status':'running', 'started_unix':time.time()-60, 'launch':None,
                  'child':{'key':'k', 'token':'t', 'pid':201, 'start_ticks':200, 'boot_id':BOOT_ID,
                           'native':{'pid':202, 'start_ticks':201}, 'deadline_unix':time.time()+60}}
        owner = {'pid':202, 'start_ticks':201, 'boot_id':BOOT_ID, 'session':201, 'pgid':201,
                 'guardian':{'pid':201, 'start_ticks':200, 'boot_id':BOOT_ID}, 'started_unix':time.time()-60}
        self.assertEqual(sorted(p[:2] for p in S.owner_processes(record)), [(201, 200), (202, 201)])
        self.assertEqual(sorted(p[:2] for p in S.owner_processes(owner)), [(201, 200), (202, 201)])
        # The native process survives in its own session after its guardian exits.
        base = fake_process(self.proc, 202, started=BOOT+201.5/TICKS, ppid=1, session=202, pgrp=202)
        for value in (record, owner):
            self.worker_input(dict(value, status='staged'))
            with self.subTest(value=sorted(value)):
                self.refuses('live recorded owner PID 202')
        # The native identity inherits its guardian's boot.
        self.worker_input(dict(record, status='staged', child=dict(record['child'], boot_id=EARLIER_BOOT_ID)))
        self.snapshot().preflight()
        shutil.rmtree(base)
        fake_process(self.proc, 201, started=BOOT+200.5/TICKS)
        self.worker_input(dict(owner, status='staged'))
        self.refuses('live recorded owner PID 201')

    def test_owned_containers_beyond_the_depth_bound_refuse(self):
        nested = {'pid':301, 'start_ticks':300}
        for _ in range(S.OWNED_DEPTH):
            nested = {'child':nested}
        self.assertEqual([p[:2] for p in S.owner_processes(nested)], [(301, 300)])
        for deeper in ({'child':nested}, {'children':[{'identity':nested}]}, {'native':{'guardian':nested}}):
            with self.subTest(deeper=list(deeper)):
                with self.assertRaisesRegex(ValueError, 'beyond the depth bound'):
                    S.owner_processes(deeper)
        self.worker_input({'status':'staged', 'finished_unix':time.time()-60, **nested})
        self.snapshot().preflight()
        self.worker_input({'status':'staged', 'finished_unix':time.time()-60, 'launch':nested})
        self.refuses('nests owned processes beyond the depth bound')

    def test_detached_native_survivor_without_markers_refuses(self):
        # Own session, reparented, lock descriptor and environment gone, no wrapper argv.
        tool = str(S.P.ROOT/'build/evidence/release-x/artifacts/shard-publish')
        base = fake_process(self.proc, 5000, started=time.time()-30, ppid=1, exe=tool, argv=[tool, '--data-dir', 'j'])
        self.refuses('unattributable live mutation or native process: PID 5000')
        shutil.rmtree(base)
        base = fake_process(self.proc, 5001, started=time.time()-30, exe='/usr/bin/python3',
                            argv=['/usr/bin/python3', '-B', str(S.I.SOURCE/('a'*40)/'transparent/ops/lib/x.py')])
        self.refuses('unattributable live mutation or native process: PID 5001')
        shutil.rmtree(base)
        base = fake_process(self.proc, 5002, started=time.time()-30, exe='/tmp/copied', argv=['event-spotcheck'])
        self.refuses('PID 5002')
        shutil.rmtree(base)
        # The same classes inside a reviewed long-running service are attributable.
        fake_process(self.proc, 5003, started=time.time()-30, exe='/usr/bin/python3',
                     argv=['/usr/bin/python3', '-B', str(S.I.SOURCE/('a'*40)/'transparent/ops/scripts/transparent-live-fleet.py')],
                     cgroup='/system.slice/transparent-publish-controller.service')
        fake_process(self.proc, 5004, started=time.time()-30, exe='/usr/local/bin/transparent-shard-server',
                     cgroup='/system.slice/transparent-shard-server.service')
        self.snapshot().preflight()
        base = fake_process(self.proc, 5005, started=time.time()-30, exe=tool,
                            cgroup='/system.slice/transparent-activity-full-publication-v11.service')
        self.refuses('PID 5005')  # A transient operation unit is not a reviewed service.

    def test_parent_closure_attributes_unmarked_children_of_live_wrappers(self):
        fake_process(self.proc, 5100, started=time.time()-30, wrapper=True)
        fake_process(self.proc, 5101, started=time.time()-20, ppid=5100, session=5101)
        fake_process(self.proc, 5102, started=time.time()-10, ppid=5101, session=5102)
        self.refuses('live wrapper process or descendant: PID 5100, 5101, 5102')

    def test_probe_reads_real_proc_markers_without_contents(self):
        lock = self.root/'real.lock'
        lock.touch(mode=0o600)
        holder = subprocess.Popen([PYTHON, '-c', 'import os,sys;f=os.open(sys.argv[1],os.O_RDONLY);'
                                   'print(1,flush=True);sys.stdin.read()', str(lock)],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  env=dict(os.environ, WALLET_PIR_PRODUCTION_LOCK_FDS='3'))
        holder.stdout.readline(); holder.stdout.close()
        self.addCleanup(release, holder)
        probe = S.host_helper.ownership_probe([], str(lock), str(S.MACHINE_ID), {'roots':[], 'tools':[]})
        entries = {p['pid']:p for p in probe['processes']}
        self.assertEqual((entries[holder.pid]['lock_fd'], entries[holder.pid]['inherited']), (True, True))
        self.assertTrue(entries[os.getpid()].get('self'))
        self.assertFalse(any({'environ', 'cmdline'} & set(p) for p in probe['processes']))

    def test_owner_rechecks_every_host_before_any_effect(self):
        snapshot = self.snapshot()
        original = self.systemd.state()['pid']
        with self.assertRaisesRegex(ValueError, 'live wrapper process'):
            self.inprocess(snapshot, lambda: fake_process(self.proc, 4800, started=time.time(), wrapper=True))
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['phase']), ('failed', 'adopted'))
        self.assertEqual((record['restoration']['started'], record['restoration']['writer']['pid']), (False, original))
        self.assertFalse(snapshot.partial.exists())
        self.assertEqual(self.receipts(snapshot, 'owner')['worker-01']['result'], 'refused')

    def test_reconcile_rechecks_every_host_and_its_inventory(self):
        state = self.systemd.state(); state['stop_error'] = True; self.systemd.write(state)
        snapshot = self.snapshot()
        with self.assertRaises(subprocess.CalledProcessError):
            self.inprocess(snapshot)
        (self.root/'hosts/worker-01.unreachable').touch()
        with self.assertRaises(S.RemoteError):
            self.reconcile(snapshot)
        self.assertEqual(self.record(snapshot)['status'], 'failed')
        self.assertEqual(self.receipts(snapshot, 'reconcile')['worker-01']['result'], 'refused')
        (self.root/'hosts/worker-01.unreachable').unlink()
        snapshot.inventory = S.descriptors.load_inventory(self.write_inventory(extra={'machine_id':'f'*32}))
        with self.assertRaisesRegex(ValueError, 'reconcile inventory differs'):
            self.reconcile(snapshot)
        snapshot.inventory = S.descriptors.load_inventory(self.write_inventory())
        self.assertEqual(self.reconcile(snapshot)['status'], 'reconciled')
        self.assertEqual(self.receipts(snapshot, 'reconcile')['worker-01']['result'], 'passed')


class Bounds(Fixture):
    """One total deadline and sampled floors reach every long step."""

    def test_every_long_step_refuses_an_expired_budget(self):
        expired = S.Budget(time.monotonic()-1, 'expired')
        request = self.request()
        _, blocks = lengths(self.journal)
        steps = [lambda: S.sidecar_estimate(self.journal, blocks, request['sidecars'], expired),
                 lambda: list(S.iterate_records(self.journal/'blocks.bin', blocks, expired)),
                 lambda: S.hash_file(self.journal/'events.bin', expired),
                 lambda: self.snapshot().anchors(expired, 0, display(0)),
                 lambda: S.fleet(self.inventory, expired, S.SSHExecutor, lock_path=S.ProductionLock.PATH),
                 lambda: self.snapshot().preflight(expired)]
        for number, step in enumerate(steps):
            with self.subTest(step=number), self.assertRaises(subprocess.TimeoutExpired):
                step()

    def test_plan_scans_are_bounded_by_the_total(self):
        with patch.object(S.Snapshot, 'budget', lambda self, label: S.Budget(time.monotonic()-1, label)):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.snapshot().plan()

    def test_verification_samples_resources(self):
        snapshot = self.snapshot()
        real = S.inspect

        def inspect(*args):
            (self.root/'meminfo').write_text('MemTotal: 1000 kB\nMemAvailable: 100 kB\n')
            return real(*args)
        with patch.object(S, 'inspect', inspect):
            with self.assertRaisesRegex(ValueError, 'memory headroom'):
                self.inprocess(snapshot)
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['restoration']['status']), ('failed', 'proven'))
        self.assertTrue(snapshot.partial.is_dir() and not snapshot.target.exists())
        samples = [json.loads(line) for line in snapshot.health.read_text().splitlines()]
        refused = [s for s in samples if 'refused' in s]
        self.assertEqual([s['phase'] for s in refused], ['journal snapshot verification'])
        self.assertIn('memory headroom', refused[0]['error'])

    def test_total_deadline_reaches_verification(self):
        bounds = {'precopy_seconds':1, 'stop_seconds':1, 'copy_seconds':1, 'restart_seconds':2,
                  'restart_attempts':1, 'total_seconds':6}
        snapshot = self.snapshot(self.request(bounds=bounds))
        real = S.hash_file

        def slow(path, budget):
            if budget.label == 'journal snapshot verification':
                time.sleep(max(0, budget.remaining())+.05)
            return real(path, budget)
        with patch.object(S, 'hash_file', slow):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.inprocess(snapshot)
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['restoration']['status']), ('interrupted', 'proven'))
        self.assertTrue(snapshot.partial.is_dir() and not snapshot.target.exists())

    def test_quiesced_reserve_counts_sidecars_added_before_quiescence(self):
        big = internal(3)
        body = b'TPIRTX01'+big+(0).to_bytes(4, 'little')+b'\x00'+bytes(5 << 20)

        def late():
            (self.journal/'display-v1'/display_file(3)).write_bytes(body+hashlib.sha256(body).digest())
            (self.root/'disk.json').write_text(json.dumps({'bavail':204, 'blocks':1000}))
        real = S.Snapshot.precopy

        def precopy(self_, *args):
            result = real(self_, *args)
            late()
            return result
        snapshot = self.snapshot()
        with patch.object(S.Snapshot, 'precopy', precopy):
            with self.assertRaisesRegex(ValueError, 'disk headroom'):
                self.inprocess(snapshot)
        record = self.record(snapshot)
        self.assertEqual((record['status'], record['phase'], record['restoration']['status']),
                         ('failed', 'restored', 'proven'))
        self.assertFalse((snapshot.partial/'journal/events.bin').exists())  # Refused before writing.

    def test_full_verification_rederives_boundaries_and_anchors(self):
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        manifest = S.manifest_of(snapshot.target, record['manifest'])
        budget = S.Budget(time.monotonic()+60, 'test')
        expected = S.expected_hashes(snapshot.request, manifest)
        self.assertTrue({0, THROUGH, BLOCKS-1} <= set(expected))
        self.assertEqual(S.inspect(snapshot.target, snapshot.request, expected, budget), manifest['contents'])
        with self.assertRaisesRegex(ValueError, 'differs at height 2'):
            S.inspect(snapshot.target, snapshot.request, {**expected, 2:'0'*64}, budget)
        blocks = snapshot.target/'journal/blocks.bin'
        blocks.chmod(0o600)
        with open(blocks, 'r+b') as stream:
            stream.seek(2*48+32); stream.write((10**6).to_bytes(8, 'little'))
        blocks.chmod(0o400)
        with self.assertRaisesRegex(ValueError, 'boundary|decrease|past the committed'):
            S.verify_tree(snapshot.target, record['manifest'], request=snapshot.request, budget=budget)


class Transport(unittest.TestCase):
    """The real anchor RPC client against local loopback servers."""

    SECRET = 'rpcuser:cookie-secret-value'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        cookie = Path(self.temp.name)/'cookie'
        cookie.write_text(self.SECRET)
        self.server = socket.socket(); self.server.bind(('127.0.0.1', 0)); self.server.listen(4)
        self.addCleanup(self.server.close)
        # Transport only: resource sampling is covered by the fixture suites.
        for name, value in (('COOKIE', cookie), ('RPC', self.server.getsockname()), ('resources', lambda *a, **k: {})):
            p = patch.object(S, name, value); p.start(); self.addCleanup(p.stop)

    def serve(self, answer):
        """Accept one request and answer with `answer(connection, request)`."""
        seen = {}

        def run():
            connection, _ = self.server.accept()
            with connection:
                request = b''
                while b'\r\n\r\n' not in request:
                    request += connection.recv(4096)
                seen['request'] = request
                try:
                    answer(connection)
                except OSError:
                    pass
        thread = threading.Thread(target=run, daemon=True); thread.start()
        self.addCleanup(thread.join, 5)
        return seen

    def body(self, height=7, value=None):
        return json.dumps({'id':height, 'error':None, 'result':value or 'ab'*32}).encode()

    def ask(self, seconds=5):
        return S.Node().block_hash(7, S.Budget(time.monotonic()+seconds, 'rpc'))

    def test_content_length_and_chunked_answers(self):
        body = self.body()
        seen = self.serve(lambda c: c.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n' % len(body)+body))
        self.assertEqual(self.ask(), 'ab'*32)
        self.assertIn(b'Authorization: Basic', seen['request'])
        chunked = b'%x\r\n%s\r\n0\r\n\r\n' % (len(body), body)
        self.serve(lambda c: c.sendall(b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'+chunked))
        self.assertEqual(self.ask(), 'ab'*32)

    def test_redirects_errors_and_oversized_answers_refuse(self):
        for answer, message in ((b'HTTP/1.1 302 Found\r\nLocation: http://192.0.2.1/\r\nContent-Length: 0\r\n\r\n', 'not 200'),
                                (b'HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n{"id":7}', 'length differs'),
                                (b'HTTP/1.1 200 OK\r\nContent-Length: 70000\r\n\r\n'+b' '*70000, 'exceeds bound'),
                                (b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}', 'invalid anchor RPC response')):
            self.serve(lambda c, answer=answer: c.sendall(answer))
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.ask()

    def test_trickled_bytes_cannot_extend_the_aggregate_deadline(self):
        def trickle(connection):
            for byte in b'HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n'+b' '*100:
                connection.send(bytes([byte])); time.sleep(.1)
        self.serve(trickle)
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            self.ask(seconds=1)
        self.assertLess(time.monotonic()-started, 1.5)

    def test_failures_never_carry_credentials(self):
        self.server.close()  # Nothing listens: connection refused.
        with self.assertRaises(ValueError) as caught:
            self.ask()
        self.assertEqual(str(caught.exception), 'anchor RPC failed: ConnectionRefusedError')
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)
        for text in (self.SECRET, S.base64.b64encode(self.SECRET.encode()).decode()):
            self.assertNotIn(text, str(caught.exception))

    def test_budget_seconds_never_outlive_the_deadline(self):
        budget = S.Budget(time.monotonic()+.3, 'short')
        self.assertLessEqual(budget.seconds(), .3)
        with self.assertRaises(subprocess.TimeoutExpired):
            S.Budget(time.monotonic()-.01, 'over').seconds()


class Consumer(Fixture):
    """Root's locked consumer gets full verification or a refusal, never a manifest alone."""

    def test_full_verification_contract(self):
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        calls = []
        health = self.root/'consumer-health.ndjson'
        manifest = S.verify_snapshot(snapshot.identifier, deadline=time.monotonic()+60,
                                     guard=lambda: calls.append(1), health=health)
        self.assertEqual(manifest['contents'], S.manifest_of(snapshot.target, record['manifest'])['contents'])
        self.assertGreaterEqual(len(calls), 3)  # Before reading, with sampling, and at the end.
        self.assertIn('journal snapshot consumer verification', health.read_text())
        for options, message in (({'guard':None}, 'needs a guard'),
                                 ({'deadline':time.monotonic()-1}, 'future deadline')):
            arguments = {'deadline':time.monotonic()+60, 'guard':lambda: None, **options}
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                S.verify_snapshot(snapshot.identifier, **arguments)

        def lost():
            raise ValueError('consumer lock lost')
        with self.assertRaisesRegex(ValueError, 'consumer lock lost'):
            S.verify_snapshot(snapshot.identifier, deadline=time.monotonic()+60, guard=lost)
        events = snapshot.target/'journal/events.bin'
        events.chmod(0o600)
        with open(events, 'r+b') as stream:
            stream.seek(3); value = stream.read(1); stream.seek(3); stream.write(bytes([value[0] ^ 1]))
        events.chmod(0o400)
        S.manifest_of(snapshot.target, record['manifest'])  # Status alone cannot see the change.
        with self.assertRaisesRegex(ValueError, 'contents differ'):
            S.verify_snapshot(snapshot.identifier, deadline=time.monotonic()+60, guard=lambda: None)

    def test_nonfinite_deadlines_refuse_before_any_read(self):
        called = []
        for deadline in (float('inf'), float('nan'), float('-inf')):
            with self.subTest(deadline=deadline), self.assertRaisesRegex(ValueError, 'finite future deadline'):
                S.verify_snapshot('a'*64, deadline=deadline, guard=lambda: called.append(1))
            with self.assertRaisesRegex(ValueError, 'must be finite'):
                S.Budget(deadline, 'test')
        self.assertEqual(called, [])

    def test_unstaged_snapshots_refuse(self):
        state = self.systemd.state(); state['stop_error'] = True; self.systemd.write(state)
        snapshot = self.snapshot()
        with self.assertRaises(subprocess.CalledProcessError):
            self.inprocess(snapshot)
        with self.assertRaisesRegex(ValueError, 'not staged'):
            S.verify_snapshot(snapshot.identifier, deadline=time.monotonic()+60, guard=lambda: None)


class Tree(Fixture):
    def test_staged_snapshot_status_detects_namespace_misuse(self):
        snapshot = self.snapshot()
        record = self.inprocess(snapshot)
        events = snapshot.target/'journal/events.bin'
        os.link(events, self.root/'alias')
        with self.assertRaisesRegex(ValueError, 'single-link'):
            snapshot.status()
        (self.root/'alias').unlink()
        (snapshot.target/'journal/extra').write_bytes(b'x')
        with self.assertRaisesRegex(ValueError, 'file set'):
            snapshot.status()
        (snapshot.target/'journal/extra').unlink()
        events.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'private single-link'):
            snapshot.status()
        events.chmod(0o400)
        side = next((snapshot.target/'journal/display-v1').iterdir())
        side.unlink(); side.symlink_to(self.root/'elsewhere')
        with self.assertRaisesRegex(ValueError, 'private single-link'):
            S.verify_tree(snapshot.target, record['manifest'], request=snapshot.request, budget=S.Budget(time.monotonic()+60, 'test'))


class Wrapper(unittest.TestCase):
    def test_closed_wrapper_actions(self):
        from wallet_pir_ops.deploy import cli
        parser = cli.parser()
        for action in ('plan', 'preflight', 'status', 'reconcile'):
            args = parser.parse_args(['schema-snapshot-'+action, '--request', 'r', '--request-sha256', 'h'])
            self.assertEqual(args.command, 'schema-snapshot-'+action)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            parser.parse_args(['schema-snapshot-stage', '--request', 'r', '--request-sha256', 'h'])
        parser.parse_args(['schema-snapshot-stage', '--request', 'r', '--request-sha256', 'h', '--expect-plan-sha256', 'p'])
        self.assertEqual(parser.parse_args(['schema-snapshot-owner', '--request-sha256', 'h']).command, 'schema-snapshot-owner')
        output = []
        self.assertEqual(cli.main(['schema-snapshot-owner', '--request-sha256', 'not-hex'], out=output.append), 1)
        self.assertIn('invalid journal snapshot request digest', output[0])

    def test_production_constants(self):
        self.assertEqual(S.JOURNAL, Path('/srv/transparent-activity/full-v3/journal'))
        self.assertEqual(S.SNAPSHOTS, Path('/srv/transparent-activity/snapshots/journal'))
        self.assertEqual(S.OWNERS, Path('/srv/transparent-activity/ops/input-staging'))
        self.assertEqual((S.RECORD_BYTES, S.CHECKPOINT_BYTES, S.FORMAT_VERSION, S.START_HEIGHT), (48, 16, 3, 0))
        source = (HERE.parents[2]/'transparent/services/transparent-filter-server/src/events.rs').read_text()
        self.assertIn('const STORE_VERSION: u16 = 3;', source)
        self.assertIn('const BLOCK_RECORD_BYTES: usize = 32 + 8 + 8;', source)
        self.assertIn('lock.try_lock()', source)
        self.assertIn('.open(dir.join("writer.lock"))', source)
        events = (HERE.parents[2]/'transparent/crates/transparent-events/src/lib.rs').read_text()
        self.assertIn('pub const EVENT_BYTES: usize = %d;' % S.EVENT_BYTES, events)


if __name__ == '__main__':
    unittest.main()
