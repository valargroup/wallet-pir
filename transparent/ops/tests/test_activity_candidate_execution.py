"""Closed, locked candidate native execution with retained raw evidence.

Fixture executables (Python scripts with fixture pins) stand in for the
candidate's `shard-verify` and `native_certificate`; a fixture publication with
the pinned coverage, anchor and cutoff stands in for the initial v11 set. A
second fixture host (its own machine ID, owners, fence and lock) is surveyed by
a real subprocess. Real child processes, real flock locks, real rlimits, real
pidfd signals and real signals are used. Nothing contacts a host or runs a real
candidate executable.
"""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
from wallet_pir_ops import hostlock  # noqa: E402
from wallet_pir_ops.deploy import cli  # noqa: E402

MODULE = HERE.parent/'lib/activity_candidate_execution.py'


def _frames():
    frame = sys._getframe(1)
    while frame is not None:
        yield frame
        frame = frame.f_back


def load():
    import importlib.util
    spec = importlib.util.spec_from_file_location('candidate_execution_test', MODULE)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


E = load()
C, I, R = E.C, E.I, E.R
MACHINE = 'c'*32
WORKER = 'd'*32
OLD = C.HISTORICAL_SHA
HEALTHY = {'memory_available': .5, 'disk_available': {'/': .5}}
HOSTS = [{'host': 'coordinator', 'machine_id': MACHINE}, {'host': 'worker-a', 'machine_id': WORKER}]

# One fixture program for both executables: behaviour comes from a control file
# keyed by the dispatch, so the pinned script bytes never change per test.
SCRIPT = r'''#!{python} -B
import fcntl, hashlib, json, os, resource, subprocess, sys, time
control = json.load(open({control!r}))
args = sys.argv[1:]
flags = dict(zip(args[1::2], args[2::2])) if args and args[0] == 'segment' else dict(zip(args[::2], args[1::2]))
key = flags.get('--rows-bin', 'artifact')
mode = control.get('keys', {{}}).get(key, control.get('default', 'pass'))
fds = os.environ.get('WALLET_PIR_PRODUCTION_LOCK_FDS', '')
target = os.stat(control['lock'])
inherited = any((os.fstat(int(fd)).st_dev, os.fstat(int(fd)).st_ino) == (target.st_dev, target.st_ino) for fd in fds.split(',') if fd)
probe = os.open(control['lock'], os.O_RDWR)
try:
    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB); held = False
except BlockingIOError:
    held = True
os.close(probe)
sys.stderr.write('lock-inherited=%s lock-held=%s sid=%d as=%d cpu=%d\n' % (
    inherited, held, os.getsid(0), resource.getrlimit(resource.RLIMIT_AS)[0], resource.getrlimit(resource.RLIMIT_CPU)[0]))
sys.stderr.flush()
open(control['ran'], 'a').write(key + '\n')
if mode == 'fail':
    sys.exit(3)
if mode == 'allocate':
    block = bytearray(1 << 30)
if mode == 'spin':
    while True:
        pass
if mode in ('sleep', 'fork', 'escape', 'block', 'unlock'):
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=mode == 'escape',
                             pass_fds=[] if mode == 'unlock' else [int(f) for f in fds.split(',') if f])
    open(control['mark'], 'w').write(json.dumps({{'pid': os.getpid(), 'grandchild': child.pid}}))
    if mode == 'block':
        # Blocked in a read that never completes: no CPU, so RLIMIT_CPU never fires.
        blocked, writer = os.pipe()
        os.read(blocked, 1)
    elif mode != 'fork':
        time.sleep(60)
    sys.exit(0)
if mode == 'noisy':
    sys.stderr.write('e' * 4096)
if '--rows-bin' in flags:
    raw = open(flags['--rows-bin'], 'rb').read()
    print(json.dumps({{'database_sha256': 'rows_sha256:' + hashlib.sha256(raw).hexdigest(), 'rows': len(raw) // 4096,
                      'product': 'transparent-segment', 'served_public_sha256': 'c' * 64, 'fixture_bits': 128,
                      'geometry': flags['--geometry'], 'table': flags['--table']}}))
else:
    mapping = open(os.path.join(flags['--shard-dir'], 'shards.json'), 'rb').read()
    rows = json.loads(mapping)['shards']
    checks = ['load', 'coverage-contiguous', 'coverage-start', 'coverage-through', 'anchor-hash', 'tiers', 'map-sha256']
    print(json.dumps({{'schema': 'transparent-shard-verify-v1', 'tool_sha': flags['--source-sha'], 'failures': 0,
                      'checks': [{{'check': c, 'ok': True}} for c in checks],
                      'set': {{'map_file_sha256': hashlib.sha256(mapping).hexdigest(), 'shards': len(rows),
                              'start_height': 0, 'through': rows[-1]['end_height'],
                              'terminal_block_hash': rows[-1]['terminal_block_hash'],
                              'geometries': ['archive-wide', 'recent-4k-8k']}}}}))
'''

# One pinned host as the staged wrapper would run it. argv after the config is
# the real receiver argv shape, so process scans see a real receiver.
HARNESS = r'''
import argparse, json, os, sys, time
from pathlib import Path
config = json.loads(Path(sys.argv[1]).read_text())
sys.path.insert(0, config['lib'])
import importlib.util
spec = importlib.util.spec_from_file_location('candidate_execution_child', config['module'])
E = importlib.util.module_from_spec(spec); spec.loader.exec_module(E)
from wallet_pir_ops import hostlock
C = E.C
C.ARTIFACTS.update(config['artifacts']); C.ROOT = Path(config['candidates']); C.TARGET = Path(config['target'])
C.OWNER = os.getuid(); C.abi = lambda name, data: None
E.OWNERS = Path(config['owners']); E.PUBLICATION = Path(config['publication']); E.MAP_SHA256 = config['map']
E.SCHEMA = Path(config['fence']) / 'schema'
E.MACHINE_ID = Path(config['machine'])
def source():
    if config['source'] is None:
        raise ValueError('operations source is not staged on this host')
    return config['source']
E.staged_source = source
E.host_abi = lambda: {'machine': 'x86_64', 'libc': E.GLIBC, 'cpu_flags': sorted(E.CPU_FLAGS)}
E.resources = lambda paths: {'observed_unix': time.time(), 'memory_available': .5, 'disk_available': {'/': .5}}
E.SAMPLE_SECONDS = .1
for name, value in config.get('constants', {}).items():
    setattr(E, name, value.encode() if name == 'RECEIVE' else value)
for mode, budget in config.get('budgets', {}).items():
    E.BUDGETS[mode] = budget
fence = E.schema_fence.local_schema_fence
E.schema_fence.INPUT_STAGING = Path(config['owners'])
E.schema_fence.HOST_ACTIONS = Path(config['fence']) / 'host-actions'
E.schema_fence.local_schema_fence = lambda **options: fence(Path(config['fence']) / 'schema', **options)
class Lock(hostlock.PinnedHostLock):
    ROOT_UID = os.getuid()
    MACHINE_ID = Path(config['machine'])
E.ProductionLock = lambda value: Lock(value, path=config['lock'])
if config.get('crash'):
    # Model the receiver dying at one exact point of the launch window.
    write_once, save = E.write_once, E.Receiver.save
    def crashing_write(path, raw, mode=0o400):
        if config['crash'] == 'before-owner' and Path(path).name == 'owner.json':
            os._exit(137)
        return write_once(path, raw, mode)
    def crashing_save(self, record):
        save(self, record)
        if config['crash'] == 'before-go' and record.get('child'):
            os._exit(137)
    E.write_once, E.Receiver.save = crashing_write, crashing_save
parser = argparse.ArgumentParser()
parser.add_argument('command'); parser.add_argument('--action'); parser.add_argument('--request-sha256')
parser.add_argument('--expect-plan-sha256')
args = parser.parse_args(sys.argv[2:])
sys.exit(E.receive(args.action, args.request_sha256, 0, print, args.expect_plan_sha256))
'''

# Plays a lost SSH session: the remote receiver keeps running in its own session
# with the original pipes after the local transport process dies.
RELAY = r'''
import subprocess, sys
child = subprocess.Popen(sys.argv[1:], start_new_session=True)
child.wait()
'''


class Lock(hostlock.PinnedHostLock):
    ROOT_UID = os.getuid()


class Channel:
    """In-process stand-in for the root client's side of the lock handshake."""

    def __init__(self, fixture, skip=None, mutate=None):
        self.fixture, self.skip, self.mutate = fixture, skip, mutate
        self.emitted, self.line, self.request = [], None, None

    def emit(self, value):
        self.emitted.append(value)
        if value.get('phase') == 'locked':
            surveys = {'worker-a': self.fixture.survey_worker(value['nonce'], self.skip, self.request)}
            if self.mutate:
                surveys = self.mutate(surveys, value)
            self.line = json.dumps({'surveys': surveys})

    def read(self, timeout):
        if self.line is None:
            time.sleep(min(timeout, .05))
            raise TimeoutError
        line, self.line = self.line, None
        return line


class Fixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.machine = self.root/'machine-id'; self.machine.write_text(MACHINE+'\n')
        Lock.MACHINE_ID = self.machine
        self.lockpath = self.root/'production.lock'
        self.control = self.root/'control.json'; self.mark = self.root/'mark.json'; self.ran = self.root/'ran.log'
        self.behave()
        self.owners = self.root/'owners'; self.owners.mkdir(mode=0o700)
        self.fence = self.root/'fence'
        self.worker = self.root/'worker'; (self.worker/'owners').mkdir(parents=True, mode=0o700)
        (self.worker/'machine-id').write_text(WORKER+'\n')
        self.worker_source = 'e'*40
        candidates = self.root/'candidates'
        script = SCRIPT.format(python=sys.executable, control=str(self.control)).encode()
        data = {name: (script if name in E.MODES.values() else b'\x7fELF fixture '+name.encode()) for name in C.ARTIFACTS}
        self.pins = {name: hashlib.sha256(raw).hexdigest() for name, raw in data.items()}
        for thing, name, value in ((C, 'ROOT', candidates), (C, 'TARGET', candidates/('release-'+C.SOURCE_SHA)),
                                   (C, 'OWNER', os.getuid()), (C, 'abi', lambda name, raw: None),
                                   (E, 'OWNERS', self.owners), (E, 'MACHINE_ID', self.machine),
                                   (E, 'staged_source', lambda: 'e'*40), (E, 'SAMPLE_SECONDS', .1),
                                   (E, 'host_abi', lambda: {'machine': 'x86_64', 'libc': E.GLIBC, 'cpu_flags': sorted(E.CPU_FLAGS)}),
                                   (E, 'resources', lambda paths: dict(HEALTHY, observed_unix=time.time())),
                                   (E.schema_fence, 'INPUT_STAGING', self.owners), (E, 'SCHEMA', self.fence/'schema'),
                                   (E.schema_fence, 'HOST_ACTIONS', self.fence/'host-actions')):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        p = patch.dict(C.ARTIFACTS, self.pins); p.start(); self.addCleanup(p.stop)
        real = E.schema_fence.local_schema_fence
        p = patch.object(E.schema_fence, 'local_schema_fence', lambda **o: real(self.fence/'schema', **o))
        p.start(); self.addCleanup(p.stop)
        self.bundle(data)
        self.publication = self.root/'publication'
        self.publish()
        p = patch.object(E, 'PUBLICATION', self.publication); p.start(); self.addCleanup(p.stop)
        self.constants, self.budgets = {}, {}

    def tearDown(self):
        # Never leave fixture processes behind, even after a failed assertion.
        for item in E.scan():
            if item['token'] and item['token'].startswith(tuple('0123456789abcdef')) and item['pid'] != os.getpid():
                try:
                    os.kill(item['pid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if self.mark.exists() and self.mark.stat().st_size:
            for pid in json.loads(self.mark.read_text()).values():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def behave(self, default='pass', **keys):
        self.control.write_text(json.dumps({'default': default, 'keys': keys, 'lock': str(self.lockpath),
                                            'mark': str(self.mark), 'ran': str(self.ran)}))

    def bundle(self, data):
        target = C.TARGET
        (target/'artifacts/examples').mkdir(parents=True)
        for name, raw in data.items():
            path = target/'artifacts'/name; path.write_bytes(raw); path.chmod(C.BINARY_MODE)
        record = {**C.provenance(), 'identity': C.identity(),
                  'archives': {**{kind: hashlib.sha256(kind.encode()).hexdigest() for kind in C.CI_KINDS},
                               'supplemental': C.SUPPLEMENTAL_ARCHIVE_SHA256}}
        provenance = target/'provenance.json'; provenance.write_bytes(I.durable.canonical(record)+b'\n'); provenance.chmod(0o400)
        prepared = {'version': 1, 'source_sha': 'e'*40, 'candidate_sha': C.SOURCE_SHA, 'ci_run': C.CI_RUN, 'attempt': 1,
                    'archives': {kind: '/retained/'+kind for kind in (*C.CI_KINDS, 'supplemental')}}
        self.preparation = I.digest(prepared)
        files = {'provenance.json': C.checksum(provenance), **{'artifacts/'+n: s for n, s in self.pins.items()}}
        I.durable.atomic_json(self.owners/(self.preparation+'.request.json'), prepared, mode=0o400)
        I.durable.atomic_json(self.owners/(self.preparation+'.json'),
            {'kind': I.CandidatePreparation.KIND, 'request_sha256': self.preparation, 'status': 'staged',
             'files': files, 'target': str(target), 'plan_sha256': '0'*64})
        I.durable.atomic_json(self.owners/'latest.json', {'request_sha256': self.preparation})

    def publish(self, extra_segment=False):
        """90 shards, pinned coverage/anchor/cutoff, 180 one-row table segments."""
        root = self.publication; root.mkdir()
        bounds = [round(i*E.RECENT_FROM/82) for i in range(82)]+[E.RECENT_FROM+round(i*(E.THROUGH+1-E.RECENT_FROM)/8) for i in range(8)]
        ends = [b-1 for b in bounds[1:]]+[E.THROUGH]
        rows = []
        for shard, (start, end) in enumerate(zip(bounds, ends)):
            geometry = 'recent-4k-8k' if start >= E.RECENT_FROM else 'archive-wide'
            tables = {}
            for table in ('directory', 'pages'):
                count = 2 if extra_segment and shard == 5 and table == 'pages' else 1
                tables[table] = [bytes([shard, len(table), index])*(4096//3)+b'\x00' for index in range(count)]
            manifest = {'schema': 'transparent-shard-v11', 'shard_id': shard, 'geometry': geometry,
                        'directory_segments': [{'rows': 1, 'row_bytes': 4096, 'sha256': hashlib.sha256(raw).hexdigest()}
                                               for raw in tables['directory']],
                        'page_segments': [{'rows': 1, 'row_bytes': 4096, 'sha256': hashlib.sha256(raw).hexdigest()}
                                          for raw in tables['pages']]}
            raw = json.dumps(manifest, sort_keys=True).encode()
            name = hashlib.sha256(raw).hexdigest()
            directory = root/name; directory.mkdir()
            (directory/'manifest.json').write_bytes(raw); (directory/'filter.bin').write_bytes(b'filter')
            for table, items in tables.items():
                for index, item in enumerate(items):
                    (directory/('%s.%d.bin' % (table, index))).write_bytes(item)
            rows.append({'shard_id': shard, 'geometry': geometry, 'start_height': start, 'end_height': end,
                         'manifest_digest': name, 'terminal_block_hash': E.ANCHOR_HASH if shard == 89 else '%064x' % shard})
        mapping = json.dumps({'start_height': 0, 'shards': rows}).encode()
        (root/'shards.json').write_bytes(mapping)
        for path in [root, *root.rglob('*')]:
            path.chmod(0o755 if path.is_dir() else 0o444)
        self.rows = rows
        p = patch.object(E, 'MAP_SHA256', hashlib.sha256(mapping).hexdigest()); p.start(); self.addCleanup(p.stop)

    def request(self, mode='artifact-verification', **change):
        value = {'version': 2, 'kind': E.KIND, 'mode': mode, 'source_sha': 'e'*40, 'candidate_sha': C.SOURCE_SHA,
                 'candidate_identity': C.identity(), 'preparation_request_sha256': self.preparation,
                 'publication_sha256': E.MAP_SHA256, 'coordinator': 'coordinator', 'machine_id': MACHINE,
                 'hosts': copy.deepcopy(HOSTS), 'attempt': 1}
        value.update(change)
        return value

    def lock(self):
        return Lock({'type': 'pinned_host', 'machine_id': MACHINE}, path=self.lockpath)

    def receiver(self, mode='artifact-verification', **change):
        return E.Receiver(self.request(mode, **change), lock_factory=self.lock)

    def stage(self, receiver, channel=None, plan=None):
        channel = channel or Channel(self)
        channel.request = channel.request or receiver.request
        return receiver.stage(plan or I.digest(receiver.plan()), channel)

    def reconcile(self, receiver, channel=None):
        channel = channel or Channel(self, skip=receiver.identifier)
        channel.request = channel.request or receiver.request
        return receiver.reconcile(channel)

    def fenced(self):
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            E.schema_fence.local_schema_fence()

    def exited(self, receiver):
        """Model the receiver process having ended, as it does on the coordinator."""
        dead = subprocess.Popen(['true']); start = E.process(dead.pid)['start_ticks']; dead.wait()
        record = json.loads(receiver.owner.read_text())
        record['receiver'] = {'pid': dead.pid, 'start_ticks': start, 'boot_id': E.boot_id()}
        receiver.save(record)
        return record

    def captures(self, receiver):
        return sorted(p for p in receiver.evidence.iterdir() if p.is_dir() and p.name != 'inputs')

    def wait_for(self, predicate, timeout=20):
        deadline = time.monotonic()+timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.05)
        self.fail('condition not reached')

    def kill(self, pid, start_ticks):
        """Cleanup of an exact fixture process; a reused PID is untouched."""
        E.signal_exact(pid, start_ticks, signal.SIGKILL)

    def gone(self, pid):
        current = E.process(pid)
        return current is None or current['state'] == 'Z'

    def shorten(self, wall, grace=1):
        """Short artifact wall and guardian deadlines, the same in this process and every harness."""
        budget = dict(E.BUDGETS['artifact-verification'], wall_seconds=wall)
        self.budgets['artifact-verification'] = budget
        self.constants['GUARDIAN_GRACE_SECONDS'] = grace
        for p in (patch.dict(E.BUDGETS, {'artifact-verification': budget}), patch.object(E, 'GUARDIAN_GRACE_SECONDS', grace)):
            p.start(); self.addCleanup(p.stop)

    def config(self, host='coordinator', **change):
        values = {'lib': str(HERE.parents[2]/'ops/lib'), 'module': str(MODULE), 'artifacts': self.pins,
                  'candidates': str(C.ROOT), 'target': str(C.TARGET), 'publication': str(self.publication),
                  'map': E.MAP_SHA256, 'constants': self.constants, 'budgets': self.budgets, 'source': 'e'*40}
        if host == 'coordinator':
            values.update(owners=str(self.owners), machine=str(self.machine), fence=str(self.fence), lock=str(self.lockpath))
        else:
            # Both fixture hosts share one /proc: the worker recognises only
            # receivers named for it, as a separate machine would.
            values.update(owners=str(self.worker/'owners'), machine=str(self.worker/'machine-id'),
                          fence=str(self.worker/'fence'), lock=str(self.worker/'production.lock'), source=self.worker_source,
                          constants=dict(self.constants, RECEIVE='worker-only-receiver'))
        values.update(change)
        path = self.root/('harness-%s-%d.json' % (host, time.monotonic_ns()))
        path.write_text(json.dumps(values))
        script = self.root/'harness.py'
        if not script.exists():
            script.write_text(HARNESS)
        return [sys.executable, '-B', str(script), str(path)]

    def harness(self, action, identifier, expect_plan=None, host='coordinator', **change):
        argv = [*self.config(host, **change), 'schema-candidate-execute-receive', '--action', action,
                '--request-sha256', identifier]
        return argv+(['--expect-plan-sha256', expect_plan] if expect_plan else [])

    def survey_worker(self, nonce, skip=None, request=None):
        request = request or self.request()
        envelope = I.durable.canonical({'request': request, 'nonce': nonce, 'host': 'worker-a', 'skip': skip})+b'\n'
        result = subprocess.run(self.harness('survey', I.digest(request), host='worker'), input=envelope,
                                capture_output=True, timeout=60)
        return result.stdout.decode().strip()

    def surveys(self, receiver):
        found = receiver.surveys.iterdir() if receiver.surveys.exists() else []
        return sorted(found, key=lambda path: json.loads((path/'index.json').read_text())['retained_unix'])

    def holder(self, path):
        """A separate process holding a lock, as a foreign owner would."""
        process = subprocess.Popen([sys.executable, '-c', 'import fcntl, os, sys, time\n'
                                    'fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)\n'
                                    'fcntl.flock(fd, fcntl.LOCK_EX); print("held", flush=True); time.sleep(60)',
                                    str(path)], stdout=subprocess.PIPE)
        self.assertEqual(process.stdout.readline(), b'held\n')
        self.addCleanup(lambda: (process.kill(), process.wait(), process.stdout.close()))
        return process


class Closed(Fixture):
    def test_artifact_plan_is_one_closed_argv_with_every_expectation_and_reviewed_budgets(self):
        plan = self.receiver().plan()
        self.assertEqual(len(plan['dispatches']), 1)
        argv = plan['dispatches'][0]['argv']
        self.assertEqual(argv, [str(C.TARGET/'artifacts/shard-verify'), '--shard-dir', str(self.publication),
                                '--expect-start', '0', '--expect-through', '3500738', '--expect-anchor-hash', E.ANCHOR_HASH,
                                '--expect-recent-from', '3289805', '--expect-recent-geometry', 'recent-4k-8k',
                                '--expect-archive-geometry', 'archive-wide', '--expect-map-sha256', E.MAP_SHA256,
                                '--source-sha', C.SOURCE_SHA])
        self.assertFalse({'--data-dir', '--rebuild', '--rebuild-all', '--publication', '--out'} & set(argv))
        budget = plan['budgets']
        self.assertEqual((budget['wall_seconds'], budget['cpu_seconds'], budget['address_space_bytes']),
                         (1800, 1800, 14 << 30))
        self.assertEqual(budget['aggregate_seconds'], 1800+3600)
        self.assertEqual(budget['transport_seconds'], 1800+3600+900)
        self.assertIn('not a shard-verify measurement', budget['basis'])
        self.assertIn('lock acquisition', budget['aggregate_scope'])
        self.assertEqual((budget['concurrency'], budget['floor'], budget['max_gap_seconds']), (1, .2, 10))
        self.assertNotIn('memory_bytes', budget)
        self.assertEqual(budget['guardian_seconds'], 1800+60)
        self.assertEqual(plan['hosts'], HOSTS); self.assertEqual(plan['guardian_sha256'], E.GUARDIAN_SHA256)
        self.assertEqual(I.digest(plan), I.digest(self.receiver().plan()))
        self.assertEqual({p.name for p in self.owners.iterdir()},
                         {'latest.json', self.preparation+'.json', self.preparation+'.request.json'})
        self.assertFalse((C.ROOT/'executions').exists())

    def test_certificate_plan_covers_all_180_manifest_segments_with_root_limits(self):
        plan = self.receiver('native-certificates').plan()
        items = plan['dispatches']
        self.assertEqual(len(items), 180); self.assertEqual(len({i['key'] for i in items}), 180)
        self.assertEqual({(i['segment']['shard_id'], i['segment']['table']) for i in items},
                         {(s, t) for s in range(90) for t in ('directory', 'pages')})
        for item in items:
            segment = item['segment']
            self.assertEqual(item['argv'], [str(C.TARGET/'artifacts/examples/native_certificate'), 'segment',
                             '--geometry', segment['geometry'], '--table', segment['table'], '--rows-bin',
                             str(self.publication/segment['manifest_digest']/('%s.0.bin' % segment['table']))])
            self.assertNotIn('synthetic', item['argv'])
        self.assertEqual(sum(i['segment']['geometry'] == 'recent-4k-8k' for i in items), 16)
        budget = plan['budgets']
        self.assertEqual((budget['wall_seconds'], budget['cpu_seconds'], budget['address_space_bytes']), (600, 600, 14 << 30))
        self.assertEqual(budget['aggregate_seconds'], 180*600+3600)
        self.assertIn('root decision', budget['basis'])

    def test_closed_request_refuses_historical_foreign_partial_and_host_inventory_drift(self):
        good = self.request()
        for change in ({'candidate_sha': OLD}, {'candidate_identity': '0'*64}, {'publication_sha256': '0'*64},
                       {'mode': 'independent-chain-oracle'}, {'mode': 'load'}, {'attempt': 0}, {'attempt': True},
                       {'machine_id': 'C'*32}, {'version': 1}, {'kind': 'candidate-archive-upload'},
                       {'argv': ['/bin/sh']}, {'preparation_request_sha256': 'x'},
                       {'hosts': HOSTS[1:]}, {'hosts': HOSTS[::-1]}, {'hosts': HOSTS+[{'host': 'w', 'machine_id': WORKER}]},
                       {'hosts': [HOSTS[0], dict(HOSTS[1], host='worker a')]}, {'coordinator': 'worker-a'},
                       {'hosts': [dict(h, extra=1) for h in HOSTS]}, {'hosts': []}):
            with self.subTest(change), self.assertRaises(ValueError):
                E.validate(dict(copy.deepcopy(good), **change))
        partial = dict(good); del partial['preparation_request_sha256']
        with self.assertRaises(ValueError): E.validate(partial)
        for change in ({'status': 'failed'}, {'kind': 'coordinator-input-preparation'}, {'target': '/elsewhere'}):
            record = json.loads((self.owners/(self.preparation+'.json')).read_text())
            I.durable.atomic_json(self.owners/(self.preparation+'.json'), dict(record, **change))
            with self.subTest(change), self.assertRaisesRegex(ValueError, 'staged candidate preparation'):
                self.receiver().plan()
            I.durable.atomic_json(self.owners/(self.preparation+'.json'), record)
        with self.assertRaises(FileNotFoundError):
            self.receiver(preparation_request_sha256='f'*64).plan()

    def test_binary_publication_manifest_namespace_and_abi_drift_refuse_before_any_owner(self):
        binary = C.TARGET/'artifacts/shard-verify'
        cases = []
        def mutate_binary():
            binary.chmod(0o755); binary.write_bytes(binary.read_bytes()+b'#'); binary.chmod(C.BINARY_MODE)
        cases.append(('identity/mode', mutate_binary))
        cases.append(('map file hash', lambda: self.rewrite(self.publication/'shards.json', b' ')))
        cases.append(('manifest bytes', lambda: self.rewrite(self.publication/self.rows[3]['manifest_digest']/'manifest.json', b' ')))
        cases.append(('file boundary', lambda: self.rewrite(self.publication/self.rows[4]['manifest_digest']/'pages.0.bin', b'x')))
        cases.append(('foreign or missing', lambda: (self.publication/'extra').write_bytes(b'')))
        cases.append(('foreign or missing', lambda: (self.publication/self.rows[6]['manifest_digest']/'pages.9.bin').write_bytes(b'')))
        cases.append(('immutable', lambda: (self.publication/'shards.json').chmod(0o666)))
        for message, mutate in cases:
            with self.subTest(message):
                self.fresh_publication()
                mutate()
                with self.assertRaisesRegex(ValueError, message): self.receiver().preflight()
                with self.assertRaisesRegex(ValueError, message): self.stage(self.receiver())
                self.assertFalse((self.owners/(I.digest(self.request())+'.json')).exists())
        self.fresh_publication()
        link = self.root/'linked'; link.symlink_to(self.publication)
        with patch.object(E, 'PUBLICATION', link), self.assertRaisesRegex(ValueError, 'symlink'):
            self.receiver().plan()
        for observed in ({'machine': 'aarch64'}, {'libc': 'glibc 2.40'}, {'cpu_flags': sorted(set(E.CPU_FLAGS)-{'avx2'})}):
            host = dict({'machine': 'x86_64', 'libc': E.GLIBC, 'cpu_flags': sorted(E.CPU_FLAGS)}, **observed)
            with self.subTest(observed), patch.object(E, 'host_abi', lambda: host), \
                    self.assertRaisesRegex(ValueError, 'host ABI'):
                self.receiver().plan()

    def rewrite(self, path, suffix):
        path.chmod(0o644); path.write_bytes(path.read_bytes()+suffix); path.chmod(0o444)

    def fresh_publication(self):
        """Fresh publication and pins for each drift case."""
        import shutil
        for path in self.publication.rglob('*'):
            path.chmod(0o755 if path.is_dir() else 0o644)
        shutil.rmtree(self.publication)
        self.publish()
        binary = C.TARGET/'artifacts/shard-verify'; binary.chmod(0o755)
        binary.write_bytes(SCRIPT.format(python=sys.executable, control=str(self.control)).encode()); binary.chmod(C.BINARY_MODE)

    def test_coverage_other_than_exactly_180_segments_refuses(self):
        import shutil
        for path in self.publication.rglob('*'):
            path.chmod(0o755 if path.is_dir() else 0o644)
        shutil.rmtree(self.publication); self.publish(extra_segment=True)
        with self.assertRaisesRegex(ValueError, 'exactly 180'): self.receiver('native-certificates').plan()
        with self.assertRaisesRegex(ValueError, 'exactly 180'): self.receiver().plan()

    def test_real_host_sampler_abi_reader_and_process_reader_have_the_contract_shape(self):
        real = load()
        observed = real.resources([self.root])
        self.assertEqual(set(observed), {'observed_unix', 'memory_available', 'disk_available'})
        self.assertTrue(0 < observed['memory_available'] <= 1 and observed['disk_available'])
        self.assertEqual(set(real.host_abi()), {'machine', 'libc', 'cpu_flags'})
        mine = real.me()
        self.assertEqual(real.process(mine['pid'])['start_ticks'], mine['start_ticks'])
        self.assertTrue(real.alive(mine)); self.assertFalse(real.alive(dict(mine, boot_id='0')))

    def test_pidfd_signal_refuses_a_reused_pid_and_hits_the_exact_process(self):
        process = subprocess.Popen(['sleep', '30'])
        self.addCleanup(lambda: (process.poll() is None and process.kill(), process.wait()))
        ticks = E.process(process.pid)['start_ticks']
        self.assertFalse(E.signal_exact(process.pid, ticks-1, signal.SIGKILL))
        time.sleep(.2); self.assertIsNone(process.poll())
        self.assertTrue(E.signal_exact(process.pid, ticks, signal.SIGKILL))
        self.assertEqual(process.wait(10), -signal.SIGKILL)
        self.assertFalse(E.signal_exact(process.pid, ticks, signal.SIGKILL))

    def guardian(self, argv, budget, go=True):
        """One real guardian launch, as the receiver drives it."""
        gate, release = os.pipe(); notify, told = os.pipe()
        path = self.root/('guardian-%d.json' % time.monotonic_ns())
        report = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        child = subprocess.Popen(E.guardian_argv(gate, told, report, budget, argv), pass_fds=(gate, told, report),
                                 close_fds=True, start_new_session=True)
        self.addCleanup(lambda: (child.poll() is None and child.kill(), child.wait()))
        for fd in (gate, told, report):
            os.close(fd)
        native = int(os.read(notify, 32)); os.close(notify)
        current = E.process(native)
        self.addCleanup(self.kill, native, current['start_ticks'])
        self.assertEqual((current['ppid'], current['session'], current['pgid']), (child.pid,)*3)
        if go:
            os.write(release, b'go\n')
        os.close(release)
        return child, native, path

    def test_guardian_gates_exec_and_applies_hard_limits_only_to_the_native_process(self):
        target = self.root/'target.sh'
        target.write_text('#!/bin/sh\nulimit -v > %s\nulimit -t >> %s\n' % (self.root/'limits', self.root/'limits'))
        target.chmod(0o755)
        budget = {'address_space_bytes': 1 << 30, 'cpu_seconds': 7, 'wall_seconds': 30}
        for go in (False, True):
            child, native, path = self.guardian([str(target)], budget, go)
            self.assertEqual(child.wait(30), 0)
            report = E.guardian_report(path, {'pid': native})
            self.assertEqual((report['expired'], report['killed'], report['survivors']), (False, [], []))
            if not go:
                self.assertEqual(report['returncode'], E.LAUNCH_REFUSED); self.assertFalse((self.root/'limits').exists())
            else:
                self.assertEqual(report['returncode'], 0)
                self.assertEqual((self.root/'limits').read_text().split(), [str((1 << 30)//1024), '7'])
        self.assertEqual(E.GUARDIAN_SHA256, hashlib.sha256(E.GUARDIAN.encode()).hexdigest())

    def blocked_native(self):
        """A native stand-in blocked in IO with an escaped (setsid) grandchild."""
        script = self.root/'blocked.py'
        script.write_text('import os, subprocess, sys, time\n'
                          'child = subprocess.Popen(["sleep", "60"], start_new_session=True)\n'
                          'open(sys.argv[1], "w").write(str(child.pid))\n'
                          'blocked, writer = os.pipe()\nos.read(blocked, 1)\n')
        mark = self.root/('grandchild-%d' % time.monotonic_ns())
        return [sys.executable, '-B', str(script), str(mark)], mark

    def test_guardian_deadline_kills_an_io_blocked_native_and_escaped_descendants_without_the_receiver(self):
        argv, mark = self.blocked_native()
        budget = {'address_space_bytes': 1 << 31, 'cpu_seconds': 600, 'wall_seconds': 1.5}
        with patch.object(E, 'GUARDIAN_GRACE_SECONDS', 0):
            started = time.monotonic()
            child, native, path = self.guardian(argv, budget)
        self.wait_for(lambda: mark.exists() and mark.read_text())
        grandchild = int(mark.read_text())
        self.addCleanup(self.kill, grandchild, E.process(grandchild)['start_ticks'])
        self.assertEqual(E.process(native)['state'], 'S')
        # Nobody waits on the guardian or signals anything: its own deadline ends the run.
        self.assertEqual(child.wait(30), 0)
        self.assertGreaterEqual(time.monotonic()-started, 1.4)
        report = E.guardian_report(path, {'pid': native})
        self.assertEqual((report['expired'], report['returncode'], report['survivors']), (True, -signal.SIGKILL, []))
        self.assertIn(grandchild, [k['pid'] for k in report['killed']])
        self.wait_for(lambda: self.gone(grandchild) and self.gone(native))

    def test_guardian_death_kills_its_native_child_and_term_stops_the_subtree(self):
        argv, mark = self.blocked_native()
        budget = {'address_space_bytes': 1 << 31, 'cpu_seconds': 600, 'wall_seconds': 60}
        child, native, path = self.guardian(argv, budget)
        self.wait_for(lambda: mark.exists() and mark.read_text())
        grandchild = int(mark.read_text())
        self.addCleanup(self.kill, grandchild, E.process(grandchild)['start_ticks'])
        os.kill(child.pid, signal.SIGKILL); child.wait(10)
        # The kernel parent-death signal ends the native process; an escaped grandchild outlives it.
        self.wait_for(lambda: self.gone(native))
        self.assertEqual(path.stat().st_size, 0)
        self.assertIsNotNone(E.process(grandchild))
        os.kill(grandchild, signal.SIGKILL)
        argv, mark = self.blocked_native()
        child, native, path = self.guardian(argv, budget)
        self.wait_for(lambda: mark.exists() and mark.read_text())
        grandchild = int(mark.read_text())
        self.addCleanup(self.kill, grandchild, E.process(grandchild)['start_ticks'])
        os.kill(child.pid, signal.SIGTERM)
        self.assertEqual(child.wait(30), 0)
        report = E.guardian_report(path, {'pid': native})
        self.assertEqual((report['stopped'], report['expired'], report['returncode']), (True, False, -signal.SIGKILL))
        self.assertIn(grandchild, [k['pid'] for k in report['killed']])
        self.wait_for(lambda: self.gone(grandchild))


class Run(Fixture):
    def test_artifact_run_surveys_all_hosts_inherits_lock_and_limits_and_feeds_the_offline_producer(self):
        receiver = self.receiver()
        channel = Channel(self)
        record = self.stage(receiver, channel)
        self.assertEqual(record['status'], 'staged'); self.assertTrue(record['gate'].startswith('unevaluated'))
        self.assertEqual(channel.emitted[0]['phase'], 'locked')
        stderr = (receiver.evidence/'artifact/stderr.log').read_text()
        self.assertIn('lock-inherited=True lock-held=True', stderr)
        self.assertIn('as=%d cpu=1800' % (14 << 30), stderr)
        owner = json.loads((receiver.evidence/'artifact/owner.json').read_text())
        # The native process runs in its guardian's session, under its guardian's deadline.
        self.assertIn('sid=%d ' % owner['guardian']['pid'], stderr)
        self.assertEqual((owner['session'], owner['pgid']), (owner['guardian']['pid'],)*2)
        self.assertNotEqual(owner['pid'], owner['guardian']['pid'])
        self.assertEqual(owner['guardian_seconds'], 1860); self.assertEqual(owner['guardian_sha256'], E.GUARDIAN_SHA256)
        result = json.loads((receiver.evidence/'artifact/result.json').read_text())
        guardian = R.value(result['guardian']['report'])
        self.assertEqual((guardian['native_pid'], guardian['returncode'], guardian['expired'], guardian['killed']),
                         (owner['pid'], 0, False, []))
        self.assertEqual((result['pid'], result['guardian']['pid'], result['guardian']['exit_code']),
                         (owner['pid'], owner['guardian']['pid'], 0))
        self.assertEqual({k: owner[k] for k in ('native_source_sha', 'candidate_sha256', 'binary_sha256', 'publication_sha256')},
                         {'native_source_sha': C.SOURCE_SHA, 'candidate_sha256': C.identity(),
                          'binary_sha256': self.pins['shard-verify'], 'publication_sha256': E.MAP_SHA256})
        self.assertTrue(owner['start_ticks'] > 0 and owner['operations_source_sha'] == 'e'*40 and owner['boot_id'])
        self.assertEqual(owner['limits'], {'cpu_seconds': 1800, 'address_space_bytes': 14 << 30})
        self.assertTrue(owner['token'].startswith(receiver.identifier+':artifact:'))
        for name in ('owner.json', 'native.json', 'stderr.log', 'health.json', 'result.json'):
            info = (receiver.evidence/'artifact'/name).stat()
            self.assertEqual((info.st_mode & 0o777, info.st_nlink), (0o400, 1))
        # Every host's raw survey is retained and bound to the owner.
        index = R.value(record['survey'])
        self.assertEqual((index['phase'], sorted(index['hosts'])), ('stage', ['coordinator', 'worker-a']))
        self.assertEqual(index['nonce'], channel.emitted[0]['nonce'])
        for host, ref in index['hosts'].items():
            value = R.value(ref)
            self.assertEqual((value['host'], value['status'], value['nonce']), (host, 'clear', index['nonce']))
        coordinator = R.value(index['hosts']['coordinator'])
        self.assertEqual([h['pid'] for h in coordinator['lock']['holders']], [os.getpid()])
        self.assertEqual(R.value(index['hosts']['worker-a'])['machine_id'], WORKER)
        references = R.value(record['references'])
        report = R.artifact_report(references['mapping'], references['execution'])
        self.assertEqual(report['status'], 'passed'); self.assertEqual(report['verified']['through'], 3500738)
        E.schema_fence.local_schema_fence()
        self.assertEqual(receiver.status(), record)
        with self.lock(): pass
        with self.assertRaisesRegex(ValueError, 'already owned'): self.stage(receiver)

    def test_certificates_run_all_180_one_at_a_time_with_whole_interval_sampling(self):
        receiver = self.receiver('native-certificates')
        record = self.stage(receiver)
        self.assertEqual((record['status'], record['completed']), ('staged', 180))
        references = R.value(record['references'])
        self.assertEqual(len(references['executions']), 180); self.assertEqual(len(references['manifests']), 90)
        intervals = []
        for item in references['executions']:
            owner, result = R.value(item['execution']['owner']), R.value(item['execution']['result'])
            intervals.append((owner['started_unix'], result['ended_unix']))
            self.assertEqual((owner['timeout_seconds'], result['limits']['address_space_bytes']), (600, 14 << 30))
        intervals.sort()
        self.assertTrue(all(a[1] <= b[0] for a, b in zip(intervals, intervals[1:])))
        # Stage-level samples cover the whole interval, including between children.
        samples = [json.loads(line) for line in (receiver.evidence/'health.ndjson').read_text().splitlines()]
        times = [s['observed_unix'] for s in samples]
        self.assertEqual(len(samples), record['samples'])
        self.assertTrue(times == sorted(times) and all(b-a <= 10 for a, b in zip(times, times[1:])))
        self.assertLessEqual(times[0], record['started_unix']); self.assertGreaterEqual(times[-1], intervals[-1][1])
        evaluator = SimpleNamespace(evaluate=lambda native: {'actual_profile': {'certified_failure_bits': native['fixture_bits']}})
        with patch.object(R, 'certifier', return_value=evaluator):
            report = R.certificate_report(references['mapping'], references['manifests'], references['executions'], '/fixture')
        self.assertEqual((report['segments'], len(report['setup_bindings'])), (180, 180))

    def test_nonzero_exit_pauses_retains_failure_and_fences_until_reconciled(self):
        self.behave('fail')
        receiver = self.receiver()
        with self.assertRaisesRegex(ValueError, 'exited 3'): self.stage(receiver)
        record = json.loads(receiver.owner.read_text())
        self.assertEqual(record['status'], 'failed')
        result = json.loads((receiver.evidence/'artifact/result.json').read_text())
        self.assertEqual((result['status'], result['exit_code']), ('failed', 3))
        refs = {k: E.reference(receiver.evidence/'artifact'/n) for k, n in (('owner', 'owner.json'), ('result', 'result.json'),
                ('native', 'native.json'), ('stderr', 'stderr.log'), ('health', 'health.json'))}
        with self.assertRaises(ValueError): R.capture(refs, 'shard-verify', E.MAP_SHA256)
        self.fenced()
        before = receiver.owner.read_bytes()
        with self.assertRaisesRegex(ValueError, 'receiver is still active'): self.reconcile(receiver)
        self.fenced()
        # Refused without the lock: retained evidence only, the owner record is untouched.
        self.assertEqual(receiver.owner.read_bytes(), before)
        [unlocked] = receiver.evidence.glob('reconciliation-unlocked-*.json')
        self.assertTrue(json.loads(unlocked.read_text())['receiver_alive'])
        self.exited(receiver)
        reconciled = self.reconcile(receiver)
        self.assertEqual(reconciled['status'], 'reconciled')
        self.assertEqual([r['outcome'] for r in reconciled['recoveries']], ['reconciled'])
        self.assertEqual(R.value(reconciled['reconciliation']['survey'])['phase'], 'reconcile')
        E.schema_fence.local_schema_fence()
        self.assertTrue((receiver.evidence/'artifact/stderr.log').exists())
        self.assertTrue((receiver.evidence/'reconciliation-1.json').exists())
        with self.assertRaisesRegex(ValueError, 'does not need'): self.reconcile(receiver)

    def test_certificate_failure_stops_before_any_later_segment(self):
        receiver = self.receiver('native-certificates')
        plan = receiver.plan()
        self.behave(**{plan['dispatches'][2]['segment']['path']: 'fail'})
        with self.assertRaises(ValueError): self.stage(receiver, plan=I.digest(plan))
        record = json.loads(receiver.owner.read_text())
        self.assertEqual((record['status'], record['completed']), ('failed', 2))
        self.assertEqual(len(self.captures(receiver)), 3)
        self.assertEqual(len(self.ran.read_text().split()), 3)
        self.assertFalse((receiver.evidence/'references.json').exists())
        self.fenced()

    def test_table_hash_drift_after_plan_refuses_that_segment_before_dispatch(self):
        receiver = self.receiver('native-certificates')
        plan = receiver.plan()
        target = Path(plan['dispatches'][4]['segment']['path'])
        target.chmod(0o644); raw = bytearray(target.read_bytes()); raw[0] ^= 1; target.write_bytes(bytes(raw)); target.chmod(0o444)
        with self.assertRaisesRegex(ValueError, 'table bytes differ'): self.stage(receiver, plan=I.digest(plan))
        self.assertEqual(json.loads(receiver.owner.read_text())['completed'], 4)
        self.assertFalse((receiver.evidence/plan['dispatches'][4]['key']/'owner.json').exists())

    def test_plan_digest_drift_refuses_before_owner(self):
        receiver = self.receiver()
        with self.assertRaisesRegex(ValueError, 'plan changed'): self.stage(receiver, plan='0'*64)
        self.assertFalse(receiver.owner.exists()); self.assertFalse(receiver.evidence.exists())

    def test_output_bounds_wall_deadline_aggregate_floor_and_sampling_gap_stop_the_run(self):
        artifact = E.BUDGETS['artifact-verification']
        cases = [('output', 'pass', {'MAX_STDOUT': 10}), ('output', 'noisy', {'MAX_STDERR': 100}),
                 ('wall deadline', 'sleep', {'BUDGETS': {'artifact-verification': dict(artifact, wall_seconds=.3)}})]
        for attempt, (message, behaviour, limits) in enumerate(cases, 1):
            with self.subTest(message=message, limits=limits):
                self.behave(behaviour); self.mark.unlink(missing_ok=True)
                with patch.multiple(E, **limits):
                    receiver = self.receiver(attempt=attempt)
                    with self.assertRaisesRegex(ValueError, message): self.stage(receiver)
                result = json.loads((receiver.evidence/'artifact/result.json').read_text())
                self.assertEqual(result['status'], 'failed')
                if behaviour == 'sleep':
                    owner = json.loads((receiver.evidence/'artifact/owner.json').read_text())
                    self.assertFalse(any(E.classify(E.scan(), owner['token'], owner)))
                self.exited(receiver); self.reconcile(receiver)
        # The aggregate budget refuses a child it cannot cover, before spawning.
        self.behave('pass'); self.ran.unlink(missing_ok=True)
        with patch.object(E, 'NON_CHILD_SECONDS', -1700):
            receiver = self.receiver(attempt=9)
            with self.assertRaisesRegex(ValueError, 'aggregate remote budget'): self.stage(receiver)
        self.assertFalse(self.ran.exists()); self.assertFalse((receiver.evidence/'artifact').exists())
        self.exited(receiver); self.reconcile(receiver)
        # Lock acquisition, preflight, survey, before spawn, then the running child.
        samples = iter([.5]*12)
        floor = lambda paths: {'observed_unix': time.time(), 'memory_available': next(samples, .1), 'disk_available': {'/': .5}}
        self.behave('sleep'); self.mark.unlink(missing_ok=True); receiver = self.receiver(attempt=10)
        with patch.object(E, 'resources', floor), self.assertRaisesRegex(ValueError, '20 percent'): self.stage(receiver)
        health = json.loads((receiver.evidence/'artifact/health.json').read_text())
        self.assertEqual(health[0]['memory_available'], .5); self.assertIn(.1, [h['memory_available'] for h in health])
        self.exited(receiver); self.reconcile(receiver)
        clock = iter(range(1000, 100000, 30))
        gap = lambda paths: dict(HEALTHY, observed_unix=next(clock))
        receiver = self.receiver(attempt=11)
        with patch.object(E, 'resources', gap), self.assertRaisesRegex(ValueError, 'sampling gap'): self.stage(receiver)
        self.assertFalse(receiver.owner.exists())

    def test_hard_memory_and_cpu_limits_stop_the_child_in_the_kernel(self):
        artifact = E.BUDGETS['artifact-verification']
        cases = [('allocate', {'address_space_bytes': 512 << 20}, 'exited 1'),
                 ('spin', {'cpu_seconds': 1}, 'exited -')]
        for attempt, (behaviour, limits, message) in enumerate(cases, 1):
            with self.subTest(behaviour):
                self.behave(behaviour)
                with patch.object(E, 'BUDGETS', {**E.BUDGETS, 'artifact-verification': dict(artifact, **limits)}):
                    receiver = self.receiver(attempt=attempt)
                    with self.assertRaisesRegex(ValueError, message): self.stage(receiver)
                result = json.loads((receiver.evidence/'artifact/result.json').read_text())
                self.assertEqual(result['limits'], {'cpu_seconds': artifact['cpu_seconds'],
                                                    'address_space_bytes': artifact['address_space_bytes'], **limits})
                if behaviour == 'allocate':
                    self.assertIn('MemoryError', (receiver.evidence/'artifact/stderr.log').read_text())
                else:
                    self.assertIn(result['signal'], ('SIGXCPU', 'SIGKILL'))
                self.exited(receiver); self.reconcile(receiver)

    def test_floor_before_spawn_starts_no_child(self):
        receiver = self.receiver()
        plan = receiver.plan()
        # Healthy until the child's directory exists: the before-spawn sample fails.
        floor = lambda paths: dict(HEALTHY, observed_unix=time.time(),
                                   memory_available=.1 if (receiver.evidence/'artifact').exists() else .5)
        with patch.object(E, 'resources', floor), self.assertRaisesRegex(ValueError, '20 percent'):
            self.stage(receiver, plan=I.digest(plan))
        self.assertFalse((receiver.evidence/'artifact/owner.json').exists())
        result = json.loads((receiver.evidence/'artifact/result.json').read_text())
        self.assertEqual((result['pid'], result['status']), (None, 'failed'))
        self.assertIsNone(json.loads(receiver.owner.read_text())['launch'])
        self.assertFalse(self.ran.exists())

    def test_descendants_left_by_a_successful_child_are_stopped_and_fail(self):
        self.behave('fork')
        receiver = self.receiver()
        with self.assertRaisesRegex(ValueError, 'descendants'): self.stage(receiver)
        marks = json.loads(self.mark.read_text())
        self.wait_for(lambda: E.process(marks['grandchild']) is None or E.process(marks['grandchild'])['state'] == 'Z')
        with self.lock(): pass


class Survey(Fixture):
    def refused(self, message, receiver=None, channel=None):
        receiver = receiver or self.receiver()
        with self.assertRaisesRegex(ValueError, message):
            self.stage(receiver, channel)
        self.assertFalse(receiver.owner.exists()); self.assertFalse(receiver.evidence.exists())
        self.assertFalse(self.ran.exists())
        attempts = self.surveys(receiver)
        if attempts:
            index = json.loads((attempts[-1]/'index.json').read_text())
            self.assertIn('coordinator', index['hosts'])
        return attempts

    def test_remote_lock_holder_unfinished_owner_and_missing_source_refuse_before_mutation(self):
        holder = self.holder(self.worker/'production.lock')
        attempts = self.refused('worker-a')
        survey = json.loads((attempts[-1]/'worker-a.json').read_text())
        self.assertEqual([h['pid'] for h in survey['lock']['holders']], [holder.pid])
        self.assertIn('production lock holder', ' '.join(survey['blocked']))
        holder.kill(); holder.wait()
        owners = self.worker/'owners'
        I.durable.atomic_json(owners/('a'*64+'.json'), {'request_sha256': 'a'*64, 'status': 'receiving'})
        I.durable.atomic_json(owners/'latest.json', {'request_sha256': 'a'*64})
        attempts = self.refused('worker-a')
        self.assertTrue(json.loads((attempts[-1]/'worker-a.json').read_text())['fence'].startswith('unfinished input'))
        (owners/'latest.json').unlink()
        I.durable.atomic_json(owners/('b'*64+'.json'), {'kind': E.KIND, 'request_sha256': 'b'*64, 'status': 'interrupted'})
        attempts = self.refused('worker-a')
        self.assertIn('unfinished candidate execution owner', ' '.join(json.loads((attempts[-1]/'worker-a.json').read_text())['blocked']))
        (owners/('b'*64+'.json')).unlink()
        self.worker_source = None
        attempts = self.refused('worker-a')
        survey = json.loads((attempts[-1]/'worker-a.json').read_text())
        self.assertEqual((survey['status'], survey['source_sha']), ('blocked', None))
        self.assertIn('operations source missing', ' '.join(survey['blocked']))
        self.worker_source = 'f'*40
        self.refused('worker-a')
        self.worker_source = 'e'*40
        self.assertEqual(self.stage(self.receiver())['status'], 'staged')

    def test_partial_foreign_stale_and_failed_transport_surveys_refuse(self):
        def drop(surveys, line): return {}
        def extra(surveys, line): return dict(surveys, **{'worker-b': surveys['worker-a']})
        def stale(surveys, line):
            value = json.loads(surveys['worker-a']); value['nonce'] = 'f'*64
            return {'worker-a': json.dumps(value)}
        def foreign(surveys, line):
            value = json.loads(surveys['worker-a']); value['machine_id'] = 'e'*32
            return {'worker-a': json.dumps(value)}
        def transport(surveys, line): return {'worker-a': json.dumps({'host': 'worker-a', 'transport': 'timeout'})}
        def garbage(surveys, line): return {'worker-a': 'not json'}
        for name, mutate, message in (('drop', drop, 'partial or foreign'), ('extra', extra, 'partial or foreign'),
                                      ('stale', stale, 'stale'), ('foreign', foreign, 'foreign'),
                                      ('transport', transport, 'foreign, stale or partial'), ('garbage', garbage, 'not JSON')):
            with self.subTest(name):
                self.refused(message, self.receiver(attempt=len(name)), Channel(self, mutate=mutate))

    def test_live_candidate_process_or_receiver_anywhere_refuses(self):
        token = 'a'*64+':artifact:'+'b'*32
        live = subprocess.Popen(['sleep', '30'], env=dict(os.environ, **{E.MARKER: token}))
        self.addCleanup(lambda: (live.kill(), live.wait()))
        attempts = self.refused('live, unreadable or foreign')
        self.assertIn(live.pid, [p['pid'] for p in json.loads((attempts[-1]/'coordinator.json').read_text())['processes']])
        live.kill(); live.wait()
        other = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)', 'schema-candidate-execute-receive',
                                  '--action', 'stage'])
        self.addCleanup(lambda: (other.kill(), other.wait()))
        self.wait_for(lambda: any(p['receiver'] for p in E.scan() if p['pid'] == other.pid))
        self.refused('live, unreadable or foreign', self.receiver(attempt=2))

    def live(self, **options):
        process = subprocess.Popen(['sleep', '30'], **options)
        self.addCleanup(lambda: (process.poll() is None and process.kill(), process.wait()))
        return process

    def associated(self, attempts, host='coordinator'):
        survey = json.loads((attempts[-1]/(host+'.json')).read_text())
        self.assertEqual(survey['associated_count'], len(survey['associated']))
        return [(p['pid'], p['association']) for p in survey['associated']]

    def test_nonlatest_owner_of_any_kind_in_every_namespace_with_a_live_process_refuses(self):
        live = self.live()
        ticks = E.process(live.pid)['start_ticks']
        cases = [(self.owners/('a'*64+'.json'), {'kind': 'coordinator-input-preparation', 'status': 'running', 'pid': live.pid}),
                 (self.fence/'host-actions/transparent-schema-x/r1.json', {'status': 'passed', 'pid': live.pid}),
                 (self.fence/'schema/transparent-schema-old.json', {'status': 'committed', 'events': [{'relay_pid': live.pid}]}),
                 (self.owners/('b'*64+'.native')/'owner.json', {'pid': live.pid, 'process_start': ticks}),
                 (self.owners/('c'*64+'.json'), {'kind': E.KIND, 'status': 'reconciled',
                                                 'child': {'pid': live.pid, 'start_ticks': ticks, 'boot_id': E.boot_id()}})]
        for attempt, (path, record) in enumerate(cases, 1):
            with self.subTest(str(path.relative_to(self.root))):
                path.parent.mkdir(parents=True, exist_ok=True)
                I.durable.atomic_json(path, record)
                attempts = self.refused('associated with a retained owner record', self.receiver(attempt=attempt))
                self.assertEqual(self.associated(attempts), [(live.pid, 'recorded-process')])
                path.unlink()
        # A reused PID, by other start ticks, boot or a start after the record's last write, is not the owner.
        I.durable.atomic_json(self.owners/('c'*64+'.json'), {'status': 'failed', 'pid': live.pid, 'start_ticks': ticks-1})
        I.durable.atomic_json(self.owners/('d'*64+'.json'), {'status': 'failed', 'pid': live.pid, 'boot_id': '0'})
        old = self.owners/('e'*64+'.json')
        I.durable.atomic_json(old, {'status': 'failed', 'pid': live.pid})
        os.utime(old, (time.time()-3600,)*2)
        self.assertEqual(self.stage(self.receiver(attempt=9))['status'], 'staged')

    def test_terminal_owner_whose_process_died_refuses_while_a_descendant_lives(self):
        tree = self.root/'tree.py'
        tree.write_text('import json, os, subprocess, sys, time\n'
                        'if sys.argv[1] == "leader":\n'
                        '    subprocess.Popen([sys.executable, __file__, "parent"]).wait()\n'
                        'else:\n'
                        '    started = time.time()\n'
                        '    child = subprocess.Popen(["sleep", "30"])\n'
                        '    print(json.dumps({"parent": os.getpid(), "started": started, "descendant": child.pid}), flush=True)\n')
        # A recorded session leader died; its descendant keeps the session. On the remote host.
        parent = subprocess.Popen([sys.executable, str(tree), 'parent'], stdout=subprocess.PIPE, start_new_session=True)
        ticks = E.process(parent.pid)['start_ticks']
        descendant = json.loads(parent.stdout.readline())['descendant']
        self.addCleanup(lambda: E.signal_exact(descendant, E.process(descendant)['start_ticks'], signal.SIGKILL)
                        if E.process(descendant) else None)
        parent.wait(); parent.stdout.close()
        self.assertEqual(E.process(descendant)['session'], parent.pid)
        I.durable.atomic_json(self.worker/'owners'/('a'*64+'.json'),
                              {'kind': 'candidate-archive-upload', 'status': 'staged', 'pid': parent.pid, 'process_start': ticks})
        attempts = self.refused('worker-a')
        self.assertEqual(self.associated(attempts, 'worker-a'), [(descendant, 'recorded-session-or-group')])
        E.signal_exact(descendant, E.process(descendant)['start_ticks'], signal.SIGKILL)
        self.wait_for(lambda: self.gone(descendant))
        # A non-leader parent died inside a leader that also died: the orphan started within its record.
        leader = subprocess.Popen([sys.executable, str(tree), 'leader'], stdout=subprocess.PIPE, start_new_session=True)
        value = json.loads(leader.stdout.readline())
        leader.wait(); leader.stdout.close()
        orphan = value['descendant']
        self.addCleanup(lambda: E.signal_exact(orphan, E.process(orphan)['start_ticks'], signal.SIGKILL)
                        if E.process(orphan) else None)
        self.wait_for(lambda: E.process(value['parent']) is None)
        path = self.fence/'host-actions/transparent-schema-t/run.json'; path.parent.mkdir(parents=True)
        I.durable.atomic_json(path, {'status': 'passed', 'pid': value['parent'], 'started_unix': value['started']})
        attempts = self.refused('associated with a retained owner record', self.receiver(attempt=2))
        self.assertEqual(self.associated(attempts), [(orphan, 'orphan-in-recorded-window')])

    def test_association_rules_bind_start_identity_session_cgroup_window_and_boot(self):
        hz, booted = os.sysconf('SC_CLK_TCK'), 1000.0
        def proc(pid, at, session=None, cgroup='0::/observer'):
            session = pid if session is None else session
            return {'pid': pid, 'start_ticks': int((at-booted)*hz), 'session': session, 'pgid': session, 'cgroup': cgroup}
        def ref(pid, **change):
            return dict({'record': 'r.json', 'key': 'pid', 'pid': pid, 'window': None, 'mtime': 2000.0,
                         'start_ticks': None, 'boot_id': None, 'cgroup': None}, **change)
        def found(references, processes, excluded=()):
            return [(p['pid'], p['association']) for p in
                    E.associate(references, processes, set(excluded), '0::/observer', booted)]
        early, late = proc(10, 1500), proc(10, 2500)
        self.assertEqual(found([ref(10)], [early]), [(10, 'recorded-process')])
        self.assertEqual(found([ref(10)], [late]), [])
        self.assertEqual(found([ref(10, start_ticks=early['start_ticks'])], [early]), [(10, 'recorded-process')])
        self.assertEqual(found([ref(10, start_ticks=7)], [early]), [])
        self.assertEqual(found([ref(10, boot_id='0')], [early]), [])
        self.assertEqual(found([ref(10, mtime=900.0)], [early]), [])
        self.assertEqual(found([ref(10)], [early], excluded=[10]), [])
        self.assertEqual(found([ref(11)], [proc(20, 2500, session=11)]), [(20, 'recorded-session-or-group')])
        self.assertEqual(found([ref(11)], [proc(11, 2500), proc(20, 2600, session=11)]), [])
        self.assertEqual(found([ref(11, cgroup='0::/old')], [proc(21, 2500, cgroup='0::/old')]), [(21, 'recorded-cgroup')])
        self.assertEqual(found([ref(11, cgroup='0::/observer')], [proc(21, 2500)]), [])
        self.assertEqual(found([ref(12, window=1400.0)], [proc(22, 1500, session=99)]), [(22, 'orphan-in-recorded-window')])
        self.assertEqual(found([ref(12, window=1600.0)], [proc(22, 1500, session=99)]), [])
        self.assertEqual(found([ref(12, window=1400.0)], [proc(99, 1000), proc(22, 1500, session=99)]), [])
        self.assertEqual(found([ref(12, window=1400.0)], [proc(23, 1500, session=0)]), [])
        # As root sees it: kernel threads are never scanned, so never associated.
        kthreadd = E.process(2)
        self.assertTrue(kthreadd['kernel'])
        with patch.object(E.os, 'geteuid', lambda: 0):
            scanned = E.scan()
        self.assertNotIn(2, [p['pid'] for p in scanned]); self.assertIn(os.getpid(), [p['pid'] for p in scanned])
        records = E.recorded({'started_unix': 5.0, 'pid': 3, 'start_ticks': 4, 'boot_id': 'b',
                              'events': [{'relay_pid': 6, 'ssh_pid': 7}, {'ppid': 8, 'pid': True}],
                              'child': {'pid': 9, 'process_start': 10, 'started': 11.0}}, 'x.json', 12.0)
        self.assertEqual(sorted((r['pid'], r['key'], r['start_ticks'], r['window']) for r in records),
                         [(3, 'pid', 4, 5.0), (6, 'relay_pid', None, 5.0), (7, 'ssh_pid', None, 5.0), (9, 'pid', 10, 11.0)])

    def test_owner_namespace_overflow_unreadable_records_and_links_refuse(self):
        with patch.object(E, 'MAX_OWNER_ENTRIES', 2):
            attempts = self.refused('exceed 2 entries')
        self.assertEqual(json.loads((attempts[-1]/'coordinator.json').read_text())['inventory']['entries'], 3)
        bad = self.fence/'schema/transparent-schema-bad.json'; bad.parent.mkdir(parents=True); bad.write_text('{')
        self.refused('owner record unreadable', self.receiver(attempt=2)); bad.unlink()
        link = self.owners/('e'*64+'.json'); link.symlink_to(self.owners/'latest.json')
        self.refused('link or special file', self.receiver(attempt=3)); link.unlink()
        with patch.object(E, 'MAX_OWNER_FILE', 10):
            self.refused('owner record exceeds bound', self.receiver(attempt=4))
        with patch.object(E, 'MAX_REFERENCES', 0):
            I.durable.atomic_json(self.fence/'schema/transparent-schema-ok.json', {'status': 'committed', 'pid': 1})
            self.refused('more than 0 processes', self.receiver(attempt=5))
        self.assertEqual(self.stage(self.receiver(attempt=6))['status'], 'staged')

    def test_a_floor_or_gap_during_the_local_survey_still_retains_every_received_reply(self):
        # Healthy until the coordinator's own owner inventory runs, then below the floor.
        def floor(paths):
            surveying = any(f.f_code.co_name == 'owner_inventory' for f in _frames())
            return dict(HEALTHY, observed_unix=time.time(), memory_available=.1 if surveying else .5)
        with patch.object(E, 'resources', floor), patch.object(E, 'SAMPLE_SECONDS', 0):
            attempts = self.refused('20 percent')
        index = json.loads((attempts[-1]/'index.json').read_text())
        self.assertEqual(sorted(index['hosts']), ['coordinator', 'worker-a'])
        self.assertEqual(json.loads((attempts[-1]/'coordinator.json').read_text())['error_type'], 'Budget')
        self.assertEqual(json.loads((attempts[-1]/'worker-a.json').read_text())['status'], 'clear')
        self.assertEqual(self.stage(self.receiver(attempt=2))['status'], 'staged')

    def test_surveys_that_never_arrive_refuse_before_mutation(self):
        class Silent(Channel):
            def emit(self, value): self.emitted.append(value)
        with patch.object(E, 'SURVEY_WAIT_SECONDS', .5):
            self.refused('did not arrive', channel=Silent(self))


class Interruption(Fixture):
    def launch(self, receiver, **change):
        """Stage through a real receiver process, driving the lock handshake."""
        plan = I.digest(receiver.plan())
        process = subprocess.Popen(self.harness('stage', receiver.identifier, plan, **change), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, start_new_session=True)
        self.addCleanup(lambda: (process.poll() is None and process.kill(), process.wait(), process.stdout.close()))
        process.stdin.write(I.durable.canonical(receiver.request)+b'\n'); process.stdin.flush()
        locked = json.loads(process.stdout.readline())
        self.assertEqual(locked['phase'], 'locked')
        surveys = {'worker-a': self.survey_worker(locked['nonce'], request=receiver.request)}
        process.stdin.write(json.dumps({'surveys': surveys}).encode()+b'\n'); process.stdin.close()
        return process

    def started(self, receiver):
        """The recorded child, once it has also started its grandchild."""
        child = self.wait_for(lambda: receiver.owner.exists() and json.loads(receiver.owner.read_text()).get('child'))
        self.wait_for(lambda: self.mark.exists() and self.mark.stat().st_size)
        return child

    def unlocked(self, receiver):
        return sorted(receiver.evidence.glob('reconciliation-unlocked-*.json'))

    def test_receiver_death_with_io_blocked_child_fences_until_the_guardian_deadline_then_recovers_locked(self):
        self.shorten(wall=3, grace=1)
        self.behave('block')
        receiver = self.receiver()
        process = self.launch(receiver)
        child = self.started(receiver)
        grandchild = json.loads(self.mark.read_text())['grandchild']
        os.kill(process.pid, signal.SIGKILL); process.wait()
        native = child['native']
        self.assertTrue(E.alive(child)); self.assertEqual(E.process(native['pid'])['state'], 'S')
        with self.assertRaises(BlockingIOError):
            with self.lock(): pass
        self.fenced()
        holders = {p['pid'] for p in E.scan(self.lockpath) if p['holds']}
        self.assertEqual(holders, {child['pid'], native['pid'], grandchild})
        # Before the deadline: no lock, so no owner write and nothing signalled.
        before = receiver.owner.read_bytes()
        with patch.object(E, 'LOCK_WAIT_SECONDS', .3), self.assertRaisesRegex(ValueError, 'until its deadline'):
            self.reconcile(receiver)
        self.assertEqual(receiver.owner.read_bytes(), before)
        self.assertTrue(E.alive(child) and not self.gone(native['pid']) and not self.gone(grandchild))
        refusal = json.loads(self.unlocked(receiver)[-1].read_text())
        self.assertEqual({p['pid'] for p in refusal['holders']}, holders)
        self.assertEqual(refusal['outcome']['outcome'], 'blocked-unlocked')
        # The guardian's own deadline kills the IO-blocked child and its descendant and frees the lock.
        self.wait_for(lambda: not any(p['holds'] for p in E.scan(self.lockpath)), timeout=30)
        self.assertTrue(self.gone(native['pid']) and self.gone(grandchild))
        report = json.loads((receiver.evidence/'artifact/guardian.json').read_text())
        self.assertEqual((report['native_pid'], report['expired']), (native['pid'], True))
        self.assertIn(grandchild, [k['pid'] for k in report['killed']])
        self.fenced()
        record = self.reconcile(receiver)
        self.assertEqual(record['reconciliation']['action'], 'no-live-child')
        self.assertEqual([r['outcome'] for r in record['recoveries']], ['reconciled'])
        evidence = R.value(record['recoveries'][-1]['evidence'])
        self.assertEqual(R.value(evidence['guardian'])['expired'], True)
        E.schema_fence.local_schema_fence()
        self.assertTrue((receiver.evidence/'health.ndjson').stat().st_size > 0)

    def test_guardian_killed_leaves_own_unlocked_descendant_stopped_only_under_the_lock(self):
        self.behave('unlock')
        receiver = self.receiver()
        process = self.launch(receiver)
        child = self.started(receiver)
        grandchild = json.loads(self.mark.read_text())['grandchild']
        os.kill(process.pid, signal.SIGKILL); process.wait()
        # Guardian killed from outside: the kernel kills the native process; the
        # grandchild that dropped the lock descriptor stays in the session.
        os.kill(child['pid'], signal.SIGKILL)
        self.wait_for(lambda: self.gone(child['native']['pid']))
        self.wait_for(lambda: not any(p['holds'] for p in E.scan(self.lockpath)))
        self.assertFalse(self.gone(grandchild))
        record = self.reconcile(receiver)
        self.assertEqual(record['reconciliation']['action'], 'terminated-own-processes')
        self.assertEqual([s['pid'] for s in record['reconciliation']['signalled']], [grandchild])
        self.wait_for(lambda: self.gone(grandchild))
        E.schema_fence.local_schema_fence()

    def test_receiver_death_before_child_owner_write_never_execs_and_recovers_bounded(self):
        for attempt, crash in enumerate(('before-owner', 'before-go'), 1):
            with self.subTest(crash):
                self.behave('sleep'); self.ran.unlink(missing_ok=True); self.mark.unlink(missing_ok=True)
                receiver = self.receiver(attempt=attempt)
                process = self.launch(receiver, crash=crash)
                self.assertEqual(process.wait(30), 137)
                record = json.loads(receiver.owner.read_text())
                self.assertEqual(record['status'], 'running'); self.assertTrue(record['launch'])
                self.assertEqual(bool(record['child']), crash == 'before-go')
                self.assertEqual((receiver.evidence/'artifact/owner.json').exists(), crash == 'before-go')
                self.fenced()
                # The launcher saw EOF on its gate: no native program ran, the lock frees.
                self.wait_for(lambda: not any(p['holds'] for p in E.scan(self.lockpath)))
                self.assertFalse(self.ran.exists())
                reconciled = self.reconcile(receiver)
                self.assertEqual(reconciled['reconciliation']['action'], 'no-live-child')
                E.schema_fence.local_schema_fence()

    def test_signalled_receiver_stops_its_own_child_records_interrupted_and_frees_lock(self):
        self.behave('sleep')
        receiver = self.receiver()
        process = self.launch(receiver)
        child = self.started(receiver)
        os.kill(process.pid, signal.SIGHUP)
        self.assertEqual(process.wait(30), 75)
        self.assertEqual(json.loads(process.stdout.read())['status'], 'interrupted')
        self.assertFalse(E.alive(child))
        self.wait_for(lambda: self.gone(json.loads(self.mark.read_text())['grandchild']))
        self.assertEqual(json.loads(receiver.owner.read_text())['status'], 'interrupted')
        result = json.loads((receiver.evidence/'artifact/result.json').read_text())
        self.assertEqual(result['status'], 'interrupted')
        with self.lock(): pass
        self.fenced()
        self.assertEqual(self.reconcile(receiver)['reconciliation']['action'], 'no-live-child')

    def failed(self, receiver):
        self.behave('fail')
        with self.assertRaises(ValueError): self.stage(receiver)
        return self.exited(receiver)

    def test_reused_pid_foreign_session_and_unknown_lock_holder_are_never_signalled(self):
        receiver = self.receiver()
        record = self.failed(receiver)
        token = receiver.identifier+':artifact:'+'a'*32
        # A foreign session leader at the recorded PID: its members carry no token.
        foreign = subprocess.Popen(['sleep', '30'], start_new_session=True)
        self.addCleanup(lambda: (foreign.kill(), foreign.wait()))
        ticks = E.process(foreign.pid)['start_ticks']
        record.update(launch={'key': 'artifact', 'token': token, 'intent_unix': time.time()},
                      child={'key': 'artifact', 'token': token, 'pid': foreign.pid, 'start_ticks': ticks,
                             'boot_id': E.boot_id(), 'deadline_unix': time.time()-100})
        receiver.save(record)
        with self.assertRaisesRegex(ValueError, 'cannot be attributed'): self.reconcile(receiver)
        self.assertIsNone(foreign.poll()); self.fenced()
        # The same PID with other start ticks is a reused PID: untouched, nothing of ours remains.
        record = json.loads(receiver.owner.read_text())
        record['child']['start_ticks'] = ticks-1
        receiver.save(record)
        before = receiver.owner.read_bytes()
        holder = self.holder(self.lockpath)
        with patch.object(E, 'LOCK_WAIT_SECONDS', .3), self.assertRaisesRegex(ValueError, 'cannot prove its own'):
            self.reconcile(receiver)
        self.assertIsNone(holder.poll()); self.assertIsNone(foreign.poll()); self.fenced()
        self.assertEqual(receiver.owner.read_bytes(), before)
        self.assertEqual([p['pid'] for p in json.loads(self.unlocked(receiver)[-1].read_text())['holders']], [holder.pid])
        holder.kill(); holder.wait()
        reconciled = self.reconcile(receiver)
        self.assertEqual(reconciled['reconciliation']['action'], 'no-live-child')
        self.assertIsNone(foreign.poll())
        self.assertEqual([r['outcome'] for r in reconciled['recoveries']], ['blocked', 'reconciled'])

    def test_escaped_lock_holding_descendant_blocks_until_the_guardian_deadline_kills_it(self):
        self.shorten(wall=3, grace=1)
        self.behave('escape')
        receiver = self.receiver()
        process = self.launch(receiver)
        child = self.started(receiver)
        escaped = json.loads(self.mark.read_text())['grandchild']
        os.kill(process.pid, signal.SIGKILL); process.wait()
        # A token-bearing process outside the guardian session holds the lock: not provably own.
        with patch.object(E, 'LOCK_WAIT_SECONDS', .3), self.assertRaisesRegex(ValueError, 'cannot prove its own'):
            self.reconcile(receiver)
        self.assertTrue(E.alive(child)); self.assertFalse(self.gone(escaped)); self.fenced()
        self.assertEqual(json.loads(receiver.owner.read_text())['recoveries'], [])
        refusal = json.loads(self.unlocked(receiver)[-1].read_text())
        self.assertIn(escaped, [p['pid'] for p in refusal['holders']])
        self.assertNotIn(escaped, [p['pid'] for p in refusal['own']])
        # As the guardian is a subreaper, even a setsid descendant stays within its deadline.
        self.wait_for(lambda: self.gone(escaped) and not any(p['holds'] for p in E.scan(self.lockpath)), timeout=30)
        self.assertEqual(self.reconcile(receiver)['reconciliation']['action'], 'no-live-child')

    def test_concurrent_reconciliation_refuses_without_effect_while_the_first_holds_the_lock(self):
        receiver = self.receiver()
        self.failed(receiver)
        self.constants['LOCK_WAIT_SECONDS'] = .3
        competing = {}

        class Racing(Channel):
            def emit(inner, value):
                if value.get('phase') == 'locked':
                    before = receiver.owner.read_bytes()
                    result = subprocess.run(self.harness('reconcile', receiver.identifier), stdin=subprocess.DEVNULL,
                                            capture_output=True, timeout=60)
                    competing.update(code=result.returncode, reply=json.loads(result.stdout.decode().splitlines()[-1]),
                                     unchanged=receiver.owner.read_bytes() == before)
                super().emit(value)
        record = self.reconcile(receiver, Racing(self, skip=receiver.identifier))
        self.assertEqual(record['status'], 'reconciled')
        self.assertEqual(competing['code'], 1); self.assertTrue(competing['unchanged'])
        self.assertIn('cannot prove its own', competing['reply']['error'])
        self.assertEqual([r['outcome'] for r in record['recoveries']], ['reconciled'])
        self.assertEqual(len(self.unlocked(receiver)), 1)

    def test_live_or_missing_remote_owner_blocks_recovery_and_keeps_the_fence(self):
        receiver = self.receiver()
        self.failed(receiver)
        holder = self.holder(self.worker/'production.lock')
        with self.assertRaisesRegex(ValueError, 'worker-a'): self.reconcile(receiver)
        self.fenced()
        holder.kill(); holder.wait()
        self.worker_source = None
        with self.assertRaisesRegex(ValueError, 'worker-a'): self.reconcile(receiver)
        self.fenced()
        self.worker_source = 'e'*40
        record = self.reconcile(receiver)
        self.assertEqual([r['outcome'] for r in record['recoveries']], ['blocked', 'blocked', 'reconciled'])
        for attempt in record['recoveries']:
            self.assertTrue(R.value(attempt['evidence']))
        self.assertEqual(len(self.surveys(receiver)), 4)

    def inventory(self, **change):
        hosts = {'coordinator': {'address': '192.0.2.10', 'machine_id': MACHINE},
                 'worker-a': {'address': '192.0.2.11', 'machine_id': WORKER}}
        value = dict(hosts=hosts, ssh={'mode': 'pinned', 'key': 'k', 'known_hosts': 'h', 'known_hosts_sha256': '0'*64},
                     lock={'type': 'remote', 'host': 'coordinator'}, services={})
        value.update(change)
        return SimpleNamespace(**value)

    def client(self, relay=False, **options):
        values = dict(mode='artifact-verification', attempt=1, preparation=self.preparation)
        values.update(options)
        client = E.Execution(self.inventory(), 'e'*40, **values)
        def argv(action, identifier, expect_plan=None):
            result = self.harness(action, identifier, expect_plan)
            return [sys.executable, '-c', RELAY, *result] if relay and action == 'stage' else result
        client.argv = argv
        client.survey_argv = lambda host, identifier: self.harness('survey', identifier, host='worker')
        return client

    def test_client_drives_fixed_actions_surveys_every_host_and_requires_the_reviewed_plan(self):
        reply = self.client().run('plan')
        self.assertEqual(reply['plan_sha256'], I.digest(self.receiver().plan()))
        self.assertEqual(self.client().run('preflight')['plan_sha256'], reply['plan_sha256'])
        with self.assertRaisesRegex(ValueError, 'plan changed'): self.client().run('stage', '0'*64)
        self.assertFalse(self.receiver().owner.exists())
        staged = self.client().run('stage', reply['plan_sha256'])
        self.assertEqual(staged['status'], 'staged')
        self.assertEqual(sorted(R.value(staged['survey'])['hosts']), ['coordinator', 'worker-a'])
        identifier = I.digest(self.request())
        status = self.client(request_sha256=identifier).run('status')
        self.assertEqual((status['status'], status['request']), ('staged', self.request()))
        with self.assertRaisesRegex(ValueError, 'does not need'): self.client(request_sha256=identifier).run('reconcile')
        holder = self.holder(self.worker/'production.lock')
        with self.assertRaisesRegex(ValueError, 'refused: .*worker-a'):
            self.client(attempt=2).run('stage', I.digest(self.receiver(attempt=2).plan()))
        holder.kill(); holder.wait()
        self.assertFalse(self.receiver(attempt=2).owner.exists())

    def test_lost_transport_is_unknown_while_the_remote_owner_survives_and_reconciles(self):
        self.behave('sleep')
        plan = self.client().run('plan')['plan_sha256']
        client = self.client(relay=True)
        with patch.object(E, 'aggregate_seconds', lambda mode: 1.5-E.TRANSPORT_MARGIN_SECONDS), \
                self.assertRaisesRegex(E.Unknown, 'unknown'):
            client.run('stage', plan)
        receiver = self.receiver()
        child = self.started(receiver)
        self.assertTrue(E.alive(child))
        status = self.client(request_sha256=receiver.identifier).run('status')
        self.assertEqual(status['status'], 'running')
        os.kill(child['pid'], signal.SIGKILL)
        self.wait_for(lambda: json.loads(receiver.owner.read_text())['status'] == 'failed')
        self.fenced()
        self.wait_for(lambda: not E.alive(json.loads(receiver.owner.read_text())['receiver']) or None)
        reconciled = self.client(request_sha256=receiver.identifier).run('reconcile')
        self.assertEqual(reconciled['status'], 'reconciled')
        E.schema_fence.local_schema_fence()

    def test_unparseable_or_unfinished_replies_are_unknown(self):
        client = self.client()
        for code, raw in ((0, 'garbage'), (255, ''), (75, json.dumps({'request_sha256': 'a'*64, 'status': 'interrupted'})),
                          (0, json.dumps({'request_sha256': 'b'*64, 'status': 'staged'}))):
            client.argv = lambda *args, code=code, raw=raw: [sys.executable, '-c',
                                                             'import sys; print(sys.argv[1]); sys.exit(int(sys.argv[2]))',
                                                             raw, str(code)]
            with self.subTest(code=code), self.assertRaises(E.Unknown):
                client.call('status', 'a'*64)
        client.argv = lambda *args: [sys.executable, '-c', 'import time; time.sleep(30)']
        with patch.object(E, 'HANDSHAKE_SECONDS', .3), self.assertRaises(E.Unknown):
            client.call('stage', 'a'*64, b'{}\n', request=self.request())

    def test_client_requires_remote_pinned_root_inventory_with_every_host_pinned(self):
        base = self.inventory()
        for change in (dict(lock={'type': 'pinned_host', 'machine_id': MACHINE}), dict(ssh={'mode': 'config'}),
                       dict(hosts={'coordinator': {'address': '192.0.2.10', 'machine_id': MACHINE},
                                   'worker-a': {'address': '192.0.2.11'}}),
                       dict(hosts={'coordinator': {'address': '192.0.2.10', 'machine_id': MACHINE},
                                   'worker-a': {'address': '192.0.2.11', 'machine_id': WORKER, 'user': 'deploy'}})):
            inventory = SimpleNamespace(**dict(vars(base), **change))
            with self.subTest(change), self.assertRaises(ValueError):
                E.Execution(inventory, 'e'*40, mode='artifact-verification', attempt=1, preparation=self.preparation)
        client = E.Execution(base, 'e'*40, mode='native-certificates', attempt=1, preparation=self.preparation)
        self.assertEqual(client.request()['hosts'], HOSTS)
        client.executor = SimpleNamespace(transport=lambda host: ['ssh', '-F', '/dev/null', 'root@'+host])
        argv = client.argv('stage', 'a'*64, 'b'*64)
        self.assertTrue(argv[-1].startswith('/usr/bin/python3 -B /srv/transparent-activity/ops/sources/'+'e'*40+'/'))
        self.assertIn('schema-candidate-execute-receive --action stage --request-sha256 '+'a'*64+' --expect-plan-sha256 '+'b'*64,
                      argv[-1])
        survey = client.survey_argv('worker-a', 'a'*64)
        self.assertIn('root@worker-a', survey); self.assertTrue(survey[-1].endswith('--action survey --request-sha256 '+'a'*64))
        drifted = E.Execution(self.inventory(hosts={'coordinator': base.hosts['coordinator']}), 'e'*40,
                              request_sha256=I.digest(self.request()))
        drifted.call = lambda action, *args, **kwargs: {'status': 'failed', 'request': self.request(),
                                                        'request_sha256': I.digest(self.request())}
        with self.assertRaisesRegex(ValueError, 'original inventory'): drifted.run('reconcile')


class Wrapper(unittest.TestCase):
    def test_hidden_receiver_is_wired_before_inventory_and_refuses_outside_staged_source(self):
        lines = []
        code = cli.main(['schema-candidate-execute-receive', '--action', 'status', '--request-sha256', 'a'*64],
                        out=lines.append)
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(lines[-1])['status'], 'failed')
        args = cli.parser().parse_args(['schema-candidate-execute-receive', '--action', 'survey', '--request-sha256', 'a'*64])
        self.assertEqual(args.action, 'survey')
        with self.assertRaises(SystemExit):
            cli.parser().parse_args(['schema-candidate-execute-plan', '--source-sha', 'e'*40, '--mode', 'load',
                                     '--attempt', '1', '--preparation-request-sha256', 'a'*64])
        args = cli.parser().parse_args(['schema-candidate-execute-stage', '--source-sha', 'e'*40, '--mode',
                                        'native-certificates', '--attempt', '1', '--preparation-request-sha256', 'a'*64,
                                        '--expect-plan-sha256', 'b'*64])
        self.assertEqual(args.mode, 'native-certificates')


if __name__ == '__main__':
    unittest.main()
