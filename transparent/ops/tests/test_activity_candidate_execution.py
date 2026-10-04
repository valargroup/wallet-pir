"""Closed, locked candidate native execution with retained raw evidence.

Fixture executables (Python scripts with fixture pins) stand in for the
candidate's `shard-verify` and `native_certificate`; a fixture publication with
the pinned coverage, anchor and cutoff stands in for the initial v11 set. Real
child processes, a real flock production lock and real signals are used.
Nothing contacts a host or runs a real candidate executable.
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


def load():
    import importlib.util
    spec = importlib.util.spec_from_file_location('candidate_execution_test', MODULE)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


E = load()
C, I, R = E.C, E.I, E.R
MACHINE = 'c'*32
OLD = C.HISTORICAL_SHA
HEALTHY = {'memory_available': .5, 'disk_available': {'/': .5}}

# One fixture program for both executables: behaviour comes from a control file
# keyed by the dispatch, so the pinned script bytes never change per test.
SCRIPT = r'''#!{python} -B
import fcntl, hashlib, json, os, subprocess, sys, time
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
sys.stderr.write('lock-inherited=%s lock-held=%s sid=%d\n' % (inherited, held, os.getsid(0))); sys.stderr.flush()
if mode == 'fail':
    sys.exit(3)
if mode in ('sleep', 'fork'):
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], pass_fds=[int(f) for f in fds.split(',') if f])
    open(control['mark'], 'w').write(json.dumps({{'pid': os.getpid(), 'grandchild': child.pid}}))
    if mode == 'sleep':
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

HARNESS = r'''
import json, os, sys, time
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
E.MACHINE_ID = Path(config['machine']); E.staged_source = lambda: 'e' * 40
E.host_abi = lambda: {'machine': 'x86_64', 'libc': E.GLIBC, 'cpu_flags': sorted(E.CPU_FLAGS)}
E.resources = lambda paths: {'observed_unix': time.time(), 'memory_available': .5, 'disk_available': {'/': .5}}
E.SAMPLE_SECONDS = .1
fence = E.schema_fence.local_schema_fence
E.schema_fence.INPUT_STAGING = Path(config['owners'])
E.schema_fence.HOST_ACTIONS = Path(config['fence']) / 'host-actions'
E.schema_fence.local_schema_fence = lambda **options: fence(Path(config['fence']) / 'schema', **options)
class Lock(hostlock.PinnedHostLock):
    ROOT_UID = os.getuid()
    MACHINE_ID = Path(config['machine'])
E.ProductionLock = lambda value: Lock(value, path=config['lock'])
if len(sys.argv) > 5:
    relay = os.open(sys.argv[5], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    out = lambda text: os.write(relay, (text + '\n').encode())
else:
    out = print
sys.exit(E.receive(sys.argv[2], sys.argv[3], 0, out, sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] != '-' else None))
'''

# Plays a lost SSH session: the remote receiver keeps running in its own session
# after the local transport dies; its reply goes to a file nobody reads.
RELAY = r'''
import subprocess, sys
child = subprocess.Popen(sys.argv[1:], stdin=sys.stdin, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
child.wait()
'''


class Lock(hostlock.PinnedHostLock):
    ROOT_UID = os.getuid()


class Fixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.machine = self.root/'machine-id'; self.machine.write_text(MACHINE+'\n')
        Lock.MACHINE_ID = self.machine
        self.lockpath = self.root/'production.lock'
        self.control = self.root/'control.json'; self.mark = self.root/'mark.json'
        self.behave()
        self.owners = self.root/'owners'; self.owners.mkdir(mode=0o700)
        self.fence = self.root/'fence'
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
                                   (E.schema_fence, 'INPUT_STAGING', self.owners),
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

    def behave(self, default='pass', **keys):
        self.control.write_text(json.dumps({'default': default, 'keys': keys, 'lock': str(self.lockpath),
                                            'mark': str(self.mark)}))

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
        value = {'version': 1, 'kind': E.KIND, 'mode': mode, 'source_sha': 'e'*40, 'candidate_sha': C.SOURCE_SHA,
                 'candidate_identity': C.identity(), 'preparation_request_sha256': self.preparation,
                 'publication_sha256': E.MAP_SHA256, 'machine_id': MACHINE, 'attempt': 1}
        value.update(change)
        return value

    def lock(self):
        return Lock({'type': 'pinned_host', 'machine_id': MACHINE}, path=self.lockpath)

    def receiver(self, mode='artifact-verification', **change):
        return E.Receiver(self.request(mode, **change), lock_factory=self.lock)

    def stage(self, receiver):
        return receiver.stage(I.digest(receiver.plan()))

    def fenced(self):
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            E.schema_fence.local_schema_fence()

    def exited(self, receiver):
        """Model the receiver process having ended, as it does on the coordinator."""
        dead = subprocess.Popen(['true']); start = E.process(dead.pid)['start_ticks']; dead.wait()
        record = json.loads(receiver.owner.read_text())
        record['receiver'] = {'pid': dead.pid, 'start_ticks': start}
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


class Closed(Fixture):
    def test_artifact_plan_is_one_closed_argv_with_every_expectation_and_no_journal(self):
        plan = self.receiver().plan()
        self.assertEqual(len(plan['dispatches']), 1)
        argv = plan['dispatches'][0]['argv']
        self.assertEqual(argv, [str(C.TARGET/'artifacts/shard-verify'), '--shard-dir', str(self.publication),
                                '--expect-start', '0', '--expect-through', '3500738', '--expect-anchor-hash', E.ANCHOR_HASH,
                                '--expect-recent-from', '3289805', '--expect-recent-geometry', 'recent-4k-8k',
                                '--expect-archive-geometry', 'archive-wide', '--expect-map-sha256', E.MAP_SHA256,
                                '--source-sha', C.SOURCE_SHA])
        self.assertFalse({'--data-dir', '--rebuild', '--rebuild-all', '--publication', '--out'} & set(argv))
        self.assertEqual(plan['budgets']['timeout_seconds'], 1800); self.assertEqual(plan['budgets']['concurrency'], 1)
        self.assertEqual(I.digest(plan), I.digest(self.receiver().plan()))
        self.assertEqual({p.name for p in self.owners.iterdir()},
                         {'latest.json', self.preparation+'.json', self.preparation+'.request.json'})
        self.assertFalse((C.ROOT/'executions').exists())

    def test_certificate_plan_covers_all_180_manifest_segments_with_fixed_paths(self):
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

    def test_closed_request_refuses_historical_foreign_and_partial_identities(self):
        good = self.request()
        for change in ({'candidate_sha': OLD}, {'candidate_identity': '0'*64}, {'publication_sha256': '0'*64},
                       {'mode': 'independent-chain-oracle'}, {'mode': 'load'}, {'attempt': 0}, {'attempt': True},
                       {'machine_id': 'C'*32}, {'version': 2}, {'kind': 'candidate-archive-upload'},
                       {'argv': ['/bin/sh']}, {'preparation_request_sha256': 'x'}):
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
                self.tearDown_publication()
                mutate()
                with self.assertRaisesRegex(ValueError, message): self.receiver().preflight()
                self.assertFalse((self.owners/(I.digest(self.request())+'.json')).exists())
        self.tearDown_publication()
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

    def tearDown_publication(self):
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

    def test_real_host_sampler_and_abi_reader_have_the_contract_shape(self):
        real = load()
        observed = real.resources([self.root])
        self.assertEqual(set(observed), {'observed_unix', 'memory_available', 'disk_available'})
        self.assertTrue(0 < observed['memory_available'] <= 1 and observed['disk_available'])
        abi = real.host_abi()
        self.assertEqual(set(abi), {'machine', 'libc', 'cpu_flags'})


class Run(Fixture):
    def test_artifact_run_inherits_real_lock_retains_raw_capture_and_feeds_the_offline_producer(self):
        receiver = self.receiver()
        record = self.stage(receiver)
        self.assertEqual(record['status'], 'staged'); self.assertTrue(record['gate'].startswith('unevaluated'))
        stderr = (receiver.evidence/'artifact/stderr.log').read_text()
        self.assertIn('lock-inherited=True lock-held=True', stderr)
        owner = json.loads((receiver.evidence/'artifact/owner.json').read_text())
        self.assertEqual(stderr.split('sid=')[1].strip(), str(owner['pid']))
        self.assertEqual({k: owner[k] for k in ('native_source_sha', 'candidate_sha256', 'binary_sha256', 'publication_sha256')},
                         {'native_source_sha': C.SOURCE_SHA, 'candidate_sha256': C.identity(),
                          'binary_sha256': self.pins['shard-verify'], 'publication_sha256': E.MAP_SHA256})
        self.assertTrue(owner['start_ticks'] > 0 and owner['operations_source_sha'] == 'e'*40)
        for name in ('owner.json', 'native.json', 'stderr.log', 'health.json', 'result.json', 'health.ndjson'):
            info = (receiver.evidence/'artifact'/name).stat()
            self.assertEqual((info.st_mode & 0o777, info.st_nlink), (0o400, 1))
        references = R.value(record['references'])
        report = R.artifact_report(references['mapping'], references['execution'])
        self.assertEqual(report['status'], 'passed'); self.assertEqual(report['verified']['through'], 3500738)
        E.schema_fence.local_schema_fence()
        self.assertEqual(receiver.status(), record)
        with self.lock(): pass
        with self.assertRaisesRegex(ValueError, 'already owned'): self.stage(receiver)

    def test_certificates_run_all_180_one_at_a_time_and_bind_actual_table_hashes(self):
        receiver = self.receiver('native-certificates')
        record = self.stage(receiver)
        self.assertEqual((record['status'], record['completed']), ('staged', 180))
        references = R.value(record['references'])
        self.assertEqual(len(references['executions']), 180); self.assertEqual(len(references['manifests']), 90)
        intervals = []
        for item in references['executions']:
            owner, result = R.value(item['execution']['owner']), R.value(item['execution']['result'])
            intervals.append((owner['started_unix'], result['ended_unix']))
        intervals.sort()
        self.assertTrue(all(a[1] <= b[0] for a, b in zip(intervals, intervals[1:])))
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
        with self.assertRaisesRegex(ValueError, 'receiver is still active'): receiver.reconcile()
        self.exited(receiver)
        reconciled = receiver.reconcile()
        self.assertEqual(reconciled['status'], 'reconciled')
        E.schema_fence.local_schema_fence()
        self.assertTrue((receiver.evidence/'artifact/stderr.log').exists())
        with self.assertRaisesRegex(ValueError, 'does not need'): receiver.reconcile()

    def test_certificate_failure_stops_before_any_later_segment(self):
        receiver = self.receiver('native-certificates')
        plan = receiver.plan()
        self.behave(**{plan['dispatches'][2]['segment']['path']: 'fail'})
        with self.assertRaises(ValueError): receiver.stage(I.digest(plan))
        record = json.loads(receiver.owner.read_text())
        self.assertEqual((record['status'], record['completed']), ('failed', 2))
        self.assertEqual(len(self.captures(receiver)), 3)
        self.assertFalse((receiver.evidence/'references.json').exists())
        self.fenced()

    def test_table_hash_drift_after_plan_refuses_that_segment_before_dispatch(self):
        receiver = self.receiver('native-certificates')
        plan = receiver.plan()
        target = Path(plan['dispatches'][4]['segment']['path'])
        target.chmod(0o644); raw = bytearray(target.read_bytes()); raw[0] ^= 1; target.write_bytes(bytes(raw)); target.chmod(0o444)
        with self.assertRaisesRegex(ValueError, 'table bytes differ'): receiver.stage(I.digest(plan))
        self.assertEqual(json.loads(receiver.owner.read_text())['completed'], 4)
        self.assertFalse((receiver.evidence/plan['dispatches'][4]['key']/'owner.json').exists())

    def test_plan_digest_drift_refuses_before_owner(self):
        receiver = self.receiver()
        with self.assertRaisesRegex(ValueError, 'plan changed'): receiver.stage('0'*64)
        self.assertFalse(receiver.owner.exists()); self.assertFalse(receiver.evidence.exists())

    def test_output_bounds_memory_time_floor_and_sampling_gap_stop_the_child(self):
        cases = [('output', 'pass', {'MAX_STDOUT': 10}), ('output', 'noisy', {'MAX_STDERR': 100}),
                 ('memory budget', 'sleep', {'MEMORY_BYTES': 1}), ('time budget', 'sleep', {'TIMEOUT_SECONDS': .3})]
        for attempt, (message, behaviour, limits) in enumerate(cases, 1):
            with self.subTest(message=message, limits=limits):
                self.behave(behaviour); self.mark.unlink(missing_ok=True)
                receiver = self.receiver(attempt=attempt)
                with patch.multiple(E, **limits), self.assertRaisesRegex(ValueError, message): self.stage(receiver)
                result = json.loads((receiver.evidence/'artifact/result.json').read_text())
                self.assertEqual(result['status'], 'failed')
                if behaviour == 'sleep':
                    owner = json.loads((receiver.evidence/'artifact/owner.json').read_text())
                    self.assertFalse(E.members(owner['pid'], owner['start_ticks']))
                self.exited(receiver); receiver.reconcile()
        # Preflight, before spawn, then the running child.
        samples = iter([.5, .5, .1])
        floor = lambda paths: {'observed_unix': time.time(), 'memory_available': next(samples, .1), 'disk_available': {'/': .5}}
        self.behave('sleep'); receiver = self.receiver(attempt=10)
        with patch.object(E, 'resources', floor), self.assertRaisesRegex(ValueError, '20 percent'): self.stage(receiver)
        health = json.loads((receiver.evidence/'artifact/health.json').read_text())
        self.assertEqual(health[1]['memory_available'], .1)
        self.exited(receiver); receiver.reconcile()
        clock = iter(range(1000, 2000, 30))
        gap = lambda paths: dict(HEALTHY, observed_unix=next(clock))
        receiver = self.receiver(attempt=11)
        with patch.object(E, 'resources', gap), self.assertRaisesRegex(ValueError, 'sampling gap'): self.stage(receiver)
        self.exited(receiver); receiver.reconcile()
        floor = lambda paths: dict(HEALTHY, observed_unix=time.time(), memory_available=.19)
        receiver = self.receiver(attempt=12)
        with patch.object(E, 'resources', floor), self.assertRaisesRegex(ValueError, '20 percent'): self.stage(receiver)

    def test_floor_before_spawn_starts_no_child(self):
        receiver = self.receiver()
        plan = receiver.plan()
        calls = iter([.5])
        floor = lambda paths: dict(HEALTHY, observed_unix=time.time(), memory_available=next(calls, .1))
        with patch.object(E, 'resources', floor), self.assertRaisesRegex(ValueError, '20 percent'):
            receiver.stage(I.digest(plan))
        self.assertFalse((receiver.evidence/'artifact/owner.json').exists())
        self.assertEqual(json.loads((receiver.evidence/'artifact/result.json').read_text())['pid'], None)

    def test_descendants_left_by_a_successful_child_are_stopped_and_fail(self):
        self.behave('fork')
        receiver = self.receiver()
        with self.assertRaisesRegex(ValueError, 'descendants'): self.stage(receiver)
        marks = json.loads(self.mark.read_text())
        self.wait_for(lambda: E.process(marks['grandchild']) is None or E.process(marks['grandchild'])['state'] == 'Z')
        with self.lock(): pass


class Interruption(Fixture):
    def harness(self, *args, relay=None, **options):
        config = self.root/'harness.json'
        config.write_text(json.dumps({'lib': str(HERE.parents[2]/'ops/lib'), 'module': str(MODULE),
            'artifacts': self.pins, 'candidates': str(C.ROOT), 'target': str(C.TARGET), 'owners': str(self.owners),
            'publication': str(self.publication), 'map': E.MAP_SHA256, 'machine': str(self.machine),
            'fence': str(self.fence), 'lock': str(self.lockpath)}))
        script = self.root/'harness.py'; script.write_text(HARNESS)
        argv = [sys.executable, '-B', str(script), str(config), *args]
        return argv if relay is None else [sys.executable, '-c', RELAY, *argv]

    def started(self, receiver):
        return self.wait_for(lambda: receiver.owner.exists() and json.loads(receiver.owner.read_text()).get('child'))

    def test_receiver_killed_mid_child_leaves_lock_with_child_until_verified_reconcile(self):
        self.behave('sleep')
        receiver = self.receiver()
        plan = I.digest(receiver.plan())
        header = I.durable.canonical(receiver.request)+b'\n'
        process = subprocess.Popen(self.harness('stage', receiver.identifier, plan), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, start_new_session=True)
        process.stdin.write(header); process.stdin.close()
        child = self.started(receiver)
        os.kill(process.pid, signal.SIGKILL); process.wait(); process.stdout.close()
        self.assertTrue(E.alive(child['pid'], child['start_ticks']))
        with self.assertRaises(BlockingIOError):
            with self.lock(): pass
        self.fenced()
        record = receiver.reconcile()
        self.assertEqual(record['reconciliation']['action'], 'terminated-own-process-group')
        self.assertFalse(E.alive(child['pid'], child['start_ticks']))
        marks = json.loads(self.mark.read_text())
        self.assertFalse(E.process(marks['grandchild']) and E.process(marks['grandchild'])['state'] != 'Z')
        E.schema_fence.local_schema_fence()
        self.assertTrue((receiver.evidence/'artifact/health.ndjson').stat().st_size > 0)

    def test_signalled_receiver_stops_its_own_child_records_interrupted_and_frees_lock(self):
        self.behave('sleep')
        receiver = self.receiver()
        process = subprocess.Popen(self.harness('stage', receiver.identifier, I.digest(receiver.plan())),
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True)
        process.stdin.write(I.durable.canonical(receiver.request)+b'\n'); process.stdin.close()
        child = self.started(receiver)
        os.kill(process.pid, signal.SIGTERM)
        self.assertEqual(process.wait(30), 75)
        self.assertEqual(json.loads(process.stdout.read())['status'], 'interrupted'); process.stdout.close()
        self.assertFalse(E.alive(child['pid'], child['start_ticks']))
        self.assertEqual(json.loads(receiver.owner.read_text())['status'], 'interrupted')
        self.assertEqual(json.loads((receiver.evidence/'artifact/result.json').read_text())['status'], 'interrupted')
        with self.lock(): pass
        self.fenced()
        self.assertEqual(receiver.reconcile()['reconciliation']['action'], 'no-live-child')

    def test_reused_pid_and_foreign_process_are_never_signalled(self):
        self.behave('fail')
        receiver = self.receiver()
        with self.assertRaises(ValueError): self.stage(receiver)
        record = self.exited(receiver)
        foreign = subprocess.Popen(['sleep', '30'])
        self.addCleanup(lambda: (foreign.kill(), foreign.wait()))
        ticks = E.process(foreign.pid)['start_ticks']
        record['child'] = {'key': 'artifact', 'pid': foreign.pid, 'start_ticks': ticks}
        receiver.save(record)
        with self.assertRaisesRegex(ValueError, 'outside its own session'): receiver.reconcile()
        self.assertIsNone(foreign.poll())
        record['child']['start_ticks'] = ticks-1
        receiver.save(record)
        self.assertEqual(receiver.reconcile()['reconciliation']['action'], 'pid-reused-untouched')
        self.assertIsNone(foreign.poll())

    def test_lock_held_by_unknown_owner_refuses_reconciliation(self):
        self.behave('fail')
        receiver = self.receiver()
        with self.assertRaises(ValueError): self.stage(receiver)
        self.exited(receiver)
        with self.lock(), patch.object(E, 'LOCK_WAIT_SECONDS', .2), self.assertRaisesRegex(ValueError, 'still held'):
            receiver.reconcile()
        self.assertEqual(json.loads(receiver.owner.read_text())['status'], 'failed')

    def inventory(self):
        return SimpleNamespace(hosts={'coordinator': {'address': '192.0.2.10', 'machine_id': MACHINE}},
                               ssh={'mode': 'pinned', 'key': 'k', 'known_hosts': 'h', 'known_hosts_sha256': '0'*64},
                               lock={'type': 'remote', 'host': 'coordinator'}, services={})

    def client(self, relay=None, **options):
        values = dict(mode='artifact-verification', attempt=1, preparation=self.preparation)
        values.update(options)
        client = E.Execution(self.inventory(), 'e'*40, **values)
        reply = self.root/'reply.json'
        def argv(action, identifier, expect_plan=None):
            # Only the long stage transport is lost; preflight replies normally.
            lost = relay and action == 'stage'
            return self.harness(action, identifier, expect_plan or '-', *([str(reply)] if lost else []),
                                relay=True if lost else None)
        client.argv = argv
        return client

    def test_client_plans_through_the_fixed_action_and_requires_the_reviewed_plan(self):
        reply = self.client().run('plan')
        self.assertEqual(reply['plan_sha256'], I.digest(self.receiver().plan()))
        self.assertEqual(self.client().run('preflight')['plan_sha256'], reply['plan_sha256'])
        with self.assertRaisesRegex(ValueError, 'plan changed'): self.client().run('stage', '0'*64)
        self.assertFalse(self.receiver().owner.exists())
        staged = self.client().run('stage', reply['plan_sha256'])
        self.assertEqual(staged['status'], 'staged')
        identifier = I.digest(self.request())
        self.assertEqual(self.client(request_sha256=identifier).run('status')['status'], 'staged')
        with self.assertRaisesRegex(ValueError, 'does not need'): self.client(request_sha256=identifier).run('reconcile')

    def test_lost_transport_is_unknown_while_the_remote_owner_survives(self):
        self.behave('sleep')
        plan = self.client().run('plan')['plan_sha256']
        client = self.client(relay=True)
        with patch.object(E, 'STAGE_SECONDS', 1.5), self.assertRaisesRegex(E.Unknown, 'unknown'):
            client.run('stage', plan)
        receiver = self.receiver()
        child = self.started(receiver)
        self.assertTrue(E.alive(child['pid'], child['start_ticks']))
        status = self.client(request_sha256=receiver.identifier).run('status')
        self.assertEqual(status['status'], 'running')
        os.kill(child['pid'], signal.SIGKILL)
        self.wait_for(lambda: json.loads(receiver.owner.read_text())['status'] == 'failed')
        self.fenced()
        self.wait_for(lambda: not E.alive(*json.loads(receiver.owner.read_text())['receiver'].values()) or None)
        self.assertEqual(self.client(request_sha256=receiver.identifier).run('reconcile')['status'], 'reconciled')

    def test_unparseable_or_unfinished_replies_are_unknown(self):
        client = self.client()
        for code, raw in ((0, b'garbage'), (255, b''), (75, json.dumps({'request_sha256': 'a'*64, 'status': 'interrupted'}).encode()),
                          (0, json.dumps({'request_sha256': 'b'*64, 'status': 'staged'}).encode())):
            result = SimpleNamespace(returncode=code, stdout=raw)
            with self.subTest(code=code), patch.object(E.subprocess, 'run', return_value=result), \
                    self.assertRaises(E.Unknown):
                client.call('status', 'a'*64)
        with patch.object(E.subprocess, 'run', side_effect=subprocess.TimeoutExpired('ssh', 1)), self.assertRaises(E.Unknown):
            client.call('stage', 'a'*64)

    def test_client_requires_remote_pinned_root_inventory(self):
        base = self.inventory()
        for change in (dict(lock={'type': 'pinned_host', 'machine_id': MACHINE}), dict(ssh={'mode': 'config'}),
                       dict(hosts={'coordinator': {'address': '192.0.2.10'}}),
                       dict(hosts={'coordinator': {'address': '192.0.2.10', 'machine_id': MACHINE, 'user': 'deploy'}})):
            inventory = SimpleNamespace(**dict(vars(base), **change))
            with self.subTest(change), self.assertRaises(ValueError):
                E.Execution(inventory, 'e'*40, mode='artifact-verification', attempt=1, preparation=self.preparation)
        client = E.Execution(base, 'e'*40, mode='native-certificates', attempt=1, preparation=self.preparation)
        client.executor = SimpleNamespace(transport=lambda host: ['ssh', '-F', '/dev/null', 'root@192.0.2.10'])
        argv = client.argv('stage', 'a'*64, 'b'*64)
        self.assertTrue(argv[-1].startswith('/usr/bin/python3 -B /srv/transparent-activity/ops/sources/'+'e'*40+'/'))
        self.assertIn('schema-candidate-execute-receive --action stage --request-sha256 '+'a'*64+' --expect-plan-sha256 '+'b'*64,
                      argv[-1])


class Wrapper(unittest.TestCase):
    def test_hidden_receiver_is_wired_before_inventory_and_refuses_outside_staged_source(self):
        lines = []
        code = cli.main(['schema-candidate-execute-receive', '--action', 'status', '--request-sha256', 'a'*64],
                        out=lines.append)
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(lines[-1])['status'], 'failed')
        with self.assertRaises(SystemExit):
            cli.parser().parse_args(['schema-candidate-execute-plan', '--source-sha', 'e'*40, '--mode', 'load',
                                     '--attempt', '1', '--preparation-request-sha256', 'a'*64])
        args = cli.parser().parse_args(['schema-candidate-execute-stage', '--source-sha', 'e'*40, '--mode',
                                        'native-certificates', '--attempt', '1', '--preparation-request-sha256', 'a'*64,
                                        '--expect-plan-sha256', 'b'*64])
        self.assertEqual(args.mode, 'native-certificates')


if __name__ == '__main__':
    unittest.main()
