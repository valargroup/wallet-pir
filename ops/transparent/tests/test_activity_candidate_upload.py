"""Guarded local-to-coordinator transfer of the three candidate archives.

Fixture archives and fixture pins replace the reviewed digests only inside
these tests. Real pipes and real child processes carry the stream; nothing here
contacts a host, installs a service or runs a candidate executable.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
sys.path.insert(0, str(HERE))
import test_activity_candidate as T  # noqa: E402
from test_activity_input_stage import Lock  # noqa: E402

U = T.module('candidate_upload_test', HERE.parent/'lib/activity_candidate_upload.py')
I, C = U.I, U.C
# The shared fixture builds archives against this suite's candidate module.
T.C = C
MACHINE = 'c'*32
OLD = T.OLD

CHILD = r'''
import json, os, sys
from pathlib import Path
config = json.loads(Path(sys.argv[1]).read_text())
sys.path.insert(0, config['lib'])
import importlib.util
spec = importlib.util.spec_from_file_location('candidate_upload_child', config['module'])
U = importlib.util.module_from_spec(spec); spec.loader.exec_module(U)
C, I = U.C, U.I
C.ARTIFACTS.update(config['artifacts']); C.CI_PINS.update({k: config['artifacts'][k] for k in C.CI_PINS})
C.SUPPLEMENTAL_PINS.update({k: config['artifacts'][k] for k in C.SUPPLEMENTAL_PINS})
C.SUPPLEMENTAL_ARCHIVE_SHA256 = config['archives']['supplemental']; U.ARCHIVES.update(config['archives'])
C.OWNER = os.getuid(); U.UPLOADS = Path(config['uploads']); U.OWNERS = Path(config['owners'])
I.resources = lambda *_: {}
for name in ('SCHEMA_STATE', 'HOST_ACTIONS', 'INPUT_STAGING'):
    setattr(U.schema_fence, name, Path(config['fence'])/name)
U.schema_fence.INPUT_STAGING = Path(config['owners'])
fence = U.schema_fence.local_schema_fence
U.schema_fence.local_schema_fence = lambda **options: fence(Path(config['fence'])/'schema', **options)
U.Receiver.identity = lambda self: None
U.staged_source = lambda: None
# This transport fixture isolates archive ownership; fleet logic has its own suite.
U.G.fleet = lambda *a, **kw: {'schema':'fictional-fleet-proof'}
U.G.S.boot_id = lambda: '00000000-0000-0000-0000-000000000001'
class Lock:
    def __enter__(self): self.fd = os.open(config['lock'], os.O_CREAT | os.O_RDWR, 0o600); return self
    def __exit__(self, *_): os.close(self.fd)
    def verify(self): pass
U.ProductionLock = lambda _config: Lock()
U.Receiver.__init__.__kwdefaults__['scratch'] = config['scratch']
sys.exit(U.receive(sys.argv[2], sys.argv[3], 0, print))
'''


def writer(fd, data, close=True):
    """Feed a real pipe from another thread, like SSH's stdin."""
    def run():
        with os.fdopen(fd, 'wb', buffering=0) as stream:
            try:
                stream.write(data)
            except BrokenPipeError:
                return
            if not close:
                threading.Event().wait(2)
    thread = threading.Thread(target=run, daemon=True); thread.start()
    return thread


class Upload(T.Fixture):
    def setUp(self):
        super().setUp()
        p=patch.object(U.G,'fleet',return_value={'schema':'fictional-fleet-proof'});p.start();self.addCleanup(p.stop)
        p=patch.object(U.G.S,'boot_id',return_value='00000000-0000-0000-0000-000000000001');p.start();self.addCleanup(p.stop)
        pins = {kind:C.checksum(path) for kind, path in self.archives.items()}
        for thing, name, value in ((U, 'UPLOADS', C.ROOT/'archives'), (U, 'OWNERS', self.root/'owners'),
                                   (I, 'OWNERS', self.root/'owners'), (I, 'resources', lambda *_: {})):
            p = patch.object(thing, name, value); p.start(); self.addCleanup(p.stop)
        p = patch.dict(U.ARCHIVES, pins); p.start(); self.addCleanup(p.stop)
        self.fence = self.root/'fence'
        for name in ('HOST_ACTIONS', 'INPUT_STAGING'):
            value = self.root/'owners' if name == 'INPUT_STAGING' else self.fence/name
            p = patch.object(U.schema_fence, name, value); p.start(); self.addCleanup(p.stop)
        real = U.schema_fence.local_schema_fence
        p = patch.object(U.schema_fence, 'local_schema_fence', lambda **o: real(self.fence/'schema', **o))
        p.start(); self.addCleanup(p.stop)
        self.lock = Lock(self.root/'lock')
        self.live = self.root/'live'; self.live.mkdir()
        (self.live/'transparent-filter-server').write_bytes(b'running predecessor')
        self.inventory = SimpleNamespace(hosts={'coordinator':{'address':'192.0.2.10', 'machine_id':MACHINE}},
                                         ssh={'mode':'pinned', 'key':'k', 'known_hosts':'h', 'known_hosts_sha256':'0'*64},
                                         lock={'type':'remote', 'host':'coordinator'}, services={})
        self.paths = dict(self.archives)
        self.request = U.validate({'version':1, 'kind':U.KIND, 'source_sha':'e'*40, 'candidate_sha':C.SOURCE_SHA,
                                   'ci_run':C.CI_RUN, 'attempt':1, 'machine_id':MACHINE,
                                   'archives':{k:{'sha256':pins[k], 'size':os.stat(v).st_size} for k, v in self.archives.items()}})

    def client(self, **options):
        values = dict(attempt=1, archives=self.paths)
        values.update(options)
        return U.Upload(self.inventory, 'e'*40, out=lambda _: None, **values)

    def receiver(self, request=None):
        return U.Receiver(request or self.request, lock_factory=lambda: self.lock, scratch=str(self.root))

    def body(self, request=None):
        request = request or self.request
        return I.durable.canonical(request)+b'\n'+b''.join(Path(self.archives[k]).read_bytes() for k in U.ORDER)

    def stage(self, data, receiver=None, close=True, idle=5):
        receiver = receiver or self.receiver()
        read, write = os.pipe()
        thread = writer(write, data, close)
        try:
            incoming = U.Incoming(read, idle=idle)
            U.read_header(incoming, receiver.identifier)
            return receiver.stage(incoming)
        finally:
            os.close(read); thread.join(5)

    def fenced(self):
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            U.schema_fence.local_schema_fence()


class Local(Upload):
    def test_plan_verifies_all_eighteen_locally_binds_fixed_target_and_writes_nothing(self):
        plan = self.client().run('plan')
        self.assertEqual(plan['request'], self.request); self.assertEqual(plan['artifacts'], 18)
        self.assertEqual(plan['candidate_identity'], C.identity())
        self.assertEqual(plan['target'], str(U.UPLOADS/I.digest(self.request)))
        self.assertEqual(plan['preparation_request']['archives'],
                         {k:str(U.UPLOADS/I.digest(self.request)/(k+'.tar.gz')) for k in U.ORDER})
        # The retained request is exactly CandidatePreparation's closed form.
        I.CandidatePreparation(self.inventory, plan['preparation_request'], plan['preparation_request_sha256'])
        self.assertEqual(I.digest(plan), I.digest(self.client().run('plan')))
        self.assertEqual([argv[5] for argv in plan['coordinator_next']],
                         ['schema-candidate-plan', 'schema-candidate-preflight', 'schema-candidate-stage', 'schema-candidate-status'])
        self.assertFalse((self.root/'owners').exists()); self.assertFalse(U.UPLOADS.exists())

    def test_foreign_linked_hardlinked_duplicate_relative_and_oversized_archives_refuse(self):
        foreign = self.ci('transparent-filter', revision=OLD, name='foreign.tar.gz')
        link = self.root/'link.tar.gz'; link.symlink_to(self.archives['supplemental'])
        cases = [('digest', {'transparent-filter':foreign}), ('symlink', {'supplemental':str(link)}),
                 ('absolute', {'supplemental':'relative.tar.gz'})]
        for message, change in cases:
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.client(archives=dict(self.paths, **change)).run('plan')
        hard = self.root/'hard.tar.gz'; os.link(self.archives['transparent-publisher'], hard)
        with self.assertRaisesRegex(ValueError, 'without links'):
            self.client(archives=dict(self.paths, **{'transparent-publisher':str(hard)})).run('plan')
        os.unlink(hard)
        with patch.object(C, 'MAX_ARCHIVE', 16), self.assertRaisesRegex(ValueError, 'bounded'):
            self.client().run('plan')
        with self.assertRaisesRegex(ValueError, 'three'):
            self.client(archives={k:v for k, v in self.paths.items() if k != 'supplemental'}).run('plan')
        # A pinned digest whose bundle fails exact-revision checks still refuses.
        with patch.dict(U.ARCHIVES, {'transparent-filter':C.checksum(foreign)}), self.assertRaisesRegex(ValueError, 'revision'):
            self.client(archives=dict(self.paths, **{'transparent-filter':foreign})).run('plan')

    def test_closed_request_refuses_foreign_target_names_identities_and_bounds(self):
        for change in ({'target':'/etc/systemd/system'}, {'kind':'upload'}, {'candidate_sha':OLD}, {'ci_run':1},
                       {'attempt':True}, {'attempt':0}, {'machine_id':'C'*32}, {'version':2}):
            with self.subTest(change), self.assertRaises(ValueError):
                U.validate(dict(copy.deepcopy(self.request), **change))
        for kind, change in (('transparent-filter', {'size':0}), ('supplemental', {'size':C.MAX_ARCHIVE+1}),
                             ('transparent-publisher', {'sha256':'0'*64}), ('supplemental', {'path':'/tmp/x'})):
            bad = copy.deepcopy(self.request); bad['archives'][kind].update(change)
            with self.subTest(kind=kind, change=change), self.assertRaises(ValueError): U.validate(bad)
        bad = copy.deepcopy(self.request); bad['archives']['shard-prune'] = bad['archives'].pop('supplemental')
        with self.assertRaises(ValueError): U.validate(bad)

    def test_inventory_requires_remote_pinned_coordinator_with_machine_pin(self):
        for change in (dict(lock={'type':'pinned_host', 'machine_id':MACHINE}), dict(ssh={'mode':'config'}),
                       dict(hosts={'coordinator':{'address':'192.0.2.10'}}),
                       dict(hosts={'coordinator':{'address':'192.0.2.10', 'machine_id':MACHINE, 'user':'deploy'}})):
            inventory = SimpleNamespace(**dict(vars(self.inventory), **change))
            with self.subTest(change), self.assertRaises(ValueError):
                U.Upload(inventory, 'e'*40, attempt=1, archives=self.paths)

    def test_stream_refuses_source_changed_after_verification_or_during_stream(self):
        archive = U.Local('supplemental', self.archives['supplemental'])
        self.assertEqual(b''.join(archive.chunks()), Path(self.archives['supplemental']).read_bytes())
        os.utime(self.archives['supplemental'], ns=(1, 1))
        with self.assertRaisesRegex(ValueError, 'after verification'): b''.join(archive.chunks())
        archive = U.Local('supplemental', self.archives['supplemental'])
        stream = archive.chunks(); next(stream)
        with open(self.archives['supplemental'], 'ab') as change: change.write(b'x')
        with self.assertRaisesRegex(ValueError, 'during streaming'): b''.join(stream)
        archive = U.Local('transparent-publisher', self.archives['transparent-publisher'])
        stream = archive.chunks(); next(stream); os.utime(self.archives['transparent-publisher'], ns=(2, 2))
        with self.assertRaisesRegex(ValueError, 'during SSH streaming'): b''.join(stream)

    def test_stage_requires_reviewed_plan_digest_before_any_remote_call(self):
        client = self.client()
        with patch.object(client, 'call') as call, self.assertRaisesRegex(ValueError, 'plan changed'):
            client.run('stage', '0'*64)
        call.assert_not_called()


class Receive(Upload):
    def test_fleet_refusal_keeps_durable_failed_owner_and_reads_no_archive_bytes(self):
        receiver=self.receiver()
        with patch.object(U.G,'fleet',side_effect=ValueError('fictional stale fleet')), \
             patch.object(receiver,'receive',side_effect=AssertionError('archive bytes must wait')):
            with self.assertRaisesRegex(ValueError,'stale fleet'):receiver.stage(None)
        self.assertEqual(receiver.status()['status'],'failed')
        self.assertEqual(receiver.status()['received_bytes'],0)
        self.assertFalse(receiver.partial.exists());self.fenced()


    def test_real_pipe_stage_retains_private_exact_archives_then_existing_preparation_reads_them(self):
        with patch.object(subprocess, 'Popen', side_effect=AssertionError('no process')), \
                patch.object(subprocess, 'run', side_effect=AssertionError('no process')):
            record = self.stage(self.body())
        receiver = self.receiver()
        self.assertEqual(record['status'], 'staged'); self.assertEqual(record['artifacts'], 18)
        self.assertEqual(record['received_bytes'], sum(i['size'] for i in self.request['archives'].values()))
        self.assertEqual(receiver.status()['status'], 'staged'); self.assertFalse(receiver.partial.exists())
        self.assertEqual(receiver.target.stat().st_mode & 0o777, 0o700)
        for entry in receiver.target.iterdir():
            info = entry.lstat()
            self.assertEqual((info.st_mode & 0o777, info.st_nlink), (0o400, 1))
        self.assertEqual(json.loads((self.root/'owners/latest.json').read_text()), {'request_sha256':receiver.identifier})
        self.assertEqual((self.root/'owners'/(receiver.identifier+'.request.json')).stat().st_mode & 0o777, 0o400)
        prepared = json.loads((receiver.target/U.PREPARATION).read_bytes())
        self.assertEqual(I.digest(prepared), record['preparation_request_sha256'])
        U.schema_fence.local_schema_fence()
        # CandidatePreparation's unchanged plan accepts the retained archives.
        job = I.CandidatePreparation(self.inventory, prepared, record['preparation_request_sha256'])
        with patch.object(job, 'identity'):
            self.assertEqual(job.run('plan')['target'], str(C.TARGET))
        self.assertEqual((self.live/'transparent-filter-server').read_bytes(), b'running predecessor')
        self.assertFalse(C.TARGET.exists())
        with self.assertRaisesRegex(ValueError, 'already owned'): self.stage(self.body())

    def test_owner_and_fence_pointer_are_durable_before_the_first_byte(self):
        receiver = self.receiver()
        def receive(incoming, record, lock):
            owner = json.loads(receiver.owner.read_text())
            self.assertEqual((owner['status'], owner['pid'], owner['received_bytes']), ('receiving', os.getpid(), 0))
            self.assertEqual(json.loads((self.root/'owners/latest.json').read_text()), {'request_sha256':receiver.identifier})
            self.assertEqual(list(receiver.partial.iterdir()), [])
            self.fenced()
            raise KeyboardInterrupt
        with patch.object(receiver, 'receive', receive), self.assertRaises(KeyboardInterrupt):
            self.stage(self.body(), receiver)
        self.assertEqual(receiver.status()['status'], 'interrupted')

    def test_truncated_extra_and_corrupt_real_pipes_retain_raw_partial_and_fence_until_reconcile(self):
        body = self.body()
        for label, data in (('truncated', body[:-1]), ('extra', body+b'x'), ('checksum', body[:-1]+bytes([body[-1] ^ 1]))):
            with self.subTest(label):
                request = dict(self.request, attempt=len(label))
                data = I.durable.canonical(request)+b'\n'+data.split(b'\n', 1)[1]
                receiver = self.receiver(request)
                with self.assertRaisesRegex(ValueError, label): self.stage(data, receiver)
                self.assertEqual(receiver.status()['status'], 'failed')
                self.assertFalse(receiver.target.exists())
                retained = b''.join((receiver.partial/(k+'.tar.gz')).read_bytes() for k in U.ORDER)
                self.assertEqual(retained, data.split(b'\n', 1)[1][:len(retained)])
                self.fenced()
                with self.assertRaisesRegex(ValueError, 'unfinished'): self.stage(self.body(dict(request, attempt=99)), self.receiver(dict(request, attempt=99)))
                # The owner is this live process: reconciliation must refuse.
                with self.assertRaisesRegex(ValueError, 'still active'): receiver.reconcile()
                with patch.object(U, 'process_active', return_value=False):
                    self.assertEqual(receiver.reconcile()['status'], 'reconciled')
                abandoned = U.UPLOADS/(receiver.partial.name+'.abandoned-'+receiver.identifier)
                self.assertEqual(b''.join((abandoned/(k+'.tar.gz')).read_bytes() for k in U.ORDER), retained)
                U.schema_fence.local_schema_fence()
                with self.assertRaisesRegex(ValueError, 'already owned'): self.stage(data, receiver)
        self.assertEqual(self.stage(self.body())['status'], 'staged')

    def test_idle_stream_is_interrupted_and_reconciles_only_after_owner_exit(self):
        receiver = self.receiver()
        header = I.durable.canonical(self.request)+b'\n'
        with self.assertRaises(subprocess.TimeoutExpired):
            self.stage(header+b'x', receiver, close=False, idle=.2)
        self.assertEqual(receiver.status()['status'], 'interrupted')
        self.fenced()
        with patch.object(U, 'process_active', return_value=False): receiver.reconcile()
        self.assertEqual(receiver.status()['status'], 'reconciled')

    def test_artifact_revalidation_refuses_before_rename(self):
        # Pinned archive bytes whose CI revision is old still fail collect.
        old = self.ci('transparent-filter', revision=OLD, name='old.tar.gz')
        self.archives['transparent-filter'] = old
        with patch.dict(U.ARCHIVES, {'transparent-filter':C.checksum(old)}):
            request = copy.deepcopy(self.request)
            request['archives']['transparent-filter'] = {'sha256':C.checksum(old), 'size':os.stat(old).st_size}
            receiver = self.receiver(request)
            with self.assertRaisesRegex(ValueError, 'revision'): self.stage(self.body(request), receiver)
            self.assertFalse(receiver.target.exists()); self.assertEqual(receiver.status()['status'], 'failed')
            self.assertEqual(sorted(p.name for p in receiver.partial.iterdir()), sorted(k+'.tar.gz' for k in U.ORDER))

    def test_completed_drift_extra_link_hardlink_mode_and_request_refuse_status(self):
        self.stage(self.body()); receiver = self.receiver()
        archive = receiver.target/'supplemental.tar.gz'
        def extra():
            (receiver.target/'extra').write_bytes(b'x'); (receiver.target/'extra').chmod(0o400)
        moved = self.root/'moved.tar.gz'
        changes = [(extra, lambda: (receiver.target/'extra').unlink()),
                   (lambda: archive.rename(moved), lambda: moved.rename(archive)),
                   (lambda: (receiver.target/'alias').symlink_to(archive), lambda: (receiver.target/'alias').unlink()),
                   (lambda: os.link(archive, self.root/'hard'), lambda: (self.root/'hard').unlink()),
                   (lambda: archive.chmod(0o444), lambda: archive.chmod(0o400))]
        for change, undo in changes:
            change()
            with self.assertRaises((ValueError, OSError)): receiver.status()
            undo()
        receiver.status()
        prepared = receiver.target/U.PREPARATION; prepared.chmod(0o600); prepared.write_bytes(b'{}\n'); prepared.chmod(0o400)
        with self.assertRaisesRegex(ValueError, 'preparation'): receiver.status()

    def test_header_framing_binds_reviewed_request(self):
        raw = I.durable.canonical(self.request)+b'\n'
        for data in (raw[:-1], b'{"version":1,"version":1}\n', b'x'*(U.MAX_HEADER+2)):
            read, write = os.pipe(); writer(write, data).join(5)
            with self.assertRaises(ValueError): U.read_header(U.Incoming(read), I.digest(self.request))
            os.close(read)
        read, write = os.pipe(); writer(write, raw).join(5)
        with self.assertRaisesRegex(ValueError, 'reviewed plan'): U.read_header(U.Incoming(read), '0'*64)
        os.close(read)

    def test_symlinked_namespace_refuses_before_owner(self):
        (self.root/'elsewhere').mkdir(); C.ROOT.mkdir(parents=True)
        U.UPLOADS.symlink_to(self.root/'elsewhere', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'): self.receiver().preflight()
        self.assertFalse((self.root/'owners').exists())

    def test_receiver_requires_coordinator_root_and_staged_source_with_release_tool(self):
        receiver = U.Receiver(self.request)
        with patch.object(U.os, 'geteuid', return_value=0), self.assertRaises((ValueError, OSError)): receiver.identity()
        source = self.root/'ops'
        with patch.object(I, 'SOURCE', source/'sources'):
            (source/'staging').mkdir(parents=True)
            (source/'staging'/('e'*40+'.json')).write_text(json.dumps({'files':{'ops/scripts/wallet-pir-deploy.py':'0'*64}}))
            with self.assertRaisesRegex(ValueError, 'release.py'): I.require_release_tool('e'*40)
            prepared = U.preparation(self.request)
            job = I.CandidatePreparation(self.inventory, prepared, I.digest(prepared))
            with patch.object(I.Preparation, 'identity'), self.assertRaisesRegex(ValueError, 'release.py'): job.run('plan')
            (source/'staging'/('e'*40+'.json')).write_text(json.dumps({'files':{'tools/ci/release.py':'0'*64}}))
            I.require_release_tool('e'*40)
        with self.assertRaises(ValueError): U.staged_source()


class Transport(Upload):
    """The client drives a real child receiver through the bounded pipe pump."""

    def setUp(self):
        super().setUp()
        config = {'lib':str(HERE.parents[2]/'ops/lib'), 'module':str(HERE.parent/'lib/activity_candidate_upload.py'),
                  'artifacts':dict(C.ARTIFACTS), 'archives':dict(U.ARCHIVES), 'uploads':str(U.UPLOADS),
                  'owners':str(self.root/'owners'), 'fence':str(self.fence), 'lock':str(self.root/'lock'),
                  'scratch':str(self.root)}
        (self.root/'child.json').write_text(json.dumps(config))
        (self.root/'child.py').write_text(CHILD)

    def client(self, **options):
        client = super().client(**options)
        client.argv = lambda action, identifier: [sys.executable, '-B', str(self.root/'child.py'),
                                                  str(self.root/'child.json'), action, identifier]
        return client

    def test_plan_preflight_stage_status_through_real_child_process(self):
        client = self.client()
        plan = client.run('plan')
        self.assertEqual(client.run('preflight')['remote']['status'], 'preflight-passed')
        self.assertFalse((self.root/'owners').exists())
        reply = client.run('stage', I.digest(plan))
        self.assertEqual((reply['status'], reply['preparation_request_sha256']), ('staged', plan['preparation_request_sha256']))
        status = self.client(attempt=None, archives=None, request_sha256=plan['request_sha256']).run('status')
        self.assertEqual(status['status'], 'staged')
        with self.assertRaisesRegex(ValueError, 'already owned'): client.run('preflight')
        with self.assertRaisesRegex(ValueError, 'does not need'):
            self.client(attempt=None, archives=None, request_sha256=plan['request_sha256']).run('reconcile')
        absent = self.client(attempt=None, archives=None, request_sha256='f'*64).run('status')
        self.assertEqual(absent['status'], 'absent')

    def test_local_transport_loss_is_unknown_then_observed_and_explicitly_reconciled(self):
        client = self.client(); plan = client.run('plan')
        original = U.Local.chunks
        def chunks(archive):
            stream = original(archive)
            yield next(stream)
            if archive.kind == 'supplemental':
                # Lose transport after the guard and partial namespace exist.
                # The durable intent now precedes the fleet survey.
                owner = self.root/'owners'/(plan['request_sha256']+'.json')
                for _ in range(200):
                    if owner.exists() and (U.UPLOADS/(plan['request_sha256']+'.receiving')).exists(): break
                    threading.Event().wait(.05)
                os.utime(archive.path, ns=(1, 1))
            yield from stream
        with patch.object(U.Local, 'chunks', chunks), self.assertRaisesRegex(U.Unknown, 'unknown'):
            client.run('stage', I.digest(plan))
        # The killed child never finished: its durable owner still fences.
        observer = self.client(attempt=None, archives=None, request_sha256=plan['request_sha256'])
        observed = observer.run('status')
        self.assertEqual(observed['status'], 'receiving')
        self.fenced()
        with self.assertRaisesRegex(ValueError, 'unfinished'): self.client(attempt=2).run('preflight')
        self.assertEqual(observer.run('reconcile')['status'], 'reconciled')
        self.assertEqual(observer.run('status')['status'], 'reconciled')
        abandoned = list(U.UPLOADS.glob('*.abandoned-'+plan['request_sha256']))
        self.assertEqual(len(abandoned), 1)
        # A fresh attempt with unchanged bytes is a new, separately owned request.
        retry = self.client(attempt=2)
        self.assertEqual(retry.run('stage', I.digest(retry.run('plan')))['status'], 'staged')

    def test_remote_timeout_or_unparseable_reply_is_unknown_without_retry(self):
        client = self.client(); identifier = I.digest(self.request)
        calls = []
        def run(argv, **options):
            calls.append(argv)
            raise subprocess.TimeoutExpired(argv, 1)
        with patch.object(U.subprocess, 'run', run), self.assertRaises(U.Unknown): client.call('status', identifier)
        self.assertEqual(len(calls), 1)
        for code, raw in ((0, b'not json'), (255, b''), (0, json.dumps({'request_sha256':'0'*64, 'status':'staged'}).encode()),
                          (75, json.dumps({'request_sha256':identifier, 'status':'interrupted'}).encode())):
            with self.subTest(code=code, raw=raw[:12]), self.assertRaises(U.Unknown):
                client.reply(identifier, code, raw, 'stage')
        with self.assertRaises(U.Unknown):
            client.reply(identifier, 0, json.dumps({'request_sha256':identifier, 'status':'receiving'}).encode(), 'stage')
        self.assertEqual(client.reply(identifier, 0, json.dumps({'request_sha256':identifier, 'status':'receiving'}).encode(),
                                      'status')['status'], 'receiving')


class Wrapper(unittest.TestCase):
    def test_remote_argv_is_fixed_staged_wrapper_over_pinned_ssh_without_masters(self):
        inventory = SimpleNamespace(hosts={'coordinator':{'address':'192.0.2.10', 'machine_id':MACHINE}},
                                    ssh={'mode':'pinned'}, lock={'type':'remote', 'host':'coordinator'})
        client = U.Upload(inventory, 'e'*40, request_sha256='f'*64)
        client.executor = SimpleNamespace(transport=lambda host: ['ssh', '-F', '/dev/null', 'root@192.0.2.10'])
        argv = client.argv('stage', 'f'*64)
        self.assertEqual(argv[:5], ['ssh', '-F', '/dev/null', '-oControlMaster=no', '-oControlPath=none'])
        self.assertEqual(argv[-1], '/usr/bin/python3 -B /srv/transparent-activity/ops/sources/'+'e'*40+
                         '/ops/scripts/wallet-pir-deploy.py schema-candidate-receive --action stage --request-sha256 '+'f'*64)

    def test_cli_has_no_target_option_and_receiver_reply_is_bounded(self):
        from wallet_pir_ops.deploy import cli
        with self.assertRaises(SystemExit), patch('sys.stderr'):
            cli.parser().parse_args(['schema-candidate-upload-plan', '--source-sha', 'e'*40, '--attempt', '1',
                                     '--transparent-filter', '/a', '--transparent-publisher', '/b', '--supplemental', '/c',
                                     '--target', '/etc'])
        lines = []
        with tempfile.TemporaryFile() as empty:
            with patch.object(sys, 'stdin', SimpleNamespace(fileno=empty.fileno)):
                code = cli.main(['schema-candidate-receive', '--action', 'status', '--request-sha256', 'bad'], out=lines.append)
        self.assertEqual(code, 1); self.assertEqual(json.loads(lines[0])['status'], 'failed')

    def test_real_reviewed_archive_pins(self):
        real = T.module('candidate_upload_real', HERE.parent/'lib/activity_candidate_upload.py')
        self.assertEqual(real.ARCHIVES, {
            'transparent-filter':'1a3dcc5805dbb4b0cfbf82fe32c0be72b501d350d3f7ae2385b7e3f378506ea1',
            'transparent-publisher':'092fe69ce76f8003714524f77741754448913efb7ca483d3fe36e056a50708fa',
            'supplemental':'d3a8f60a76e0fa59288fe670275abed75b304b7d390cde368effb842c840a9c3'})
        self.assertEqual(real.UPLOADS, Path('/srv/transparent-activity/candidates/archives'))
        self.assertEqual(real.OWNERS, Path('/srv/transparent-activity/ops/input-staging'))


if __name__ == '__main__':
    unittest.main()
