"""The operations-package transaction against one coordinator run by the real host helper.

The layout points at a temporary directory owned by the test user, so links,
modes, renames and partial directories are real. Archives are built from this
checkout's operations files under fake clean revisions and published as the
artifacts of fake full-CI runs, which is the only way an archive qualifies.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from local_host import LocalHost  # noqa: E402
from wallet_pir_ops import bundle, durable  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, engine, package  # noqa: E402
from wallet_pir_ops.deploy.remote import LockHeld  # noqa: E402
from wallet_pir_ops.deploy.transaction import Journal  # noqa: E402

A, B = 'a' * 40, 'b' * 40
MACHINE = '0123456789abcdef0123456789abcdef'
EXAMPLE = ROOT / 'enhance/ops/deploy/deploy-inventory.example.json'
FILES = bundle.contents(ROOT)


RUNS = {A: 42, B: 43}


def ci_run(revision, **changes):
    return {'id': RUNS[revision], 'head_sha': revision, 'head_branch': 'main', 'event': 'push',
            'status': 'completed', 'conclusion': 'success', 'path': '.github/workflows/ci-full.yml', **changes}


class FakeGitHub:
    """Runs, their artifacts and artifact ZIP bytes, as the Actions API returns them."""
    repository = 'example/wallet-pir'

    def __init__(self):
        self.runs, self.listed, self.blobs, self.downloads = {}, {}, {}, 0

    def publish(self, revision, archive, digest=True, members=None):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as artifact:
            for name, data in (members or {package.ARCHIVE: archive.read_bytes()}).items():
                artifact.writestr(name, data)
        blob, run_id = buffer.getvalue(), RUNS[revision]
        self.runs[run_id] = ci_run(revision)
        self.listed[run_id] = [{'id': run_id * 10, 'name': 'wallet-pir-ops-' + revision, 'expired': False,
                                **({'digest': 'sha256:' + hashlib.sha256(blob).hexdigest()} if digest else {})}]
        self.blobs[run_id * 10] = blob

    def run(self, run_id):
        return dict(self.runs[run_id])

    def artifacts(self, run_id):
        return [dict(a) for a in self.listed.get(run_id, [])]

    def download(self, artifact_id):
        self.downloads += 1
        return self.blobs[artifact_id]


class PackageCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        (self.dir / 'opt').mkdir(mode=0o755)
        (self.dir / 'machine-id').write_text(MACHINE + '\n')
        self.layout = package.Layout(root=str(self.dir / 'opt/wallet-pir-ops'),
                                     fleet=str(self.dir / 'etc/wallet-pir/fleet.json'),
                                     known_hosts=str(self.dir / 'etc/wallet-pir/known_hosts'),
                                     machine_id=str(self.dir / 'machine-id'), owner_uid=os.getuid())
        self.state = self.dir / 'state'
        self.inventory_path = self.dir / 'inventory.json'
        self.inventory_path.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'coordinator': {'machine_id': MACHINE}}, 'services': {}}))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        self.host = LocalHost()
        self.fleet = self.dir / 'fleet.json'
        shutil.copyfile(EXAMPLE, self.fleet)
        self.archives = {}
        self.lines = []
        self.github = FakeGitHub()

    def archive(self, revision, dirty=False, files=None, publish=True):
        """A clean (or dirty) archive of `revision`, published as its run's artifact unless told not to."""
        path = Path(tempfile.mkdtemp(dir=self.dir)) / ('%s.tar.gz' % revision[:4])
        bundle.write_archive(path, revision, files or FILES, dirty)
        if publish:
            self.github.publish(revision, path)
            self.archives[revision] = path
        return path

    def package(self, state=None, host=None):
        return package.Package(self.inventory, host or self.host, state or self.state, layout=self.layout,
                               out=self.lines.append, inventory_path=self.inventory_path, github=self.github)

    def plan(self, revision=A, fleet=None, run=True, archive=None, known_hosts=None, target=None):
        """`(plan, problems, archive path)`; by default from the published artifact of `revision`'s run."""
        if run and revision not in self.archives:
            self.archive(revision)
        return (target or self.package()).build_plan(revision, fleet or self.fleet, RUNS[revision] if run else None,
                                                     archive, known_hosts)

    def install(self, revision=A, fleet=None, known_hosts=None, target=None):
        plan, problems, _ = self.plan(revision, fleet=fleet, known_hosts=known_hosts, target=target)
        self.assertEqual(problems, [])
        return (target or self.package()).install(revision, fleet or self.fleet, durable.digest(plan),
                                                  RUNS[revision], known_hosts=known_hosts)

    def selection(self):
        current = Path(self.layout.current)
        return os.readlink(current) if current.is_symlink() else None

    def fleet_bytes(self):
        path = Path(self.layout.fleet)
        return path.read_bytes() if path.exists() else None

    def release(self, revision):
        return Path(self.layout.releases) / revision

    def staged(self):
        staging = Path(self.layout.staging)
        return list(staging.iterdir()) if staging.exists() else []


class PlanTests(PackageCase):
    def test_plan_reads_only_and_binds_every_input(self):
        plan, problems, archive = self.plan()
        self.assertEqual(problems, [])
        self.assertEqual(self.host.log, [])
        again, _, _ = self.plan()
        self.assertEqual(durable.digest(plan), durable.digest(again))
        self.assertEqual(plan['package']['revision'], A)
        published = self.archives[A].read_bytes()
        self.assertEqual(plan['package']['archive_sha256'], hashlib.sha256(published).hexdigest())
        self.assertEqual(archive.read_bytes(), published)
        self.assertEqual(plan['coordinator'], {'host': 'coordinator', 'machine_id': MACHINE})
        self.assertEqual(plan['prior'], {'selection': None, 'files': {'fleet': None, 'known-hosts': None},
                                         'transaction': None, 'release': 'absent'})
        self.assertEqual(plan['target']['files'], {'fleet': hashlib.sha256(self.fleet.read_bytes()).hexdigest(),
                                                   'known-hosts': None})
        # Integrity of the artifact and its full-CI provenance stay separate records.
        self.assertEqual(plan['integrity']['manifest_qualification'], 'unqualified')
        self.assertEqual(plan['qualification']['provenance'], 'full-ci-main')
        self.assertEqual(plan['qualification']['run']['id'], 42)
        self.assertEqual(plan['qualification']['artifact']['digest_source'], 'github')
        self.assertEqual(plan['qualification']['archive_sha256'], plan['package']['archive_sha256'])

    def test_refusals_are_reported_before_any_change(self):
        cases = {
            'dirty source': lambda: (self.archive(A, dirty=True), self.plan())[1],
            'no full-CI run': lambda: self.plan(run=False, archive=self.archive(A, publish=False)),
            'local copy differs from the artifact': lambda: self.plan(archive=self.archive(B, publish=False)),
            'incomplete fleet': lambda: self.plan(fleet=self.incomplete_fleet()),
        }
        for name, case in cases.items():
            with self.subTest(name):
                _, problems, _ = case()
                self.assertTrue(problems, name)
        self.assertEqual(self.host.log, [])

    def incomplete_fleet(self):
        document = json.loads(self.fleet.read_text())
        del document['services']['transparent-history']['roles']['controller']
        path = self.dir / 'incomplete.json'
        path.write_text(json.dumps(document))
        return path

    def test_altered_missing_or_unsafe_archive_files_refuse(self):
        changed = dict(FILES)
        changed['ops/lib/wallet_pir_ops/deploy/host_helper.py'] += b'# altered'
        archive = self.archive(A, files=changed)
        # Re-label the archive's manifest digest without the change: a checksum mismatch.
        with self.assertRaises(ValueError):
            bundle.planned_files(self.tamper(archive), A)
        missing = dict(FILES)
        del missing['ops/deploy/services.toml']
        with self.assertRaises(ValueError):
            self.archive(A, files=missing)
            self.plan()
        with self.assertRaises(ValueError):
            self.plan(revision=B, run=False, archive=self.archive(A, publish=False))
        self.assertEqual(self.host.log, [])

    def tamper(self, archive):
        import io
        import tarfile
        with tarfile.open(archive) as source:
            payload = {m.name: source.extractfile(m).read() for m in source}
        payload['ops/deploy/services.toml'] += b'\n# changed after the manifest\n'
        tampered = self.dir / 'tampered.tar.gz'
        with tarfile.open(tampered, 'w:gz') as target:
            for name, data in payload.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                target.addfile(member, io.BytesIO(data))
        return tampered

    def test_coordinator_state_refusals(self):
        root = lambda: Path(self.layout.root)  # noqa: E731 (each subtest has its own directory)
        cases = {
            'machine identity': lambda: Path(self.layout.machine_id).write_text('f' * 32),
            'group-writable package root': lambda: (root().mkdir(), root().chmod(0o775)),
            'insufficient space': lambda: setattr(self.host, 'free', 1 << 20),
            'final directory collision': lambda: (self.release(A).mkdir(parents=True),
                                                  (self.release(A) / 'other').write_text('x')),
            'unowned staging': lambda: (Path(self.layout.staging).mkdir(parents=True),
                                        (Path(self.layout.staging) / 'leftover').mkdir()),
            'current is not a link': lambda: (root().mkdir(), Path(self.layout.current).mkdir()),
        }
        for name, change in cases.items():
            with self.subTest(name):
                self.setUp()
                change()
                _, problems, _ = self.plan()
                self.assertTrue(problems, name)
                self.assertEqual(self.host.log, [])

    def test_install_refuses_a_stale_plan_and_problems_with_zero_effects(self):
        plan, _, archive = self.plan()
        changed = self.dir / 'changed.json'
        document = json.loads(self.fleet.read_text())
        document['hosts']['worker-01']['address'] = '192.0.2.99'
        changed.write_text(json.dumps(document))
        with self.assertRaisesRegex(package.PackageError, 'changed since the reviewed plan'):
            self.package().install(A, changed, durable.digest(plan), RUNS[A])
        self.archive(A, dirty=True)
        dirty, problems, _ = self.plan()
        with self.assertRaisesRegex(package.PackageError, 'refused before any change'):
            self.package().install(A, self.fleet, durable.digest(dirty), RUNS[A])
        unqualified, _, _ = self.plan(run=False, archive=self.archives[A])
        with self.assertRaisesRegex(package.PackageError, 'never install'):
            self.package().install(A, self.fleet, durable.digest(unqualified), archive=self.archives[A])
        self.assertEqual(self.host.log, [])
        self.assertIsNone(Journal.load(self.state, package.SERVICE))


class InstallTests(PackageCase):
    def test_first_install_status_and_rollback_to_absence(self):
        journal = self.install()
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.selection(), 'releases/' + A)
        self.assertEqual(self.fleet_bytes(), self.fleet.read_bytes())
        self.assertNotIn('systemctl', self.host.mutations())
        self.assertEqual(oct(self.release(A).stat().st_mode & 0o777), oct(0o755))
        self.assertTrue(os.access(self.release(A) / 'ops/scripts/wallet-pir-deploy.py', os.X_OK))
        self.assertFalse(os.access(self.release(A) / 'ops/deploy/services.toml', os.X_OK))
        self.assertEqual(self.staged(), [])
        status = self.package().status()
        self.assertEqual((status['revision'], status['integrity'], status['phase'], status['next']),
                         (A, 'verified', 'committed', None))
        self.assertEqual(status['qualification']['provenance'], 'full-ci-main')
        # Integrity is re-derived from the installed files, so a later edit shows.
        (self.release(A) / 'ops/deploy/services.toml').chmod(0o666)
        self.assertTrue(self.package().status()['integrity'].startswith('changed'))
        (self.release(A) / 'ops/deploy/services.toml').chmod(0o644)

        rolled = self.package().rollback()
        self.assertEqual(rolled.status, 'rolled-back')
        self.assertIsNone(self.selection())
        self.assertIsNone(self.fleet_bytes())
        # The release and the transaction's records are retained.
        self.assertTrue((self.release(A) / 'operations.json').exists())
        records = Path(self.layout.transaction(journal.id))
        self.assertEqual(json.loads((records / 'final.json').read_text())['status'], 'rolled-back')
        self.assertTrue((records / 'record.json').exists())
        self.assertEqual(self.package().rollback().status, 'rolled-back')

    def test_upgrade_to_a_newer_revision_and_rollback_restores_previous_selection_and_inventory(self):
        self.install(A)
        first_fleet = self.fleet.read_bytes()
        document = json.loads(first_fleet)
        document['hosts']['worker-02']['address'] = '192.0.2.26'
        self.fleet.write_text(json.dumps(document, indent=2))
        upgrade = self.install(B)
        self.assertEqual(self.selection(), 'releases/' + B)
        self.assertEqual(self.fleet_bytes(), self.fleet.read_bytes())
        self.assertTrue(self.release(A).is_dir())
        self.package().rollback(upgrade.id)
        self.assertEqual(self.selection(), 'releases/' + A)
        self.assertEqual(self.fleet_bytes(), first_fleet)
        self.assertTrue(self.release(B).is_dir())
        # Reinstalling a retained, intact release selects it without staging again.
        plan, problems, _ = self.plan(B)
        self.assertEqual((problems, plan['prior']['release']), ([], 'retained'))
        self.host.log.clear()
        self.package().install(B, self.fleet, durable.digest(plan), RUNS[B])
        self.assertNotIn('package_unpack', self.host.mutations())
        self.assertEqual(self.selection(), 'releases/' + B)

    def test_noop_when_already_selected(self):
        self.install()
        plan, problems, archive = self.plan()
        self.assertEqual((plan['changes'], problems), ([], []))
        self.host.log.clear()
        self.assertIsNone(self.package().install(A, self.fleet, durable.digest(plan), RUNS[A]))
        self.assertEqual(self.host.log, [])

    def test_lock_contention_and_schema_fence_change_nothing(self):
        plan, _, archive = self.plan()
        self.host.held.add(('coordinator', descriptors.LOCK_PATH))
        with self.assertRaises(LockHeld):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        self.host.held.clear()
        with mock.patch.object(engine.schema_fence, 'schema_mutation_fence',
                               side_effect=ValueError('unfinished schema transaction')):
            with self.assertRaisesRegex(ValueError, 'unfinished schema'):
                self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
            _, problems = self.package().preflight(A, self.fleet, RUNS[A])
            self.assertIn('unfinished schema transaction', problems)
        self.assertEqual(self.host.log, [])

    def test_lost_lock_stops_before_the_next_step(self):
        plan, _, archive = self.plan()
        def lose(op, arguments):
            if op == 'package_unpack':
                self.host.lock.lost = True
        self.host.after = lose
        with self.assertRaisesRegex(package.PackageError, 'unknown'):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        self.assertNotIn('rename', self.host.mutations())
        self.assertIsNone(self.selection())

    def test_unfinished_transaction_blocks_another_runner_and_this_one(self):
        plan, _, archive = self.plan()
        self.host.fail = (8, 'before')
        with self.assertRaises(package.PackageError):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        _, problems, _ = self.plan(target=self.package(state=self.dir / 'other-runner'))
        self.assertTrue(any('unfinished' in p for p in problems), problems)
        self.host.fail = None
        with self.assertRaisesRegex(package.PackageError, 'is unknown'):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        status = self.package(state=self.dir / 'other-runner').status()
        self.assertEqual(status['phase'], 'unfinished')
        self.assertIn('ops-package-reconcile --transaction ' + status['transaction'], status['next'])

    def test_altered_staged_files_are_refused_before_selection(self):
        for name, alter in (('content', lambda p: p.write_bytes(p.read_bytes() + b'#')),
                            ('executable bit', lambda p: p.chmod(0o644)),
                            ('extra file', lambda p: (p.parent / 'added.py').write_text(''))):
            with self.subTest(name):
                self.setUp()
                plan, _, archive = self.plan()
                def change(op, arguments):
                    if op == 'package_unpack':
                        alter(Path(arguments['path']) / 'ops/scripts/wallet-pir-deploy.py')
                self.host.after = change
                with self.assertRaisesRegex(package.PackageError, 'not exactly the planned package'):
                    self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
                self.assertIsNone(self.selection())
                self.assertFalse(self.release(A).exists())
                self.assertEqual(Journal.load(self.state, package.SERVICE).status, 'refused')
                self.host.after = None
                # The staged partial is the transaction's own; rollback discards it.
                self.package().rollback()
                self.assertEqual(self.staged(), [])
                self.assertIsNone(self.fleet_bytes())

    def test_every_interrupted_effect_is_unknown_until_reconciled(self):
        total = len(self.reference_log())
        # Candidate validation and the entrypoint runs are covered by other tests;
        # here every step's file effects are still checked.
        for name, value in (('candidate_problems', []), ('verify_entrypoints', None)):
            patch = mock.patch.object(package.Package, name, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)
        for index in range(total):
            for mode in ('before', 'after'):
                with self.subTest(index=index, mode=mode):
                    self.setUp()
                    # The required files only: the effects are the same, the fsyncs fewer.
                    self.archive(A, files={name: FILES[name] for name in bundle.REQUIRED})
                    plan, _, archive = self.plan()
                    self.host.fail = (index, mode)
                    with self.assertRaisesRegex(package.PackageError, 'unknown until reconciled'):
                        self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
                    journal = Journal.load(self.state, package.SERVICE)
                    self.assertEqual(journal.status, 'unknown')
                    self.host.fail = None
                    if index % 2:
                        self.package().reconcile(journal.id, archive, self.fleet)
                        self.assertEqual(Journal.load(self.state, package.SERVICE).status, 'committed')
                        self.assertEqual(self.selection(), 'releases/' + A)
                        self.assertEqual(self.fleet_bytes(), self.fleet.read_bytes())
                        self.assertEqual(self.package().status()['integrity'], 'verified')
                    else:
                        self.package().rollback(journal.id)
                        self.assertIsNone(self.selection())
                        self.assertIsNone(self.fleet_bytes())
                        self.assertEqual(self.staged(), [])
                    self.assertNotIn('systemctl', self.host.mutations())

    def reference_log(self):
        """The mutating calls of an uninterrupted first install, with this case's paths."""
        reference = PackageCase('run')
        reference.setUp()
        reference.install()
        log = [(op, path.replace(reference.tmp.name, self.tmp.name)) for op, path in reference.host.log]
        reference.tmp.cleanup()
        return log

    def test_partial_staging_is_discarded_and_restaged_by_reconcile(self):
        plan, _, archive = self.plan()
        index = [op for op, _ in self.reference_log()].index('package_unpack')
        self.host.fail = (index, 'partial')
        with self.assertRaisesRegex(package.PackageError, 'unknown'):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        identifier = Journal.load(self.state, package.SERVICE).id
        self.assertTrue((Path(self.layout.staging) / identifier).is_dir())
        self.host.fail = None
        self.package().reconcile(identifier, archive, self.fleet)
        self.assertIn('remove_tree', self.host.mutations())
        self.assertEqual(self.selection(), 'releases/' + A)

    def test_reconcile_and_rollback_refuse_external_changes(self):
        plan, _, archive = self.plan()
        fleet_write = self.reference_log().index(('write', self.layout.fleet))
        self.host.fail = (fleet_write, 'after')
        with self.assertRaises(package.PackageError):
            self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        self.host.fail = None
        Path(self.layout.fleet).write_text('{"edited": "outside the transaction"}')
        identifier = Journal.load(self.state, package.SERVICE).id
        self.host.log.clear()
        with self.assertRaisesRegex(package.PackageError, 'no longer matches'):
            self.package().reconcile(identifier, archive, self.fleet)
        with self.assertRaisesRegex(package.PackageError, 'changed outside this transaction'):
            self.package().rollback(identifier)
        self.assertEqual(self.host.log, [])

    def test_rollback_refuses_a_changed_selection_and_a_later_transaction(self):
        first = self.install(A)
        self.install(B)
        with self.assertRaisesRegex(package.PackageError, 'is not rolled back'):
            self.package().rollback(first.id)
        os.unlink(self.layout.current)
        os.symlink('releases/' + 'c' * 40, self.layout.current)
        self.host.log.clear()
        with self.assertRaisesRegex(package.PackageError, 'changed outside this transaction'):
            self.package().rollback()
        self.assertEqual(self.host.log, [])

    def test_rollback_refuses_a_corrupted_retained_release_and_keeps_the_current_one(self):
        def replace_with_link(path):
            path.unlink()
            path.symlink_to('/etc/hostname')
        corruptions = {
            'content': lambda r: (r / 'ops/deploy/services.toml').write_text('changed'),
            'missing file': lambda r: (r / 'ops/deploy/services.toml').unlink(),
            'symlink': lambda r: replace_with_link(r / 'ops/scripts/wallet-pir-deploy.py'),
            'file mode': lambda r: (r / 'ops/deploy/services.toml').chmod(0o666),
            'lost executable bit': lambda r: (r / 'ops/scripts/wallet-pir-deploy.py').chmod(0o644),
            'directory mode': lambda r: (r / 'ops').chmod(0o777),
            'release replaced by a link': lambda r: (shutil.rmtree(r), r.symlink_to(self.release(B))),
        }
        for name, corrupt in corruptions.items():
            with self.subTest(name):
                self.setUp()
                self.install(A)
                first_fleet = self.fleet.read_bytes()
                document = json.loads(first_fleet)
                document['hosts']['worker-02']['address'] = '192.0.2.26'
                self.fleet.write_text(json.dumps(document, indent=2))
                upgrade = self.install(B)
                corrupt(self.release(A))
                self.host.log.clear()
                with self.assertRaisesRegex(package.PackageError, 'rollback refused; nothing was changed'):
                    self.package().rollback(upgrade.id)
                self.assertEqual(self.host.mutations(), [])
                self.assertEqual(self.selection(), 'releases/' + B)
                self.assertEqual(self.fleet_bytes(), self.fleet.read_bytes())
                final = Path(self.layout.transaction(upgrade.id)) / 'final.json'
                self.assertEqual(json.loads(final.read_text())['status'], 'committed')

    def test_rollback_refuses_a_retained_release_no_transaction_promoted(self):
        upgrade = self.install(B)
        self.release(A).parent.mkdir(parents=True, exist_ok=True)
        bundle.extract(self.archive(A), self.release(A), A)
        # A record naming an unpromoted, identical directory as the prior selection is not proof.
        record_path = Path(self.layout.transaction(upgrade.id)) / 'record.json'
        record = json.loads(record_path.read_text())
        record['plan']['prior']['selection'] = 'releases/' + A
        record['plan_sha256'] = durable.digest(record['plan'])
        record_path.write_text(json.dumps(record))
        (self.state / (upgrade.id + '.json')).unlink()
        self.host.log.clear()
        with self.assertRaisesRegex(package.PackageError, 'no single recorded package transaction'):
            self.package().rollback(upgrade.id)
        self.assertEqual((self.host.mutations(), self.selection()), ([], 'releases/' + B))

    def test_rollback_to_an_intact_retained_release_repeats_and_reinstalls(self):
        first = self.install(A)
        upgrade = self.install(B)
        for _ in range(2):
            self.assertEqual(self.package().rollback(upgrade.id).status, 'rolled-back')
            self.assertEqual(self.selection(), 'releases/' + A)
        self.assertEqual(self.package().status()['integrity'], 'verified')
        self.package().rollback(first.id)
        self.assertIsNone(self.selection())
        # The rolled-back B is reselected as a retained release, then rolled back to A again.
        again = self.install(A)
        later = self.install(B)
        self.package().rollback(later.id)
        self.assertEqual(self.selection(), 'releases/' + A)
        self.assertEqual(again.status, 'committed')

    def test_rollback_from_another_runner_uses_the_coordinator_record(self):
        journal = self.install()
        other = self.package(state=self.dir / 'other-runner')
        other.rollback(journal.id)
        self.assertIsNone(self.selection())
        self.assertEqual(Journal.load(self.dir / 'other-runner', package.SERVICE, journal.id).status, 'rolled-back')


class CommandTests(PackageCase):
    def run_cli(self, *argv):
        output = []
        code = cli.main(['--inventory', str(self.inventory_path), '--state-dir', str(self.state), '--json', *argv],
                        executor=self.host, out=output.append, package_layout=self.layout,
                        package_github=self.github)
        return code, json.loads(output[-1])

    def test_plan_install_status_and_rollback_commands(self):
        self.archive(A)
        inputs = ['--sha', A, '--ci-run-id', '42', '--fleet', str(self.fleet)]
        code, plan = self.run_cli('ops-package-plan', *inputs)
        self.assertEqual((code, plan['problems']), (0, []))
        self.assertIn('--expect-plan-sha256 ' + plan['plan_sha256'], plan['install'])
        code, preflight = self.run_cli('ops-package-preflight', *inputs)
        self.assertEqual((code, preflight['plan_sha256']), (0, plan['plan_sha256']))
        self.assertEqual(self.host.log, [])
        code, installed = self.run_cli('ops-package-install', *inputs, '--expect-plan-sha256', plan['plan_sha256'])
        self.assertEqual((code, installed['phase']), (0, 'committed'))
        self.assertIn('ops-package-rollback --transaction ' + installed['transaction'], installed['rollback'])
        code, status = self.run_cli('ops-package-status')
        self.assertEqual((status['revision'], status['integrity']), (A, 'verified'))
        self.assertIn('service adoption', status['services'])
        code, rolled = self.run_cli('ops-package-rollback')
        self.assertEqual(rolled['phase'], 'rolled-back')

    def test_package_commands_do_not_require_binary_revision_equality(self):
        calls = []
        with mock.patch('wallet_pir_ops.bundle.check_installed', side_effect=lambda **k: calls.append(k)):
            self.run_cli('ops-package-status')
            cli.main(['--inventory', str(self.inventory_path), 'plan', 'enhance', '--sha256', 'f' * 64],
                     executor=self.host, out=lambda line: None)
            cli.main(['--inventory', str(self.inventory_path), 'deploy', 'enhance', '--archive', 'x', '--sha', B],
                     executor=self.host, out=lambda line: None)
        self.assertEqual(calls[0], {'mutation': False, 'revision': None})
        self.assertEqual(calls[2], {'mutation': True, 'revision': B})

    def test_installed_package_upgrades_but_keeps_binary_revision_matching(self):
        destination = self.dir / 'installed-a'
        bundle.extract(self.archive(A), destination, A)
        bundle.check_installed(root=destination, mutation=True)
        with self.assertRaisesRegex(ValueError, 'same source revision'):
            bundle.check_installed(root=destination, mutation=True, revision=B)


PIN = b'192.0.2.10 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHVq2DYPmKqNPAwmIRDmMqNwZB1kOk3ZXgyLyUXyCNo9\n'
PIN_2 = PIN + b'192.0.2.20 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK7j5ZlJzYtq1wRL9DcJvF0hHnq5i2Qq0r8nqJm8eW1T\n'


class ProvenanceTests(PackageCase):
    def test_the_artifact_bytes_are_what_installs(self):
        plan, problems, archive = self.plan()
        self.assertEqual(problems, [])
        # The plan resolves the archive path; on macOS the temporary root is a link.
        self.assertTrue(str(archive).startswith(str((self.state / 'artifacts' / '420').resolve())))
        # A second plan reuses this runner's copy only while it matches GitHub's digest.
        self.plan()
        self.assertEqual(self.github.downloads, 1)
        (self.state / 'artifacts/420/artifact.zip').write_bytes(b'corrupted')
        self.plan()
        self.assertEqual(self.github.downloads, 2)

    def test_a_local_archive_or_run_record_cannot_stand_in_for_the_artifact(self):
        local = self.archive(A, dirty=True, publish=False)
        self.archive(A)
        _, problems, _ = self.plan(archive=local)
        self.assertTrue(any('is not the archive of artifact' in p for p in problems), problems)
        _, problems, _ = self.plan(archive=self.archives[A])
        self.assertEqual(problems, [])
        refusals = {
            'pull request run': lambda: self.github.runs[42].update(head_branch='feature'),
            'failed run': lambda: self.github.runs[42].update(conclusion='failure'),
            'another revision': lambda: self.github.runs[42].update(head_sha=B),
            'expired artifact': lambda: self.github.listed[42][0].update(expired=True),
            'missing artifact': lambda: self.github.listed[42].clear(),
            'digest mismatch': lambda: self.github.listed[42][0].update(digest='sha256:' + '0' * 64),
            'extra member': lambda: self.github.publish(A, self.archives[A], members={
                package.ARCHIVE: self.archives[A].read_bytes(), 'other': b'x'}),
        }
        for name, change in refusals.items():
            with self.subTest(name):
                self.setUp()
                self.archive(A)
                change()
                with self.assertRaises(package.PackageError):
                    self.plan()
        self.assertEqual(self.host.log, [])

    def test_artifacts_without_a_github_digest_use_the_downloaded_bytes(self):
        self.github.publish(A, self.archive(A, publish=False), digest=False)
        self.archives[A] = None
        plan, problems, _ = self.package().build_plan(A, self.fleet, RUNS[A])
        self.assertEqual((problems, plan['qualification']['artifact']['digest_source']), ([], 'downloaded'))


class KnownHostsTests(PackageCase):
    def pins(self, data):
        path = self.dir / ('pins-%d' % len(data))
        path.write_bytes(data)
        return path

    def fleet_naming_pins(self, data):
        document = json.loads(self.fleet.read_text())
        document['hosts']['transparent-router-01'].update(
            known_hosts=self.layout.known_hosts, known_hosts_sha256=hashlib.sha256(data).hexdigest())
        path = self.dir / ('fleet-%d.json' % len(data))
        path.write_text(json.dumps(document, indent=2))
        return path

    def test_pins_install_beside_the_inventory_and_roll_back(self):
        first = self.install(A, fleet=self.fleet_naming_pins(PIN), known_hosts=self.pins(PIN))
        installed = Path(self.layout.known_hosts)
        self.assertEqual((installed.read_bytes(), installed.stat().st_mode & 0o777), (PIN, 0o644))
        upgrade = self.install(B, fleet=self.fleet_naming_pins(PIN_2), known_hosts=self.pins(PIN_2))
        self.assertEqual(installed.read_bytes(), PIN_2)
        self.package().rollback(upgrade.id)
        self.assertEqual(installed.read_bytes(), PIN)
        self.package().rollback(first.id)
        self.assertFalse(installed.exists())
        self.assertIsNone(self.fleet_bytes())

    def test_pins_must_match_the_inventory_and_hold_only_public_keys(self):
        _, problems, _ = self.plan(fleet=self.fleet_naming_pins(PIN), known_hosts=self.pins(PIN_2))
        self.assertTrue(any('not the planned' in p for p in problems), problems)
        _, problems, _ = self.plan(fleet=self.fleet_naming_pins(PIN))
        self.assertTrue(any('leaves absent' in p for p in problems), problems)
        for data in (b'-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n', b'192.0.2.10 ssh-dss AAAA\n',
                     b'192.0.2.10 ssh-ed25519 not*base64\n', b'# nothing pinned\n'):
            _, problems, _ = self.plan(known_hosts=self.pins(data))
            self.assertTrue(problems, data)
        self.assertEqual(self.host.log, [])


class RehearsalTests(PackageCase):
    def test_install_rollback_then_a_fresh_plan_reselects_the_retained_release(self):
        first = self.install()
        self.package().rollback(first.id)
        self.assertIsNone(self.selection())
        self.assertTrue((self.release(A) / 'operations.json').exists())
        plan, problems, _ = self.plan()
        self.assertEqual((problems, plan['prior']['release'], plan['prior']['transaction']),
                         ([], 'retained', first.id))
        self.host.log.clear()
        again = self.package().install(A, self.fleet, durable.digest(plan), RUNS[A])
        self.assertEqual((again.status, self.selection()), ('committed', 'releases/' + A))
        self.assertNotIn('package_unpack', self.host.mutations())
        self.assertNotIn('rename', self.host.mutations())
        status = self.package().status()
        self.assertEqual((status['integrity'], status['qualification']['provenance']), ('verified', 'full-ci-main'))

    def test_an_identical_directory_without_a_recorded_owner_is_not_reused(self):
        self.release(A).parent.mkdir(parents=True)
        bundle.extract(self.archive(A), self.release(A), A)
        _, problems, _ = self.plan()
        self.assertTrue(any('no recorded package transaction created it' in p for p in problems), problems)

    def test_a_retained_release_changed_since_is_refused(self):
        first = self.install()
        self.package().rollback(first.id)
        (self.release(A) / 'ops/deploy/services.toml').write_text('changed')
        _, problems, _ = self.plan()
        self.assertTrue(any('never overwritten' in p for p in problems), problems)


class HelperProtocolTests(PackageCase):
    def test_package_operations_over_the_helper_protocol(self):
        from test_wallet_pir_deploy import LocalExecutor
        executor = LocalExecutor(None)
        archive = self.archive(A)
        manifest, files, _ = bundle.planned_files(archive, A)
        observed = executor.package_observe('local', self.layout.root, [self.layout.fleet], A, self.layout.machine_id)
        self.assertEqual((observed['machine_id'], observed['current'], observed['release']), (MACHINE, None, None))
        staging = self.dir / 'staging'
        staging.mkdir()
        target = str(staging / 'transaction')
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        with self.assertRaisesRegex(Exception, 'manifest differs'):
            executor.package_unpack('local', archive, target, digest, '0' * 64)
        executor.package_unpack('local', archive, target, digest, files['operations.json'][0])
        tree = executor.package_tree('local', target)
        self.assertEqual(package.tree_problems(tree, files, os.getuid()), [])
        with self.assertRaisesRegex(Exception, 'FileExistsError'):
            executor.package_unpack('local', archive, target, digest, files['operations.json'][0])
        link = str(self.dir / 'current')
        executor.replace_link('local', link, 'staging/transaction')
        self.assertEqual(os.readlink(link), 'staging/transaction')
        source = self.dir / 'fleet.json'
        executor.package_copy('local', str(source), str(self.dir / 'copy.json'),
                              hashlib.sha256(source.read_bytes()).hexdigest(), 0o600)
        self.assertEqual((self.dir / 'copy.json').read_bytes(), source.read_bytes())
        with self.assertRaisesRegex(Exception, 'not directly below'):
            executor.remove_tree('local', target, str(self.dir))
        executor.remove_tree('local', target, str(staging))
        self.assertFalse(Path(target).exists())


class RelocatedBundleTests(PackageCase):
    def test_extracted_bundle_checks_itself_and_loads_product_adapters(self):
        destination = self.dir / 'elsewhere/release'
        destination.parent.mkdir()
        bundle.extract(self.archive(A), destination, A)
        # The entrypoints' `env python3` finds only this interpreter (the supported
        # one running the tests, not whatever the system ships) and no checkout path.
        interpreter = self.dir / 'interpreter'
        interpreter.mkdir()
        (interpreter / 'python3').symlink_to(sys.executable)
        environment = {**os.environ, 'PYTHONPATH': '', 'PATH': '%s:/usr/bin:/bin' % interpreter}
        commands = [
            [str(destination / 'ops/scripts/wallet-pir-bundle.py'), 'self-check', '--sha', A],
            [str(destination / 'ops/scripts/wallet-pir-deploy.py'), 'ops-package-install', '--help'],
            [str(destination / 'ops/scripts/wallet-pir-deploy.py'), 'schema-status', '--help'],
            [str(destination / 'ops/scripts/wallet-pir-deploy.py'), '--inventory', str(EXAMPLE), 'fleet', '--complete'],
        ]
        for argv in commands:
            result = subprocess.run(argv, cwd=self.dir, env=environment, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, (argv, result.stderr))
        self.assertEqual(json.loads(result.stdout)['complete'], True)
        result = subprocess.run([str(destination / 'ops/scripts/wallet-pir-bundle.py'), 'self-check', '--sha', B],
                                cwd=self.dir, env=environment, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.rglob('*.pyc')))


if __name__ == '__main__':
    unittest.main()
