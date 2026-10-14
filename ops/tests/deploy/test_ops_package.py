"""The operations-package transaction against one coordinator run by the real host helper.

The layout points at a temporary directory owned by the test user, so links,
modes, renames and partial directories are real. Archives are built from this
checkout's operations files under fake clean revisions.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

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


def ci_run(revision, **changes):
    return {'id': 42, 'head_sha': revision, 'head_branch': 'main', 'event': 'push', 'status': 'completed',
            'conclusion': 'success', 'path': '.github/workflows/ci-full.yml', **changes}


class PackageCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        (self.dir / 'opt').mkdir(mode=0o755)
        (self.dir / 'machine-id').write_text(MACHINE + '\n')
        self.layout = package.Layout(root=str(self.dir / 'opt/wallet-pir-ops'),
                                     fleet=str(self.dir / 'etc/wallet-pir/fleet.json'),
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

    def archive(self, revision, dirty=False, files=None):
        path = Path(tempfile.mkdtemp(dir=self.dir)) / ('%s.tar.gz' % revision[:4])
        bundle.write_archive(path, revision, files or FILES, dirty)
        self.archives[revision] = path
        return path

    def package(self, state=None, host=None):
        return package.Package(self.inventory, host or self.host, state or self.state, layout=self.layout,
                               out=self.lines.append, inventory_path=self.inventory_path)

    def plan(self, revision=A, archive=None, fleet=None, run=True, target=None):
        archive = archive or self.archives.get(revision) or self.archive(revision)
        plan, problems = (target or self.package()).build_plan(archive, revision, fleet or self.fleet,
                                                               ci_run(revision) if run else None)
        return plan, problems, archive

    def install(self, revision=A, fleet=None, target=None):
        plan, problems, archive = self.plan(revision, fleet=fleet, target=target)
        self.assertEqual(problems, [])
        return (target or self.package()).install(archive, revision, fleet or self.fleet, durable.digest(plan),
                                                  ci_run(revision))

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
        self.assertEqual(plan['package']['archive_sha256'], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(plan['coordinator'], {'host': 'coordinator', 'machine_id': MACHINE})
        self.assertEqual(plan['prior'], {'selection': None, 'fleet_sha256': None, 'transaction': None,
                                         'release': 'absent'})
        self.assertEqual(plan['target']['fleet_sha256'], hashlib.sha256(self.fleet.read_bytes()).hexdigest())
        # Integrity of the artifact and its full-CI provenance stay separate records.
        self.assertEqual(plan['integrity']['manifest_qualification'], 'unqualified')
        self.assertEqual(plan['qualification']['provenance'], 'full-ci-main')
        self.assertEqual(plan['qualification']['run']['id'], 42)

    def test_refusals_are_reported_before_any_change(self):
        cases = {
            'dirty source': lambda: self.plan(archive=self.archive(A, dirty=True)),
            'no full-CI run': lambda: self.plan(run=False),
            'pull request run': lambda: (*self.package().build_plan(self.archive(A), A, self.fleet,
                                                                    ci_run(A, head_branch='feature')), None),
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
            self.plan(archive=self.archive(A, files=missing))
        with self.assertRaises(ValueError):
            self.plan(archive=self.archive(A), revision=B)
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
            self.package().install(archive, A, changed, durable.digest(plan), ci_run(A))
        dirty, problems, dirty_archive = self.plan(archive=self.archive(A, dirty=True))
        with self.assertRaisesRegex(package.PackageError, 'refused before any change'):
            self.package().install(dirty_archive, A, self.fleet, durable.digest(dirty), ci_run(A))
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
        self.assertEqual((problems, plan['prior']['release']), ([], 'present-verified'))
        self.host.log.clear()
        self.package().install(self.archives[B], B, self.fleet, durable.digest(plan), ci_run(B))
        self.assertNotIn('package_unpack', self.host.mutations())
        self.assertEqual(self.selection(), 'releases/' + B)

    def test_noop_when_already_selected(self):
        self.install()
        plan, problems, archive = self.plan()
        self.assertEqual((plan['changes'], problems), ([], []))
        self.host.log.clear()
        self.assertIsNone(self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A)))
        self.assertEqual(self.host.log, [])

    def test_lock_contention_and_schema_fence_change_nothing(self):
        plan, _, archive = self.plan()
        self.host.held.add(('coordinator', descriptors.LOCK_PATH))
        with self.assertRaises(LockHeld):
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
        self.host.held.clear()
        with mock.patch.object(engine.schema_fence, 'schema_mutation_fence',
                               side_effect=ValueError('unfinished schema transaction')):
            with self.assertRaisesRegex(ValueError, 'unfinished schema'):
                self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
            _, problems = self.package().preflight(archive, A, self.fleet, ci_run(A))
            self.assertIn('unfinished schema transaction', problems)
        self.assertEqual(self.host.log, [])

    def test_lost_lock_stops_before_the_next_step(self):
        plan, _, archive = self.plan()
        def lose(op, arguments):
            if op == 'package_unpack':
                self.host.lock.lost = True
        self.host.after = lose
        with self.assertRaisesRegex(package.PackageError, 'unknown'):
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
        self.assertNotIn('rename', self.host.mutations())
        self.assertIsNone(self.selection())

    def test_unfinished_transaction_blocks_another_runner_and_this_one(self):
        plan, _, archive = self.plan()
        self.host.fail = (8, 'before')
        with self.assertRaises(package.PackageError):
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
        _, problems, _ = self.plan(target=self.package(state=self.dir / 'other-runner'))
        self.assertTrue(any('unfinished' in p for p in problems), problems)
        self.host.fail = None
        with self.assertRaisesRegex(package.PackageError, 'is unknown'):
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
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
                    self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
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
                        self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
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
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
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
            self.package().install(archive, A, self.fleet, durable.digest(plan), ci_run(A))
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
        with self.assertRaisesRegex(package.PackageError, 'later package transaction'):
            self.package().rollback(first.id)
        os.unlink(self.layout.current)
        os.symlink('releases/' + 'c' * 40, self.layout.current)
        self.host.log.clear()
        with self.assertRaisesRegex(package.PackageError, 'changed outside this transaction'):
            self.package().rollback()
        self.assertEqual(self.host.log, [])

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
                        executor=self.host, out=output.append, package_layout=self.layout)
        return code, json.loads(output[-1])

    def test_plan_install_status_and_rollback_commands(self):
        archive = self.archive(A)
        run = self.dir / 'run.json'
        run.write_text(json.dumps(ci_run(A)))
        inputs = ['--sha', A, '--archive', str(archive), '--fleet', str(self.fleet), '--ci-run', str(run)]
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


class HelperProtocolTests(PackageCase):
    def test_package_operations_over_the_helper_protocol(self):
        from test_wallet_pir_deploy import LocalExecutor
        executor = LocalExecutor(None)
        archive = self.archive(A)
        manifest, files, _ = bundle.planned_files(archive, A)
        observed = executor.package_observe('local', self.layout.root, self.layout.fleet, A, self.layout.machine_id)
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
        environment = {**os.environ, 'PYTHONPATH': '', 'PATH': '/usr/bin:/bin'}
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
