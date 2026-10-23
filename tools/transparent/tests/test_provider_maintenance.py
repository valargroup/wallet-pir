"""Offline collection on real hard links, and maintenance through the deploy CLI."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'tools/lib'), str(ROOT / 'tools/tests/operations/deploy')]
from tools.transparent.lib import provider_collection, provider_maintenance  # noqa: E402
from tools.transparent.tests import test_binary_upgrade as fixtures  # noqa: E402
from wallet_pir_ops.deploy import cli  # noqa: E402


class OfflineCollection(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for name in ('bootstrap.lock', '.sources.lock'):
            (self.root / name).touch()
        (self.root / '.sources').mkdir()
        (self.root / 'objects').mkdir()

    def stage(self, revision):
        digest = '%064x' % revision
        obj = self.root / 'objects' / digest
        obj.write_bytes(bytes([revision]) * 4096)
        source = self.root / '.sources' / digest
        source.mkdir()
        os.link(obj, source / 'rows')
        stage = self.root / ('staging-%d-100' % revision)
        stage.mkdir()
        os.link(obj, stage / 'shards.json')
        (stage / digest).symlink_to(source, target_is_directory=True)
        os.utime(stage, ns=(revision + 1, revision + 1))
        return stage

    def populate(self):
        active = self.stage(0)
        (self.root / 'active.json').write_text(json.dumps({'directory': str(active), 'runtimes': str(active)}))
        (self.root / 'revocation-floor.json').write_text('{"revision":42,"digests":["never erase"]}')
        for revision in range(1, 12):
            self.stage(revision)
        return active

    def test_exact_manifest_reclaims_unique_data_and_preserves_active_and_controls(self):
        active = self.populate()
        before = (self.root / 'revocation-floor.json').read_bytes()
        planned = provider_collection.run(self.root, [])
        self.assertEqual(len(list((self.root / 'objects').iterdir())), 12)
        applied = provider_collection.run(self.root, [], planned['manifest_sha256'])
        self.assertTrue(applied['applied'])
        self.assertEqual(len(list((self.root / 'objects').iterdir())), 4)
        self.assertEqual((active / 'shards.json').read_bytes(), bytes(4096))
        self.assertEqual((self.root / 'revocation-floor.json').read_bytes(), before)
        self.assertGreater(applied['manifest']['reclaimable_bytes'], 0)

    def test_changed_input_or_retention_refuses_without_deleting(self):
        self.populate()
        planned = provider_collection.run(self.root, [])
        (self.root / 'revocation-floor.json').write_text('{"revision":43,"digests":[]}')
        with self.assertRaisesRegex(ValueError, 'manifest changed'):
            provider_collection.run(self.root, [], planned['manifest_sha256'])
        self.assertEqual(len(list((self.root / 'objects').iterdir())), 12)

    def test_live_owner_lock_prevents_scan_and_deletion(self):
        import fcntl
        self.populate()
        with (self.root / 'bootstrap.lock').open('r+') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                provider_collection.run(self.root, [])

    def test_malformed_records_and_external_retention_refuse(self):
        self.populate()
        with self.assertRaises(ValueError):
            provider_collection.run(self.root, ['/'])
        (self.root / 'active.json').write_text('{')
        with self.assertRaises(ValueError):
            provider_collection.run(self.root, [])
        self.assertEqual(len(list((self.root / 'objects').iterdir())), 12)

    def test_foreign_files_symlinks_and_explicit_rollback_tree_survive(self):
        self.populate()
        foreign = self.root / 'not-a-publication'
        foreign.mkdir()
        (foreign / 'keep').write_text('foreign')
        (self.root / 'staging-99-100').symlink_to(foreign, target_is_directory=True)
        keep = [str(self.root / 'staging-1-100')]
        planned = provider_collection.run(self.root, keep)
        provider_collection.run(self.root, keep, planned['manifest_sha256'])
        self.assertTrue((self.root / 'staging-1-100').is_dir())
        self.assertTrue((foreign / 'keep').is_file())
        self.assertTrue((self.root / 'staging-99-100').is_symlink())


class MaintenanceCli(unittest.TestCase):
    def setUp(self):
        self.fleet = fixtures.ProviderHistoryUpgrade()
        self.fleet.setUp()
        self.addCleanup(self.fleet.tearDown)
        self.unit = 'transparent-v12-recent.service'
        self.key = 'worker@v12:' + self.unit
        self.path = Path(self.fleet.dir.name) / 'maintenance.json'
        old_http = self.fleet.fake.http_get
        self.fleet.fake.http_get = lambda host, url, timeout=5: (
            (503, json.dumps({'error': 'publication invalidated by reorg'}))
            if url == 'http://10.142.0.9:8193/v2/shards/init' else old_http(host, url, timeout))

    def request(self, action):
        state = self.fleet.fake.probe_unit('v12', self.unit)
        request = {'schema': provider_maintenance.SCHEMA, 'git_sha': fixtures.GIT, 'worker': self.key, 'action': action,
                   'expected': {'unit_sha256': provider_maintenance.binary_upgrade.unit_digest(state),
                                'config_sha256': self.fleet.fake.sha256('v12', fixtures.history_config(self.unit)),
                                'binary_sha256': fixtures.sha256(fixtures.OLD), 'invocation_id': state['invocation_id']},
                   'retained_publications': []}
        self.path.write_text(json.dumps(request))

    def run_cli(self, action, *extra):
        lines = []
        code = cli.main([*self.fleet.descriptors, '--inventory', str(self.fleet.inventory),
                         '--state-dir', str(self.fleet.state), 'transparent-provider-maintenance-' + action,
                         '--request', str(self.path), '--request-sha256', hashlib.sha256(self.path.read_bytes()).hexdigest(),
                         *extra], executor=self.fleet.fake, out=lines.append,
                        lock=lambda: self.fleet.fake.hold_lock('coordinator', '/run/lock/wallet-pir-deploy.lock'))
        return code, lines

    def plan_digest(self):
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 0, lines)
        return next(line.split(': ', 1)[1] for line in lines if line.startswith('plan_sha256: '))

    def test_quiesce_and_resume_only_touch_the_exact_recent_worker(self):
        archive = self.fleet.fake.probe_unit('v12', 'transparent-v12-archive.service')
        self.request('quiesce')
        digest = self.plan_digest()
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', digest)
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fleet.fake.probe_unit('v12', self.unit)['main_pid'], 0)
        self.request('resume')
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', self.plan_digest())
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fleet.fake.probe_unit('v12', self.unit)['exe_sha256'], fixtures.sha256(fixtures.OLD))
        self.assertEqual(self.fleet.fake.probe_unit('v12', 'transparent-v12-archive.service'), archive)
        self.fleet.publisher_untouched()

    def test_restart_after_plan_refuses_before_a_stop(self):
        self.request('quiesce')
        digest = self.plan_digest()
        self.fleet.fake.systemctl('v12', 'restart', self.unit)
        before = list(self.fleet.fake.log)
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', digest)
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fleet.fake.log, before)

    def test_collection_requires_a_stopped_worker(self):
        self.request('collect')
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('stopped' in line for line in lines), lines)

    def test_collection_executes_only_the_reviewed_manifest_and_journals_the_result(self):
        self.fleet.fake.systemctl('v12', 'stop', self.unit)
        self.request('collect')
        calls = []
        manifest = {'delete': ['staging-1-100'], 'reclaimable_bytes': 4096, 'retained': ['staging-2-100']}
        def collect(host, root, retained, expected=None):
            calls.append((host, root, retained, expected))
            return {'manifest': manifest, 'manifest_sha256': durable_digest(manifest),
                    'applied': expected is not None, 'available_bytes': 8192}
        self.fleet.fake.provider_collection = collect
        plan = self.plan_digest()
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', plan)
        self.assertEqual(code, 0, lines)
        self.assertEqual(calls[-1][-1], durable_digest(manifest))
        self.assertEqual(self.fleet.fake.probe_unit('v12', self.unit)['main_pid'], 0)
        self.assertTrue(any('no rollback' in line for line in lines), lines)

    def test_an_unavailable_archive_refuses_quiescence_before_any_effect(self):
        self.request('quiesce')
        self.fleet.fake.unhealthy.add(('v12', fixtures.sha256(fixtures.OLD)))
        before = list(self.fleet.fake.log)
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fleet.fake.log, before)


def durable_digest(value):
    from wallet_pir_ops import durable
    return durable.digest(value)


if __name__ == '__main__':
    unittest.main()
