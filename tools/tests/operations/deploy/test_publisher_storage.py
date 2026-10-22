"""Separate-root preparation, transactional handoff, rollback and explicit retirement."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from publisher_fixture import PublisherFixture
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import publisher, publisher_products, publisher_storage as storage
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class StorageTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.installed = copy.deepcopy(self.request['configuration'])
        self.identifier = 'checked-storage'
        candidate = self.request['configuration']
        for product, (field, _) in storage.PRODUCTS.items():
            settings = candidate[product]['settings']
            old = settings[field]
            settings[field] += '.prepared-' + self.identifier
            if product == 'transparent_history':
                settings['initial_publication'] = settings['initial_publication'].replace(old, settings[field], 1)
            self.ex.put('coordinator', old + '/active.json', '{"directory":"' + old + '/active","map_sha256":"abc"}')
            self.ex.put('coordinator', old + '/distribution-revision.json', '41')
            self.ex.put('coordinator', old + '/active/legacy.bin', 'immutable legacy body')
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.preparation = storage.Preparation(self.target, self.identifier)
        self.roots = storage.configuration(self.installed, candidate, self.identifier)
        self.rust_fail = False
        original = self.ex.run
        def run(host, argv, timeout):
            if 'prepare-transparent-storage' not in argv:
                return original(host, argv, timeout)
            self.ex.runs.append((host, argv))
            if self.rust_fail:
                return 1, 'changed or unsupported material'
            source, target = (argv[argv.index(flag) + 1] for flag in ('--source-root', '--target-root'))
            product = next(p for p, pair in self.roots.items() if pair['source'] == source)
            receipt_path = target + '/prepared-storage.json'
            if '--verify-only' in argv:
                receipt = json.loads(self.ex.read(host, receipt_path))
                return (0, 'verified') if self.ex.sha256(host, target + '/body') == receipt['files']['body'] else (1, 'changed body')
            self.ex.put(host, target + '/body', 'independent verified bytes')
            self.ex.put(host, target + '/distribution-revision.json', self.ex.read(host, source + '/distribution-revision.json'))
            receipt = {'schema': 'transparent-prepared-storage/1',
                       'ownership': {'schema': 'transparent-storage-preparation/1', 'source': source, 'target': target, 'product': product},
                       'configuration_sha256': self.ex.sha256(host, argv[argv.index('--config') + 1]),
                       'source_active_sha256': self.ex.sha256(host, source + '/active.json'),
                       'map_sha256': 'c' * 64, 'anchor_height': 2, 'anchor_hash': 'd' * 64, 'revision_floor': 41,
                       'files': {'body': self.ex.sha256(host, target + '/body')}}
            self.ex.put(host, receipt_path, json.dumps(receipt))
            return 0, 'prepared'
        self.ex.run = run
        def tree(owner, root, *, remove=None):
            files = self.ex.host(owner.host).files
            value = {str(k): self.ex.read(owner.host, k) for k in files if str(k).startswith(root + '/')}
            if remove is not None:
                if value:
                    self.assertEqual(value, remove)
                for key in value:
                    del files[key]
            return value
        self.tree = tree
        self.tree_patch = patch.object(storage, 'tree', side_effect=tree)
        self.tree_patch.start()
        self.addCleanup(self.tree_patch.stop)
        original_unlocked = self.ex.publisher_unlocked
        def unlocked(host, paths):
            if any(path.endswith('/writer.lock') for path in paths):
                return original_unlocked(host, paths)
            self.assertTrue(all(path in [pair['source'] + '/controller.lock' for pair in self.roots.values()] for path in paths))
        self.ex.publisher_unlocked = unlocked

    def prepared(self):
        before_config = self.ex.read('coordinator', publisher.CONFIG)
        before_units = copy.deepcopy(self.ex.host('coordinator').units)
        plan = self.preparation.plan()
        result = self.preparation.prepare(self.binary, plan['plan_sha256'])
        self.assertEqual(before_config, self.ex.read('coordinator', publisher.CONFIG))
        self.assertEqual(before_units, self.ex.host('coordinator').units)
        self.assertEqual(self.ex.effects, [])
        self.request['prepared_storage'] = result['prepared_storage']
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        return storage.Handoff(self.target)

    def test_preparation_handoff_and_rollback_keep_legacy_bytes_and_revision_floor(self):
        self.prepared()
        original = {p: self.tree(self.target, pair['source']) for p, pair in self.roots.items()}
        for pair in self.roots.values():
            self.ex.put('coordinator', pair['source'] + '/distribution-revision.json', '56')
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertNotIn('history_restore', journal)
        for pair in self.roots.values():
            self.assertEqual(storage.counter(self.target, pair['target']), 56)
            self.ex.put('coordinator', pair['target'] + '/distribution-revision.json', '82')
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(json.loads(self.ex.read('coordinator', publisher.CONFIG)), self.installed)
        for product, pair in self.roots.items():
            self.assertEqual(storage.counter(self.target, pair['source']), 82)
            self.assertEqual(self.ex.read('coordinator', pair['source'] + '/active/legacy.bin'), 'immutable legacy body')
            self.assertEqual(self.ex.read('coordinator', pair['source'] + '/active.json'), original[product][pair['source'] + '/active.json'])

    def test_failed_preparation_never_stops_or_reconfigures_services(self):
        self.rust_fail = True
        with self.assertRaisesRegex(DeployError, 'storage preparation refused'):
            self.preparation.prepare(self.binary, self.preparation.plan()['plan_sha256'])
        self.assertEqual(self.ex.effects, [])
        self.assertEqual(json.loads(self.ex.read('coordinator', publisher.CONFIG)), self.installed)
        self.assertIsNone(self.ex.read('coordinator', self.preparation.manifest_path))

    def test_changed_or_foreign_manifest_is_refused_before_stop(self):
        handoff = self.prepared()
        self.ex.put('coordinator', handoff.preparation.manifest_path, '{}')
        with self.assertRaisesRegex(DeployError, 'manifest changed'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_damaged_prepared_file_is_refused_before_stop(self):
        self.prepared()
        self.ex.put('coordinator', self.roots['transparent_history']['target'] + '/body', 'damage')
        with self.assertRaisesRegex(DeployError, 'storage preparation refused'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_source_advance_after_stop_refuses_handoff_and_rollback_restores_old_owner(self):
        self.prepared()
        original = self.ex.systemctl
        def systemctl(host, *argv):
            result = original(host, *argv)
            if argv == ('stop', publisher.UNIT):
                self.ex.put(host, self.roots['transparent_history']['source'] + '/active.json', '{"advanced":true}')
            return result
        self.ex.systemctl = systemctl
        with self.assertRaisesRegex(DeployError, 'source advanced'):
            self.deploy()
        self.ex.systemctl = original
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_interrupted_floor_write_recovers_without_rewinding(self):
        self.prepared()
        write = self.ex.write
        lost = False
        def write_lost(host, path, data, mode=0o600):
            nonlocal lost
            result = write(host, path, data, mode)
            if path == self.roots['transaction_details']['target'] + '/distribution-revision.json' and not lost:
                lost = True
                raise RemoteError('reply lost after revision write')
            return result
        self.ex.write = write_lost
        with self.assertRaises(RemoteError):
            self.deploy()
        self.ex.write = write
        self.assertIn('prepared_storage', self.target.latest())
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertTrue(all(storage.counter(self.target, p['source']) >= 41 for p in self.roots.values()))

    def test_cleanup_requires_acceptance_is_explicit_and_retires_rollback(self):
        handoff = self.prepared()
        with self.assertRaisesRegex(DeployError, 'accepted committed'):
            handoff.cleanup_plan()
        self.deploy()
        plan = handoff.cleanup_plan()
        with self.assertRaisesRegex(DeployError, 'cleanup plan changed'):
            handoff.cleanup('0' * 64)
        result = handoff.cleanup(durable.digest(plan))
        self.assertTrue(result['rollback_retired'])
        for pair in self.roots.values():
            self.assertIsNone(self.ex.read('coordinator', pair['source'] + '/active.json'))
            self.assertIsNotNone(self.ex.read('coordinator', pair['target'] + '/body'))
        with self.assertRaisesRegex(DeployError, 'retired rollback'):
            self.target.rollback()
        self.assertEqual(result, handoff.cleanup(durable.digest(plan)))

    def test_cleanup_resumes_a_lost_reply_without_repeating_or_expanding_deletion(self):
        handoff = self.prepared()
        self.deploy()
        plan = handoff.cleanup_plan()
        original = self.tree
        lost = False
        def remove(owner, root, *, remove=None):
            nonlocal lost
            result = original(owner, root, remove=remove)
            if remove is not None and not lost:
                lost = True
                raise RemoteError('reply lost after removing retained root')
            return result
        storage.tree.side_effect = remove
        with self.assertRaises(RemoteError):
            handoff.cleanup(durable.digest(plan))
        self.assertEqual(self.target.latest()['storage_cleanup']['phase'], 'removing')
        with self.assertRaisesRegex(DeployError, 'retired rollback'):
            self.target.rollback()
        self.assertTrue(handoff.cleanup(durable.digest(plan))['rollback_retired'])
        self.assertIsNotNone(self.ex.read('coordinator', self.roots['transparent_history']['target'] + '/body'))

    def test_cleanup_refuses_a_changed_retained_source(self):
        handoff = self.prepared()
        self.deploy()
        root = self.roots['transparent_history']['source']
        self.ex.put('coordinator', root + '/active/legacy.bin', 'foreign edit')
        with self.assertRaisesRegex(DeployError, 'retained source changed'):
            handoff.cleanup_plan()

    def test_root_moves_cannot_reconfigure_products_or_move_a_journal(self):
        for product, key, value in [('transparent_history', 'data_dir', '/other/journal'),
                                    ('transaction_details', 'root', '/other/root'),
                                    ('transparent_history', 'recent_from', 999)]:
            candidate = copy.deepcopy(self.request['configuration'])
            candidate[product]['settings'][key] = value
            with self.assertRaises(DeployError):
                storage.configuration(self.installed, candidate, self.identifier)

    def test_read_only_receipt_check_rejects_unresolved_invalidation(self):
        self.prepared()
        for name, value in [('invalidate.json', {'rollback': {'ancestor': 1}}),
                            ('integrity-failures.json', [{'path': 'corrupt'}])]:
            root = self.roots['transaction_details']['source']
            self.ex.put('coordinator', root + '/' + name, json.dumps(value))
            with self.assertRaisesRegex(DeployError, 'unresolved integrity or rollback'):
                self.target.plan()
            self.ex.remove('coordinator', root + '/' + name)
        self.assertEqual(self.ex.effects, [])

    def test_reference_and_command_options_are_closed(self):
        import argparse
        parser = argparse.ArgumentParser(allow_abbrev=False)
        storage.register(parser.add_subparsers(dest='command'))
        args = parser.parse_args(['publisher-storage-plan', '--request', 'reviewed.json',
                                 '--request-sha256', '0' * 64, '--preparation', self.identifier])
        self.assertEqual(args.command, 'publisher-storage-plan')
        for invalid in [{'id': '../outside', 'manifest_sha256': 'a' * 64},
                        {'id': 'safe', 'manifest_sha256': 'short'},
                        {'id': 'safe', 'manifest_sha256': 'a' * 64, 'root': '/elsewhere'}]:
            with self.assertRaises(DeployError):
                storage.validate_reference(invalid)

    def test_removed_trust_flag_requires_false_and_does_not_need_transport_fields(self):
        legacy = copy.deepcopy(self.installed)
        legacy['transaction_details']['settings']['trust_sealed'] = False
        self.assertEqual(publisher_products.configuration_for_update(legacy), self.installed)
        for value in (True, 0, 'false'):
            legacy['transaction_details']['settings']['trust_sealed'] = value
            with self.assertRaises(DeployError):
                publisher_products.configuration_for_update(legacy)


class NativeTreeTests(unittest.TestCase):
    def run_helper(self, root, *, expected=None):
        args = {'root': str(root)}
        if expected is not None:
            args['expected'] = expected
        return subprocess.run([sys.executable, '-c', storage.TREE_PROGRAM,
                               'snapshot' if expected is None else 'remove', json.dumps(args)],
                              capture_output=True, text=True, timeout=10)

    def test_cleanup_does_not_follow_links_and_refuses_replaced_bytes(self):
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            root = parent / 'retained'
            root.mkdir()
            outside = parent / 'journal'
            outside.write_bytes(b'committed journal')
            (root / 'link').symlink_to(outside)
            body = root / 'body'
            body.write_bytes(b'legacy')
            result = self.run_helper(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            expected = json.loads(result.stdout)
            body.write_bytes(b'changed')
            self.assertNotEqual(self.run_helper(root, expected=expected).returncode, 0)
            expected = json.loads(self.run_helper(root).stdout)
            result = self.run_helper(root, expected=expected)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(root.exists())
            self.assertEqual(outside.read_bytes(), b'committed journal')
            self.assertEqual(self.run_helper(root, expected=expected).returncode, 0)


if __name__ == '__main__':
    unittest.main()
