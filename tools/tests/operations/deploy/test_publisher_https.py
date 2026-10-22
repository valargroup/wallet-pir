"""A guarded distribution switch preserves worker ownership and can recover."""
import json
import hashlib
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher, publisher_https, units
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class HttpsTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.request['schema'] = publisher.HTTPS_SCHEMA
        for product in publisher.PRODUCTS:
            settings = self.request['configuration'][product]['settings']
            settings.update(http_only=True, ship_runtimes=True)
            if product == 'transparent_history':
                settings['distribute_prepared'] = True
        consumers = []
        for kind, service, index in [('history-archive', 'transparent-history', 0),
                                     ('history-recent', 'transparent-history', 1),
                                     ('transaction-details', 'transparent-display', 0)]:
            entry = self.request['inventory']['services'][service]['roles']['worker'][index]
            host, unit = entry['host'], entry['unit']
            section = 'transaction_details' if kind == 'transaction-details' else 'transparent_history'
            body = {'publisher': 'https://example.test', 'publisher_network': 'main',
                    'publisher_genesis': '0' * 64, 'listen': entry['vars']['listen'],
                    'cache_bytes': 8 << 30, 'bootstrap_max_bytes': 1 << 30}
            root = '/srv/https-' + kind
            if section == 'transaction_details':
                body.update(role='combined', state_dir=root)
            else:
                body.update(shard_dir=root, full_holdings=kind == 'history-archive')
                if kind == 'history-recent':
                    body['archive_upstream'] = 'https://archive.example.test'
            config = {'management_listen': '127.0.0.1:' + str(18300 + len(consumers)),
                      section: {'enabled': True, 'settings': body}}
            consumers.append({'kind': kind, 'host': host, 'unit': unit, 'configuration': config,
                              'config_path': '/etc/wallet-pir/https-' + kind + '.json', 'budget_bytes': 1 << 30})
            self.ex.install(host, unit, '[Service]\nExecStart=/bin/old-transparent --listen '
                            + entry['vars']['listen'] + '\nMemoryMax=48G\n',
                            files={'/bin/old-transparent': b'previous transparent executable'})
        self.request['https'] = {'origin': 'https://example.test', 'publisher_budget_bytes': 32 << 30,
                                 'consumers': consumers,
                                 'history_edge': {'host': consumers[0]['host'], 'origin': 'https://archive.example.test',
                                                  'before_sha256': ''}}
        host = consumers[0]['host']
        address = consumers[0]['configuration']['transparent_history']['settings']['listen'].rsplit(':', 1)[0]
        old_edge = 'archive.example.test {\n}\nhttp://' + address + ':8080 {\n}\n'
        self.ex.put(host, '/etc/caddy/Caddyfile', old_edge)
        self.request['https']['history_edge']['before_sha256'] = publisher.sha(old_edge)
        self.ex.install(host, 'caddy.service', '[Service]\nExecStart=/bin/caddy run --config /etc/caddy/Caddyfile\n',
                        files={'/bin/caddy': b'caddy'})
        for section, key in [('transparent_history', 'publication_root'), ('transaction_details', 'root')]:
            root = self.request['configuration'][section]['settings'][key]
            source = root + '/immutable-snapshot'
            content = '{\n  "production": true\n}'
            digest = publisher.sha('{"production":true}') if section == 'transparent_history' else publisher.sha(content)
            self.ex.put('coordinator', root + '/active.json',
                        json.dumps({'directory': source, 'map_sha256': digest}))
            name = 'shards.json' if section == 'transparent_history' else 'txid-shards.json'
            self.ex.put('coordinator', source + '/' + name, content)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.effects.clear()
        original_run, original_get = self.ex.run, self.ex.http_get
        self.ex.put('coordinator', '/usr/bin/python3', b'host Python interpreter')
        def run(host, argv, timeout):
            if len(argv) > 2 and argv[1].endswith('/snapshot-warmer.py'):
                self.ex.runs.append((host, argv))
                if argv[2] == 'pin':
                    source, destination = argv[3:]
                    for path, content in list(self.ex.host(host).files.items()):
                        if path.startswith(source + '/'):
                            self.ex.put(host, destination + path[len(source):], content)
                    return 0, '{"pinned_files":1}'
                if argv[2] == 'seed':
                    self.assertEqual(self.ex.probe_unit(host, publisher.UNIT)['main_pid'], 0)
                    return 0, '{"linked_runtimes":1,"runtime_bytes":1024}'
                if argv[2] == 'reuse':
                    return 0, '{"reused_runtimes":0,"runtime_bytes":1024}'
            if len(argv) > 1 and argv[1] == 'verify-filters':
                raw = self.ex.read(host, argv[argv.index('--shard-dir') + 1] + '/shards.json')
                return 0, json.dumps({'map_sha256': publisher.sha(json.dumps(json.loads(raw), separators=(',', ':'))), 'schema': 'transparent-shard-v1', 'network': 'main', 'genesis_hash': '0' * 64, 'shards': 1, 'start_height': 0, 'covered_through': 1})
            if '--check' in argv and argv[0].endswith('/pir-query-server'):
                config = json.loads(self.ex.read(host, argv[argv.index('--config') + 1]))
                if 'transparent_history' in config or 'transaction_details' in config:
                    return 0, 'configuration accepted'
            return original_run(host, argv, timeout)
        def get(host, url, timeout=5):
            if url.endswith('/current'):
                return 200, json.dumps({'eligibility': 'canonical', 'network': 'main',
                    'inventory_checksum': {'algorithm':'blake3','digest':'a' * 64}})
            if ':1830' in url:
                return 200, '{"ready":true}'
            return original_get(host, url, timeout)
        self.addCleanup(patch.stopall)
        patch.object(self.ex, 'run', side_effect=run).start()
        patch.object(self.ex, 'http_get', side_effect=get).start()

    def test_cutover_loads_only_the_real_services(self):
        self.deploy()
        self.assertNotIn('warm_snapshots', self.target.latest())
        self.assertFalse(any(args[0] == 'start' and args[1].startswith('pir-snapshot-')
                             for args in self.ex.effects if len(args) > 1))
        self.assertFalse(any('--verify-only' in argv or any(value in ('warm', 'capacity') for value in argv)
                             for _, argv in self.ex.runs))

    def test_recovery_still_removes_a_retained_temporary_validator(self):
        from wallet_pir_ops.deploy.publisher_warmup import SnapshotPreparation
        from wallet_pir_ops.deploy.remote import production_lock
        name = 'pir-snapshot-retained.service'
        text = '[Service]\nExecStart=/bin/retained-validator\n'
        self.ex.install('coordinator', name, text,
                        files={'/bin/retained-validator': b'retained temporary validator'})
        record = {'unit': name, 'unit_text': text, 'inputs': {}}
        with production_lock(self.target.inventory, self.ex) as self.target.lock:
            SnapshotPreparation(self.target.serving).cleanup({'warm_snapshots': {'transparent_history': record}})
        self.assertEqual(self.ex.probe_unit('coordinator', name)['load_state'], 'not-found')

    def test_changed_cache_helper_is_never_executed_during_reuse(self):
        from wallet_pir_ops.deploy.publisher_warmup import SnapshotPreparation
        self.deploy()
        journal = self.target.latest()
        script = journal['prepared_snapshots']['transparent_history']['script']
        self.ex.put('coordinator', script, 'changed helper')
        self.ex.runs.clear()
        with self.assertRaisesRegex(DeployError, 'snapshot cache helper changed'):
            SnapshotPreparation(self.target.serving).seed(journal)
        self.assertEqual(self.ex.runs, [])

    def test_reused_files_do_not_consume_the_additional_staging_allowance(self):
        self.deploy()
        snapshots = self.target.latest()['prepared_snapshots']
        for record in snapshots.values():
            self.assertEqual(record['retained_runtime_bytes'], 1024)

    def test_smaller_additional_allowance_still_preserves_free_disk(self):
        self.request['https']['publisher_budget_bytes'] = 16 << 30
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.deploy()
        self.assertEqual(self.target.latest()['phase'], 'committed')

        self.request['https']['publisher_budget_bytes'] = 1 << 60
        self.target = self.make_target()
        with self.assertRaisesRegex(DeployError, '20 percent disk headroom'):
            self.target.plan()

    def test_additional_allowance_must_be_positive(self):
        for allowance in (0, -1, True):
            self.request['https']['publisher_budget_bytes'] = allowance
            with self.assertRaisesRegex(DeployError, 'positive additional disk allowance'):
                self.make_target()

    def test_archive_memory_budget_cannot_be_an_implicit_native_default(self):
        del self.request['https']['consumers'][0]['configuration']['transparent_history']['settings']['cache_bytes']
        with self.assertRaisesRegex(DeployError, 'archive cache memory budget must be explicit'):
            self.make_target()
        self.assertEqual(self.ex.mutations(), [])

    def test_plan_captures_consumers_and_scoped_public_routes_without_mutation(self):
        plan = self.target.plan()['plan']
        self.assertEqual(self.ex.mutations(), [])
        self.assertEqual(len(plan['serving']['https_consumers']), 3)
        self.assertIn('/provider/txid/v1/*', plan['serving']['edge_after'])
        self.assertNotIn('handle /metrics {', plan['serving']['edge_after'])
        for consumer in plan['serving']['https_consumers']:
            self.assertIn('MemoryMax=48G', consumer['unit_text'])
            self.assertIn(' --config ', consumer['unit_text'])
            self.assertNotIn('--control-socket', consumer['unit_text'])

    def test_changing_other_product_policy_is_refused(self):
        self.request['configuration']['enhance']['settings']['sealed_shards'] = 7
        self.target = self.make_target()
        with self.assertRaisesRegex(DeployError, 'only Transparent distribution flags'):
            self.target.plan()
        self.assertEqual(self.ex.mutations(), [])

    def test_moving_a_wallet_listener_or_missing_a_worker_is_refused(self):
        self.request['https']['consumers'][0]['configuration']['transparent_history']['settings']['listen'] = '10.0.0.8:9999'
        with self.assertRaisesRegex(DeployError, 'preserve every existing wallet listener'):
            self.make_target()
        self.request['https']['consumers'].pop()
        with self.assertRaisesRegex(DeployError, 'needs archive, recent'):
            self.make_target()

    def test_prepared_publishing_and_distinct_state_are_required(self):
        self.request['configuration']['transparent_history']['settings']['ship_runtimes'] = False
        with self.assertRaisesRegex(DeployError, 'complete prepared distribution'):
            self.make_target()

    def test_foreign_consumer_changes_prevent_rollback(self):
        parent = {'plan': self.target.plan()['plan']}
        # The normal transaction checks the edge and the unchanged original workers.
        self.target.serving.restore_guard(parent)
        worker = self.request['https']['consumers'][0]
        self.ex.put(worker['host'], '/etc/systemd/system/' + worker['unit'], '[Service]\nExecStart=/bin/foreign\n')
        with self.assertRaisesRegex(DeployError, 'outside the HTTPS transaction'):
            self.target.serving.restore_guard(parent)
        self.assertEqual(self.ex.effects, [])

    def test_consumer_disk_budget_is_checked_before_staging(self):
        self.request['https']['consumers'][0]['budget_bytes'] = 70 << 30
        self.request['https']['consumers'][0]['configuration']['transparent_history']['settings']['bootstrap_max_bytes'] = 70 << 30
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        with self.assertRaisesRegex(DeployError, 'disk headroom'):
            self.target.plan()
        self.assertEqual(self.ex.mutations(), [])

    def test_unfinished_switch_cannot_create_a_second_transaction(self):
        plan = self.target.plan()
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)
        with self.assertRaisesRegex(DeployError, 'unfinished'):
            self.target.plan()

    def test_switch_and_rollback_restore_consumers_and_both_edges(self):
        before = self.target.plan()['plan']
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertEqual(set(journal['production_snapshots']), {'transparent_history', 'transaction_details'})
        for consumer in before['serving']['https_consumers']:
            self.assertEqual(self.ex.probe_unit(consumer['host'], consumer['unit'])['exe_sha256'],
                             self.request['serving']['binary_sha256'])
        edge = before['serving']['history_edge']
        self.assertEqual(self.ex.read(edge['host'], '/etc/caddy/Caddyfile'), edge['after'])
        self.assertNotIn('path_regexp', edge['after'])
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read(edge['host'], '/etc/caddy/Caddyfile'), edge['before'])
        for consumer in before['serving']['https_consumers']:
            self.assertEqual(self.ex.probe_unit(consumer['host'], consumer['unit'])['exe_sha256'],
                             consumer['before']['exe_sha256'])

    def test_lost_worker_stop_reply_is_recoverable(self):
        worker = self.request['https']['consumers'][0]
        self.ex.crash_on = ('stop', worker['unit'])
        with self.assertRaises(RemoteError):
            self.deploy()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit(worker['host'], worker['unit'])['active_state'], 'active')

    def test_rejected_staged_consumer_leaves_workers_untouched(self):
        self.ex.refuse_configuration = True
        with self.assertRaisesRegex(DeployError, 'staged publisher refuses'):
            self.deploy()
        self.target.rollback()
        units = {c['unit'] for c in self.request['https']['consumers']}
        self.assertFalse(any(action[0] == 'stop' and action[1] in units for action in self.ex.effects))

    def test_managed_worker_overlay_is_retired_and_restored(self):
        worker = self.request['https']['consumers'][0]
        path = '/etc/systemd/system/' + worker['unit'] + '.d/' + units.MANAGED_DROP_IN
        text = '[Service]\nExecStart=\nExecStart=/bin/old-transparent --listen 192.0.2.21:8093\n'
        self.ex.put(worker['host'], path, text)
        self.ex.systemctl(worker['host'], 'daemon-reload')
        self.ex.systemctl(worker['host'], 'start', worker['unit'])
        self.deploy()
        self.assertIsNone(self.ex.read(worker['host'], path))
        self.target.rollback()
        self.assertEqual(self.ex.read(worker['host'], path), text)

    def test_recent_starts_only_after_terminal_archive_tls_route(self):
        recent = self.request['https']['consumers'][1]
        edge = self.target.plan()['plan']['serving']['history_edge']
        original = self.ex.systemctl
        def systemctl(host, *args):
            if args == ('start', recent['unit']):
                self.assertEqual(self.ex.read(edge['host'], '/etc/caddy/Caddyfile'), edge['after'])
            return original(host, *args)
        with patch.object(self.ex, 'systemctl', side_effect=systemctl):
            self.assertEqual(self.deploy()['phase'], 'committed')

    def test_unready_consumer_never_commits_and_can_restore_the_fleet(self):
        original = self.ex.http_get
        def get(host, url, timeout=5):
            if ':1830' in url:
                return 503, 'not ready yet'
            return original(host, url, timeout)
        with patch.object(self.ex, 'http_get', side_effect=get):
            with self.assertRaisesRegex(DeployError, 'consumer readiness deadline exceeded'):
                self.deploy()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_collected_production_snapshot_refuses_before_stopping_owners(self):
        root = self.request['configuration']['transparent_history']['settings']['publication_root']
        self.ex.remove('coordinator', root + '/immutable-snapshot/shards.json')
        with self.assertRaisesRegex(DeployError, 'snapshot map is missing'):
            self.deploy()
        result = self.target.rollback()
        self.assertTrue(result['staging_only'])
        self.assertFalse(any(action[0] == 'stop' for action in self.ex.effects))

    def test_lifecycle_ready_publisher_without_a_prepared_view_cannot_start_consumers(self):
        original = self.ex.http_get
        def get(host, url, timeout=5):
            if url.endswith('/current'):
                return 503, '{"error":"preparing"}'
            return original(host, url, timeout)
        with patch.object(self.ex, 'http_get', side_effect=get):
            with self.assertRaisesRegex(DeployError, 'prepared HTTPS publication readiness deadline exceeded'):
                self.deploy()
        units = {c['unit'] for c in self.request['https']['consumers']}
        self.assertFalse(any(action[0] == 'restart' and action[1] in units for action in self.ex.effects))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_reusing_legacy_bytes_does_not_copy_authority_or_follow_symlinks(self):
        source, root = self.root / 'legacy', self.root / 'provider'
        source.mkdir(); root.mkdir()
        (source / 'table.bin').write_bytes(b'immutable table')
        (source / 'active.json').write_bytes(b'old authority')
        os.link(source / 'table.bin', source / 'same-table.bin')
        outside = self.root / 'outside'
        outside.write_bytes(b'outside the legacy tree')
        (source / 'link').symlink_to(outside)
        helper = self.root / 'artifact-helper'
        helper.write_text('#!/usr/bin/env python3\nimport hashlib,json,pathlib,sys\n'
                          'assert sys.argv[1:3] == ["artifact-checksum","--input"]\n'
                          'print(json.dumps({"algorithm":"fixture","digest":hashlib.sha256(pathlib.Path(sys.argv[3]).read_bytes()).hexdigest()}))\n')
        helper.chmod(0o755)
        result = subprocess.run([sys.executable, '-c', publisher_https.SEED_OBJECTS,
            json.dumps({'root': str(root), 'sources': [str(source)], 'hash_command':str(helper)})], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['linked_objects'], 2)
        digest = hashlib.sha256(b'immutable table').hexdigest()
        self.assertEqual((root / 'objects' / 'fixture' / digest).stat().st_ino, (source / 'table.bin').stat().st_ino)
        self.assertFalse((root / 'active.json').exists())
        self.assertFalse((root / 'objects' / 'fixture' / hashlib.sha256(outside.read_bytes()).hexdigest()).exists())


if __name__ == '__main__':
    unittest.main()
