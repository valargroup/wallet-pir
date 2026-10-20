"""An upgrade may become ready on a newer publication than its predecessor."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
SPEC = importlib.util.spec_from_file_location('deploy', Path(__file__).resolve().parents[1]/'scripts/deploy-transparent-publisher.py')
M = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(M)


def staged(fleet):
    """(unit, history settings, verify command) the install staged on the worker."""
    calls = fleet.ssh.await_args_list
    unit = next(call.args[2].decode() for call in calls if call.args[1].endswith('/worker.service'))
    config = json.loads(next(call.args[2].decode() for call in calls if call.args[1].startswith('cat > ') and call.args[1].endswith('/shard-server.json')))
    verify = next(call for call in calls if call.args[1].endswith('--verify-only'))
    return unit, config['transparent_history']['settings'], verify


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Unit/activation tests substitute the independently tested SSH digest boundary.
        checksum_patch = patch.object(M, 'verify_worker_artifacts', new=AsyncMock())
        checksum_patch.start()
        self.addCleanup(checksum_patch.stop)

    async def test_explicit_runtime_cache_is_preserved_and_partial_cache_rejected(self):
        for flags, valid in [
            ('--runtime-cache-dir=/custom --runtime-cache-max-bytes=1234', True),
            ('--runtime-cache-dir /custom --runtime-cache-max-bytes 1234', True),
            ('--runtime-cache-dir /custom', False),
            ('--runtime-cache-max-bytes=1234', False),
        ]:
            with self.subTest(flags=flags):
                fleet = AsyncMock()
                fleet.c = {}
                fleet.ssh_args = []
                fleet.ssh.return_value = ('[Service]\nExecStart=/worker --shard-dir /set '+flags+'\n').encode()
                worker = dict(id='archive', role='archive-owner', ssh_host='host',
                              upstream='host:8093', cache_bytes=51539607552)
                with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
                    if valid:
                        await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
                        unit, settings, _ = staged(fleet)
                        self.assertIn('\nExecStart=/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json\n', unit)
                        self.assertEqual((settings['runtime_cache_dir'], settings['runtime_cache_max_bytes']), ('/custom', 1234))
                    else:
                        with self.assertRaisesRegex(ValueError, 'incomplete runtime cache'):
                            await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
                        self.assertFalse(any(call.args[1].startswith('cat >') for call in fleet.ssh.await_args_list))

    async def test_legacy_archive_unit_gains_bounded_cache_before_restart(self):
        fleet = AsyncMock()
        fleet.c = {}
        fleet.ssh_args = []
        fleet.ssh.return_value = b'[Service]\nExecStart=/worker --shard-dir /set\n'
        worker = dict(id='archive', role='archive-owner', ssh_host='host',
                      upstream='host:8093', cache_bytes=51539607552)
        with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
            await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
        _, settings, verify = staged(fleet)
        self.assertEqual(settings['runtime_cache_dir'], '/srv/transparent-pir/runtime-cache')
        self.assertEqual(settings['runtime_cache_max_bytes'], 103079215104)
        # The staged server verifies exactly the staged configuration.
        self.assertEqual(verify.args[1], '/opt/transparent-publisher/staged/pir-query-server --config '
                         '/opt/transparent-publisher/staged/shard-server.json --verify-only')
        commands = [call.args[1] for call in fleet.ssh.await_args_list]
        self.assertLess(commands.index(verify.args[1].replace('--verify-only', '--check')), commands.index(verify.args[1]))
        self.assertFalse(any('systemctl restart' in call.args[1] for call in fleet.ssh.await_args_list))

    async def test_current_binary_and_warm_control_attest_advancing_publication(self):
        unit = '[Service]\nRuntimeDirectory=transparent-pir\nExecStart=/usr/local/bin/transparent-shard-server --shard-dir /set\nMemoryMax=7G\n'
        fleet = AsyncMock()
        fleet.c = {}
        fleet.ssh_args = []
        fleet.ssh.return_value = unit.encode()
        fleet.control.return_value = {'active': {'map_sha256': 'new'}, 'warm': True}
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='10.0.0.7:8093')
        with tempfile.TemporaryDirectory() as directory:
            artifacts = Path(directory)
            (artifacts/'pir-query-server').write_bytes(b'binary')
            digest = hashlib.sha256(b'binary').hexdigest()
            with patch.object(M, 'read_json', side_effect=[{'ready':True,'mode':'warm','map_sha256':'old'}, {'ready':True,'binary_sha256':digest,'map_sha256':'new'}]), patch.object(M.LIVE, 'run', new=AsyncMock()):
                await M.install_worker(fleet, worker, artifacts, '/rollback')
        uploaded, settings, verify = staged(fleet)
        self.assertEqual(uploaded.count('RuntimeDirectory=transparent-pir'), 1)
        self.assertEqual(uploaded.count('MemoryHigh=5905580032'), 1)
        self.assertEqual(uploaded.count('ExecStart='), 1)
        # The retired unit's settings, on the worker's own VPC listener.
        self.assertEqual((settings['shard_dir'], settings['listen']), ('/set', '10.0.0.7:8093'))
        self.assertFalse(verify.kwargs['multiplex'])
        install = next(call.args[1] for call in fleet.ssh.await_args_list if 'systemctl restart' in call.args[1])
        # The retired executable and the absent configuration are what a rollback restores.
        self.assertIn('for name in pir-query-server transparent-query-server transparent-shard-server', install)
        self.assertIn('mv /usr/local/bin/pir-query-server.next /usr/local/bin/pir-query-server', install)
        self.assertIn('mv /etc/transparent-pir/shard-server.json.next /etc/transparent-pir/shard-server.json', install)
        fleet.control.assert_awaited_once()

    async def test_configured_worker_is_read_through_its_configuration(self):
        unit = '[Service]\nExecStart=/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json\n'
        current = json.dumps({'management_listen': '127.0.0.1:18190', 'transparent_history': {'enabled': True, 'settings': {
            'listen': '10.0.0.7:8093', 'shard_dir': '/set', 'build_slots': 1, 'prune_excess': True,
            'runtime_cache_dir': '/custom', 'runtime_cache_max_bytes': 1234,
            'control_socket': '/run/transparent-pir/control.sock', 'active_record': '/opt/transparent-publisher/active.json'}}})
        async def ssh(host, command, *args, **kwargs):
            return {'cat /etc/systemd/system/transparent-shard-server.service': unit.encode(),
                    'cat /etc/transparent-pir/shard-server.json': current.encode()}.get(command, b'')
        fleet = AsyncMock()
        fleet.c = {}
        fleet.ssh_args = []
        fleet.ssh.side_effect = ssh
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='10.0.0.7:8093', build_slots=2)
        with patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
            await M.install_worker(fleet, worker, Path('/unused'), '/rollback', stage_only=True)
        _, settings, _ = staged(fleet)
        self.assertEqual(settings, {'listen': '10.0.0.7:8093', 'shard_dir': '/set', 'prune_excess': True,
                                    'runtime_cache_dir': '/custom', 'runtime_cache_max_bytes': 1234,
                                    'control_socket': '/run/transparent-pir/control.sock',
                                    'active_record': '/opt/transparent-publisher/active.json', 'build_slots': 2})

    def test_rollback_restores_or_removes_the_configuration(self):
        command = M.restore_config('/rollback dir')
        self.assertIn("if [ -f '/rollback dir'/shard-server.json ]", command)
        self.assertIn('else rm -f /etc/transparent-pir/shard-server.json; fi', command)

    async def test_roster_build_slots_replace_both_cli_forms_before_verification(self):
        for old in ['--build-slots 1', '--build-slots=1', '']:
            with self.subTest(old=old):
                fleet = AsyncMock()
                fleet.c = {}
                fleet.ssh_args = []
                fleet.ssh.return_value = ('[Service]\nExecStart=/worker --shard-dir /set '+old+'\n').encode()
                worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093', build_slots=2)
                with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
                    await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
                uploaded, settings, _ = staged(fleet)
                self.assertNotIn('--build-slots', uploaded)
                self.assertEqual(settings['build_slots'], 2)
                self.assertFalse(any('systemctl restart' in call.args[1] for call in fleet.ssh.await_args_list))

    async def test_headless_preflight_stages_without_changing_runtime(self):
        fleet = AsyncMock()
        fleet.c = {'headless_console': True}
        fleet.ssh_args = []
        fleet.ssh.return_value = b'[Service]\nExecStart=/worker --shard-dir /set\n'
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
            await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
        calls = fleet.ssh.await_args_list
        self.assertTrue(any(call.args[1].endswith('headless-console.py --preflight') for call in calls))
        unit = next(call.args[2].decode() for call in calls if call.args[1].endswith('/worker.service'))
        self.assertEqual(unit.count(M.HEADLESS_PRESTART), 1)
        self.assertFalse(any('systemctl restart' in call.args[1] or 'install -Dm755' in call.args[1] for call in calls))

    async def test_headless_install_checks_loaded_hook_before_returning_warm(self):
        fleet = AsyncMock()
        fleet.c = {'headless_console': True}
        fleet.ssh_args = []
        helper_sha = hashlib.sha256((M.SCRIPT/'transparent-headless-console.py').read_bytes()).hexdigest()
        async def ssh(host, command, *args, **kwargs):
            if command.startswith('cat /etc/'):
                return b'[Service]\nExecStart=/worker --shard-dir /set\n'
            if command.endswith('--check'):
                return ('{"persistent":true,"helper_sha256":"'+helper_sha+'"}').encode()
            return b''
        fleet.ssh.side_effect = ssh
        fleet.control.return_value = {'active': {'map_sha256': 'new'}, 'warm': True}
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        with tempfile.TemporaryDirectory() as directory:
            artifacts=Path(directory);(artifacts/'pir-query-server').write_bytes(b'binary')
            digest=hashlib.sha256(b'binary').hexdigest()
            with patch.object(M,'read_json',side_effect=[{'ready':True,'mode':'warm'}, {'ready':True,'binary_sha256':digest,'map_sha256':'new'}]), patch.object(M.LIVE,'run',new=AsyncMock()):
                await M.install_worker(fleet,worker,artifacts,'/rollback')
        commands=[call.args[1] for call in fleet.ssh.await_args_list]
        self.assertTrue(commands[-1].endswith('headless-console.py --check'))
        install=next(command for command in commands if 'systemctl restart' in command)
        self.assertLess(install.index('cp '+M.HEADLESS_PATH),install.index('install -Dm755'))

    async def test_storage_preflight_stages_without_changing_runtime(self):
        fleet = AsyncMock()
        fleet.c = {'storage_nodiscard': True}
        fleet.ssh_args = []
        fleet.ssh.return_value = b'[Service]\nExecStart=/worker --shard-dir /set\n'
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
            await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
        calls = fleet.ssh.await_args_list
        self.assertTrue(any(call.args[1].endswith('storage-policy.py --preflight') for call in calls))
        unit = next(call.args[2].decode() for call in calls if call.args[1].endswith('/worker.service'))
        self.assertEqual(unit.count(M.STORAGE_PRESTART), 1)
        self.assertFalse(any('systemctl restart' in call.args[1] or 'install -Dm755' in call.args[1] for call in calls))

    async def test_storage_install_checks_loaded_hook_before_returning_warm(self):
        fleet = AsyncMock()
        fleet.c = {'storage_nodiscard': True}
        fleet.ssh_args = []
        helper_sha = hashlib.sha256((M.SCRIPT/'transparent-storage-policy.py').read_bytes()).hexdigest()
        async def ssh(host, command, *args, **kwargs):
            if command.startswith('cat /etc/'):
                return b'[Service]\nExecStart=/worker --shard-dir /set\n'
            if command.endswith('--check'):
                return ('{"persistent":true,"online_discard":false,"helper_sha256":"'+helper_sha+'"}').encode()
            return b''
        fleet.ssh.side_effect = ssh
        fleet.control.return_value = {'active': {'map_sha256': 'new'}, 'warm': True}
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        with tempfile.TemporaryDirectory() as directory:
            artifacts=Path(directory);(artifacts/'pir-query-server').write_bytes(b'binary')
            digest=hashlib.sha256(b'binary').hexdigest()
            with patch.object(M,'read_json',side_effect=[{'ready':True,'mode':'warm'}, {'ready':True,'binary_sha256':digest,'map_sha256':'new'}]), patch.object(M.LIVE,'run',new=AsyncMock()):
                await M.install_worker(fleet,worker,artifacts,'/rollback')
        commands=[call.args[1] for call in fleet.ssh.await_args_list]
        self.assertTrue(commands[-1].endswith('storage-policy.py --check'))
        install=next(command for command in commands if 'systemctl restart' in command)
        self.assertLess(install.index('cp '+M.STORAGE_PATH),install.index('install -Dm755'))
        self.assertLess(install.index('/restore-storage-policy.py'),install.index('cp /etc/systemd/system/transparent-shard-server.service'))

    async def test_invalid_build_slots_fail_before_staging(self):
        for slots in [0, 100, True, '2']:
            fleet = AsyncMock()
            fleet.c = {}
            fleet.ssh.return_value = b'[Service]\nExecStart=/worker\n'
            worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093', build_slots=slots)
            with patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), self.assertRaises(ValueError):
                await M.install_worker(fleet, worker, Path('/unused'), '/rollback', stage_only=True)
            self.assertEqual(fleet.ssh.await_count, 1)


class SchemaNamespaceTests(unittest.IsolatedAsyncioTestCase):
    CONFIG = {'worker_schema':'transparent-shard-v11',
              'worker_active_record':'/opt/transparent-publisher/v11/active.json',
              'worker_runtime_cache_dir':'/srv/transparent-pir/v11/runtime-cache',
              'worker_root':'/srv/transparent-pir/v11/publications'}

    def test_old_control_record_and_cache_are_replaced_in_both_cli_forms(self):
        for old in [[], ['--control-socket','/run/transparent-pir/control.sock',
                         '--active-record','/opt/transparent-publisher/active.json',
                         '--runtime-cache-dir','/srv/transparent-pir/runtime-cache'],
                    ['--control-socket=/run/transparent-pir/control.sock',
                     '--active-record=/opt/transparent-publisher/active.json',
                     '--runtime-cache-dir=/srv/transparent-pir/runtime-cache']]:
            result = M.worker_control_args(['worker', '--shard-dir','/srv/transparent-pir/v11/publications/candidate',
                '--runtime-cache-max-bytes','1234', *old], self.CONFIG)
            self.assertEqual(M.argument_values(result, '--active-record'), [self.CONFIG['worker_active_record']])
            self.assertEqual(M.argument_values(result, '--runtime-cache-dir'), [self.CONFIG['worker_runtime_cache_dir']])
            self.assertEqual(M.argument_values(result, '--runtime-cache-max-bytes'), ['1234'])
            self.assertNotIn('/opt/transparent-publisher/active.json', result)

    def test_missing_namespace_duplicate_and_partial_control_are_refused(self):
        for key in ('worker_active_record','worker_runtime_cache_dir','worker_root'):
            config = dict(self.CONFIG)
            config.pop(key)
            with self.assertRaises(ValueError):
                M.worker_control_args(['worker'], config)
        for flags in [['--active-record=/old'], ['--control-socket=/run/socket'],
                      ['--active-record=/old','--active-record','/other','--control-socket=/run/socket'],
                      ['--active-record','--control-socket=/run/socket']]:
            with self.assertRaises(ValueError):
                M.worker_control_args(['worker', *flags], self.CONFIG)

    def test_v10_static_publication_is_refused_before_addition_of_v11_control(self):
        for path in ('/srv/transparent-pir/publications/old',
                     '/srv/transparent-pir/v11/publications/../../publications/old','relative'):
            with self.assertRaises(ValueError):
                M.worker_control_args(['worker','--shard-dir',path],self.CONFIG)

    async def test_transfer_corruption_is_rejected_for_either_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            artifacts = Path(directory)
            for name in ('pir-query-server','shard-control'):
                (artifacts/name).write_bytes(name.encode())
            lines = [hashlib.sha256((artifacts/name).read_bytes()).hexdigest()+'  /stage/'+name
                     for name in ('pir-query-server','shard-control')]
            fleet = AsyncMock()
            fleet.ssh.return_value = ('\n'.join(lines)+'\n').encode()
            await M.verify_worker_artifacts(fleet,'host',artifacts,'/stage')
            for index in (0,1):
                corrupted = list(lines)
                corrupted[index] = '0'*64+corrupted[index][64:]
                fleet.ssh.return_value = ('\n'.join(corrupted)+'\n').encode()
                with self.assertRaisesRegex(RuntimeError, 'checksum mismatch'):
                    await M.verify_worker_artifacts(fleet,'host',artifacts,'/stage')
