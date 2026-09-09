"""An upgrade may become ready on a newer publication than its predecessor."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
SPEC = importlib.util.spec_from_file_location('deploy', Path(__file__).resolve().parents[1]/'scripts/deploy-transparent-publisher.py')
M = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(M)

class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    async def test_current_binary_and_warm_control_attest_advancing_publication(self):
        unit = '[Service]\nRuntimeDirectory=transparent-pir\nExecStart=/usr/local/bin/transparent-shard-server --shard-dir /set\nMemoryMax=7G\n'
        fleet = AsyncMock()
        fleet.ssh_args = []
        fleet.ssh.return_value = unit.encode()
        fleet.control.return_value = {'active': {'map_sha256': 'new'}, 'warm': True}
        worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        with tempfile.TemporaryDirectory() as directory:
            artifacts = Path(directory)
            (artifacts/'transparent-shard-server').write_bytes(b'binary')
            digest = hashlib.sha256(b'binary').hexdigest()
            with patch.object(M, 'read_json', side_effect=[{'ready':True,'mode':'warm','map_sha256':'old'}, {'ready':True,'binary_sha256':digest,'map_sha256':'new'}]), patch.object(M.LIVE, 'run', new=AsyncMock()):
                await M.install_worker(fleet, worker, artifacts, '/rollback')
        uploaded = next(call.args[2].decode() for call in fleet.ssh.await_args_list if call.args[1].startswith('cat >'))
        self.assertEqual(uploaded.count('RuntimeDirectory=transparent-pir'), 1)
        self.assertEqual(uploaded.count('MemoryHigh=5905580032'), 1)
        verify = next(call for call in fleet.ssh.await_args_list if '--verify-only' in call.args[1])
        self.assertFalse(verify.kwargs['multiplex'])
        fleet.control.assert_awaited_once()

    async def test_roster_build_slots_replace_both_cli_forms_before_verification(self):
        for old in ['--build-slots 1', '--build-slots=1', '']:
            with self.subTest(old=old):
                fleet = AsyncMock()
                fleet.ssh_args = []
                fleet.ssh.return_value = ('[Service]\nExecStart=/worker --shard-dir /set '+old+'\n').encode()
                worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093', build_slots=2)
                with tempfile.TemporaryDirectory() as directory, patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), patch.object(M.LIVE, 'run', new=AsyncMock()):
                    await M.install_worker(fleet, worker, Path(directory), '/rollback', stage_only=True)
                uploaded = next(call.args[2].decode() for call in fleet.ssh.await_args_list if call.args[1].startswith('cat >'))
                self.assertEqual(uploaded.count('--build-slots'), 1)
                self.assertIn('--build-slots 2', uploaded)
                verify = next(call.args[1] for call in fleet.ssh.await_args_list if '--verify-only' in call.args[1])
                self.assertIn('--build-slots 2', verify)
                self.assertFalse(any('systemctl restart' in call.args[1] for call in fleet.ssh.await_args_list))

    async def test_invalid_build_slots_fail_before_staging(self):
        for slots in [0, 100, True, '2']:
            fleet = AsyncMock()
            fleet.ssh.return_value = b'[Service]\nExecStart=/worker\n'
            worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093', build_slots=slots)
            with patch.object(M, 'read_json', return_value={'ready':True,'mode':'warm'}), self.assertRaises(ValueError):
                await M.install_worker(fleet, worker, Path('/unused'), '/rollback', stage_only=True)
            self.assertEqual(fleet.ssh.await_count, 1)
