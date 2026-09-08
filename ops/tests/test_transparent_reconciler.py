"""Catch-up must never restore routing after withdrawal or a newer publication."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

SPEC = importlib.util.spec_from_file_location('live', Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
LIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LIVE)

class ReconcilerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        roster = [{'id':'owner','role':'archive-owner','ssh_host':'owner','upstream':'owner:1'},
                  {'id':'fast','role':'recent-replica','ssh_host':'fast','upstream':'fast:1'},
                  {'id':'slow','role':'recent-replica','ssh_host':'slow','upstream':'slow:1'}]
        (self.root/'roster.json').write_text(json.dumps(roster))
        self.fleet = LIVE.Fleet(dict(roster=str(self.root/'roster.json'), state_dir=str(self.root), known_hosts='unused', ssh_key='unused'))
        self.assignment = self.root/'assignment.json'
        self.assignment.write_text('{}')
        LIVE.atomic_json(self.root/'active.json', dict(map_sha256='a'*64, workers=['owner','fast'], assignment=str(self.assignment)))
        LIVE.atomic_json(self.root/'desired.json', dict(map_sha256='a'*64, directory=str(self.root)))
        LIVE.atomic_json(self.root/('a'*64+'.request.json'), dict(map_sha256='a'*64, directory=str(self.root)))
        (self.root/'shards.json').write_text(json.dumps({'shards':[{'end_height':1,'terminal_block_hash':'canonical'}]}))
        self.fleet.canonical_hash = AsyncMock(return_value='canonical')
        self.fleet.stage = AsyncMock(return_value={'expected':'old'})
        self.fleet.control = AsyncMock(return_value={'active':{'map_sha256':'a'*64},'warm':True})
        self.fleet.route = AsyncMock()

    async def test_slow_replica_joins_without_delaying_quorum(self):
        await self.fleet.reconcile()
        self.assertEqual(self.fleet.stage.await_count, 1)
        self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'], ['fast','owner','slow'])
        self.fleet.route.assert_awaited_once()
        await self.fleet.reconcile()
        self.assertEqual(self.fleet.stage.await_count, 1)

    async def test_withdrawal_during_prepare_prevents_activation(self):
        async def stage(*args):
            LIVE.atomic_json(self.root/'withdrawn.json', {'withdrawn':True})
            return {'expected':'old'}
        self.fleet.stage.side_effect = stage
        await self.fleet.reconcile()
        self.fleet.control.assert_not_awaited()
        self.fleet.route.assert_not_awaited()

    async def test_new_candidate_does_not_starve_current_publication_catch_up(self):
        async def stage(*args):
            LIVE.atomic_json(self.root/'desired.json', {'map_sha256':'b'*64})
            return {'expected':'old'}
        self.fleet.stage.side_effect = stage
        await self.fleet.reconcile()
        self.fleet.route.assert_awaited_once()
        await self.fleet.reconcile()
        self.assertEqual(self.fleet.stage.await_count, 1)

    async def test_new_publication_rejects_late_completion(self):
        async def stage(*args):
            LIVE.atomic_json(self.root/'active.json', dict(map_sha256='b'*64, workers=['owner','fast'], assignment=str(self.assignment)))
            LIVE.atomic_json(self.root/'desired.json', dict(map_sha256='b'*64, directory=str(self.root)))
            return {'expected':'old'}
        self.fleet.stage.side_effect = stage
        await self.fleet.reconcile()
        self.fleet.control.assert_not_awaited()
        self.fleet.route.assert_not_awaited()

    async def test_nonwarm_worker_is_never_routed(self):
        self.fleet.control.return_value = {'active':{'map_sha256':'a'*64}, 'warm':False}
        await self.fleet.reconcile()
        self.fleet.route.assert_not_awaited()

    async def test_reorg_before_controller_withdrawal_prevents_rejoin(self):
        self.fleet.canonical_hash.return_value = 'forked'
        await self.fleet.reconcile()
        self.fleet.control.assert_not_awaited()
        self.fleet.route.assert_not_awaited()

    async def test_canary_scope_does_not_prepare_other_replicas(self):
        self.fleet.c['reconcile_workers'] = ['fast']
        await self.fleet.reconcile()
        self.fleet.stage.assert_not_awaited()

    async def test_worker_lock_excludes_competing_preparation(self):
        async with self.fleet.lock('worker-slow'):
            with self.assertRaises(RuntimeError):
                async with self.fleet.lock('worker-slow', wait=False):
                    self.fail('entered twice')
        async with self.fleet.lock('worker-slow', wait=False):
            pass

if __name__ == '__main__':
    unittest.main()
