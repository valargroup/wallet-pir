#!/usr/bin/env python3
"""Offline failure tests for continuous publication's fleet activation boundary."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('live_fleet', Path(__file__).parents[1]/'scripts'/'transparent-live-fleet.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FleetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.roster = [dict(id=f'a{i}',role='archive-owner',ssh_host=f'10.0.0.{i}',upstream=f'10.0.0.{i}:8093') for i in (1,2)]
        self.roster += [dict(id=f'r{i}',role='recent-replica',ssh_host=f'10.0.1.{i}',upstream=f'10.0.1.{i}:8093') for i in (1,2)]
        roster = self.root/'roster.json'
        roster.write_text(json.dumps(self.roster))
        self.fleet = module.Fleet(dict(roster=str(roster),state_dir=str(self.root),known_hosts='known',ssh_key='key',
                                     public_host='pir.example',authority_upstream='https://filters.example',router_host='10.0.0.9'))

    def tearDown(self):
        self.tmp.cleanup()

    def test_quorum_requires_all_archive_owners_and_only_one_recent(self):
        self.assertTrue(self.fleet.quorum({'a1','a2','r2'}))
        self.assertFalse(self.fleet.quorum({'a1','r1','r2'}))
        self.assertFalse(self.fleet.quorum({'a1','a2'}))

    async def test_slow_replica_does_not_hold_quorum(self):
        async def prepare(worker):
            if worker['id']=='r1':
                await asyncio.sleep(100)
            return True
        ready = await asyncio.wait_for(self.fleet.collect(prepare,self.roster,early=True),2)
        self.assertEqual(set(ready),{'a1','a2','r2'})

    async def test_reorg_withdraws_routing_before_worker_invalidation(self):
        events=[]
        async def route(workers,assignment=None):
            events.append(('route',workers))
        async def control(worker,value):
            events.append((value['operation'],worker['id']))
            if value['operation']=='status':
                return {'active':{'map_sha256':'old'}}
            return {}
        self.fleet.route=route
        self.fleet.control=control
        result=await self.fleet.invalidate({'from_height':12})
        self.assertTrue(result['ok'])
        self.assertEqual(events[0],('route',[]))
        self.assertEqual(len([e for e in events if e[0]=='invalidate']),4)

    async def test_failed_owner_activation_withdraws_instead_of_publishing(self):
        routed=[]
        async def route(workers,assignment=None):
            routed.append(workers)
        async def control(worker,value):
            if worker['id']=='a2':
                raise RuntimeError('owner lost during activation')
            return {'active':{'map_sha256':'new'},'warm':True}
        self.fleet.route=route
        self.fleet.control=control
        with self.assertRaisesRegex(RuntimeError,'activation quorum'):
            await self.fleet.activate({'map_sha256':'new','prepared':{'workers':{w['id']:{'expected':'old'} for w in self.roster}}})
        self.assertEqual(routed,[[]])

    async def test_routes_exclude_lagging_replicas_and_share_public_authority(self):
        captured=[]
        async def ssh(host,command,data=None,timeout=25):
            captured.append(data.decode())
            return b''
        self.fleet.ssh=ssh
        workers=[w for w in self.roster if w['id']!='r1']
        assignment={'workers':[dict(id=w['id'],shards=[0] if w['role']=='archive-owner' else [1,2]) for w in workers]}
        await self.fleet.route(workers,assignment)
        text=captured[0]
        self.assertNotIn('10.0.1.1:8093',text)
        self.assertIn('10.0.1.2:8093',text)
        self.assertIn('https://filters.example',text)
        self.assertIn('/v1/filters/shards',text)


if __name__=='__main__':
    unittest.main()
