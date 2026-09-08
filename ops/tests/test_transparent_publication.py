#!/usr/bin/env python3
"""Offline failure tests for continuous publication's fleet activation boundary."""
import asyncio
import importlib.util
import io
import json
import os
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location('live_fleet', Path(__file__).parents[1]/'scripts'/'transparent-live-fleet.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
deploy_spec = importlib.util.spec_from_file_location('publisher_deploy', Path(__file__).parents[1]/'scripts'/'deploy-transparent-publisher.py')
deploy = importlib.util.module_from_spec(deploy_spec)
deploy_spec.loader.exec_module(deploy)


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

    async def test_regressed_tip_errors_are_absent_canonical_endpoints(self):
        cookie=self.root/'test-cookie'
        cookie.write_text('synthetic:fixture')
        self.fleet.c['rpc_cookie']=str(cookie)
        body=json.dumps({'error':{'code':-1,'message':'Provided index is greater than the current tip'}}).encode()
        with patch('urllib.request.urlopen',return_value=io.BytesIO(body)):
            self.assertIsNone(await self.fleet.canonical_hash(101))
        body=json.dumps({'error':{'code':-8,'message':'Block height out of range'}}).encode()
        error=urllib.error.HTTPError('http://fixture',500,'RPC error',{},io.BytesIO(body))
        with patch('urllib.request.urlopen',side_effect=error):
            self.assertIsNone(await self.fleet.canonical_hash(102))

    async def test_cancelled_replica_preserves_shared_canonical_lookup(self):
        pending=asyncio.create_task(asyncio.sleep(.05,result='canonical'))
        self.fleet.canonical[100]=pending
        replica=asyncio.create_task(self.fleet.canonical_hash(100))
        await asyncio.sleep(0)
        replica.cancel()
        await asyncio.gather(replica,return_exceptions=True)
        self.assertEqual(await self.fleet.canonical_hash(100),'canonical')

    async def test_control_preserves_a_rejection_returned_with_exit_status_one(self):
        binary=self.root/'shard-control'
        binary.write_text('#!/bin/sh\nprintf \'%s\\n\' \'{"ok":false,"error":"candidate map digest mismatch"}\'\nexit 1\n')
        binary.chmod(0o700)
        self.fleet.c['control_binary']=str(binary)
        async def local_ssh(host,command,data=None):
            return await module.run(['sh','-c',command],data)
        self.fleet.ssh=local_ssh
        with self.assertRaisesRegex(RuntimeError,'candidate map digest mismatch'):
            await self.fleet.control({'id':'fixture','ssh_host':'unused'}, {'operation':'status'})

    @unittest.skipUnless(shutil.which('ssh-keygen'),'requires OpenSSH parser')
    def test_secret_without_final_newline_is_an_openssh_key_file(self):
        source=self.root/'generated-key'
        subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(source)],check=True)
        target=self.root/'installed-key'
        deploy.secret_file(target,source.read_text().rstrip('\n'))
        subprocess.run(['ssh-keygen','-y','-f',str(target)],check=True,stdout=subprocess.DEVNULL)
        self.assertEqual(target.stat().st_mode & 0o777,0o600)

    async def test_failed_router_backup_is_completed_on_retry(self):
        saved=self.root/'backup'
        coordinator=self.root/'Caddyfile'
        coordinator.write_text('original coordinator')
        (self.root/'controller.json').write_text('{}')
        async def fail(*args,**kwargs):
            raise RuntimeError('SSH unavailable')
        self.fleet.ssh=fail
        with patch.object(deploy,'ROOT',self.root):
            with self.assertRaisesRegex(RuntimeError,'SSH unavailable'):
                await deploy.save_baseline(self.fleet,saved,coordinator)
            coordinator.write_text('later coordinator')
            async def succeed(*args,**kwargs):
                return b'original router'
            self.fleet.ssh=succeed
            await deploy.save_baseline(self.fleet,saved,coordinator)
        self.assertEqual((saved/'Caddyfile.coordinator').read_text(),'original coordinator')
        self.assertEqual((saved/'Caddyfile.router').read_bytes(),b'original router')
        self.assertEqual((saved/'controller.json').read_text(),'{}')

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

    async def test_prepare_uses_cancellable_direct_ssh_while_status_reuses_sessions(self):
        commands = []
        async def capture(args, data=None, timeout=25):
            commands.append(args)
            return b'{"ok":true,"result":{}}'
        with patch.object(module, 'run', capture):
            await self.fleet.control(self.roster[0], {'operation':'prepare'})
            await self.fleet.control(self.roster[0], {'operation':'status'})
        self.assertIn('-oControlPath=none', commands[0])
        self.assertNotIn('-oControlMaster=auto', commands[0])
        self.assertIn('-oControlMaster=auto', commands[1])
        self.assertNotIn('-oControlPath=none', commands[1])

    def test_ssh_reuse_socket_directory_is_private(self):
        self.assertEqual((self.root/'ssh').stat().st_mode & 0o777, 0o700)

    async def test_unchanged_route_skips_reload_but_withdrawal_still_reloads(self):
        target = self.root/'Caddyfile'
        target.write_text('previous route')
        self.fleet.c['router_file'] = str(target)
        binaries = self.root/'bin'
        binaries.mkdir()
        calls = self.root/'calls'
        for name in ['caddy', 'systemctl']:
            binary = binaries/name
            binary.write_text('#!/bin/sh\nprintf "%s\\n" "'+name+'" >> "$CALLS"\n')
            binary.chmod(0o700)
        async def local_ssh(host, command, data=None, timeout=25):
            return await module.run(['sh', '-c', command], data)
        self.fleet.ssh = local_ssh
        assignment = {'workers':[dict(id=w['id'],shards=[i]) for i,w in enumerate(self.roster)]}
        with patch.dict(os.environ, PATH=str(binaries)+os.pathsep+os.environ['PATH'], CALLS=str(calls)):
            await self.fleet.route(self.roster, assignment)
            published = target.read_bytes()
            await self.fleet.route(self.roster, assignment)
            self.assertEqual(calls.read_text().splitlines(), ['caddy', 'systemctl'])
            self.assertEqual(Path(str(target)+'.live-previous').read_text(), 'previous route')
            # Simulate interrupted activation: the intended file is already in
            # place, but successful reload was never acknowledged.
            Path(str(target)+'.live-applied.sha256').unlink()
            await self.fleet.route(self.roster, assignment)
            self.assertEqual(calls.read_text().splitlines(), ['caddy', 'systemctl']*2)
            await self.fleet.route([])
        self.assertEqual(calls.read_text().splitlines(), ['caddy', 'systemctl']*3)
        self.assertIn('503', target.read_text())
        self.assertEqual(Path(str(target)+'.live-previous').read_bytes(), published)

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
