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

    async def test_withdrawal_and_recovery_remain_visible_between_observer_polls(self):
        async def ssh(*args, **kwargs):
            return b''
        self.fleet.ssh = ssh
        assignment = {'workers':[dict(id=w['id'],shards=[i]) for i,w in enumerate(self.roster)]}
        await self.fleet.route(self.roster, assignment)
        baseline = self.fleet.routing_availability()
        await self.fleet.route([])
        await self.fleet.route(self.roster, assignment)
        recovered = self.fleet.routing_availability()
        self.assertTrue(recovered['available'])
        self.assertEqual(recovered['epoch'], baseline['epoch'])
        self.assertEqual(recovered['unavailable_events'], baseline['unavailable_events']+1)
        async def interrupted(*args, **kwargs):
            raise asyncio.CancelledError()
        self.fleet.ssh = interrupted
        with self.assertRaises(asyncio.CancelledError):
            await self.fleet.route([])
        self.assertFalse(self.fleet.routing_availability()['available'])
        self.assertEqual(self.fleet.routing_availability()['unavailable_events'], 2)

    def test_publisher_sandbox_uses_selected_v3_volume_paths(self):
        journal = Path('/srv/transparent-activity/full-v3/journal')
        publication = Path('/srv/transparent-activity/full-v11/publications')
        initial = publication/'initial set'
        service = deploy.publisher_service(journal, publication, initial)
        writable = next(line for line in service.splitlines() if line.startswith('ReadWritePaths='))
        import shlex
        self.assertEqual(shlex.split(writable.removeprefix('ReadWritePaths=')),
                         [str(journal), str(publication), str(initial), str(deploy.ROOT/'state')])
        self.assertIn('ProtectSystem=strict', service)
        self.assertNotIn('/srv/zakura/transparent-event-data-v2', service)
        self.assertNotIn('/srv/zakura/transparent-publications', service)

    def test_publisher_sandbox_rejects_relative_root_and_unit_injection(self):
        for path in ('relative', '/', '/srv/%i', '/srv/bad\nReadWritePaths=/'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                deploy.publisher_service(Path(path), Path('/srv/publications'), Path('/srv/initial'))

    def test_v11_publisher_uses_separate_config_and_state(self):
        service = deploy.publisher_service(Path('/srv/transparent-activity/full-v3/journal'),
            Path('/srv/transparent-activity/full-v11/publications'), Path('/srv/transparent-activity/full-v11/publications/initial'),
            state_dir=Path('/opt/transparent-publisher/v11/state'), config_path=Path('/opt/transparent-publisher/v11/controller.json'))
        self.assertIn('--config "/opt/transparent-publisher/v11/controller.json"', service)
        writable = next(line for line in service.splitlines() if line.startswith('ReadWritePaths='))
        self.assertIn('"/opt/transparent-publisher/v11/state"', writable)
        self.assertNotIn('controller.json', writable)
        self.assertNotIn('"/opt/transparent-publisher/state"', writable)
        with self.assertRaisesRegex(ValueError, 'config cannot'):
            deploy.publisher_service(Path('/journal'),Path('/publications'),Path('/initial'),config_path=Path('/journal'))

    async def test_read_only_fleet_preflight_does_not_create_owned_sessions(self):
        config = dict(self.fleet.c, state_dir=str(self.root/'absent'), control_sessions=True, status_socket_forwarding=True)
        fleet = module.Fleet(config, read_only=True)
        self.assertFalse((self.root/'absent').exists())
        self.assertFalse(fleet.c['control_sessions'])
        with self.assertRaisesRegex(RuntimeError, 'read-only'):
            async with fleet.lock('writer'):
                self.fail('writer lock acquired')
        with self.assertRaisesRegex(RuntimeError, 'read-only'):
            await fleet.control(self.roster[0], {'operation':'activate'})
        with self.assertRaisesRegex(ValueError, 'requires owned'):
            module.Fleet(dict(config, control_sessions=False), read_only=True)

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
        async def local_ssh(host,command,data=None,**options):
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

    async def test_unpublished_reorg_retains_only_verified_served_coverage(self):
        directory = self.root/'publication'
        directory.mkdir()
        (directory/'shards.json').write_text(json.dumps({'shards':[
            {'end_height':11,'terminal_block_hash':'canonical'}]}))
        module.atomic_json(self.root/'active.json', {'map_sha256':'old',
            'workers':[w['id'] for w in self.roster]})
        module.atomic_json(self.root/'old.request.json',
            {'map_sha256':'old','directory':str(directory)})
        events=[]
        canonical='canonical'
        failed=None
        async def lookup(height):
            self.assertEqual(height,11)
            return canonical
        async def route(workers,assignment=None):
            events.append(('route',workers))
        async def control(worker,value):
            events.append((value['operation'],worker['id']))
            if value['operation']=='status':
                return {'active':{'map_sha256':'old'},'warm':True,'revisions':[
                    {'digest':'served','end_height':11,'terminal_block_hash':'canonical'}]}
            self.assertEqual(value['keep_digests'],['served'] if canonical=='canonical' else [])
            if worker['id']==failed:
                raise RuntimeError('revocation failed')
            return {}
        self.fleet.canonical_hash=lookup
        self.fleet.route=route
        self.fleet.control=control
        req={'from_height':12,'retain_publication':
            {'map_sha256':'old','height':11,'hash':'canonical'}}
        result=await self.fleet.invalidate(req)
        self.assertTrue(result['retained_publication'])
        self.assertFalse(any(e[0]=='route' for e in events))
        self.assertEqual(len([e for e in events if e[0]=='invalidate']),4)
        # A routed owner's failed revocation fails closed, even though its
        # previous status was warm and canonical.
        events.clear()
        failed='a2'
        result=await self.fleet.invalidate(req)
        self.assertFalse(result['retained_publication'])
        self.assertIn(('route',[]),events)
        failed=None
        module.atomic_json(self.root/'withdrawn.json', {'withdrawn':False})
        # A fork reaching served coverage must withdraw before revocation.
        events.clear()
        canonical='replacement'
        result=await self.fleet.invalidate(req)
        self.assertFalse(result['retained_publication'])
        first_revoke=next(i for i,e in enumerate(events) if e[0]=='invalidate')
        self.assertLess(events.index(('route',[])),first_revoke)
        # A completed activation while invalidation waited for the routing
        # lock invalidates the controller's older retention hint.
        module.atomic_json(self.root/'withdrawn.json', {'withdrawn':False})
        module.atomic_json(self.root/'active.json', {'map_sha256':'new',
            'workers':[w['id'] for w in self.roster]})
        events.clear()
        result=await self.fleet.invalidate(req)
        self.assertFalse(result['retained_publication'])
        self.assertEqual(events[0],('route',[]))

    async def test_control_does_not_share_transfer_session_lifetime(self):
        commands = []
        async def capture(args, data=None, timeout=25):
            commands.append(args)
            return b'{"ok":true,"result":{}}'
        with patch.object(module, 'run', capture):
            await self.fleet.control(self.roster[0], {'operation':'prepare'})
            await self.fleet.control(self.roster[0], {'operation':'status'})
            await self.fleet.control(self.roster[0], {'operation':'activate'})
        self.assertIn('-oControlPath=none', commands[0])
        self.assertNotIn('-oControlMaster=auto', commands[0])
        for command in commands:
            self.assertNotIn('-oControlMaster=auto', command)
            self.assertIn('-oControlPath=none', command)

    async def test_ambiguous_mutation_and_semantic_status_errors_are_not_retried(self):
        from unittest.mock import AsyncMock
        worker=self.roster[0]
        for operation in ('activate','invalidate','prepare'):
            self.fleet.ssh=AsyncMock(side_effect=RuntimeError('connection lost after dispatch'))
            with self.assertRaises(RuntimeError):
                await self.fleet.control(worker,{'operation':operation})
            self.assertEqual(self.fleet.ssh.await_count,1)
        for reply in (b'{"ok":false,"error":"rejected"}',b'invalid JSON'):
            self.fleet.ssh=AsyncMock(return_value=reply)
            with self.assertRaises((RuntimeError,ValueError)):
                await self.fleet.control(worker,{'operation':'status'})
            self.assertEqual(self.fleet.ssh.await_count,1)

    async def test_cancelled_status_does_not_start_a_retry(self):
        calls=[];started=asyncio.Event()
        async def wait(*args,**kwargs):
            calls.append(kwargs);started.set()
            await asyncio.Event().wait()
        self.fleet.ssh=wait
        task=asyncio.create_task(self.fleet.control(self.roster[0],{'operation':'status'}))
        await started.wait();task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(len(calls),1)

    @unittest.skipUnless(hasattr(os,'fork'),'requires Unix descriptor inheritance')
    async def test_cancelled_channel_does_not_wait_for_a_descendant_output_pipe(self):
        import sys
        started=self.root/'descendant-started';release=self.root/'descendant-release'
        code="""import os,time,sys
from pathlib import Path
if os.fork()==0:
 Path(sys.argv[1]).touch()
 while not Path(sys.argv[2]).exists():time.sleep(.01)
 os._exit(0)
time.sleep(10)
"""
        task=asyncio.create_task(module.run([sys.executable,'-c',code,str(started),str(release)],file_output=True))
        try:
            async def ready():
                while not started.exists():await asyncio.sleep(.01)
            await asyncio.wait_for(ready(),3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,1)
        finally:
            release.touch()
            await asyncio.gather(task,return_exceptions=True)

    async def test_enabled_control_requires_owned_session_but_prepare_stays_direct(self):
        from unittest.mock import AsyncMock
        self.fleet.c['control_sessions']=True
        self.fleet.ssh=AsyncMock(return_value=b'{"ok":true,"result":{}}')
        with patch.object(module,'run',AsyncMock(return_value=b'{"ok":true,"result":{}}')) as run:
            await self.fleet.control(self.roster[0],{'operation':'status'})
            await self.fleet.control(self.roster[0],{'operation':'activate'})
            for call in run.await_args_list:
                self.assertTrue(call.kwargs['file_output'])
                self.assertIn('-oProxyCommand=false',call.args[0])
                self.assertIn('-oControlMaster=no',call.args[0])
            self.fleet.ssh.assert_not_awaited()
            await self.fleet.control(self.roster[0],{'operation':'prepare'})
            self.assertEqual(self.fleet.ssh.await_count,1)
            self.assertFalse(self.fleet.ssh.await_args.kwargs['multiplex'])
        self.fleet.ssh.reset_mock()
        with patch.object(module,'run',AsyncMock(side_effect=RuntimeError('session unavailable'))) as run:
            with self.assertRaises(RuntimeError):await self.fleet.control(self.roster[0],{'operation':'status'})
            self.assertEqual(run.await_count,2)
            self.fleet.ssh.assert_not_awaited()
            run.reset_mock()
            with self.assertRaises(RuntimeError):await self.fleet.control(self.roster[0],{'operation':'activate'})
            self.assertEqual(run.await_count,1)

    async def test_forwarded_status_uses_private_stream_without_exec_fallback(self):
        from unittest.mock import AsyncMock
        self.fleet.c.update(control_sessions=True,status_socket_forwarding=True)
        received=[]
        async def accept(reader,writer):
            received.append(json.loads(await reader.readline()))
            writer.write(b'{"ok":true,"result":{"warm":true}}\n');await writer.drain()
            writer.close();await writer.wait_closed()
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            server=await asyncio.start_unix_server(accept,path=self.fleet.status_forward_path(self.roster[0]))
            try:
                with patch.object(module,'run',AsyncMock(side_effect=AssertionError('must not exec'))) as run:
                    results=await asyncio.gather(*(self.fleet.control(self.roster[0],{'operation':'status'}) for _ in range(2)))
                    self.assertEqual(results,[{'warm':True}]*2)
                    self.assertEqual(received,[{'operation':'status'}]*2)
                    run.assert_not_awaited()
                with patch.object(module,'run',AsyncMock(return_value=b'{"ok":true,"result":{}}')) as run:
                    await self.fleet.control(self.roster[0],{'operation':'activate'})
                    self.assertEqual(run.await_count,1)
            finally:server.close();await server.wait_closed()

    async def test_forwarded_status_cancellation_closes_only_its_stream(self):
        self.fleet.c.update(control_sessions=True,status_socket_forwarding=True)
        started=asyncio.Event();closed=asyncio.Event();calls=[]
        async def accept(reader,writer):
            calls.append(await reader.readline())
            if len(calls)==1:
                started.set();await reader.read();closed.set()
            else:
                writer.write(b'{"ok":true,"result":{"warm":true}}\n');await writer.drain()
            writer.close();await writer.wait_closed()
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            server=await asyncio.start_unix_server(accept,path=self.fleet.status_forward_path(self.roster[0]))
            try:
                task=asyncio.create_task(self.fleet.control(self.roster[0],{'operation':'status'}))
                await asyncio.wait_for(started.wait(),1);task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
                await asyncio.wait_for(closed.wait(),1)
                self.assertEqual(await self.fleet.control(self.roster[0],{'operation':'status'}),{'warm':True})
                self.assertEqual(len(calls),2)
            finally:server.close();await server.wait_closed()

    async def test_forwarded_status_bounds_frames_and_preserves_rejections(self):
        from unittest.mock import AsyncMock
        self.fleet.c.update(control_sessions=True,status_socket_forwarding=True)
        for response,error,attempts in [(b'{}',RuntimeError,2),(b'x'*(1024*1024+1)+b'\n',ValueError,1),
                                        (b'{"ok":false,"error":"rejected"}\n',RuntimeError,1)]:
            with self.subTest(response_bytes=len(response)), tempfile.TemporaryDirectory(dir='/tmp') as directory:
                self.fleet.control_dir=Path(directory);calls=[]
                async def accept(reader,writer):
                    calls.append(await reader.readline())
                    try:writer.write(response);await writer.drain()
                    except ConnectionError:pass
                    finally:
                        writer.close()
                        try:await writer.wait_closed()
                        except ConnectionError:pass
                server=await asyncio.start_unix_server(accept,path=self.fleet.status_forward_path(self.roster[0]))
                try:
                    with patch.object(module,'run',AsyncMock(side_effect=AssertionError('must not fall back'))) as run:
                        with self.assertRaises(error):await self.fleet.control(self.roster[0],{'operation':'status'})
                        self.assertEqual(len(calls),attempts);run.assert_not_awaited()
                finally:server.close();await server.wait_closed()

    async def test_forwarded_status_keeps_existing_timeout_budget(self):
        from unittest.mock import AsyncMock
        self.fleet.c.update(control_sessions=True,status_socket_forwarding=True)
        calls=[];closed=[]
        async def accept(reader,writer):
            calls.append(await reader.readline());await reader.read();closed.append(True)
            writer.close();await writer.wait_closed()
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            server=await asyncio.start_unix_server(accept,path=self.fleet.status_forward_path(self.roster[0]))
            try:
                started=asyncio.get_running_loop().time()
                with patch.object(module,'run',AsyncMock(side_effect=AssertionError('must not fall back'))) as run:
                    with self.assertRaises(TimeoutError):await self.fleet.control(self.roster[0],{'operation':'status'})
                    elapsed=asyncio.get_running_loop().time()-started
                    self.assertGreaterEqual(elapsed,2.4);self.assertLess(elapsed,3)
                    self.assertEqual(len(calls),2);run.assert_not_awaited()
                async def drained():
                    while len(closed)<2:await asyncio.sleep(.01)
                await asyncio.wait_for(drained(),1)
            finally:server.close();await server.wait_closed()

    async def test_forward_supervisor_recovers_stale_socket_and_preserves_unexpected_paths(self):
        import socket
        self.fleet.c.update(control_sessions=True,status_socket_forwarding=True)
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            path=self.fleet.status_forward_path(self.roster[0])
            listener=socket.socket(socket.AF_UNIX);listener.bind(str(path));listener.close()
            async def spawn(*args,**kwargs):
                self.assertFalse(path.exists())
                self.assertIn('-oStreamLocalBindMask=0177',args)
                self.assertIn('-oExitOnForwardFailure=yes',args)
                self.assertIn(str(path)+':/run/transparent-pir/control.sock',args)
                raise RuntimeError('stop after stale forward recovery')
            with patch.object(module.asyncio,'create_subprocess_exec',spawn):
                with self.assertRaisesRegex(RuntimeError,'stale forward recovery'):
                    await self.fleet.control_session(self.roster[0])
            path.write_text('preserve')
            with self.assertRaisesRegex(RuntimeError,'not an owned socket'):
                await self.fleet.control_session(self.roster[0])
            self.assertEqual(path.read_text(),'preserve');path.unlink()
            async def accept(reader,writer):writer.close();await writer.wait_closed()
            server=await asyncio.start_unix_server(accept,path=path)
            try:
                with self.assertRaisesRegex(RuntimeError,'still listening'):
                    await self.fleet.control_session(self.roster[0])
                self.assertTrue(path.exists())
            finally:server.close();await server.wait_closed()

    def test_control_socket_identity_tracks_authenticated_destination(self):
        worker=self.roster[0]
        before=self.fleet.control_path(worker)
        self.assertNotEqual(before,self.fleet.control_path({**worker,'ssh_host':'other'}))
        self.fleet.c['known_hosts']='other-known-hosts'
        self.assertNotEqual(before,self.fleet.control_path(worker))
        self.assertEqual(before.parent,self.root/'ssh')

    async def test_supervisor_restarts_master_and_cancellation_reaps_owned_process(self):
        class Process:
            def __init__(self, code=None):
                self.pid=123;self.returncode=code;self.done=asyncio.Event();self.terminated=False
                if code is not None:self.done.set()
            async def wait(self):await self.done.wait();return self.returncode
            def terminate(self):self.terminated=True;self.returncode=0;self.done.set()
        first=Process(255);second=Process();started=asyncio.Event();calls=[]
        async def spawn(*args,**kwargs):
            calls.append((args,kwargs))
            if len(calls)==1:return first
            started.set();return second
        with patch.object(module.asyncio,'create_subprocess_exec',spawn):
            task=asyncio.create_task(self.fleet.control_session(self.roster[0]))
            await asyncio.wait_for(started.wait(),3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(second.terminated)
        self.assertEqual(len(calls),2)
        for args,kwargs in calls:
            self.assertIn('-N',args);self.assertNotIn('-f',args);self.assertNotIn('-M',args)
            self.assertIn('-oControlMaster=yes',args)
            self.assertIn('-oControlPersist=no',args)
            self.assertEqual(kwargs['stdin'],asyncio.subprocess.DEVNULL)
            self.assertEqual(kwargs['stdout'],asyncio.subprocess.DEVNULL)

    async def test_dead_socket_is_recovered_but_ordinary_file_is_preserved(self):
        import socket
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            path=self.fleet.control_path(self.roster[0])
            listener=socket.socket(socket.AF_UNIX);listener.bind(str(path));listener.close()
            async def spawn(*args,**kwargs):
                self.assertFalse(path.exists())
                raise RuntimeError('stop after stale socket recovery')
            with patch.object(module,'run',AsyncMock(side_effect=RuntimeError('dead master'))), patch.object(module.asyncio,'create_subprocess_exec',spawn):
                with self.assertRaisesRegex(RuntimeError,'stale socket recovery'):
                    await self.fleet.control_session(self.roster[0])
            path.write_text('preserve')
            with self.assertRaisesRegex(RuntimeError,'not a socket'):
                await self.fleet.control_session(self.roster[0])
            self.assertEqual(path.read_text(),'preserve')

    async def test_live_predecessor_is_not_unlinked_when_exit_fails(self):
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            self.fleet.control_dir=Path(directory)
            path=self.fleet.control_path(self.roster[0])
            async def accept(reader,writer):writer.close();await writer.wait_closed()
            server=await asyncio.start_unix_server(accept,path=path)
            try:
                with patch.object(module,'run',AsyncMock(side_effect=RuntimeError('exit failed'))):
                    with self.assertRaisesRegex(RuntimeError,'could not be stopped'):
                        await self.fleet.control_session(self.roster[0])
                self.assertTrue(path.exists())
            finally:
                server.close();await server.wait_closed()

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

    async def test_live_router_uses_the_saturation_tolerant_health_policy(self):
        captured=[]
        async def ssh(host,command,data=None,timeout=25):
            captured.append(data.decode())
            return b''
        self.fleet.ssh=ssh
        self.fleet.c['internal_listen']='10.0.0.9:8080'
        assignment={'workers':[dict(id=w['id'],shards=[0] if w['role']=='archive-owner' else [1,2]) for w in self.roster]}
        await self.fleet.route(self.roster,assignment)
        text=captured[0]
        # Recent pool plus one per archive owner, on both sites.
        self.assertEqual(text.count(module.ROUTER_HEALTH), 2*3)
        self.assertEqual(text.count(module.ROUTER_PROXY_ERRORS.strip('\n')), 2)
        self.assertNotIn('fail_duration 30s', text)
        # The same policy as shard-assign's golden router: the live publisher
        # re-renders the production router on every activation.
        golden=(Path(__file__).parents[1]/'fixtures'/'transparent-shard'/'Caddyfile.router').read_text()
        self.assertIn(module.ROUTER_HEALTH, golden)
        self.assertIn(module.ROUTER_PROXY_ERRORS, golden)
        caddy=shutil.which('caddy')
        if caddy:
            path=self.root/'Caddyfile.live'
            path.write_text(text)
            result=subprocess.run([caddy,'adapt','--adapter','caddyfile','--config',str(path)],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            config=json.loads(result.stdout)
            routes=json.dumps(config)
            self.assertIn('"max_fails": 3'.replace(' ',''), routes.replace(' ',''))


if __name__=='__main__':
    unittest.main()
