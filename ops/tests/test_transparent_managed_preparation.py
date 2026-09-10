"""Persistent preparation survives quorum cancellation and coalesces bursts."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock
SPEC=importlib.util.spec_from_file_location('fleet',Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
M=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(M)

class ManagedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.worker=dict(id='recent',role='recent-replica',ssh_host='host',upstream='host:8093')
        self.owner=dict(id='owner',role='archive-owner',ssh_host='owner',upstream='owner:8093')
        (self.root/'roster').write_text(json.dumps([self.worker,self.owner]))
        self.f=M.Fleet(dict(roster=str(self.root/'roster'),state_dir=str(self.root),known_hosts='unused',ssh_key='unused',managed_recent_workers=['recent']))
        self.assignment=self.root/'assignment';self.assignment.write_text('{}')
        self.status={'active':{'map_sha256':'a'*64},'warm':True,'invalidated':False,'candidate':None,'preparing':None}
        self.f.control=AsyncMock(side_effect=self.control)
        self.f.route=AsyncMock();self.f.canonical_hash=AsyncMock(return_value='canonical')
        self.active('a')
        self.desired('a')
        M.atomic_json(self.root/'reconciler-heartbeat.json',dict(monotonic=time.monotonic(),workers=['recent']))

    def request(self, tag):
        directory=self.root/tag;directory.mkdir(exist_ok=True)
        (directory/'shards.json').write_text(json.dumps({'shards':[{'end_height':ord(tag)-96,'terminal_block_hash':'canonical'}]}))
        req=dict(map_sha256=tag*64,directory=str(directory),assignment=str(self.assignment))
        M.atomic_json(self.root/(tag*64+'.request.json'),req)
        return req

    def active(self, tag, workers=None):
        self.request(tag)
        M.atomic_json(self.root/'active.json',dict(map_sha256=tag*64,workers=workers or ['owner','recent'],assignment=str(self.assignment)))

    def desired(self, tag):
        req=self.request(tag);M.atomic_json(self.root/'desired.json',req);return req

    async def control(self, worker, cmd):
        if cmd['operation']=='activate':
            self.assertEqual(self.status['candidate']['map_sha256'],cmd['map_sha256'])
            self.status.update(active={'map_sha256':cmd['map_sha256']},candidate=None,warm=True,invalidated=False)
        return self.status.copy()

    async def test_quorum_waiter_cancellation_keeps_one_job_and_skips_intermediate_targets(self):
        req=self.desired('b');started=asyncio.Event();release=asyncio.Event()
        async def stage(worker, req, assignment):
            started.set();await release.wait()
            self.status['candidate']={'map_sha256':req['map_sha256'],'warm':True}
            return {'expected':'a'*64}
        self.f.stage=AsyncMock(side_effect=stage)
        job=asyncio.create_task(self.f.managed_once(self.worker));await started.wait()
        waiter=asyncio.create_task(self.f.wait_managed(self.worker,req));await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):await waiter
        self.desired('c');self.desired('d')
        self.assertFalse(job.done());self.assertEqual(self.f.stage.await_count,1)
        release.set();await job
        self.assertEqual(self.status['active']['map_sha256'],'a'*64,'future revision was not advertised')
        await self.f.managed_once(self.worker)
        self.assertEqual([c.args[1]['map_sha256'] for c in self.f.stage.await_args_list],['b'*64,'d'*64])

    async def test_restart_observes_server_work_instead_of_queuing_duplicate(self):
        self.desired('b');self.status['preparing']={'map_sha256':'b'*64,'phase':'warming'}
        self.f.stage=AsyncMock()
        await self.f.managed_once(self.worker)
        self.f.stage.assert_not_awaited()
        self.assertEqual(json.loads((self.root/'job-recent.json').read_text())['phase'],'recovering')

    async def test_persisted_prepared_result_is_reattested(self):
        req=self.desired('b');self.f.job(self.worker,req,'prepared',expected='a'*64)
        waiter=asyncio.create_task(self.f.wait_managed(self.worker,req))
        await asyncio.sleep(0.02);self.assertFalse(waiter.done())
        self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        self.assertEqual(await asyncio.wait_for(waiter,1),{'expected':'a'*64})

    async def test_missing_owner_does_not_fallback_to_competing_stage(self):
        (self.root/'reconciler-heartbeat.json').unlink()
        with self.assertRaisesRegex(RuntimeError,'owner unavailable'):
            await self.f.wait_managed(self.worker,self.desired('b'))

    async def test_previous_boot_heartbeat_cannot_authorize_a_waiter(self):
        M.atomic_json(self.root/'reconciler-heartbeat.json',dict(monotonic=time.monotonic()+100,workers=['recent']))
        with self.assertRaisesRegex(RuntimeError,'owner unavailable'):
            await self.f.wait_managed(self.worker,self.desired('b'))

    async def test_only_one_reconciler_can_own_preparation(self):
        async with self.f.lock('reconciler',wait=False):
            with self.assertRaises(RuntimeError):
                async with self.f.lock('reconciler',wait=False):
                    self.fail('duplicate reconciler acquired ownership')

    async def test_unknown_canonicality_removes_member_and_withdraws_without_quorum(self):
        self.f.canonical_hash=AsyncMock(side_effect=RuntimeError('node unavailable'))
        await self.f.reconcile_member(self.worker)
        self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'],['owner'])
        self.assertEqual(self.f.route.await_args.args[0],[])

    async def test_transient_status_transport_failure_does_not_withdraw_a_warm_quorum(self):
        # Exercise real control parsing/retry and membership with a broken first
        # connection. A recovered status is checked against the active digest.
        self.f.control=M.Fleet.control.__get__(self.f)
        reply=json.dumps({'ok':True,'result':self.status}).encode()
        for failure in (RuntimeError('ssh exit 255'), TimeoutError()):
            self.f.ssh=AsyncMock(side_effect=[failure,reply])
            await self.f.reconcile_member(self.worker)
            self.assertEqual(self.f.ssh.await_count,2)
            for call in self.f.ssh.await_args_list:
                self.assertFalse(call.kwargs['multiplex'])
            self.assertEqual([c.kwargs['timeout'] for c in self.f.ssh.await_args_list],[1,1.5])
            self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'],['owner','recent'])
            self.f.route.assert_not_awaited()

    async def test_status_retry_cannot_mask_unavailable_or_invalid_worker(self):
        self.f.control=M.Fleet.control.__get__(self.f)
        cases=[RuntimeError('still offline'),
               json.dumps({'ok':True,'result':{**self.status,'warm':False}}).encode(),
               json.dumps({'ok':True,'result':{**self.status,'active':{'map_sha256':'other'}}}).encode()]
        for second in cases:
            self.active('a');self.f.route.reset_mock()
            self.f.ssh=AsyncMock(side_effect=[RuntimeError('first connection failed'),second])
            await self.f.reconcile_member(self.worker)
            self.assertEqual(self.f.ssh.await_count,2)
            self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'],['owner'])
            self.assertEqual(self.f.route.await_args.args[0],[])

    async def test_restarted_or_unreachable_member_is_removed(self):
        self.status['warm']=False
        await self.f.reconcile_member(self.worker)
        self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'],['owner'])
        self.active('a');self.f.control=AsyncMock(side_effect=RuntimeError('offline'))
        await self.f.reconcile_member(self.worker)
        self.assertEqual(json.loads((self.root/'active.json').read_text())['workers'],['owner'])

    async def test_valid_replacement_can_rejoin_with_historical_revocations(self):
        self.active('b',['owner']);self.status.update(candidate={'map_sha256':'b'*64,'warm':True},revoked_revisions=3)
        await self.f.reconcile_member(self.worker)
        self.assertIn('recent',json.loads((self.root/'active.json').read_text())['workers'])

    async def test_superseded_preparation_advances_an_unrouted_replica_during_a_burst(self):
        self.request('b');self.active('d',['owner'])
        self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        await self.f.advance_unrouted(self.worker)
        self.assertEqual(self.status['active']['map_sha256'],'b'*64)
        self.assertEqual(json.loads((self.root/'active.json').read_text())['map_sha256'],'d'*64)
        self.assertNotIn('recent',json.loads((self.root/'active.json').read_text())['workers'])
        self.f.route.assert_not_awaited()

    async def test_unrouted_advancement_refuses_future_or_orphaned_candidates_and_withdrawal(self):
        self.active('a',['owner']);self.request('b')
        self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        await self.f.advance_unrouted(self.worker)
        self.assertEqual(self.status['active']['map_sha256'],'a'*64)
        self.active('d',['owner']);self.f.canonical_hash=AsyncMock(return_value='fork')
        await self.f.advance_unrouted(self.worker)
        self.assertEqual(self.status['active']['map_sha256'],'a'*64)
        M.atomic_json(self.root/'withdrawn.json',{'withdrawn':True});self.f.control.reset_mock()
        await self.f.advance_unrouted(self.worker)
        self.f.control.assert_not_awaited()

    async def test_unrouted_advancement_never_changes_a_publicly_routed_worker(self):
        self.active('d');self.request('b')
        self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        await self.f.advance_unrouted(self.worker)
        self.f.control.assert_not_awaited()

    async def test_collected_intermediate_source_does_not_stall_the_next_target(self):
        req=self.request('b');self.active('d',['owner'])
        self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        (Path(req['directory'])/'shards.json').unlink()
        await self.f.advance_unrouted(self.worker)
        self.assertTrue(all(c.args[1]['operation']=='status' for c in self.f.control.await_args_list))

    async def test_withdrawal_or_orphan_never_activates_prepared_future(self):
        self.active('b',['owner']);self.status['candidate']={'map_sha256':'b'*64,'warm':True}
        M.atomic_json(self.root/'withdrawn.json',{'withdrawn':True})
        await self.f.reconcile_member(self.worker);self.f.control.assert_not_awaited()
        M.atomic_json(self.root/'withdrawn.json',{'withdrawn':False});self.f.canonical_hash=AsyncMock(return_value='fork')
        await self.f.reconcile_member(self.worker)
        self.assertTrue(all(c.args[1]['operation']=='status' for c in self.f.control.await_args_list))
        self.f.route.assert_not_awaited()
