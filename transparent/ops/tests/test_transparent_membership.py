"""Membership correctness: every member managed, probes off the routing lock,
stale observations discarded, bounded activation and write-once plans."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

SPEC = importlib.util.spec_from_file_location('fleet', Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class MembershipTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = dict(id='owner', role='archive-owner', ssh_host='owner', upstream='owner:8093')
        self.r1 = dict(id='r1', role='recent-replica', ssh_host='r1', upstream='r1:8093')
        self.r2 = dict(id='r2', role='recent-replica', ssh_host='r2', upstream='r2:8093')
        self.roster = [self.owner, self.r1, self.r2]
        (self.root/'roster').write_text(json.dumps(self.roster))
        self.f = M.Fleet(dict(roster=str(self.root/'roster'), state_dir=str(self.root), known_hosts='unused',
                              ssh_key='unused', manage_all_workers=True))
        self.assignment = self.root/'assignment'
        self.assignment.write_text('{}')
        self.status = {w['id']: {'active': {'map_sha256': 'a'*64}, 'warm': True, 'invalidated': False,
                                 'candidate': None, 'preparing': None} for w in self.roster}
        self.f.control = AsyncMock(side_effect=self.control)
        self.f.route = AsyncMock()
        self.f.canonical_hash = AsyncMock(return_value='canonical')
        self.publish('a', ['owner', 'r1', 'r2'])

    def request(self, tag):
        directory = self.root/tag
        directory.mkdir(exist_ok=True)
        (directory/'shards.json').write_text(json.dumps({'shards': [{'end_height': ord(tag)-96, 'terminal_block_hash': 'canonical'}]}))
        req = dict(map_sha256=tag*64, directory=str(directory), assignment=str(self.assignment))
        M.atomic_json(self.root/(tag*64+'.request.json'), req)
        return req

    def publish(self, tag, workers):
        req = self.request(tag)
        M.atomic_json(self.root/'desired.json', req)
        M.atomic_json(self.root/'active.json', dict(map_sha256=tag*64, workers=workers, assignment=str(self.assignment)))

    def routed(self):
        return json.loads((self.root/'active.json').read_text())['workers']

    async def control(self, worker, cmd):
        status = self.status[worker['id']]
        if cmd['operation'] == 'activate':
            status.update(active={'map_sha256': cmd['map_sha256']}, candidate=None, warm=True)
        return json.loads(json.dumps(status))

    def test_manage_all_includes_archive_owners(self):
        self.assertEqual(self.f.managed_ids(), {'owner', 'r1', 'r2'})
        self.f.c['manage_all_workers'] = False
        self.f.c['managed_recent_workers'] = ['r1']
        self.assertEqual(self.f.managed_ids(), {'r1'})

    async def test_reconciler_accepts_archive_owners_only_when_all_are_managed(self):
        self.f.c['manage_all_workers'] = False
        self.f.c['managed_recent_workers'] = ['owner']
        with self.assertRaisesRegex(ValueError, 'recent replicas'):
            await self.f.serve_reconciler()

    async def test_archive_owner_is_prepared_but_never_unrouted_by_a_status_blip(self):
        self.status['owner']['active'] = {'map_sha256': 'z'*64}
        async def stage(worker, req, assignment):
            self.status['owner']['candidate'] = {'map_sha256': req['map_sha256'], 'warm': True}
        self.f.stage = AsyncMock(side_effect=stage)
        await self.f.managed_once(self.owner)
        self.assertEqual(self.routed(), ['owner', 'r1', 'r2'])
        self.f.route.assert_not_awaited()
        self.assertEqual(self.f.observations['owner']['state'], 'lagging')
        self.assertEqual(json.loads((self.root/'job-owner.json').read_text())['phase'], 'prepared')

    async def test_steady_member_probe_does_not_wait_for_the_routing_lock(self):
        async with self.f.lock('routing'):
            await asyncio.wait_for(self.f.reconcile_member(self.r1), 1)
        self.f.route.assert_not_awaited()

    async def test_observation_is_discarded_when_activation_intervenes(self):
        self.publish('a', ['owner', 'r1'])
        self.status['r2']['candidate'] = {'map_sha256': 'a'*64, 'warm': True}
        self.status['r2']['active'] = {'map_sha256': 'b'*64}
        real_probe = self.f.probe
        async def probe(worker):
            result = await real_probe(worker)
            # A foreground activation lands between the probe and the apply.
            self.f.bump_generation()
            return result
        self.f.probe = probe
        await self.f.reconcile_member(self.r2)
        self.assertEqual(self.routed(), ['owner', 'r1'])
        self.assertTrue(all(c.args[1]['operation'] == 'status' for c in self.f.control.await_args_list))
        self.f.probe = real_probe
        await self.f.reconcile_member(self.r2)
        self.assertEqual(self.routed(), ['owner', 'r1', 'r2'])

    async def test_prepared_member_joins_after_a_fresh_endpoint_check(self):
        self.publish('a', ['owner', 'r1'])
        self.status['r2'].update(active={'map_sha256': 'b'*64}, candidate={'map_sha256': 'a'*64, 'warm': True})
        await self.f.reconcile_member(self.r2)
        self.assertEqual(self.routed(), ['owner', 'r1', 'r2'])
        activations = [c for c in self.f.control.await_args_list if c.args[1]['operation'] == 'activate']
        self.assertEqual(len(activations), 1)

    async def test_prepared_member_does_not_activate_when_the_endpoint_forks_before_apply(self):
        self.publish('a', ['owner', 'r1'])
        self.status['r2'].update(active={'map_sha256': 'b'*64}, candidate={'map_sha256': 'a'*64, 'warm': True})
        answers = iter(['canonical', 'fork'])
        self.f.canonical_hash = AsyncMock(side_effect=lambda height: next(answers))
        await self.f.reconcile_member(self.r2)
        self.assertEqual(self.routed(), ['owner', 'r1'])
        self.assertFalse(any(c.args[1]['operation'] == 'activate' for c in self.f.control.await_args_list))

    async def test_invalidation_and_withdrawal_advance_the_routing_generation(self):
        before = self.f.routing_generation()
        await self.f.request({'operation': 'withdraw'})
        self.assertEqual(self.f.routing_generation(), before + 1)

    async def test_activation_returns_at_quorum_and_skips_a_locked_member(self):
        self.publish('a', ['owner', 'r1', 'r2'])
        req = dict(map_sha256='b'*64, prepared={'assignment': str(self.assignment),
                   'workers': {w['id']: {'expected': 'a'*64} for w in self.roster}})
        (self.root/'assignment').write_text(json.dumps({'workers': [
            {'id': w['id'], 'shards': [0]} for w in self.roster]}))
        self.f.c.update(router_host='router', activation_lock_seconds=0.2)
        async with self.f.lock('worker-r2'):
            started = time.monotonic()
            result = await self.f.activate(req)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(self.routed(), ['owner', 'r1'])
        self.assertEqual(result['recent_replicas'], 1)
        self.assertNotEqual(self.status['r2']['active']['map_sha256'], 'b'*64)

    def test_membership_file_reports_routed_recent_replicas(self):
        self.f.observe(self.r1, self.status['r1'])
        self.f.observe(self.r2, None)
        self.f.write_membership()
        value = json.loads((self.root/'membership.json').read_text())
        self.assertEqual(value['routed_recent'], 2)
        self.assertEqual(value['members']['r1']['state'], 'serving')
        self.assertEqual(value['members']['r2']['state'], 'unreachable')

    async def test_desired_change_wakes_the_worker_loop(self):
        mark = self.f.desired_mark()
        async def change():
            await asyncio.sleep(0.1)
            M.atomic_json(self.root/'desired.json', self.request('b'))
        started = time.monotonic()
        await asyncio.gather(self.f.wait_desired_change(mark, timeout=5), change())
        self.assertLess(time.monotonic() - started, 1)

    def test_publisher_redeploy_carries_operational_settings(self):
        previous = dict(roster='old', control_sessions=True, status_socket_forwarding=True,
                        manage_all_workers=True, reconcile_workers=[])
        merged = M.carry_operational(dict(roster='new', state_dir='s'), previous)
        self.assertEqual(merged, dict(roster='new', state_dir='s', control_sessions=True,
                                      status_socket_forwarding=True, manage_all_workers=True,
                                      reconcile_workers=[]))


class PlanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        roster = [dict(id='owner', role='archive-owner', ssh_host='owner', upstream='owner:8093'),
                  dict(id='r1', role='recent-replica', ssh_host='r1', upstream='r1:8093')]
        (self.root/'roster').write_text(json.dumps(roster))
        self.f = M.Fleet(dict(roster=str(self.root/'roster'), state_dir=str(self.root), known_hosts='unused',
                              ssh_key='unused', assign_binary='shard-assign', manage_all_workers=True))
        self.source = self.root/'source'
        self.source.mkdir()
        self.digest = 'c'*64
        self.req = dict(map_sha256=self.digest, directory=str(self.source), recent_from=10, source_sha='sha')
        self.plan = dict(schema=M.ASSIGNMENT_SCHEMA, set={'map_sha256': self.digest}, workers=[{'id': 'owner'}])
        self.f.wait_managed = AsyncMock(return_value={'expected': 'a'*64})

    async def planner(self, args, *rest, **kwargs):
        out = Path(args[args.index('--out-assignment')+1])
        out.write_text(json.dumps(self.plan))
        return b''

    async def test_truncated_plan_is_replaced_and_a_complete_plan_reused(self):
        final = self.root/(self.digest+'.assignment.json')
        final.write_text('{"schema": "transparent-assi')
        with patch.object(M, 'run', new=AsyncMock(side_effect=self.planner)) as run:
            await self.f.prepare(dict(self.req))
            self.assertEqual(json.loads(final.read_text()), self.plan)
            self.assertEqual(run.await_count, 1)
            await self.f.prepare(dict(self.req))
            self.assertEqual(run.await_count, 1)
        self.assertEqual(len(list(self.root.glob('*.invalid-*'))), 1)
        self.assertEqual(list(self.root.glob('*.partial')), [])

    async def test_prepare_waits_the_configured_grace_for_later_replicas(self):
        (self.root/(self.digest+'.assignment.json')).write_text(json.dumps(self.plan))
        self.f.c['prepare_grace_seconds'] = 3.5
        grace = []
        async def collect(operation, workers, early=False, grace_=None, **kwargs):
            grace.append(kwargs.get('grace'))
            return {w['id']: {'expected': 'a'*64} for w in workers}
        self.f.collect = collect
        await self.f.prepare(dict(self.req))
        self.assertEqual(grace, [3.5])

    async def test_incomplete_planner_output_is_never_published(self):
        self.plan['set'] = {'map_sha256': 'other'}
        with patch.object(M, 'run', new=AsyncMock(side_effect=self.planner)):
            with self.assertRaisesRegex(RuntimeError, 'incomplete assignment'):
                await self.f.prepare(dict(self.req))
        self.assertFalse((self.root/(self.digest+'.assignment.json')).exists())
        self.assertEqual(list(self.root.glob('*.partial')), [])

    async def test_remote_assignment_is_written_once(self):
        worker = dict(id='r1', role='recent-replica', ssh_host='r1', upstream='r1:8093')
        (self.source/'shards.json').write_text(json.dumps({'shards': [{'manifest_digest': 'm'}]}))
        assignment = self.root/'plan.json'
        assignment.write_text(json.dumps(self.plan))
        self.f.control = AsyncMock(return_value={'active': {'map_sha256': 'a'*64, 'directory': '/old'}, 'revisions': []})
        self.f.revoke_orphans = AsyncMock()
        commands = []
        async def ssh(host, command, data=None, **kwargs):
            commands.append(command)
            return b''
        self.f.ssh = ssh
        with patch.object(M, 'run', new=AsyncMock(return_value=b'')):
            await self.f.stage(worker, self.req, assignment)
        write = [c for c in commands if 'assignment.json' in c][0]
        self.assertIn('cmp -s -', write)
        self.assertIn('a different assignment exists', write)
        self.assertNotIn('cat > ' + '/srv/transparent-pir/publications/' + self.digest + '/assignment.json\n', write)


if __name__ == '__main__':
    unittest.main()
