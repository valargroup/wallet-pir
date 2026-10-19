"""Membership correctness: every member managed, probes off the routing lock,
stale observations discarded, bounded activation and write-once plans."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

SPEC = importlib.util.spec_from_file_location('fleet', Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


async def poll_tick(_seconds):
    """Let roster loops poll again without waiting for their production interval."""
    await asyncio.sleep(0)


async def wait_for_state(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0)


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

    async def test_https_recent_members_are_never_prepared_or_managed(self):
        https = dict(id='h1', role='https-recent', ssh_host='h1', upstream='h1:8092')
        self.f.roster = self.roster + [https]
        self.assertEqual(self.f.managed_ids(), {'owner', 'r1', 'r2'})
        self.assertNotIn(https, self.f.data_members())
        self.assertFalse(self.f.quorum({'owner', 'h1'}), 'an HTTPS member never completes quorum')
        self.f.control.reset_mock()
        await self.f.reconcile()
        self.assertNotIn('h1', [call.args[0]['id'] for call in self.f.control.await_args_list])

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


class DynamicRosterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        polling = patch.object(M, 'asyncio', SimpleNamespace(**(vars(asyncio) | {'sleep': poll_tick})))
        polling.start()
        self.addCleanup(polling.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.roster_path = self.root/'roster'
        self.members = [dict(id='owner', role='archive-owner', ssh_host='owner', upstream='owner:8093', intent='enrolled'),
                        dict(id='r1', role='recent-replica', ssh_host='r1', upstream='r1:8093', intent='enrolled'),
                        dict(id='r2', role='recent-replica', ssh_host='r2', upstream='r2:8093', intent='enrolled')]
        self.write_roster()
        self.f = M.Fleet(dict(roster=str(self.roster_path), state_dir=str(self.root), known_hosts='unused',
                              ssh_key='unused', manage_all_workers=True, public_host='pir.example',
                              authority_upstream='https://filters.example', router_host='router',
                              api_version='v2'))
        self.assignment = self.root/'assignment'
        self.assignment.write_text(json.dumps({'workers': [{'id': w['id'], 'shards': [0] if w['role'] == 'archive-owner' else [1]}
                                                           for w in self.members]}))
        req = dict(map_sha256='a'*64, directory=str(self.root), assignment=str(self.assignment))
        M.atomic_json(self.root/('a'*64+'.request.json'), req)
        M.atomic_json(self.root/'desired.json', req)
        M.atomic_json(self.root/'active.json', dict(map_sha256='a'*64, workers=['owner', 'r1', 'r2'], assignment=str(self.assignment)))
        self.rendered = []
        async def ssh(host, command, data=None, **kwargs):
            self.rendered.append(data.decode())
            return b''
        self.f.ssh = ssh

    def write_roster(self):
        M.atomic_json(self.roster_path, self.members)
        # A same-second rewrite must still register as a change.
        os.utime(self.roster_path, ns=(time.time_ns(), time.time_ns() + len(self.rendered if hasattr(self, 'rendered') else []) + 1))

    def intent(self, worker_id, intent):
        next(m for m in self.members if m['id'] == worker_id)['intent'] = intent
        self.write_roster()

    def test_roster_reloads_on_change_and_keeps_the_last_good_one(self):
        self.assertFalse(self.f.refresh_roster())
        self.members.append(dict(id='r3', role='recent-replica', ssh_host='r3', upstream='r3:8093', intent='enrolled'))
        self.write_roster()
        self.assertTrue(self.f.refresh_roster())
        self.assertEqual([w['id'] for w in self.f.roster], ['owner', 'r1', 'r2', 'r3'])
        self.roster_path.write_text('[{"id": "bad host", "ssh_host')
        self.assertFalse(self.f.refresh_roster())
        self.assertEqual(len(self.f.roster), 4)

    def test_draining_replicas_leave_the_router_unless_they_are_the_last(self):
        assignment = json.loads(self.assignment.read_text())
        self.intent('r2', 'draining'); self.f.refresh_roster()
        kept = [w['id'] for w in self.f.render_set(self.f.roster, assignment)]
        self.assertEqual(kept, ['owner', 'r1'])
        only_draining = [w for w in self.f.roster if w['id'] != 'r1']
        self.assertEqual([w['id'] for w in self.f.render_set(only_draining, assignment)], ['owner', 'r2'])

    def test_an_unassigned_member_is_never_rendered(self):
        assignment = {'workers': [{'id': 'owner', 'shards': [0]}, {'id': 'r1', 'shards': [1]}]}
        self.assertEqual([w['id'] for w in self.f.render_set(self.f.roster, assignment)], ['owner', 'r1'])

    async def test_an_intent_change_rerenders_without_a_membership_event(self):
        await self.f.refresh_routing()
        self.assertEqual(self.f.rendered_ids(), ['owner', 'r1', 'r2'])
        self.intent('r2', 'draining'); self.f.refresh_roster()
        await self.f.refresh_routing()
        self.assertEqual(self.f.rendered_ids(), ['owner', 'r1'])
        self.assertNotIn('r2:8093', self.rendered[-1])
        calls = len(self.rendered)
        await self.f.refresh_routing()
        self.assertEqual(len(self.rendered), calls, 'an unchanged render is not reapplied')

    async def test_an_inventory_change_never_withdraws(self):
        self.members = [m for m in self.members if m['role'] != 'recent-replica']
        self.write_roster(); self.f.refresh_roster()
        await self.f.refresh_routing()
        self.assertEqual(self.rendered, [])

    async def test_membership_reports_booting_rendered_and_drained_since(self):
        self.members.append(dict(id='r3', role='recent-replica', ssh_host='r3', upstream='r3:8093', intent='enrolled'))
        self.intent('r2', 'draining'); self.f.refresh_roster()
        await self.f.refresh_routing()
        self.f.write_membership()
        value = json.loads((self.root/'membership.json').read_text())
        self.assertEqual(value['members']['r3']['state'], 'booting')
        self.assertFalse(value['members']['r2']['rendered'])
        self.assertIsNotNone(value['members']['r2']['drained_since_unix'])
        self.assertEqual(value['rendered_recent'], 1)
        self.intent('r2', 'enrolled'); self.f.refresh_roster()
        await self.f.refresh_routing()
        self.f.write_membership()
        value = json.loads((self.root/'membership.json').read_text())
        self.assertIsNone(value['members']['r2']['drained_since_unix'])

    async def test_the_reconciler_follows_enrolled_and_retired_members(self):
        running = set()
        retired = asyncio.Event()
        async def managed_once(worker):
            running.add(worker['id'])
            try:
                await asyncio.Event().wait()
            finally:
                running.discard(worker['id'])
                if worker['id'] == 'r1':
                    retired.set()
        self.f.managed_once = managed_once
        self.f.refresh_routing = AsyncMock()
        task = asyncio.create_task(self.f.serve_reconciler())
        await wait_for_state(lambda: running == {'owner', 'r1', 'r2'})
        self.members.append(dict(id='r3', role='recent-replica', ssh_host='r3', upstream='r3:8093', intent='enrolled'))
        self.members = [m for m in self.members if m['id'] != 'r1']
        self.write_roster()
        await wait_for_state(lambda: retired.is_set() and running == {'owner', 'r2', 'r3'})
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(retired.is_set())
        self.assertEqual(running, set(), 'cancellation stops every member loop')

    async def test_control_sessions_follow_the_roster(self):
        running = set()
        async def control_session(worker):
            running.add(worker['id'])
            try:
                await asyncio.Event().wait()
            finally:
                running.discard(worker['id'])
        self.f.control_session = control_session
        task = asyncio.create_task(self.f.serve_control_sessions())
        await wait_for_state(lambda: running == {'owner', 'r1', 'r2'})
        self.assertEqual(running, {'owner', 'r1', 'r2'})
        self.members = [m for m in self.members if m['id'] != 'r2']
        self.members.append(dict(id='r3', role='recent-replica', ssh_host='r3', upstream='r3:8093', intent='enrolled'))
        self.write_roster()
        await wait_for_state(lambda: running == {'owner', 'r1', 'r3'})
        self.assertEqual(running, {'owner', 'r1', 'r3'})
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_each_publication_is_planned_from_one_roster_snapshot(self):
        plans = []
        async def planner(args, *rest, **kwargs):
            roster = Path(args[args.index('--roster')+1])
            plans.append(json.loads(roster.read_text()))
            out = Path(args[args.index('--out-assignment')+1])
            out.write_text(json.dumps(dict(schema=M.ASSIGNMENT_SCHEMA, set={'map_sha256': 'c'*64}, workers=[{'id': 'owner'}])))
            return b''
        self.f.c.update(assign_binary='shard-assign')
        self.f.wait_managed = AsyncMock(return_value={'expected': 'a'*64})
        req = dict(map_sha256='c'*64, directory=str(self.root), recent_from=1, source_sha='s')
        with patch.object(M, 'run', new=AsyncMock(side_effect=planner)):
            await self.f.prepare(dict(req))
        snapshot = json.loads((self.root/('c'*64+'.roster.json')).read_text())
        self.assertEqual([w['id'] for w in snapshot], ['owner', 'r1', 'r2'])
        self.assertEqual(plans, [snapshot])
        # An HTTPS member is neither planned nor prepared.
        self.f.roster = self.f.roster + [dict(id='h1', role='https-recent', ssh_host='h1', upstream='h1:8092')]
        staged = []
        async def collect(operation, workers, **kwargs):
            staged.extend(w['id'] for w in workers)
            return {w['id']: {'expected': 'a'*64} for w in workers}
        self.f.collect = collect
        req = dict(req, map_sha256='d'*64)
        async def planner_d(args, *rest, **kwargs):
            plans.append(json.loads(Path(args[args.index('--roster')+1]).read_text()))
            Path(args[args.index('--out-assignment')+1]).write_text(json.dumps(
                dict(schema=M.ASSIGNMENT_SCHEMA, set={'map_sha256': 'd'*64}, workers=[{'id': 'owner'}])))
            return b''
        with patch.object(M, 'run', new=AsyncMock(side_effect=planner_d)):
            await self.f.prepare(dict(req))
        self.assertNotIn('h1', [w['id'] for w in plans[-1]])
        self.assertNotIn('h1', staged)


class HttpsRecentTests(unittest.IsolatedAsyncioTestCase):
    """An `https-recent` member is routed only on its own observed status and
    never receives a control command, invalidation or control session."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.https = dict(id='h1', role='https-recent', ssh_host='h1', upstream='h1:8443',
                          status_listen='127.0.0.1:8092', intent='enrolled')
        self.members = [dict(id='owner', role='archive-owner', ssh_host='owner', upstream='owner:8093', intent='enrolled'),
                        dict(id='r1', role='recent-replica', ssh_host='r1', upstream='r1:8093', intent='enrolled'),
                        self.https]
        (self.root/'roster').write_text(json.dumps(self.members))
        self.f = M.Fleet(dict(roster=str(self.root/'roster'), state_dir=str(self.root), known_hosts='unused',
                              ssh_key='unused', manage_all_workers=True, public_host='pir.example',
                              authority_upstream='https://filters.example', router_host='router', api_version='v2'))
        self.assignment = self.root/'assignment'
        self.assignment.write_text(json.dumps({'workers': [{'id': 'owner', 'shards': [0]}, {'id': 'r1', 'shards': [1]}]}))
        self.map = dict(network='main', genesis_hash='g'*64, profile='zcash-transparent-range-v1', start_height=100,
                        shards=[dict(shard_id=0, manifest_digest='m0', end_height=150, terminal_block_hash='old'),
                                dict(shard_id=1, manifest_digest='m1', end_height=160, terminal_block_hash='canonical')])
        (self.root/'shards.json').write_text(json.dumps(self.map))
        req = dict(map_sha256='a'*64, directory=str(self.root), assignment=str(self.assignment))
        M.atomic_json(self.root/('a'*64+'.request.json'), req)
        M.atomic_json(self.root/'desired.json', req)
        M.atomic_json(self.root/'active.json', dict(map_sha256='a'*64, workers=['owner', 'r1'], assignment=str(self.assignment)))
        self.provider = self.serving('a'*64)
        self.rendered, self.commands = [], []
        async def ssh(host, command, data=None, **kwargs):
            self.commands.append((host, command, kwargs))
            if host == 'router':
                self.rendered.append(data.decode())
                return b''
            if isinstance(self.provider, Exception):
                raise self.provider
            status, init = self.provider
            return (json.dumps(status) + '\n' + json.dumps(init) + '\n').encode()
        self.f.ssh = ssh
        self.controlled = []
        async def control(worker, value):
            self.controlled.append((worker['id'], value['operation']))
            if worker['role'] == 'https-recent':
                raise AssertionError('a control command reached an https-recent member')
            return {'active': {'map_sha256': 'a'*64}, 'warm': True, 'invalidated': False, 'revisions': []}
        self.f.control = control
        self.f.canonical_hash = AsyncMock(return_value='canonical')

    def serving(self, digest, warm=True, invalidated=False, local=True):
        status = {'verified_publication': {'map_sha256': digest},
                  'runtime_readiness': {'warm': warm, 'invalidated': invalidated},
                  'active_coverage': [dict(shard_id=s['shard_id'], manifest_digest=s['manifest_digest'],
                                           local=local or s['shard_id'] == 0, revoked=False) for s in self.map['shards']]}
        init = dict(map_sha256=digest, covered_through=160, shards=2, schema='transparent-shard-v12',
                    **{k: self.map[k] for k in M.PROTOCOL_FIELDS})
        return status, init

    def routed(self):
        return json.loads((self.root/'active.json').read_text())['workers']

    async def test_admission_requires_exact_publication_protocol_and_warm_valid_coverage(self):
        refused = {
            'stale publication': self.serving('b'*64),
            'cold': self.serving('a'*64, warm=False),
            'invalidated': self.serving('a'*64, invalidated=True),
            'recent shard forwarded, not local': self.serving('a'*64, local=False),
        }
        status, init = self.serving('a'*64)
        refused['downloaded candidate only'] = (dict(status, verified_publication={'map_sha256': 'b'*64},
                                                     candidate={'map_sha256': 'a'*64, 'warm': True}), init)
        refused['other network'] = (status, dict(init, network='test'))
        refused['other genesis'] = (status, dict(init, genesis_hash='h'*64))
        refused['short coverage'] = (status, dict(init, covered_through=159))
        for reason, observation in refused.items():
            with self.subTest(reason):
                self.provider = observation
                await self.f.reconcile_https(self.https)
                self.assertEqual(self.routed(), ['owner', 'r1'])
                self.assertEqual(self.rendered, [])
        self.provider = RuntimeError('wallet API v2 not served')
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.rendered, [], 'a provider without the fleet API version is never admitted')
        self.provider = self.serving('a'*64)
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.routed(), ['h1', 'owner', 'r1'])
        self.assertIn('reverse_proxy r1:8093 h1:8443', self.rendered[-1])
        self.assertIn('/v2/shards/(1)/revisions', self.rendered[-1])
        self.assertTrue(all(kwargs.get('multiplex') is False for host, _, kwargs in self.commands if host == 'h1'))
        self.assertIn('http://127.0.0.1:8092/v2/shards/init', next(c for h, c, _ in self.commands if h == 'h1'))
        self.f.write_membership()
        member = json.loads((self.root/'membership.json').read_text())['members']['h1']
        self.assertEqual((member['state'], member['rendered']), ('serving', True))
        self.assertEqual(self.controlled, [])

    async def test_withdrawn_at_the_first_refusing_observation(self):
        await self.f.reconcile_https(self.https)
        self.assertIn('h1', self.routed())
        self.provider = self.serving('a'*64, invalidated=True)
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.routed(), ['owner', 'r1'])
        self.assertNotIn('h1:8443', self.rendered[-1])
        self.assertIn('reverse_proxy r1:8093', self.rendered[-1], 'withdrawing it never withdraws the fleet')
        self.f.write_membership()
        member = json.loads((self.root/'membership.json').read_text())['members']['h1']
        self.assertEqual((member['state'], member['rendered']), ('invalidated', False))

    async def test_a_noncanonical_endpoint_refuses_and_an_unknown_one_changes_nothing(self):
        self.f.canonical_hash = AsyncMock(side_effect=RuntimeError('node unavailable'))
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.routed(), ['owner', 'r1'])
        self.f.canonical_hash = AsyncMock(return_value='canonical')
        await self.f.reconcile_https(self.https)
        self.f.canonical_hash = AsyncMock(return_value='forked')
        self.f.canonical.clear()
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.routed(), ['owner', 'r1'])

    async def test_one_unobserved_status_keeps_it_and_a_run_withdraws_it(self):
        self.f.c.update(membership_failures=2, membership_failure_seconds=0)
        await self.f.reconcile_https(self.https)
        self.provider = RuntimeError('ssh: connection refused')
        await self.f.reconcile_https(self.https)
        self.assertIn('h1', self.routed())
        await self.f.reconcile_https(self.https)
        self.assertEqual(self.routed(), ['owner', 'r1'])

    async def test_never_admitted_into_or_withdrawing_a_router_without_quorum(self):
        M.atomic_json(self.root/'active.json', dict(map_sha256='a'*64, workers=['owner'], assignment=str(self.assignment)))
        await self.f.reconcile_https(self.https)
        self.assertEqual((self.routed(), self.rendered), (['owner'], []))
        M.atomic_json(self.root/'active.json', dict(map_sha256='a'*64, workers=['owner', 'h1'], assignment=str(self.assignment)))
        self.provider = self.serving('b'*64)
        await self.f.reconcile_https(self.https)
        self.assertEqual((self.routed(), self.rendered), (['owner'], []), 'no route([]) from an https member')

    async def test_activation_drops_it_until_it_attests_the_new_publication(self):
        await self.f.reconcile_https(self.https)
        self.assertIn('h1', self.routed())
        self.assertEqual(self.f.quorum({'owner', 'h1'}), False)
        prepared = {'workers': {'owner': {'expected': 'a'*64}, 'r1': {'expected': 'a'*64}},
                    'assignment': str(self.assignment)}
        async def control(worker, value):
            self.assertNotEqual(worker['id'], 'h1')
            return {'active': {'map_sha256': 'c'*64}, 'warm': True, 'invalidated': False}
        self.f.control = control
        await self.f.activate({'map_sha256': 'c'*64, 'prepared': prepared})
        self.assertEqual(self.routed(), ['owner', 'r1'])

    async def test_invalidation_sends_it_nothing_and_its_failure_never_withdraws(self):
        await self.f.reconcile_https(self.https)
        self.rendered.clear()
        result = await self.f.invalidate({'from_height': 200, 'retain_publication':
                                          {'map_sha256': 'a'*64, 'height': 160, 'hash': 'canonical'}})
        self.assertTrue(result['retained_publication'])
        self.assertEqual(sorted(result['invalidated']), ['owner', 'r1'])
        self.assertNotIn('h1', [worker for worker, _ in self.controlled])
        self.assertEqual(self.rendered, [], 'retained coverage is not withdrawn')
        self.assertFalse(json.loads((self.root/'withdrawn.json').read_text())['withdrawn']
                         if (self.root/'withdrawn.json').exists() else False)
        # Unretained invalidation withdraws, still without touching the provider.
        self.controlled.clear()
        await self.f.invalidate({'from_height': 120})
        self.assertEqual(self.rendered[-1], M.withdrawn_router(self.f.c, guarded=False))
        self.assertEqual(sorted({w for w, _ in self.controlled}), ['owner', 'r1'])

    async def test_control_sessions_exclude_it(self):
        running = set()
        async def control_session(worker):
            running.add(worker['id'])
            await asyncio.Event().wait()
        self.f.control_session = control_session
        task = asyncio.create_task(self.f.serve_control_sessions())
        await wait_for_state(lambda: {'owner', 'r1'} <= running)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(running, {'owner', 'r1'})

    async def test_the_reconciler_runs_an_admission_loop_for_it(self):
        seen = []
        admitted = asyncio.Event()
        async def reconcile_https(worker):
            seen.append(worker['id'])
            admitted.set()
        async def managed_once(worker):
            await asyncio.Event().wait()
        self.f.reconcile_https = reconcile_https
        self.f.managed_once = managed_once
        task = asyncio.create_task(self.f.serve_reconciler())
        async with asyncio.timeout(5):
            await admitted.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(set(seen), {'h1'})

    def test_its_roster_entry_must_name_a_loopback_status_listener(self):
        for listen in (None, '0.0.0.0:8092', 'h1:8092', '127.0.0.1:8092/../x'):
            with self.subTest(listen=listen):
                entry = {k: v for k, v in self.https.items() if k != 'status_listen'}
                if listen is not None:
                    entry['status_listen'] = listen
                (self.root/'roster').write_text(json.dumps(self.members[:2] + [entry]))
                with self.assertRaisesRegex(ValueError, 'status_listen'):
                    M.Fleet(dict(self.f.c))
