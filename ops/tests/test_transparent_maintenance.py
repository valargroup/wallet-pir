import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

SPEC = importlib.util.spec_from_file_location('upgrade', Path(__file__).resolve().parents[1]/'scripts/upgrade-transparent-fleet.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
RUN_SPEC = importlib.util.spec_from_file_location('rollout', Path(__file__).resolve().parents[1]/'scripts/run-transparent-hardening-rollout.py')
R = importlib.util.module_from_spec(RUN_SPEC)
RUN_SPEC.loader.exec_module(R)


class GateTests(unittest.TestCase):
    def setUp(self):
        self.gate = dict(passed=True, binary_sha256='binary', fleet_script_sha256='script', fleet_config_sha256='config',
                         worker='transparent-pir-recent-01', seconds=21600, blocks=300, replica_blocks=300,
                         exact_queries=[1000, 1000], public_budget_seconds=30, replica_budget_seconds=60,
                         maximum_visibility_seconds=29, maximum_canary_visibility_seconds=59)

    def test_gate_requires_both_duration_and_blocks_with_matching_provenance(self):
        M.validate_gate(self.gate, 'binary', 'script', 'config')
        for changed in [dict(seconds=21599), dict(blocks=299), dict(replica_blocks=299), dict(passed=False),
                        dict(binary_sha256='old'), dict(fleet_script_sha256='old'), dict(fleet_config_sha256='old'),
                        dict(exact_queries=[1000]), dict(exact_queries=[1000, 999]), dict(public_budget_seconds=60),
                        dict(maximum_visibility_seconds=31), dict(maximum_canary_visibility_seconds=61)]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                M.validate_gate({**self.gate, **changed}, 'binary', 'script', 'config')

    def test_guard_preserves_other_services_and_withdraws_parent_artifacts(self):
        config = '''example.test {
 handle @transparent_publication {
  reverse_proxy 127.0.0.1:8094
 }
 handle @legacy_transparent_filters {
  reverse_proxy 127.0.0.1:8090
 }
 handle_path /v1/filters/parents/* {
  root * /public
  file_server
 }
 handle /apm* { reverse_proxy 127.0.0.1:3002 }
 handle { reverse_proxy 127.0.0.1:8080 }
}
'''
        guarded = M.guard_coordinator(config)
        self.assertEqual(guarded.count('503'), 3)
        self.assertNotIn('8094', guarded)
        self.assertNotIn('8090', guarded)
        self.assertNotIn('file_server', guarded)
        self.assertIn('handle /apm* { reverse_proxy 127.0.0.1:3002 }', guarded)
        self.assertIn('handle { reverse_proxy 127.0.0.1:8080 }', guarded)

    def test_unknown_coordinator_config_is_refused_before_mutation(self):
        with self.assertRaises(ValueError):
            M.guard_coordinator('example.test { reverse_proxy 127.0.0.1:8080 }')


class MaintenanceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        (self.root/'roster').write_text(json.dumps([self.worker]))
        self.config = dict(roster=str(self.root/'roster'), state_dir=str(self.root/'state'), known_hosts='unused', ssh_key='unused',
                           public_host='public.test', authority_upstream='https://filters.test', router_host='router', internal_listen='router:8080')
        self.fleet = M.L.Fleet(self.config)
        self.fleet.ssh = AsyncMock()

    async def test_authority_start_waits_for_http_readiness(self):
        with patch.object(M, 'service') as service, patch.object(M.D, 'read_json', side_effect=[RuntimeError('not listening'), {'shards':[]}]) as read:
            await M.start_authority()
        self.assertEqual(read.call_count, 2)
        self.assertEqual([c.args for c in service.call_args_list], [('start', 'transparent-replica-reconciler'), ('start', 'transparent-publish-controller')])

    async def test_controller_reroute_cannot_bypass_maintenance_but_private_validation_can_run(self):
        M.L.atomic_json(self.fleet.root/'maintenance.json', {'enabled':True})
        await self.fleet.route([self.worker], {'workers':[dict(id='recent', shards=[173])]})
        text = self.fleet.ssh.await_args.args[2].decode()
        public, private = text.split('http://router:8080')
        self.assertIn('503', public)
        self.assertNotIn('host:8093', public)
        self.assertIn('host:8093', private)
        M.L.atomic_json(self.fleet.root/'maintenance.json', {'enabled':False})
        await self.fleet.route([self.worker], {'workers':[dict(id='recent', shards=[173])]})
        self.assertIn('host:8093', self.fleet.ssh.await_args.args[2].decode().split('http://router:8080')[0])

    async def test_failed_preflight_never_withdraws_or_restarts_service(self):
        artifacts = self.root/'artifacts'; artifacts.mkdir()
        (artifacts/'transparent-shard-server').write_bytes(b'fixture')
        config = self.root/'fleet.json'; config.write_text(json.dumps(self.config))
        args = argparse.Namespace(fleet_config=config, artifacts=artifacts, worker='recent', canary_result=None,
                                  source_sha='source', out=self.root/'result')
        with patch.object(M.D, 'read_json', return_value={'binary_sha256':'old'}), \
                patch.object(M.D, 'install_worker', AsyncMock(side_effect=RuntimeError('insufficient capacity'))), \
                patch.object(M, 'maintenance', AsyncMock()) as guard, patch.object(M, 'service') as service:
            with self.assertRaisesRegex(RuntimeError, 'insufficient capacity'):
                await M.upgrade(args)
            guard.assert_not_awaited()
            service.assert_not_called()

    async def test_reopen_refuses_orphan_before_changing_either_origin(self):
        directory = self.root/'publication'; directory.mkdir()
        (directory/'shards.json').write_text(json.dumps({'shards':[dict(end_height=100, terminal_block_hash='orphan')]}))
        self.fleet.reconciliation_target = lambda: ({'workers':['recent'], 'map_sha256':'digest'}, {'directory':str(directory)})
        self.fleet.quorum = lambda workers: True
        self.fleet.canonical_hash = AsyncMock(return_value='replacement')
        self.fleet.route = AsyncMock()
        with patch.object(M, 'apply_coordinator') as apply:
            with self.assertRaisesRegex(RuntimeError, 'changed chain'):
                await M.reopen(self.fleet, self.root)
            apply.assert_not_called()
            self.fleet.route.assert_not_awaited()

    async def test_old_warm_workers_do_not_allow_freezing_a_controller_that_is_catching_up(self):
        directory = self.root/'publication'; directory.mkdir()
        (directory/'shards.json').write_text(json.dumps({'shards':[dict(end_height=100)]}))
        self.fleet.reconciliation_target = lambda: ({'map_sha256':'digest'}, {'directory':str(directory)})
        self.fleet.node_height = AsyncMock(return_value=101)
        self.fleet.control = AsyncMock(return_value={'active':{'map_sha256':'digest'},'warm':True})
        with self.assertRaisesRegex(RuntimeError, 'catch up before freezing'):
            await M.ready_to_freeze(self.fleet, [self.worker])
        self.fleet.control.assert_not_awaited()
        self.fleet.node_height.return_value = 100
        await M.ready_to_freeze(self.fleet, [self.worker])

    async def test_supervisor_stops_after_a_failed_canary(self):
        artifacts = self.root/'artifacts'; artifacts.mkdir()
        (artifacts/'transparent-shard-server').write_bytes(b'fixture')
        args = argparse.Namespace(artifacts=artifacts, fleet_config=self.root/'unused', source_sha='source', out=self.root/'run')
        with patch.object(R, 'command', AsyncMock(side_effect=[None, RuntimeError('freshness exceeded')])) as command:
            with self.assertRaisesRegex(RuntimeError, 'freshness exceeded'):
                await R.run(args)
        self.assertEqual(command.await_count, 2)
        self.assertEqual(json.loads((args.out/'status.json').read_text())['phase'], 'failed')
        monitor = list(map(str, command.await_args_list[1].args[0]))
        self.assertEqual(monitor[monitor.index('--seconds')+1], '21600')
        self.assertEqual(monitor[monitor.index('--blocks')+1], '300')

    async def test_supervisor_finishes_only_after_every_fleet_monitor(self):
        artifacts = self.root/'artifacts'; artifacts.mkdir()
        (artifacts/'transparent-shard-server').write_bytes(b'fixture')
        config = self.root/'fleet.json'; config.write_text(json.dumps(self.config))
        args = argparse.Namespace(artifacts=artifacts, fleet_config=config, source_sha='source', out=self.root/'run')
        with patch.object(R, 'command', AsyncMock()) as command:
            await R.run(args)
        self.assertEqual(command.await_count, 4)
        final = list(map(str, command.await_args_list[-1].args[0]))
        self.assertEqual(final[final.index('--seconds')+1], '86400')
        self.assertEqual(json.loads((args.out/'status.json').read_text())['phase'], 'complete')
