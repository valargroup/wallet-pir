"""Rolling recent-replica upgrades keep two other replicas serving."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

SPEC = importlib.util.spec_from_file_location('roll', Path(__file__).resolve().parents[1]/'scripts/roll-recent-replicas.py')
R = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(R)


class RollingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        roster = [dict(id='owner', role='archive-owner', ssh_host='o', upstream='o:1')]
        roster += [dict(id=f'r{i}', role='recent-replica', ssh_host=f'r{i}', upstream=f'r{i}:1') for i in (1, 2, 3)]
        (self.root/'roster.json').write_text(json.dumps(roster))
        self.config = self.root/'fleet.json'
        self.config.write_text(json.dumps(dict(roster=str(self.root/'roster.json'), state_dir=str(self.root),
                                               known_hosts='k', ssh_key='k', manage_all_workers=True)))
        self.states = {'owner': 'serving', 'r1': 'serving', 'r2': 'serving', 'r3': 'serving'}
        self.write()

    def write(self, age=0):
        R.L.atomic_json(self.root/'membership.json', dict(
            updated_unix=time.time()-age, members={k: {'state': v} for k, v in self.states.items()}))

    def args(self, workers=()):
        return type('A', (), dict(fleet_config=self.config, artifacts=self.root, out=self.root/'out',
                                  workers=list(workers), warm_seconds=1, route_seconds=1))

    async def test_each_replica_is_upgraded_in_turn_and_rejoins_before_the_next(self):
        order = []
        async def install(fleet, worker, artifacts, rollback, warm_seconds):
            order.append(worker['id'])
            # Every other replica is still serving while this one restarts.
            self.assertEqual(sum(v == 'serving' for k, v in self.states.items() if k.startswith('r')), 3)
        with patch.object(R.D, 'install_worker', new=AsyncMock(side_effect=install)):
            await R.roll(self.args())
        self.assertEqual(order, ['r1', 'r2', 'r3'])

    async def test_refuses_when_fewer_than_two_others_serve(self):
        self.states['r2'] = 'lagging'
        self.write()
        with patch.object(R.D, 'install_worker', new=AsyncMock()) as install:
            with self.assertRaisesRegex(RuntimeError, 'other recent replicas serving'):
                await R.roll(self.args())
            install.assert_not_awaited()

    async def test_refuses_a_stale_membership_record(self):
        self.write(age=60)
        with self.assertRaisesRegex(RuntimeError, 'stale'):
            await R.roll(self.args())

    async def test_stops_when_an_upgraded_replica_is_not_routed_again(self):
        async def install(fleet, worker, artifacts, rollback, warm_seconds):
            self.states[worker['id']] = 'lagging'
            self.write()
        with patch.object(R.D, 'install_worker', new=AsyncMock(side_effect=install)) as install_mock:
            with self.assertRaisesRegex(RuntimeError, 'not routed again'):
                await R.roll(self.args())
            self.assertEqual(install_mock.await_count, 1)

    async def test_requires_every_member_managed(self):
        config = json.loads(self.config.read_text())
        config['manage_all_workers'] = False
        self.config.write_text(json.dumps(config))
        with self.assertRaisesRegex(RuntimeError, 'manage every member'):
            await R.roll(self.args())


if __name__ == '__main__':
    unittest.main()
