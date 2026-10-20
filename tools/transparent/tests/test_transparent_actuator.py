"""The actuator's journaled operations against fake Terraform, DigitalOcean and SSH."""
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('actuator', Path(__file__).resolve().parents[1]/'scripts/transparent-fleet-actuator.py')
A = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(A)
I = A.INVENTORY


class FakeElastic:
    """Terraform for the elastic root: `apply` makes the planned members exist."""

    def __init__(self, provider):
        self.state, self.provider, self.planned, self.fail_after_create = {}, provider, None, False
        self.next_id = 500
        self.applies = 0

    def plan(self, members, host_keys, extra):
        assert all(set(spec) == {'size', 'image'} for spec in members.values())
        self.planned = sorted(members)
        return Path('/fake/plan'), 'sha-' + '-'.join(members)

    def show(self, plan):
        return {'members': self.planned}

    def apply(self, plan, digest):
        assert digest == 'sha-' + '-'.join(self.planned)
        self.applies += 1
        for name in list(self.state):
            if name not in self.planned:
                self.provider.pop(self.state.pop(name)['id'], None)
        for name in self.planned:
            if name not in self.state:
                self.next_id += 1
                self.state[name] = {'id': str(self.next_id), 'ipv4_private': f'10.0.9.{self.next_id % 250}'}
                self.provider[str(self.next_id)] = name
        if self.fail_after_create:
            self.fail_after_create = False
            raise KeyboardInterrupt('actuator killed during apply')

    def state_members(self):
        return dict(self.state)


class FakeDO:
    def __init__(self, provider):
        self.provider = provider

    def droplets(self, tag=None):
        return [{'id': i, 'name': n} for i, n in self.provider.items()]


class FakeRemote:
    def __init__(self):
        self.commands = []

    def run(self, host, command, data=None, timeout=120):
        self.commands.append((host, command))
        return ''

    def copy(self, *args, **kwargs):
        pass


class ActuatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.state, self.scaler = root/'state', root/'scaler'
        self.scaler.mkdir()
        self.fleet = dict(state_dir=str(self.state), roster=str(root/'roster.json'), known_hosts=str(root/'known'),
                          ssh_key='key', assign_binary='shard-assign')
        (root/'known').write_text('')
        roster = [dict(id=f'transparent-pir-recent-0{i}', role='recent-replica', ssh_host=f'10.0.0.{i}',
                       upstream=f'10.0.0.{i}:8093', cache_bytes=5368709120, memory_max='7G', build_slots=1) for i in (1, 2)]
        roster.append(dict(id='transparent-pir-archive-01', role='archive-owner', ssh_host='10.0.1.1',
                           upstream='10.0.1.1:8093', cache_bytes=51539607552, memory_max='56G', build_slots=1))
        assignment = {'workers': [{'id': 'transparent-pir-archive-01', 'role': 'archive-owner', 'shards': [0, 1]}]}
        self.inventory = I.Inventory(self.state, self.fleet['roster'], self.fleet['known_hosts'])
        self.inventory.write(0, lambda _: I.seed(roster, assignment), 'test', archive=True)
        self.provider = {}
        self.elastic = FakeElastic(self.provider)
        self.clock = [1000.0]
        self.policy(mode='act')
        self.members = {w['id']: {'role': w['role'], 'state': 'serving', 'rendered': True, 'intent': 'enrolled'}
                        for w in roster}
        self.membership()
        self.actuator = self.make()

    def make(self):
        config = dict(scaler_dir=str(self.scaler), worker_tag='transparent-pir-worker', size='s-4vcpu-8gb',
                      image='ubuntu-24-04-x64', region='ams3')
        actuator = A.Actuator(config, self.fleet, self.elastic, FakeDO(self.provider), FakeRemote(),
                              now=lambda: self.clock[0])
        actuator.validate_plan = lambda plan, **kw: {'ok': True, **{k: list(v) for k, v in kw.items()}}
        actuator.fleet_release = lambda: {'sha': 'rel', 'binary_sha256': 'b' * 64, 'dir': '/rel', 'binary': 'pir-query-server'}
        actuator.generate_host_key = lambda op, name: {'public': 'ssh-ed25519 KEY' + name[-2:], 'private_path': '/nonexistent'}
        actuator.host_keys = lambda op: {}
        return actuator

    def policy(self, **values):
        (self.scaler/'policy.json').write_text(json.dumps({'mode': 'observe', 'max_recent': 6, 'max_step': 3,
                                                           'daily_destroys': 2, **values}))

    def membership(self):
        I.atomic_json(self.state/'membership.json', {'updated_unix': time.time(), 'members': self.members})

    def serve(self, name):
        self.members[name] = {'role': 'recent-replica', 'state': 'serving', 'rendered': True, 'intent': 'enrolled'}
        self.membership()

    def intent(self, name):
        return next(m['intent'] for m in self.inventory.load()['members'] if m['id'] == name)

    def test_scale_out_enrolls_before_boot_and_finishes_when_serving(self):
        with patch.object(A, 'bootstrap', return_value=True) as boot:
            self.actuator.start('scale_out', count=1)
            op = self.actuator.run_once()
            self.assertEqual(op['phase'], 'installed')
            boot.assert_called_once()
        new = 'transparent-pir-recent-03'
        self.assertEqual(self.intent(new), 'enrolled')
        member = next(m for m in self.inventory.load()['members'] if m['id'] == new)
        self.assertEqual((member['origin'], member['droplet_id']), ('elastic', self.elastic.state[new]['id']))
        self.assertIn('KEY03', Path(self.fleet['known_hosts']).read_text())
        self.serve(new)
        self.assertIsNone(self.actuator.run_once())
        done = self.actuator.history()
        self.assertEqual(done[-1]['outcome'], 'serving')

    def test_a_crash_during_apply_resumes_from_the_provider_state(self):
        self.elastic.fail_after_create = True
        self.actuator.start('scale_out', count=1)
        with self.assertRaises(KeyboardInterrupt):
            self.actuator.run_once()
        self.assertEqual(self.actuator.operation()['phase'], 'applying')
        with patch.object(A, 'bootstrap', return_value=True):
            op = self.make().run_once()
        self.assertEqual(op['phase'], 'installed')
        self.assertEqual(self.elastic.applies, 1, 'an ambiguous apply is resolved, never repeated blindly')

    def test_disagreement_between_state_and_provider_fences(self):
        self.actuator.start('scale_out', count=1)
        real = self.elastic.apply
        def apply(plan, digest):
            real(plan, digest)
            self.provider.clear()
        self.elastic.apply = apply
        op = self.actuator.run_once()
        self.assertTrue(op['fenced'])
        self.assertIn('disagree', op['fence_reason'])
        self.assertEqual(self.make().run_once()['phase'], 'applying', 'a fenced operation does not move')

    def test_an_apply_that_creates_nothing_is_retried_then_fenced(self):
        self.elastic.apply = lambda plan, digest: None
        self.actuator.start('scale_out', count=1)
        op = self.actuator.run_once()
        self.assertTrue(op['fenced'])
        self.assertEqual(op['apply_attempts'], 2)

    def elastic_member(self):
        with patch.object(A, 'bootstrap', return_value=True):
            self.actuator.start('scale_out', count=1)
            self.actuator.run_once()
        self.serve('transparent-pir-recent-03')
        self.actuator.run_once()
        return 'transparent-pir-recent-03'

    def test_scale_in_drains_waits_stops_retires_then_destroys_by_id(self):
        name = self.elastic_member()
        droplet = self.elastic.state[name]['id']
        self.actuator.start('scale_in', member=name)
        self.assertEqual(self.actuator.run_once()['phase'], 'draining')
        self.assertEqual(self.intent(name), 'draining')
        self.members[name].update(intent='draining', rendered=False, drained_since_unix=self.clock[0])
        self.membership()
        self.actuator.quiet = lambda op: True
        self.assertEqual(self.actuator.run_once()['phase'], 'draining', 'waits out the drained interval')
        self.clock[0] += 200
        self.members[name]['drained_since_unix'] = self.clock[0] - 200
        self.membership()
        self.assertIsNone(self.actuator.run_once())
        self.assertIn(('10.0.9.' + str(int(droplet) % 250), 'systemctl disable --now transparent-shard-server'),
                      self.actuator.remote.commands)
        self.assertEqual(self.intent(name), 'retired')
        self.assertNotIn(name, self.elastic.state)
        self.assertNotIn(droplet, self.provider)
        self.assertEqual(self.actuator.history()[-1]['destroyed'], droplet)

    def test_scale_in_refuses_static_members_and_an_under_served_tier(self):
        with self.assertRaisesRegex(A.ActuatorError, 'static hosts'):
            self.actuator.start('scale_in', member='transparent-pir-recent-02')
        name = self.elastic_member()
        self.members['transparent-pir-recent-02']['state'] = 'lagging'
        self.membership()
        self.actuator.start('scale_in', member=name)
        self.assertIsNone(self.actuator.run_once())
        self.assertIn('refused', self.actuator.history()[-1]['outcome'])
        self.assertEqual(self.intent(name), 'enrolled')

    def test_the_daily_destroy_budget_is_enforced(self):
        self.policy(mode='act', daily_destroys=0)
        name = self.elastic_member()
        with self.assertRaisesRegex(A.ActuatorError, 'destroy budget'):
            self.actuator.start('scale_in', member=name)

    def test_archive_owners_can_never_be_removed_or_replaced(self):
        for kind in ('scale_in', 'replace'):
            with self.assertRaisesRegex(A.ActuatorError, 'only recent replicas'):
                self.actuator.start(kind, member='transparent-pir-archive-01')

    def test_replace_quarantines_the_failed_member_only_after_the_replacement_serves(self):
        failed = self.elastic_member()
        with patch.object(A, 'bootstrap', return_value=True):
            self.actuator.start('replace', member=failed, count=1)
            self.actuator.run_once()
        self.assertEqual(self.intent(failed), 'enrolled', 'the failed member stays until its replacement serves')
        self.serve('transparent-pir-recent-04')
        self.assertIsNone(self.actuator.run_once())
        self.assertEqual(self.intent(failed), 'retired')
        self.assertNotIn(failed, self.elastic.state)
        self.assertEqual(self.actuator.history()[-1]['outcome'], 'replaced')

    def test_a_replaced_static_member_is_left_for_an_operator(self):
        with patch.object(A, 'bootstrap', return_value=True):
            self.actuator.start('replace', member='transparent-pir-recent-01', count=1)
            self.actuator.run_once()
        self.serve('transparent-pir-recent-03')
        self.actuator.run_once()
        self.assertEqual(self.intent('transparent-pir-recent-01'), 'quarantined')
        self.assertIn('awaits an operator', self.actuator.history()[-1]['outcome'])

    def test_a_member_that_never_serves_is_quarantined_and_destroyed(self):
        with patch.object(A, 'bootstrap', return_value=True):
            self.actuator.start('scale_out', count=1)
            self.actuator.run_once()
        self.clock[0] += A.SERVE_DEADLINE + 1
        self.assertIsNone(self.actuator.run_once())
        self.assertEqual(self.intent('transparent-pir-recent-03'), 'retired')
        self.assertEqual(self.elastic.state, {})
        self.assertIn('failed', self.actuator.history()[-1]['outcome'])

    def test_the_kill_switch_stops_everything(self):
        (self.scaler/'disabled').write_text('')
        with self.assertRaisesRegex(A.ActuatorError, 'disabled'):
            self.actuator.start('scale_out', count=1)
        self.assertIsNone(self.actuator.run_once())

    def test_requests_are_consumed_once_and_only_in_act_modes(self):
        request = {'schema': 1, 'decision_id': 'd1', 'action': 'scale_out', 'count': 1, 'reason': 'load'}
        (self.scaler/'request.json').write_text(json.dumps(request))
        self.policy(mode='recommend')
        self.assertIsNone(self.actuator.run_once())
        self.policy(mode='act-dry')
        op = self.actuator.run_once()
        self.assertIsNone(op)
        self.assertEqual(self.actuator.history()[-1]['outcome'], 'dry-run planned')
        self.assertEqual(self.elastic.applies, 0)
        self.policy(mode='act')
        self.assertIsNone(self.actuator.run_once(), 'a consumed decision is never acted on twice')

    def test_a_replacement_at_max_recent_is_allowed(self):
        failed = self.elastic_member()
        self.policy(mode='act', max_recent=3)
        with self.assertRaisesRegex(A.ActuatorError, 'max_recent'):
            self.actuator.start('scale_out', count=1)
        self.actuator.start('replace', member=failed, count=1)

    def test_a_refused_request_is_answered_in_the_history(self):
        self.policy(mode='act', max_recent=2)
        (self.scaler/'request.json').write_text(json.dumps(
            {'schema': 1, 'decision_id': 'd9', 'action': 'scale_out', 'count': 1}))
        self.assertIsNone(self.actuator.run_once())
        last = self.actuator.history()[-1]
        self.assertEqual((last['decision_id'], last['phase']), ('d9', 'refused'))

    def test_new_members_clone_a_peer_into_the_releases_server_form(self):
        legacy = ('[Service]\nExecStart=/usr/local/bin/transparent-shard-server --listen 0.0.0.0:8093 '
                  '--shard-dir /pub/old --cache-bytes 5368709120 --worker-id transparent-pir-recent-01 '
                  '--assignment /pub/old/assignment.json --runtime-cache-dir /srv/transparent-pir/runtime-cache '
                  '--control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json\n'
                  'MemoryMax=7G\n')
        files = {'cat /etc/systemd/system/transparent-shard-server.service': legacy}
        self.actuator.remote.run = lambda host, command, data=None, timeout=120: files[command]
        new, publication = 'transparent-pir-recent-03', '/pub/new'
        unit, config = A.render_unit(self.actuator, new, publication, '/usr/local/bin/pir-query-server', '10.0.9.3:8093')
        self.assertIn('\nExecStart=/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json\n', unit)
        self.assertIn('MemoryMax=7G\n', unit)
        document = json.loads(config)
        self.assertEqual(document['management_listen'], '127.0.0.1:18190')
        settings = document['transparent_history']['settings']
        self.assertEqual((settings['listen'], settings['worker_id'], settings['shard_dir'], settings['assignment']),
                         ('10.0.9.3:8093', new, publication, publication + '/assignment.json'))
        self.assertEqual(settings['cache_bytes'], 5368709120)
        self.assertEqual(list(A.unit_directories(unit, config)),
                         ['/srv/transparent-pir/runtime-cache', '/opt/transparent-publisher'])
        # A configured peer is read through its configuration; a release from
        # before pir-query-server still gets the retired executable's flags.
        files['cat /etc/systemd/system/transparent-shard-server.service'] = unit
        files['cat /etc/transparent-pir/shard-server.json'] = config
        retired, none = A.render_unit(self.actuator, 'transparent-pir-recent-04', '/pub/four',
                                      '/usr/local/bin/transparent-query-server')
        self.assertIsNone(none)
        self.assertIn('ExecStart=/usr/local/bin/transparent-query-server --listen 10.0.9.3:8093 --shard-dir /pub/four ', retired)
        self.assertEqual(A.release_binary({}), 'transparent-query-server')
        with self.assertRaises(A.ActuatorError):
            A.release_binary({'binary': 'other'})

    def test_the_fleet_release_names_the_server_it_ships(self):
        store = Path(self.temp.name)/'releases'
        for release, name in (('old', 'transparent-query-server'), ('new', 'pir-query-server')):
            (store/release).mkdir(parents=True)
            (store/release/'SHA256SUMS').write_text(f'{release[0] * 64}  {name}\n{"c" * 64}  shard-control\n')
        actuator = A.Actuator(dict(scaler_dir=str(self.scaler), release_store=str(store)), self.fleet, self.elastic,
                              FakeDO(self.provider), FakeRemote(), now=lambda: self.clock[0])
        for digest, release, name in (('n' * 64, 'new', 'pir-query-server'), ('o' * 64, 'old', 'transparent-query-server')):
            with patch.object(A, 'http_json', return_value={'binary_sha256': digest}):
                found = actuator.fleet_release()
            self.assertEqual((found['sha'], found['binary']), (release, name))

    def test_scale_out_respects_max_recent(self):
        self.policy(mode='act', max_recent=2)
        with self.assertRaisesRegex(A.ActuatorError, 'max_recent'):
            self.actuator.start('scale_out', count=1)


if __name__ == '__main__':
    unittest.main()
