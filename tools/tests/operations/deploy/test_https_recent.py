"""The managed HTTPS recent replica through the guarded shared engine.

Uses the repository's descriptors, renderer, guard and unit template; only
hosts are fake. The fake host answers the guard's own commands (`getent`,
`stat`, `install -d`, `setpriv`) and records every disposable check.
"""
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402
from wallet_pir_ops.deploy.lifecycle import Lifecycle  # noqa: E402
from tools.transparent.lib import https_recent  # noqa: E402

SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
SERVICE = SERVICES['transparent-history']
BINARY, NEXT = b'transparent-query-server one', b'transparent-query-server two'
SHA, NEXT_SHA = sha256(BINARY), sha256(NEXT)
UNIT = 'transparent-history-https-recent.service'
STATE = '/var/lib/transparent-provider'
STATUS = 'http://127.0.0.1:8092/v1/provider-status'
READY = 'http://127.0.0.1:8092/v1/ready'
FLEET = '/opt/transparent-publisher/state'
ACTIVE = 'ab' * 32


class Host(FakeFleet):
    """Answers the guard's host commands; anything else is the fake's."""

    def __init__(self):
        super().__init__()
        self.users = {'transparent-provider'}
        self.owner = {STATE: 'transparent-provider:transparent-provider:700'}
        self.checks = []          # (binary, argv) of each disposable check
        self.accepts = lambda binary, argv: True

    def run(self, host, argv, timeout):
        if argv[0] == https_recent.GETENT:
            return (0, argv[2]) if argv[2] in self.users else (2, '')
        if argv[0] == https_recent.STAT:
            return (0, self.owner[argv[3]] + '\n') if argv[3] in self.owner else (1, 'No such file')
        if argv[0] == https_recent.INSTALL:
            self._mutate(host, 'install-d', argv[-1])
            self.host(host).dirs.add(argv[-1])
            self.owner[argv[-1]] = '%s:%s:700' % (argv[3], argv[5])
            return 0, ''
        if argv[0] == https_recent.SETPRIV:
            command = argv[argv.index('--') + 1:]
            scratch = command[command.index('--shard-dir') + 1]
            assert self.owner.get(scratch, '').startswith(argv[2] + ':'), 'check ran outside its own state'
            self.checks.append((command[0], command))
            ok = self.accepts(sha256(self.host(host).files[command[0]]), command)
            return (0, '{"active":true}') if ok else (1, 'refused: owner floor')
        return super().run(host, argv, timeout)

    def remove_tree(self, host, path, owner):
        self._mutate(host, 'remove_tree', path)
        assert path.startswith(owner + '/') and path != STATE
        self.host(host).dirs.discard(path)
        self.owner.pop(path, None)


class HttpsRecent(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state = root / 'state'
        self.binary, self.next = root / 'one', root / 'two'
        self.binary.write_bytes(BINARY)
        self.next.write_bytes(NEXT)
        self.fake = Host()
        self.fake.host('edge-01')
        self.fake.host('coordinator').free = 1 << 50
        self.fake.host('edge-01').free = 1 << 50
        self.fake.host('edge-01').dirs.add(STATE)
        self.fake.endpoints[('edge-01', STATUS)] = UNIT
        self.fake.endpoints[('edge-01', READY)] = UNIT
        self.serving = ACTIVE
        self.invalidated = False
        self.fake.health_shapes[('edge-01', UNIT)] = lambda ready, exe: {
            'binary_sha256': exe, 'verified_publication': {'map_sha256': self.serving},
            'runtime_readiness': {'warm': ready, 'invalidated': self.invalidated}}
        self.fleet(active=ACTIVE, serving=('recent-1', 'recent-2'))
        settings = json.loads((ROOT / 'tools/transparent/deploy/history-provider.example.json').read_text())
        settings.update(member_id='https-1', fleet_host='coordinator', fleet_state=FLEET)
        self.inventory = root / 'inventory.json'
        self.inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'edge-01': {}, 'coordinator': {}},
            'services': {'transparent-history': {'roles': {
                'https-recent': [{'host': 'edge-01', 'vars': {'listen': '127.0.0.1:8092'},
                                  'settings': settings, 'install': True}],
                'provider': [{'host': 'edge-01', 'unit': 'transparent-history-provider.service',
                              'vars': {'listen': '127.0.0.1:8099'},
                              'settings': {**settings, 'listen': '127.0.0.1:8099'}}]}}}}))

    def tearDown(self):
        self.dir.cleanup()

    def fleet(self, active, serving, age=0):
        members = {i: {'role': 'recent-replica', 'state': 'serving', 'rendered': True} for i in serving}
        members['https-1'] = {'role': 'https-recent', 'state': 'serving', 'rendered': True}
        self.fake.put('coordinator', FLEET + '/active.json', json.dumps({'map_sha256': active}))
        self.fake.put('coordinator', FLEET + '/membership.json', json.dumps(
            {'updated_unix': time.time() - age, 'members': members}))

    def deployer(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        return Deployer(SERVICE, descriptors.load_inventory(self.inventory), self.fake, self.state,
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH),
                        sleep=sleep, clock=lambda: now[0], out=lambda _: None, only=['https-recent'])

    def unit(self):
        return units.effective([self.fake.read('edge-01', '/etc/systemd/system/' + UNIT)])['Service']

    def touched_state(self):
        return [entry for entry in self.fake.log if STATE in str(entry[2])]

    def install(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        return deployer.deploy(SHA, str(self.binary), skip_exact_check=True)

    def test_install_checks_in_disposable_state_and_rolls_back_to_absence_keeping_state(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        result = Lifecycle(deployer).inspect('preflight', SHA, str(self.binary))
        self.assertEqual(result.problems, ())
        self.assertEqual(self.fake.log, [], 'plan and preflight change nothing')
        self.assertEqual(self.fake.checks, [], 'preflight never runs the loader against live state')

        journal = deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        ((binary, argv),) = self.fake.checks
        self.assertEqual(binary, '/opt/transparent-publisher/releases/%s/transparent-query-server' % SHA)
        scratch = argv[argv.index('--shard-dir') + 1]
        self.assertTrue(scratch.startswith(SERVICE.transaction_dir(journal.id) + '/'))
        self.assertNotIn(STATE, argv)
        self.assertNotIn(scratch, self.fake.host('edge-01').dirs, 'disposable state is removed')
        self.assertEqual(self.touched_state(), [], 'the deploy never creates or writes live state')
        unit = self.unit()
        self.assertEqual((unit['User'], unit['Group']), ('transparent-provider', 'transparent-provider'))
        self.assertIn('--shard-dir ' + STATE, unit['ExecStartPre'][-1])
        self.assertIn('--verify-only', unit['ExecStartPre'][-1])

        self.deployer().rollback(journal.id)
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['load_state'], 'not-found')
        self.assertIn(STATE, self.fake.host('edge-01').dirs)
        self.assertEqual(self.touched_state(), [])

    def test_missing_user_or_foreign_state_is_refused_before_any_effect(self):
        for change in ('user', 'owner', 'mode', 'absent'):
            with self.subTest(change=change):
                self.tearDown()
                self.setUp()
                if change == 'user':
                    self.fake.users.clear()
                elif change == 'owner':
                    self.fake.owner[STATE] = 'root:root:700'
                elif change == 'mode':
                    self.fake.owner[STATE] = 'transparent-provider:transparent-provider:770'
                else:
                    del self.fake.owner[STATE]
                with self.assertRaisesRegex(DeployError, 'creates no (credentials|state)'):
                    self.install()
                self.assertEqual(self.fake.log, [])
                self.assertEqual(self.fake.checks, [])

    def test_candidate_refused_in_disposable_state_changes_no_unit(self):
        self.fake.accepts = lambda binary, argv: False
        with self.assertRaisesRegex(DeployError, 'refused current publications in disposable state'):
            self.install()
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['load_state'], 'not-found')
        self.assertFalse(any(op == 'systemctl' for _, op, _ in self.fake.log))

    def test_readiness_needs_the_exact_active_publication_warm_and_valid(self):
        for case in ('other publication', 'invalidated', 'cold'):
            with self.subTest(case=case):
                self.tearDown()
                self.setUp()
                if case == 'other publication':
                    self.serving = 'cd' * 32
                elif case == 'invalidated':
                    self.invalidated = True
                else:
                    self.fake.unhealthy.add(('edge-01', SHA))
                with self.assertRaisesRegex(DeployError, 'not verified'):
                    self.install()
                self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['load_state'], 'not-found')

    def test_restart_and_restore_keep_two_other_replicas_serving(self):
        self.assertEqual(self.install().status, 'committed')
        self.fleet(active=ACTIVE, serving=('recent-1',))
        with self.assertRaisesRegex(DeployError, 'only 1 other recent replicas'):
            self.deployer().deploy(NEXT_SHA, str(self.next), skip_exact_check=True)
        self.fleet(active=ACTIVE, serving=('recent-1', 'recent-2'), age=120)
        with self.assertRaisesRegex(DeployError, 'membership is'):
            self.deployer().deploy(NEXT_SHA, str(self.next), skip_exact_check=True)
        self.fleet(active=ACTIVE, serving=('recent-1', 'recent-2'))
        journal = self.deployer().deploy(NEXT_SHA, str(self.next), skip_exact_check=True)
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['exe_sha256'], NEXT_SHA)
        self.fleet(active=ACTIVE, serving=('recent-2',))
        log = len(self.fake.log)
        with self.assertRaisesRegex(DeployError, 'rollback refused before any change'):
            self.deployer().rollback(journal.id)
        self.assertEqual(self.fake.log[log:], [])
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['exe_sha256'], NEXT_SHA)

    def test_rollback_after_invalidation_needs_the_previous_binary_to_accept_current_state(self):
        self.install()
        journal = self.deployer().deploy(NEXT_SHA, str(self.next), skip_exact_check=True)
        # The new release observed an invalidation the old one refuses to load past.
        self.fake.accepts = lambda binary, argv: binary == NEXT_SHA
        with self.assertRaisesRegex(DeployError, 'previous executable refuses current publications'):
            self.deployer().rollback(journal.id)
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['exe_sha256'], NEXT_SHA)
        self.assertEqual(self.touched_state(), [], 'state is never restored from a backup')
        self.fake.accepts = lambda binary, argv: True
        self.deployer().rollback(journal.id)
        self.assertEqual(self.fake.probe_unit('edge-01', UNIT)['exe_sha256'], SHA)
        binary, argv = self.fake.checks[-1]
        self.assertEqual(binary, '/opt/transparent-publisher/releases/%s/transparent-query-server' % SHA)
        self.assertNotIn(STATE, argv)
        self.assertEqual(self.touched_state(), [])

    def test_cli_admits_https_recent_and_still_refuses_the_standalone_provider(self):
        base = ['--inventory', str(self.inventory), '--state-dir', str(self.state), '--json']
        output = []
        code = cli.main(base + ['plan', 'transparent-history', '--only', 'https-recent',
                                '--binary', str(self.binary)], executor=self.fake, out=output.append)
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output[-1])['targets'][0]['action'], 'install')
        output.clear()
        code = cli.main(base + ['plan', 'transparent-history', '--only', 'provider',
                                '--binary', str(self.binary)], executor=self.fake, out=output.append)
        self.assertEqual(code, 1)
        self.assertIn('ordinary binary deployment', output[-1])
        self.assertEqual(self.fake.log, [])


if __name__ == '__main__':
    unittest.main()
