"""Status cutover preserves other routes and recovers failures under the fleet lock."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.status.lib.routing import CONFIG, Routing, candidate, digest
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.transaction import Journal

BEFORE = 'wallet.example {\n handle /v1/status/* {\n  reverse_proxy 127.0.0.1:8489\n }\n handle /v1/enhance/* {\n  reverse_proxy 127.0.0.1:8080\n }\n}\n'
PROBE = b'independent probe'
PROBE_SHA = hashlib.sha256(PROBE).hexdigest()
CLIENT = '/opt/status-pir/releases/' + PROBE_SHA + '/status-pir'
REQUEST = dict(schema='status-wallet-route/1', host='coordinator', machine_id='1' * 32,
               router_host='status-01', router_sha256='2' * 64, public_origin='https://wallet.example')


class LostRunner(BaseException):
    pass


class Host:
    def __init__(self):
        self.files = {CONFIG: BEFORE.encode(), CLIENT: PROBE}
        self.failure = None
        self.writes = []
        self.held = False

    @contextmanager
    def lock(self):
        self.held = True
        try:
            yield self
        finally:
            self.held = False

    def verify(self):
        assert self.held

    def check_identity(self, host):
        pass

    def read(self, host, path):
        data = self.files.get(path)
        return data.decode() if data else None

    def sha256(self, host, path):
        return hashlib.sha256(self.files[path]).hexdigest()

    def mkdir(self, host, path, mode):
        self.verify()

    def write(self, host, path, data, mode):
        self.verify()
        self.writes.append(path)
        self.files[path] = data
        if path == CONFIG and self.failure == 'lost-runner':
            self.failure = None
            raise LostRunner()

    def probe_unit(self, host, unit):
        return dict(active_state='active', exe_sha256=REQUEST['router_sha256'], drop_ins=[],
                    fragment_text='[Service]\nExecStart=/usr/bin/caddy run --config /etc/caddy/Caddyfile\n')

    def http_get(self, host, url):
        return 200, '{}'

    def run(self, host, argv, timeout):
        if argv[0] == 'cat':
            return 0, REQUEST['machine_id']
        if argv[0] == 'stat':
            return 0, '644:0:0'
        kind = 'validate' if argv[0] == 'python3' else 'public' if argv[0] == CLIENT else 'exact'
        if self.failure == kind:
            self.failure = None
            return 1, 'injected ' + kind + ' failure'
        return 0, 'verified'

    def systemctl(self, host, *args):
        self.verify()
        assert args == ('reload', 'caddy.service')
        if self.failure == 'reload':
            self.failure = None
            raise DeployError('injected reload failure')


class StatusRouting(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = Path(self.temp.name) / 'inventory.json'
        path.write_text(json.dumps(dict(hosts={'coordinator': {}, 'status-01': {}}, ssh={'mode': 'config'},
            lock={'type': 'remote', 'host': 'coordinator'}, services={'status': {
                'roles': {'router': [{'host': 'status-01'}]}, 'route_probe_client': CLIENT,
                'exact_check': {'host': 'coordinator', 'argv': ['exact'], 'timeout': 10}}})))
        self.host = Host()
        self.routing = Routing(descriptors.load_inventory(path), self.host, self.temp.name, REQUEST,
                               lock=self.host.lock)

    def plan(self):
        return digest(json.dumps(self.routing.plan()[0], sort_keys=True))

    def test_changes_only_status_and_can_restore_the_exact_previous_configuration(self):
        result = self.routing.deploy(self.plan())
        self.assertEqual(result['phase'], 'committed')
        self.assertEqual(self.host.read('coordinator', CONFIG), BEFORE.replace(':8489', ':8492'))
        self.routing.rollback(result['transaction'])
        self.assertEqual(self.host.read('coordinator', CONFIG), BEFORE)
        self.assertEqual(Journal.load(self.temp.name, 'status-routing').status, 'rolled-back')

    def test_validation_reload_and_probe_failures_preserve_or_restore_the_old_route(self):
        for failure in ('validate', 'reload', 'public', 'exact'):
            with self.subTest(failure=failure):
                self.host.failure = failure
                restore = self.routing.restore
                def restore_after_alerts(journal):
                    resumed.assert_called_once_with()
                    return restore(journal)
                with patch('wallet_pir_ops.deploy.deployment_alerts.failed') as resumed, \
                        patch.object(self.routing, 'restore', side_effect=restore_after_alerts), \
                        self.assertRaises(DeployError):
                    self.routing.deploy(self.plan())
                self.assertEqual(self.host.read('coordinator', CONFIG), BEFORE)

    def test_changed_plan_and_foreign_edits_refuse_without_overwriting_them(self):
        plan = self.plan()
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'plan changed'):
            self.routing.deploy(plan)
        self.assertEqual(self.host.writes, [])
        self.host.files[CONFIG] = BEFORE.encode()
        result = self.routing.deploy(self.plan())
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'outside this transaction'):
            self.routing.rollback(result['transaction'])
        self.assertTrue(self.host.files[CONFIG].endswith(b'# another operator\n'))

    def test_interruption_after_file_installation_can_be_recovered_from_the_journal(self):
        plan = self.plan()
        self.host.failure = 'lost-runner'
        with self.assertRaises(LostRunner):
            self.routing.deploy(plan)
        journal = Journal.load(self.temp.name, 'status-routing')
        self.assertEqual(journal.status, 'installing')
        self.assertEqual(self.host.read('coordinator', CONFIG), candidate(BEFORE))
        self.routing.rollback(journal.id)
        self.assertEqual(self.host.read('coordinator', CONFIG), BEFORE)

    def test_canonical_alias_moves_with_the_legacy_route(self):
        canonical = ' handle /status/v1/* {\n  reverse_proxy 127.0.0.1:8489\n }\n'
        text = BEFORE.replace(' handle /v1/enhance/*', canonical + ' handle /v1/enhance/*')
        self.assertEqual(candidate(text), text.replace(':8489', ':8492'))
        foreign = canonical.replace(':8489', ':9000')
        for refused in (text.replace(canonical, canonical + canonical),
                        BEFORE.replace(' handle /v1/enhance/*', foreign + ' handle /v1/enhance/*'),
                        text.replace('/v1/status/*', '/other/*')):
            with self.subTest(text=refused), self.assertRaises(DeployError):
                candidate(refused)

    def test_ambiguous_or_unrecognized_routes_are_refused(self):
        for text in (BEFORE + BEFORE, BEFORE.replace(':8489', ':9000'), BEFORE.replace('/v1/status/*', '/other/*')):
            with self.subTest(text=text), self.assertRaises(DeployError):
                candidate(text)


if __name__ == '__main__':
    unittest.main()
