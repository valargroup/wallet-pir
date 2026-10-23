"""The unified site joins the live coordinator Caddyfile without disturbing it.

The live file before the transaction is the rendered template without the
unified site, as installed before that site existed. Candidates are checked in
a real `caddy adapt` when Caddy is on PATH.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from tools.status.lib.routing import candidate as status_candidate
from wallet_pir_ops.deploy import coordinator_edge, descriptors
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.transaction import Journal
from wallet_pir_ops.deploy.unified_edge import CONFIG, DNS_SOURCE, HOST, REFUSED, SERVED, UnifiedEdge, candidate, digest

ENHANCE_URL = 'https://enhance-pir.valargroup.dev'
TEMPLATE = coordinator_edge.TEMPLATE.read_text()
RENDERED = coordinator_edge.render(TEMPLATE, ENHANCE_URL)
_UNIFIED = coordinator_edge.site_span(RENDERED, HOST)
LIVE = RENDERED[:_UNIFIED[0]].rstrip('\n') + '\n' + '# operator note kept verbatim\n'
REQUEST = dict(schema='coordinator-unified-edge/1', host='coordinator', machine_id='1' * 32,
               enhance_public_url=ENHANCE_URL)


class LostRunner(BaseException):
    pass


class Host:
    def __init__(self, live=LIVE):
        self.files = {CONFIG: live.encode()}
        self.failure = None
        self.writes = []
        self.held = False
        self.reloads = 0
        self.codes = {path: '200' for path in SERVED} | {path: '404' for path in REFUSED}

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
        return dict(active_state='active', drop_ins=[],
                    fragment_text='[Service]\nExecStart=/usr/bin/caddy run --config /etc/caddy/Caddyfile\n')

    def run(self, host, argv, timeout):
        if argv[0] == 'cat':
            return 0, REQUEST['machine_id']
        if argv[0] == 'stat':
            return 0, '644:0:0'
        if argv[0] == 'curl':
            assert argv[argv.index('--resolve') + 1] == HOST + ':443:127.0.0.1'
            path = argv[-1].removeprefix('https://' + HOST)
            served = HOST in coordinator_edge.sites(self.read(host, CONFIG))
            return 0, self.codes[path] if served else '000'
        if self.failure == 'validate':
            self.failure = None
            return 1, 'injected validate failure'
        return 0, 'valid'

    def systemctl(self, host, *args):
        self.verify()
        assert args == ('reload', 'caddy.service')
        self.reloads += 1
        if self.failure == 'reload':
            self.failure = None
            raise DeployError('injected reload failure')


class UnifiedEdgeTransaction(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = Path(self.temp.name) / 'inventory.json'
        path.write_text(json.dumps(dict(hosts={'coordinator': {'address': '192.0.2.1'}}, ssh={'mode': 'config'},
                                        lock={'type': 'remote', 'host': 'coordinator'}, services={})))
        self.inventory = descriptors.load_inventory(path)
        self.host = Host()
        self.edge = self.make(self.host)

    def make(self, host):
        return UnifiedEdge(self.inventory, host, self.temp.name, REQUEST, lock=host.lock, sleep=lambda _s: None)

    def plan(self, edge=None):
        return digest(json.dumps((edge or self.edge).plan()[0], sort_keys=True))

    def test_adds_only_the_unified_site_and_restores_the_exact_previous_file(self):
        result = self.edge.deploy(self.plan())
        self.assertEqual(result['phase'], 'committed')
        after = self.host.read('coordinator', CONFIG)
        self.assertTrue(after.startswith(LIVE))
        self.assertEqual(after[slice(*coordinator_edge.site_span(after, HOST))], RENDERED[slice(*_UNIFIED)])
        self.assertEqual(set(coordinator_edge.sites(after)), {'enhance-pir.valargroup.dev', HOST})
        self.edge.rollback(result['transaction'])
        self.assertEqual(self.host.read('coordinator', CONFIG), LIVE)
        self.assertEqual(Journal.load(self.temp.name, 'coordinator-edge').status, 'rolled-back')

    def test_a_second_run_is_unchanged_and_an_edited_site_is_replaced_alone(self):
        self.edge.deploy(self.plan())
        installed = self.host.read('coordinator', CONFIG)
        reloads = self.host.reloads
        self.assertEqual(self.edge.deploy(self.plan())['phase'], 'unchanged')
        self.assertEqual((self.host.read('coordinator', CONFIG), self.host.reloads), (installed, reloads))
        drifted = installed.replace('\thandle /receiver/v1/* {', '\thandle /drift/* {\n\t\trespond 410\n\t}\n'
                                    '\thandle /receiver/v1/* {')
        host = Host(drifted)
        edge = self.make(host)
        self.assertEqual(edge.deploy(self.plan(edge))['phase'], 'committed')
        self.assertEqual(host.read('coordinator', CONFIG), installed)

    def test_validation_reload_and_probe_failures_restore_the_previous_file(self):
        for failure in ('validate', 'reload', 'probe'):
            with self.subTest(failure=failure):
                host = Host()
                edge = self.make(host)
                plan = self.plan(edge)
                if failure == 'probe':
                    host.codes['/metrics'] = '200'
                else:
                    host.failure = failure
                with self.assertRaises(DeployError):
                    edge.deploy(plan)
                self.assertEqual(host.read('coordinator', CONFIG), LIVE)

    def test_forward_only_failure_keeps_routes_and_verifies_without_reloading(self):
        plan = self.plan()
        self.host.codes['/metrics'] = '200'
        with self.assertRaises(DeployError):
            self.edge.deploy(plan, preserve_on_failure=True)
        journal = Journal.load(self.temp.name, 'coordinator-edge')
        self.assertEqual(journal.status, 'verification-failed')
        self.assertIn(HOST, coordinator_edge.sites(self.host.read('coordinator', CONFIG)))
        self.assertEqual(self.host.reloads, 1)
        self.host.codes['/metrics'] = '404'
        self.assertEqual(self.edge.verify(journal.id)['phase'], 'committed')
        self.assertEqual(self.host.reloads, 1)
        self.host.files[CONFIG] += b'# foreign change\n'
        with self.assertRaisesRegex(DeployError, 'Caddyfile differs'):
            self.edge.verify(journal.id)

    def test_dns_source_staging_keeps_serving_and_foreign_source_unchanged(self):
        plan, source = self.edge.dns_plan()
        self.edge.stage_dns(digest(json.dumps(plan, sort_keys=True)))
        self.assertEqual(self.host.read('coordinator', DNS_SOURCE), source)
        self.assertIn('content = "192.0.2.1"', source)
        self.assertEqual(self.host.read('coordinator', CONFIG), LIVE)
        self.assertEqual(self.host.reloads, 0)
        self.host.files[DNS_SOURCE] = b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'DNS source changed'):
            self.edge.dns_plan()

    def test_changed_plan_and_foreign_edits_refuse_without_overwriting_them(self):
        plan = self.plan()
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'plan changed'):
            self.edge.deploy(plan)
        self.assertEqual(self.host.writes, [])
        self.host.files[CONFIG] = LIVE.encode()
        result = self.edge.deploy(self.plan())
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'outside this transaction'):
            self.edge.rollback(result['transaction'])
        self.assertTrue(self.host.files[CONFIG].endswith(b'# another operator\n'))

    def test_interruption_after_installation_is_recovered_from_the_journal(self):
        plan = self.plan()
        self.host.failure = 'lost-runner'
        with self.assertRaises(LostRunner):
            self.edge.deploy(plan)
        journal = Journal.load(self.temp.name, 'coordinator-edge')
        self.assertEqual(journal.status, 'installing')
        with self.assertRaisesRegex(DeployError, 'interrupted'):
            self.edge.deploy(self.plan())
        self.edge.rollback(journal.id)
        self.assertEqual(self.host.read('coordinator', CONFIG), LIVE)

    def test_refuses_a_file_that_is_not_the_coordinator_edge(self):
        for live in ('other.example {\n\trespond 404\n}\n', LIVE.replace('{\n', '{{\n', 1)):
            with self.subTest(live=live[:20]), self.assertRaises(DeployError):
                candidate(live, TEMPLATE, ENHANCE_URL)
        with self.assertRaises(DeployError):
            candidate(LIVE, TEMPLATE, 'https://' + HOST)

    def test_later_status_cutover_and_rerun_keep_the_installed_unified_site(self):
        installed = candidate(LIVE, TEMPLATE, ENHANCE_URL)
        unified = installed[slice(*coordinator_edge.site_span(installed, HOST))]
        rerouted = status_candidate(installed.replace('127.0.0.1:8492', '127.0.0.1:8489'))
        self.assertEqual(rerouted, installed)
        self.assertEqual(rerouted[slice(*coordinator_edge.site_span(rerouted, HOST))], unified)
        self.assertEqual(candidate(rerouted, TEMPLATE, ENHANCE_URL), installed)

    @unittest.skipUnless(shutil.which('caddy'), 'caddy is not installed')
    def test_real_caddy_adapts_the_candidate_with_both_sites(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'Caddyfile'
            path.write_text(candidate(LIVE, TEMPLATE, ENHANCE_URL))
            result = subprocess.run([shutil.which('caddy'), 'adapt', '--config', str(path), '--adapter', 'caddyfile'],
                                    capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr[-2000:])
            hosts = {host for server in json.loads(result.stdout)['apps']['http']['servers'].values()
                     for route in server.get('routes', []) for match in route.get('match', [])
                     for host in match.get('host', [])}
            self.assertEqual(hosts, {'enhance-pir.valargroup.dev', HOST})


if __name__ == '__main__':
    unittest.main()
