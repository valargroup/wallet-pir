"""Locked pir-apm upgrade and alert-mode activation through the deploy wrapper.

Only hosts are fake. Both units run `python3 credential-exec.py <binary>`, whose
running executable is the wrapped binary; each records the modes it starts with in
its incident database, as the services do on every evaluation.
"""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'tools/lib'), str(ROOT / 'tools/tests/operations/deploy')]
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from tools.transparent.lib import alert_activation as aa  # noqa: E402

OLD, NEW = b'pir-apm 5ad1ef61', b'pir-apm next'
MONITOR_BIN = b'pir-monitor'
PYTHON = '/usr/bin/python3'
APM_WRAPPER = '/opt/enhance-pir/releases/observability-20260926/credential-exec.py'
APM_OLD = '/opt/pir-quality/releases/apm-5ad1ef61/pir-apm'
MON_WRAPPER, MON_BIN = '/opt/pir-monitor/credential-exec.py', '/opt/pir-monitor/current/pir-monitor'
CONFIG = '/etc/pir-quality/transparent-production.json'
APM_DROP_INS = [
    ('/etc/systemd/system/pir-apm.service.d/observability.conf',
     '[Service]\nEnvironment=PIR_APM_ALERT_MODE=active\nEnvironment=PIR_APM_STATE_PATH=/var/lib/pir-apm/incidents.sqlite\n'
     'Environment=PIR_APM_TRANSPARENT_CONFIG=%s\n' % CONFIG),
    ('/etc/systemd/system/pir-apm.service.d/credentials.conf',
     '[Service]\nLoadCredentialEncrypted=slack-webhook:/etc/pir-apm/slack.cred\nExecStart=\n'
     'ExecStart=%s %s %s\n' % (PYTHON, APM_WRAPPER, APM_OLD)),
]
MON_DROP_INS = [('/etc/systemd/system/pir-monitor.service.d/90-service-quality.conf',
                 '[Service]\nEnvironment=PIR_MONITOR_SERVICE_ALERT_MODE=shadow\n'
                 'LoadCredential=chain-rpc-cookie:/etc/pir-monitor/credentials/chain-rpc-cookie\n')]
SERVICES = {'host_snapshot': None, 'capacity_alert_mode': 'shadow', 'services': [
    {'service': 'transparent', 'kind': 'history', 'alert_mode': 'shadow', 'public_origins': ['https://t.example']},
    {'service': 'txid', 'kind': 'txid', 'alert_mode': 'shadow'}]}


def environment(config, host):
    """The process environment systemd builds: Environment=, then EnvironmentFile= in order."""
    values = dict(config.get('Service', {}).get('Environment', {}))
    for path in config.get('Service', {}).get('EnvironmentFile', []):
        text = host.files.get(path.lstrip('-'))
        for line in (text or b'').decode().splitlines():
            if '=' in line:
                key, value = line.split('=', 1)
                values[key] = value
    return values


class WrappedFleet(FakeFleet):
    """Units started through credential-exec.py run the wrapped binary; each records its started modes."""

    def __init__(self):
        super().__init__()
        self.modes = {}            # (host, state path) -> metadata modes the running service recorded
        self.delivery = {}         # (host, unit) -> delivery health
        self.deliver = True        # whether the running worker drains a queued test
        self.enqueued = []
        self.lost_restart = set()     # (host, unit): restart happens but its reply is lost
        self.dropped_restart = set()  # (host, unit): the request is lost before systemd sees it

    def _start(self, host, unit):
        h = self.host(host)
        state = h.units[unit]
        config = units.effective(t for _, t in state['loaded'])
        _, interpreter, tail = units.split_exec(units.exec_start(config))
        words = tail.split()
        binary = words[1] if words and words[0].endswith('credential-exec.py') else interpreter
        if (host, unit) in self.failing_restart or binary not in h.files:
            state.update(active='failed', pid=0, exe=None, exec_start=None)
            return
        self.invocations += 1
        state.update(active='active', pid=state['pid'] + 1000, exe=sha256(h.files[binary]),
                     invocation_id='%032x' % self.invocations)
        env = environment(config, h)
        if unit == 'pir-apm.service':
            document = json.loads(h.files[env['PIR_APM_TRANSPARENT_CONFIG']])
            modes = {'mode': env['PIR_APM_ALERT_MODE']}
            modes.update({'mode:quality-' + s['service']: s.get('alert_mode', 'shadow') for s in document['services']})
            self.modes[(host, env['PIR_APM_STATE_PATH'])] = modes
        else:
            self.modes[(host, '/var/lib/pir-monitor/incidents.sqlite')] = {
                'mode': env['PIR_MONITOR_ALERT_MODE'], 'mode:service-quality': env['PIR_MONITOR_SERVICE_ALERT_MODE']}

    def systemctl(self, host, *args):
        if args[0] == 'restart' and (host, args[1]) in self.dropped_restart:
            self.dropped_restart.discard((host, args[1]))
            raise RuntimeError('ssh: connection reset before restart')
        super().systemctl(host, *args)
        if args[0] == 'restart' and (host, args[1]) in self.lost_restart:
            self.lost_restart.discard((host, args[1]))
            raise RuntimeError('ssh: connection reset after restart was sent')

    def run(self, host, argv, timeout):
        if argv[:2] == [PYTHON, '-c'] and argv[2] == aa.MODE_QUERY:
            modes = self.modes.get((host, argv[3]))
            return (0, json.dumps(modes)) if modes is not None else (1, 'unable to open database file')
        if '--enqueue-test-alert' in argv:
            self.runs.append((host, list(argv)))
            unit = 'pir-apm.service' if host == 'coordinator' else 'pir-monitor.service'
            self.enqueued.append((host, argv))
            delivery = self.delivery[(host, unit)]
            delivery['pending'] += 2
            if self.deliver:
                delivery.update(pending=0, last_success=delivery['last_success'] + 10)
            else:
                delivery['failure'] = 'webhook answered 500'
            return 0, 'Queued labeled test firing/recovery for durable delivery'
        return super().run(host, argv, timeout)


class Activation(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.binary = root / 'state', root / 'pir-apm'
        self.binary.write_bytes(NEW)
        f = self.fake = WrappedFleet()
        f.install('coordinator', 'pir-apm.service',
                  '[Service]\nUser=pir-apm\nExecStart=/usr/local/bin/pir-apm\nEnvironment=PIR_APM_LISTEN=127.0.0.1:3002\n',
                  drop_ins=APM_DROP_INS,
                  files={PYTHON: b'python', APM_WRAPPER: b'wrapper', APM_OLD: OLD,
                         CONFIG: json.dumps(SERVICES, indent=1)})
        f.install('monitor', 'pir-monitor.service',
                  '[Service]\nUser=pir-monitor\nEnvironmentFile=/etc/pir-monitor/monitor.env\n'
                  'Environment=PIR_MONITOR_ALERT_MODE=shadow\nExecStart=%s %s %s\n' % (PYTHON, MON_WRAPPER, MON_BIN),
                  drop_ins=MON_DROP_INS,
                  files={PYTHON: b'python', MON_WRAPPER: b'wrapper', MON_BIN: MONITOR_BIN,
                         '/etc/pir-monitor/monitor.env': 'PIR_MONITOR_ALERT_MODE=active\n'})
        for host, unit, listen in (('coordinator', 'pir-apm.service', '127.0.0.1:3002'),
                                   ('monitor', 'pir-monitor.service', '127.0.0.1:3003')):
            f.endpoints[(host, 'http://%s/monitor-status' % listen)] = unit
            f.delivery[(host, unit)] = {'configured': True, 'pending': 0, 'last_success': 100, 'failure': None}
            f.health_shapes[(host, unit)] = lambda ready_, exe, key=(host, unit): {
                'progress_ok': True, 'delivery': dict(self.fake.delivery[key])}
        self.inventory = root / 'inventory.json'
        self.inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'coordinator': {}, 'monitor': {'jump': 'coordinator'}}, 'services': {}}))
        self.request = root / 'request.json'
        self.write_request()

    def tearDown(self):
        self.dir.cleanup()

    def component(self, host, unit, wrapper, listen, state_path):
        state = self.fake.probe_unit(host, unit)
        return {'host': host, 'unit': unit, 'unit_files_sha256': aa.unit_files_digest(state),
                'wrapper_sha256': self.fake.sha256(host, wrapper), 'running_sha256': state['exe_sha256'],
                'state_path': state_path, 'listen': listen, 'environment': 'production'}

    def write_request(self, change=lambda r: None):
        apm = self.component('coordinator', 'pir-apm.service', APM_WRAPPER, '127.0.0.1:3002',
                             '/var/lib/pir-apm/incidents.sqlite')
        apm['release'] = {'git_sha': '1' * 40, 'binary_sha256': sha256(NEW)}
        apm['config'] = {'path': CONFIG, 'previous_sha256': self.fake.sha256('coordinator', CONFIG),
                         'alert_modes': {'transparent': 'active', 'txid': 'active'}}
        monitor = self.component('monitor', 'pir-monitor.service', MON_WRAPPER, '127.0.0.1:3003',
                                 '/var/lib/pir-monitor/incidents.sqlite')
        monitor['service_alert_mode'] = 'active'
        request = {'schema': aa.SCHEMA, 'apm': apm, 'monitor': monitor}
        change(request)
        self.request.write_bytes(json.dumps(request).encode())

    def run_cli(self, action, *extra):
        lines, now = [], [0.0]

        def sleep(seconds):
            now[0] += seconds
        code = cli.main(['--inventory', str(self.inventory), '--state-dir', str(self.state),
                         'transparent-alerts-' + action, '--request', str(self.request),
                         '--request-sha256', hashlib.sha256(self.request.read_bytes()).hexdigest(), *extra],
                        executor=self.fake, out=lines.append, sleep=sleep, clock=lambda: now[0],
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH))
        return code, lines

    def running(self):
        return (self.fake.probe_unit('coordinator', 'pir-apm.service')['exe_sha256'],
                self.fake.modes[('coordinator', '/var/lib/pir-apm/incidents.sqlite')],
                self.fake.modes[('monitor', '/var/lib/pir-monitor/incidents.sqlite')])

    def files(self):
        return {h: dict(self.fake.host(h).files) for h in ('coordinator', 'monitor')}

    def test_plan_and_preflight_change_nothing_and_report_running_state(self):
        before = self.files()
        for action in ('plan', 'preflight'):
            code, lines = self.run_cli(action, *(['--binary', str(self.binary)] if action == 'preflight' else []))
            self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.mutations(), [])
        self.assertEqual(self.files(), before)
        text = '\n'.join(lines)
        self.assertIn('apm running modes: mode=active, mode:quality-transparent=shadow, mode:quality-txid=shadow', text)
        self.assertIn('monitor running modes: mode=active, mode:service-quality=shadow', text)
        self.assertIn('apm delivery: webhook configured=True pending=0', text)
        self.assertIn('write %s' % aa.MONITOR_ENV, text)

    def test_deploy_upgrades_apm_and_activates_only_the_named_modes(self):
        before = self.files()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        exe, apm, monitor = self.running()
        self.assertEqual(exe, sha256(NEW))
        self.assertEqual(apm, {'mode': 'active', 'mode:quality-transparent': 'active', 'mode:quality-txid': 'active'})
        self.assertEqual(monitor, {'mode': 'active', 'mode:service-quality': 'active'})
        state = self.fake.probe_unit('coordinator', 'pir-apm.service')
        self.assertEqual(aa.wrapped_exec(aa.unit_texts(state))[2:4], (APM_WRAPPER, aa.apm_binary(sha256(NEW))))
        after = self.files()
        # Unit fragments, credential drop-ins and wrappers are unchanged; nothing touches the databases.
        for host, paths in (('coordinator', [p for p, _ in APM_DROP_INS] + [APM_WRAPPER, APM_OLD]),
                            ('monitor', [p for p, _ in MON_DROP_INS] + [MON_WRAPPER, MON_BIN, '/etc/pir-monitor/monitor.env'])):
            for path in paths:
                self.assertEqual(after[host][path], before[host][path], path)
        self.assertFalse([m for m in self.fake.mutations() if 'sqlite' in str(m[2])])
        config = json.loads(after['coordinator'][CONFIG])
        expected = json.loads(before['coordinator'][CONFIG])
        for service in expected['services']:
            service['alert_mode'] = 'active'
        self.assertEqual(config, expected)
        # The reviewed request pins the pre-activation state, so a repeat refuses unchanged.
        self.fake.log.clear()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertNotEqual(code, 0)
        self.assertIn('unit files differ from the pinned unit_files_sha256', '\n'.join(lines))
        self.assertEqual(self.fake.mutations(), [])

    def test_changed_pins_refuse_under_the_lock_before_any_change(self):
        self.fake.put('coordinator', CONFIG, json.dumps(SERVICES))  # reformatted since review
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 1)
        self.assertIn('differs from config.previous_sha256', '\n'.join(lines))
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertNotEqual(code, 0)
        self.assertEqual([m for m in self.fake.mutations() if m[1] != 'mkdir'], [])

    def test_refuses_a_unit_without_the_credential_wrapper(self):
        self.fake.edit('coordinator', APM_DROP_INS[1][0], '[Service]\nExecStart=\nExecStart=%s\n' % APM_OLD)
        self.fake.systemctl('coordinator', 'restart', 'pir-apm.service')
        self.write_request()
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 1)
        self.assertIn('ExecStart is not python3 credential-exec.py', '\n'.join(lines))

    def test_monitor_failure_restores_both_components(self):
        before = self.files()
        self.fake.failing_restart.add(('monitor', 'pir-monitor.service'))
        original = self.fake.systemctl

        def heal(host, *args):
            # The restore restart succeeds again.
            if args[0] == 'restart' and host == 'monitor' and self.fake.host('monitor').files.get(aa.MONITOR_DROP_IN) is None:
                self.fake.failing_restart.discard(('monitor', 'pir-monitor.service'))
            return original(host, *args)
        self.fake.systemctl = heal
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertNotEqual(code, 0, lines)
        exe, apm, monitor = self.running()
        self.assertEqual(exe, sha256(OLD))
        self.assertEqual(apm['mode:quality-transparent'], 'shadow')
        self.assertEqual(monitor['mode:service-quality'], 'shadow')
        after = self.files()
        for host in after:
            unchanged = {p: d for p, d in after[host].items() if '/releases/' not in p}
            self.assertEqual(unchanged, {p: d for p, d in before[host].items() if '/releases/' not in p})
        journal = json.loads(next(self.state.glob('transparent-alerts-2*.json')).read_text())
        self.assertEqual(journal['status'], 'rolled-back')

    def test_lost_restart_reply_is_observed_not_assumed(self):
        # The restart happened: verification observes the new process and continues.
        self.fake.lost_restart.add(('coordinator', 'pir-apm.service'))
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.running()[0], sha256(NEW))
        journal = json.loads(next(self.state.glob('transparent-alerts-2*.json')).read_text())
        self.assertIn('restart outcome uncertain', [e['message'] for e in journal['events']])

    def test_restart_that_never_happened_fails_verification_and_rolls_back(self):
        self.fake.dropped_restart.add(('coordinator', 'pir-apm.service'))
        code, lines = self.run_cli('deploy', '--binary', str(self.binary))
        self.assertNotEqual(code, 0, lines)
        exe, apm, _ = self.running()
        self.assertEqual((exe, apm['mode:quality-txid']), (sha256(OLD), 'shadow'))
        self.assertIsNone(self.fake.host('monitor').files.get(aa.MONITOR_DROP_IN))
        self.assertEqual(json.loads(self.fake.host('coordinator').files[CONFIG])['services'][1]['alert_mode'], 'shadow')

    def test_explicit_rollback_restores_a_committed_activation(self):
        before = self.files()
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary))[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        exe, apm, monitor = self.running()
        self.assertEqual((exe, apm['mode:quality-transparent'], monitor['mode:service-quality']),
                         (sha256(OLD), 'shadow', 'shadow'))
        after = self.files()
        self.assertEqual({p: d for p, d in after['monitor'].items()}, before['monitor'])
        # A file edited after the transaction makes rollback refuse rather than overwrite it.
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary))[0], 0)
        self.fake.put('coordinator', CONFIG, '{}')
        code, lines = self.run_cli('rollback')
        self.assertNotEqual(code, 0)
        self.assertEqual(self.fake.host('coordinator').files[CONFIG], b'{}')

    def test_test_alert_uses_each_existing_outbox_and_waits_for_delivery(self):
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary))[0], 0)
        for component, host, user, state in (('apm', 'coordinator', 'pir-apm', '/var/lib/pir-apm/incidents.sqlite'),
                                             ('monitor', 'monitor', 'pir-monitor', '/var/lib/pir-monitor/incidents.sqlite')):
            code, lines = self.run_cli('test', '--component', component, '--binary', str(self.binary))
            self.assertEqual(code, 0, lines)
            argv = self.fake.enqueued[-1][1]
            self.assertEqual(argv[:4], ['/usr/sbin/runuser', '-u', user, '--'])
            self.assertIn('PIR_APM_STATE_PATH=' + state, argv)
            self.assertEqual(self.fake.sha256(host, argv[-2]), sha256(NEW))
        self.assertEqual(self.fake.host('monitor').files[aa.monitor_tool(sha256(NEW))], NEW)
        # Undelivered: reported, left queued, nothing restarted.
        self.fake.deliver = False
        invocation = self.fake.probe_unit('monitor', 'pir-monitor.service')['invocation_id']
        code, lines = self.run_cli('test', '--component', 'monitor')
        self.assertNotEqual(code, 0)
        self.assertEqual(self.fake.delivery[('monitor', 'pir-monitor.service')]['pending'], 2)
        self.assertEqual(self.fake.probe_unit('monitor', 'pir-monitor.service')['invocation_id'], invocation)

    def test_unreachable_status_and_database_are_unknown(self):
        self.fake.modes.clear()
        self.fake.systemctl('coordinator', 'stop', 'pir-apm.service')
        code, lines = self.run_cli('status')
        text = '\n'.join(lines)
        self.assertEqual(code, 1)
        self.assertIn('apm running modes: unknown (incident database unreadable)', text)
        self.assertIn('apm delivery: unknown (monitor-status unreachable or invalid)', text)


if __name__ == '__main__':
    unittest.main()
