"""Product host transitions: complete rollback state and fail-closed recovery."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('schema_host', Path(__file__).parents[1]/'lib/activity_schema_host.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def worker_plan():
    files = [{'path': p, 'required': True} for p in sorted(M.required_files('worker'))]
    files += [{'path': p+'.d', 'required': False} for p in sorted(M.unit_paths(M.UNITS['worker']))]
    files += [{'path': p, 'required': False} for p in ('/opt/transparent-publisher/active.invalid.json',
              '/usr/local/lib/transparent-pir/storage-policy.py', '/usr/local/lib/transparent-pir/headless-console.py')]
    txn = 'transparent-schema-20261001-proof'
    return {'version': 1, 'role': 'worker', 'machine_id': 'a'*32, 'source_sha': 'b'*40,
        'transaction': txn, 'baseline_root': '/opt/transparent-publisher/schema-rollback/'+txn,
        'baseline': {'version': 1, 'files': files, 'retained': [{'path': '/srv/transparent-pir/old',
                    'sentinel': '/srv/transparent-pir/old/shards.json', 'sha256': 'c'*64}]},
        'installs': [{'source': '/srv/transparent-activity/staged/'+name, 'target': '/usr/local/bin/'+name,
                    'sha256': 'd'*64, 'mode': 0o755} for name in M.BINARIES['worker']] +
                    [{'source': '/srv/transparent-activity/staged/worker.service',
                      'target': '/etc/systemd/system/'+M.WORKER, 'sha256': 'e'*64, 'mode': 0o644}],
        'worker': {'id': 'transparent-pir-recent-01', 'directory': '/srv/transparent-pir/v11/publications/'+'f'*64,
                   'assignment': '/srv/transparent-pir/v11/publications/'+'f'*64+'/assignment.json',
                   'map_sha256': 'f'*64, 'binary_sha256': 'd'*64, 'assignment_sha256': 'd'*64}}


class PlanTests(unittest.TestCase):
    def test_reviewed_plan_requires_all_product_state(self):
        plan = worker_plan()
        M.validate(plan)
        for path in sorted(M.required_files('worker')):
            bad = copy.deepcopy(plan)
            bad['baseline']['files'] = [i for i in bad['baseline']['files'] if i['path'] != path]
            with self.assertRaisesRegex(ValueError, 'omits'):
                M.validate(bad)
        for field in ('/opt/transparent-publisher/active.invalid.json', '/etc/systemd/system/'+M.WORKER+'.d',
                      '/usr/local/lib/transparent-pir/storage-policy.py'):
            bad = copy.deepcopy(plan)
            bad['baseline']['files'] = [i for i in bad['baseline']['files'] if i['path'] != field]
            with self.assertRaisesRegex(ValueError, 'omits'):
                M.validate(bad)

    def test_plan_cannot_mutate_routing_quality_or_old_cache(self):
        for target in ('/etc/caddy/Caddyfile', '/etc/systemd/system/'+M.QUALITY,
                       '/opt/transparent-publisher/active.json', '/srv/transparent-pir/runtime-cache/sentinel'):
            bad = worker_plan()
            bad['installs'][0]['target'] = target
            with self.assertRaisesRegex(ValueError, 'unsupported/duplicate'):
                M.validate(bad)
        for directory in ('/srv/transparent-pir/publications/'+'f'*64,
                          '/srv/transparent-pir/v11/publications/../'+'f'*64):
            bad = worker_plan()
            bad['worker']['directory'] = directory
            with self.assertRaises(ValueError):
                M.validate(bad)

    def test_duplicate_install_wrong_mode_and_binary_disagreement_refuse(self):
        for mutate in (lambda p: p['installs'].append(p['installs'][0]),
                       lambda p: p['installs'][0].update(mode=0o600),
                       lambda p: p['worker'].update(binary_sha256='1'*64)):
            p = worker_plan()
            mutate(p)
            with self.assertRaises(ValueError):
                M.validate(p)

    def test_unit_requires_v11_static_control_and_cache_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = worker_plan()
            unit = Path(tmp)/'worker.service'
            plan['installs'][-1]['source'] = str(unit)
            host = M.Host(plan)
            w = plan['worker']
            args = ['/usr/local/bin/transparent-shard-server', '--shard-dir', w['directory'],
                    '--assignment', w['assignment'], '--worker-id', w['id'],
                    '--active-record', '/opt/transparent-publisher/v11/active.json',
                    '--runtime-cache-dir', '/srv/transparent-pir/v11/runtime-cache',
                    '--control-socket', '/run/transparent-pir/control.sock']
            text = '[Service]\nRuntimeDirectory=transparent-pir\nExecStart='+M.shlex.join(args)+'\n'
            unit.write_text(text)
            host.validate_units()
            for wrong in (text.replace('/v11/active.json', '/active.json'),
                          text.replace('/v11/runtime-cache', '/runtime-cache'),
                          text.replace(w['directory'], '/srv/transparent-pir/publications/old'),
                          text+'ExecStart=/usr/local/bin/transparent-shard-server\n',
                          text.replace('--worker-id '+w['id'], '--worker-id '+w['id']+' --worker-id other')):
                unit.write_text(wrong)
                with self.assertRaises(ValueError):
                    host.validate_units()

    def test_deferred_state_cannot_resume_scaling_load_or_caddy(self):
        for path in ('/etc/caddy/Caddyfile', '/etc/caddy/Caddyfile.live-previous',
                     '/opt/transparent-publisher/scaler/policy.json',
                     '/opt/transparent-publisher/scaler/disabled', '/etc/pir-quality/qualified-workers.json',
                     '/opt/transparent-5qps-20260929'):
            self.assertTrue(M.deferred('coordinator', path))
        self.assertFalse(M.deferred('coordinator', '/opt/transparent-publisher/state'))

    def test_native_control_envelope_is_decoded(self):
        c = M.Commands()
        with patch.object(c, 'run', return_value=b'{"ok":true,"result":{"warm":true}}'):
            self.assertEqual(c.control(), {'warm': True})
        for data in (b'{"ok":false,"result":{}}', b'{"warm":true}', b'{"ok":true,"result":null}'):
            with patch.object(c, 'run', return_value=data), self.assertRaises(ValueError):
                c.control()


    def test_predecessor_static_dynamic_and_cache_namespaces_are_retained(self):
        plan = worker_plan()
        old = '/srv/transparent-pir/old'
        args = ['/usr/local/bin/transparent-shard-server', '--shard-dir', old+'/initial',
                '--assignment', old+'/assignment.json', '--runtime-cache-dir', old+'/cache',
                '--active-record', '/opt/transparent-publisher/active.json']
        active = {'directory': old+'/current', 'assignment': old+'/current/assignment.json', 'map_sha256': '1'*64}
        host = M.Host(plan)
        text = 'ExecStart='+M.shlex.join(args)+'\n'
        with patch.object(Path, 'read_text', return_value=text), patch.object(M, 'load', return_value=active):
            host.predecessor_worker()
        for wrong in (text.replace(old+'/cache', '/srv/transparent-pir/unretained-cache'),
                      text.replace(old+'/assignment.json', '/opt/unretained-assignment.json'),
                      text.replace('/opt/transparent-publisher/active.json', '/opt/transparent-publisher/v11/active.json')):
            with patch.object(Path, 'read_text', return_value=wrong), patch.object(M, 'load', return_value=active), self.assertRaises(ValueError):
                host.predecessor_worker()
        with patch.object(Path, 'read_text', return_value=text), patch.object(M, 'load', return_value=dict(active, directory='/srv/other/current')), self.assertRaises(ValueError):
            host.predecessor_worker()

    def test_warm_identity_and_every_advertised_anchor_are_required(self):
        plan = worker_plan()
        commands = type('Probe', (), {})()
        expected = {k: plan['worker'][k] for k in ('directory', 'assignment', 'map_sha256')}
        good = {'warm': True, 'invalidated': False, 'candidate': None, 'preparing': None, 'active': expected,
                'revisions': [{'digest': 'a'*64, 'end_height': 3500738, 'terminal_block_hash': 'b'*64}]}
        commands.control = lambda: copy.deepcopy(good)
        commands.state = lambda _: {'ActiveState': 'active', 'MainPID': '123'}
        host = M.Host(plan, commands)
        host.saved = lambda: ({}, {})
        with patch.object(M, 'checksum', return_value='d'*64):
            result = host.verify_worker()
            self.assertEqual(result['revisions'], good['revisions'])
            for key, value in (('warm', False), ('invalidated', True), ('candidate', {}),
                               ('preparing', {}), ('revisions', []), ('active', dict(expected, map_sha256='2'*64))):
                commands.control = lambda key=key, value=value: dict(good, **{key: value})
                with self.assertRaises(ValueError):
                    host.verify_worker()
        commands.control = lambda: copy.deepcopy(good)
        with patch.object(M, 'checksum', side_effect=['d'*64, 'c'*64]), self.assertRaisesRegex(ValueError, 'executable'):
            host.verify_worker()
        malformed = dict(good, revisions=[dict(good['revisions'][0], end_height=True)])
        commands.control = lambda: malformed
        with patch.object(M, 'checksum', return_value='d'*64), self.assertRaisesRegex(ValueError, 'revision'):
            host.verify_worker()


class Commands:
    def __init__(self):
        self.events = []
        self.states = {u: {'ActiveState': 'active', 'MainPID': '123'} for u in M.UNITS['coordinator']}
        self.status = 503
        self.empty = True
        self.fail = None
    def state(self, unit):
        return dict(self.states[unit])
    def unit(self, action, *units):
        self.events.append((action, units))
        if action == self.fail:
            raise RuntimeError('injected systemd failure')
        for unit in units:
            self.states[unit] = {'ActiveState': 'inactive' if action == 'stop' else 'active',
                                 'MainPID': '0' if action == 'stop' else '123'}
    def run(self, argv, **_):
        self.events.append(tuple(argv))
        return b''
    def metadata_status(self, _):
        return self.status
    def empty_cgroup(self, _):
        return self.empty


class HostFilesTests(unittest.TestCase):
    """Real byte/mode snapshots with a simulated service manager, never /etc writes."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name).resolve()
        root_patch = patch.object(M, 'ROOT', self.dir/'publisher')
        root_patch.start()
        self.addCleanup(root_patch.stop)
        self.unit = self.dir/'authority.service'
        self.unit.write_text('v10 unit')
        self.binary = self.dir/'binary'
        self.binary.write_bytes(b'v10 executable')
        self.binary.chmod(0o755)
        self.routing = self.dir/'Caddyfile'
        self.routing.write_text('v10 public routes')
        self.cache = self.dir/'v10-cache'
        self.cache.mkdir()
        (self.cache/'sentinel').write_text('retained warm bytes')
        self.plan = {'transaction': 'transparent-schema-local-fixture', 'role': 'coordinator',
            'baseline_root': str(self.dir/'baseline'), 'baseline': {'version': 1,
            'files': [{'path': str(p), 'required': True} for p in (self.unit, self.binary, self.routing)],
            'retained': [{'path': str(self.cache), 'sentinel': str(self.cache/'sentinel'),
                          'sha256': M.checksum(self.cache/'sentinel')}]}, 'installs': []}
        # Plan validation has separate production-path tests above. Bind the
        # actual transition implementation to fixture paths, not a fake copy.
        with patch.object(M, 'validate', side_effect=lambda p: p):
            self.host = M.Host(self.plan, Commands())
        self.host.preflight = lambda: None
        self.route_patch = patch.object(M, 'deferred', side_effect=lambda _, p: p == str(self.routing))
        self.route_patch.start()
        self.addCleanup(self.route_patch.stop)

    def test_quiesce_capture_restore_bytes_modes_and_deferred_routes(self):
        self.host.capture()
        self.assertEqual(self.host.commands.events[0], ('stop', M.WRITERS['coordinator']))
        self.unit.write_text('v11 unit')
        self.binary.write_bytes(b'v11 executable')
        self.binary.chmod(0o600)
        self.routing.write_text('guarded metadata')
        self.host.restore()
        self.assertEqual(self.unit.read_text(), 'v10 unit')
        self.assertEqual(self.binary.read_bytes(), b'v10 executable')
        self.assertEqual(self.binary.stat().st_mode & 0o777, 0o755)
        self.assertEqual(self.routing.read_text(), 'guarded metadata')
        self.assertEqual(json.loads((M.ROOT/'state/maintenance.json').read_text()), {'enabled':True})
        self.assertEqual((self.cache/'sentinel').read_text(), 'retained warm bytes')
        starts = [e for e in self.host.commands.events if e[0] == 'start']
        self.assertEqual(starts, [('start', M.START['coordinator'])])
        self.assertNotIn(M.LOAD, starts[0][1])
        self.assertNotIn(M.SCALER, starts[0][1])
        self.host.restore()  # Same restored bytes can be verified/restored again.

    def test_stop_failure_does_not_create_baseline(self):
        self.host.commands.fail = 'stop'
        with self.assertRaises(RuntimeError):
            self.host.capture()
        state=json.loads(self.host.root.with_suffix('.units.json').read_text())
        self.assertEqual(state['units'][M.AUTHORITY[0]]['ActiveState'],'active')
        self.assertEqual(state['plan_sha256'], M.hashlib.sha256(M.encode(self.plan)).hexdigest())
        self.assertFalse(self.host.root.exists())

    def test_main_pid_zero_with_surviving_child_refuses_capture(self):
        self.host.commands.empty = False
        with self.assertRaisesRegex(ValueError, 'surviving descendants'):
            self.host.capture()
        self.assertFalse(self.host.root.exists())

    def test_corrupt_or_foreign_baseline_refuses_before_service_effect(self):
        self.host.capture()
        before = list(self.host.commands.events)
        (self.host.root/'files/0').write_text('corrupted retained unit')
        with self.assertRaises(ValueError):
            self.host.restore()
        self.assertEqual(before, self.host.commands.events)

    def test_changed_unit_state_receipt_refuses_restore(self):
        self.host.capture()
        path = self.host.root.with_suffix('.units.json')
        state = json.loads(path.read_text())
        state['units'].pop(M.LOAD)
        path.write_text(json.dumps(state))
        before = list(self.host.commands.events)
        with self.assertRaisesRegex(ValueError, 'unit state'):
            self.host.restore()
        self.assertEqual(before, self.host.commands.events)

    def test_restore_requires_both_public_origins_withdrawn(self):
        self.host.capture()
        self.host.commands.status = 200
        before = list(self.host.commands.events)
        with self.assertRaisesRegex(ValueError, 'both public'):
            self.host.restore()
        self.assertEqual(before, self.host.commands.events)

    def test_restore_start_failure_keeps_public_metadata_guarded(self):
        self.host.capture()
        self.unit.write_text('candidate unit')
        self.routing.write_text('withdrawn routes')
        self.host.commands.fail = 'start'
        with self.assertRaises(RuntimeError):
            self.host.restore()
        self.assertEqual(self.unit.read_text(), 'v10 unit')
        self.assertEqual(self.routing.read_text(), 'withdrawn routes')
        self.assertEqual(self.host.commands.states[M.LOAD]['MainPID'], '0')
        self.assertEqual(self.host.commands.states[M.SCALER]['MainPID'], '0')

    def test_restored_authority_starts_only_after_maintenance_fence(self):
        self.host.capture()
        state = M.ROOT/'state'
        state.mkdir(parents=True)
        (state/'maintenance.json').write_text('{"enabled":false}')
        unit = self.host.commands.unit
        def guarded_unit(action, *units):
            if action == 'start':
                self.assertTrue(json.loads((state/'maintenance.json').read_text())['enabled'])
            unit(action, *units)
        self.host.commands.unit = guarded_unit
        self.host.restore()

    def test_router_baseline_precedes_public_withdrawal(self):
        self.host.role = 'router'
        self.host.plan['role'] = 'router'
        self.host.commands.states = {'caddy.service': {'ActiveState': 'active', 'MainPID': '123'}}
        self.host.commands.status = 200
        self.host.capture()
        self.assertEqual((self.host.root/'files/2').read_text(), 'v10 public routes')
        self.assertFalse(any(e[0] == 'stop' and e[1] for e in self.host.commands.events))

    def test_worker_capture_preserves_warm_predecessor_then_stage_stops_it(self):
        self.host.role = 'worker'
        self.host.plan['role'] = 'worker'
        self.host.commands.states = {M.WORKER: {'ActiveState': 'active', 'MainPID': '123'}}
        self.host.commands.status = 200
        old_root = self.dir/'publisher'
        old_root.mkdir()
        assignment = self.cache/'assignment.json'
        assignment.write_text('old assignment')
        active = {'directory': str(self.cache), 'assignment': str(assignment), 'map_sha256': 'a'*64}
        self.host.plan['worker'] = dict(active)
        (old_root/'active.json').write_text(json.dumps(active))
        self.host.commands.control = lambda: {'active': active, 'warm': True, 'candidate': None,
                                             'preparing': None, 'invalidated': False}
        with patch.object(M, 'ROOT', old_root), patch.object(M, 'CACHE', self.dir/'v11-cache'):
            self.host.capture()
            self.assertEqual(self.host.commands.states[M.WORKER]['MainPID'], '123')
            self.host.commands.status = 503
            self.host.stage()
            self.assertEqual(json.loads((old_root/'v11/active.json').read_text()), active)
        self.assertEqual(self.host.commands.states[M.WORKER]['MainPID'], '0')
        self.assertEqual(self.host.saved()[1]['units'][M.WORKER]['ActiveState'], 'active')

    def test_stage_stops_filter_before_install_so_activate_loads_new_binary(self):
        self.host.capture()
        self.assertEqual(self.host.commands.states[M.FILTER]['MainPID'], '123')
        with patch.object(M, 'ROOT', self.dir/'publisher'):
            self.host.stage()
        self.assertEqual(self.host.commands.states[M.FILTER]['MainPID'], '0')
        self.host.activate()
        self.assertEqual(self.host.commands.states[M.FILTER]['MainPID'], '123')

    def test_stage_displaces_old_unit_overrides_and_refuses_ambiguous_repeat(self):
        self.host.capture()
        source = self.dir/'candidate-unit'
        source.write_text('candidate unit')
        drops = Path(str(self.unit)+'.d')
        drops.mkdir()
        (drops/'old.conf').write_text('old override')
        self.host.plan['installs'] = [{'source': str(source), 'target': str(self.unit),
                                      'sha256': M.checksum(source), 'mode': 0o644}]
        self.host.saved = lambda: None
        with patch.object(M, 'ROOT', self.dir/'publisher'):
            self.host.stage()
            displaced = Path(str(drops)+'.activity-v10-'+self.host.plan['transaction'])
            self.assertFalse(drops.exists())
            self.assertEqual((displaced/'old.conf').read_text(), 'old override')
            with self.assertRaisesRegex(ValueError, 'reconciliation'):
                self.host.stage()
        self.assertEqual(self.unit.read_text(), 'candidate unit')

    def test_stage_checks_checksum_again_and_retains_partial_install(self):
        self.host.capture()
        sources = []
        for n in range(2):
            source = self.dir/('candidate-'+str(n))
            source.write_bytes(b'candidate-'+str(n).encode())
            target = self.dir/('installed-'+str(n))
            sources.append({'source': str(source), 'target': str(target), 'sha256': M.checksum(source), 'mode': 0o755})
        # Keep the captured plan's identity fixed before stage. This test binds
        # staged input failure only, while saved()/identity are covered above.
        self.host.plan['installs'] = sources
        self.host.saved = lambda: None
        Path(sources[1]['source']).write_bytes(b'corruption')
        with self.assertRaisesRegex(ValueError, 'changed during copy'):
            with patch.object(M, 'ROOT', self.dir/'publisher'):
                self.host.stage()
        self.assertEqual(Path(sources[0]['target']).read_bytes(), b'candidate-0')
        self.assertFalse(Path(sources[1]['target']).exists())
        self.assertNotIn(('systemctl', 'daemon-reload'), self.host.commands.events)


if __name__ == '__main__':
    unittest.main()
