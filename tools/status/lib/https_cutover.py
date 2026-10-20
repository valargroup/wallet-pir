"""Direct Status HTTPS cutover on the installed publisher and serving pair.

The request pins staged release binaries and existing configuration digests.
Plan is read-only; preflight validates temporary configuration copies. Deploy
journals every effect under the fleet lock and restores the captured configuration
on failure or after the twenty-minute recovery window. State/safety directories
are never restored from backups or removed.
"""
import hashlib
import json
from pathlib import Path
import re
import shlex
import time
from urllib.parse import urlsplit

from wallet_pir_ops import durable
from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.engine import DeployError, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import Journal, FINAL

SERVICE = 'status-https'
CADDY = '/etc/caddy/Caddyfile'
INVENTORY = '/etc/wallet-pir/transparent-fleet.json'
UNITS = {'publisher': 'pir-publisher.service', 'worker': 'status-worker.service', 'router': 'status-router.service'}
DROP = '/etc/systemd/system/{unit}.d/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-status-https.conf'
PRODUCTS = {'enhance', 'status', 'transparent_history', 'transaction_details'}
WINDOW = 1200


def require(condition, message):
    if not condition:
        raise DeployError('Status HTTPS: ' + message)


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def load_request(path, expected):
    raw = Path(path).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == expected, 'request checksum differs')
    request = json.loads(raw)
    require(set(request) == {'schema', 'source_sha', 'public_origin', 'machine_ids', 'roles', 'checks'},
            'unknown or missing request fields')
    require(request['schema'] == 'status-https-cutover/1', 'unsupported request schema')
    require(re.fullmatch('[0-9a-f]{40}', request['source_sha']) is not None, 'full source revision required')
    origin = urlsplit(request['public_origin'])
    require(origin.scheme == 'https' and origin.hostname and not origin.username and not origin.password
            and origin.path in ('', '/') and not origin.query and not origin.fragment and origin.port in (None, 443),
            'use the existing HTTPS wallet origin')
    require(set(request['roles']) == set(UNITS), 'publisher, worker and router required')
    hosts = set()
    for name, role in request['roles'].items():
        require(set(role) == {'host', 'unit', 'binary', 'binary_sha256', 'config_path', 'config_sha256', 'configuration'},
                'unknown or missing role fields')
        hosts.add(role['host'])
        require(role['unit'] == UNITS[name], 'unsupported installed unit')
        binary = 'pir-publisher' if name == 'publisher' else 'pir-query-server'
        require(re.fullmatch(r'/opt/[A-Za-z0-9_/-]+/releases/[0-9a-f]{64}/' + binary, role['binary']) is not None,
                'binary must be staged in an immutable release directory')
        for field in ('binary_sha256', 'config_sha256'):
            require(re.fullmatch('[0-9a-f]{64}', role[field]) is not None, 'invalid ' + field)
        require(role['binary'].split('/')[-2] == role['binary_sha256'], 'release path must bind the binary digest')
        expected_path = '/etc/wallet-pir/publisher.json' if name == 'publisher' else '/etc/status-pir/' + name + '.json'
        require(role['config_path'] == expected_path, 'unsupported installed configuration path')
    require(set(request['machine_ids']) == hosts, 'pin every affected machine')
    require(all(re.fullmatch('[0-9a-f]{32}', value) for value in request['machine_ids'].values()), 'invalid machine identity')
    require(set(request['checks']) == PRODUCTS, 'all four publisher products need independent exact checks')
    for check in request['checks'].values():
        require(set(check) == {'host', 'argv', 'timeout'} and isinstance(check['argv'], list) and check['argv']
                and all(isinstance(a, str) and a for a in check['argv']) and type(check['timeout']) is int
                and 1 <= check['timeout'] <= 120, 'invalid exact check')
    return request


def configuration(name, before, desired, public_origin):
    """Refuse product-policy, wallet-route, source, peer or geometry changes."""
    old, new = json.loads(before), desired
    require(isinstance(new, dict), 'configuration must be an object')
    if name == 'publisher':
        previous = json.loads(before)
        settings = previous['status']['settings']['publisher']
        require({'worker_origin', 'router_origin'} <= set(settings), 'publisher is not the installed push-controlled predecessor')
        settings.pop('worker_origin')
        settings.pop('router_origin')
        require(previous == new, 'only Status control origins may change in publisher configuration')
    else:
        require(set(old) == set(new) == {'management_listen', 'status'}, 'unexpected role configuration sections')
        require(old['management_listen'] == new['management_listen'], 'management listener may not change')
        expected = json.loads(before)
        settings = expected['status']['settings']
        require(settings['role'] == name and 'artifact_origin' in settings, 'not the installed predecessor role')
        settings.pop('artifact_origin')
        settings['publisher'] = public_origin
        settings['state_dir'] += '-https'
        if 'publisher_ca' in new['status']['settings']:
            ca = new['status']['settings']['publisher_ca']
            require(isinstance(ca, str) and ca.startswith('/etc/') and '..' not in Path(ca).parts, 'CA must be installed under /etc')
            settings['publisher_ca'] = ca
        require(expected == new, 'only HTTPS source and separate state directory may change in a query role')
    return json.dumps(new, indent=2) + '\n'


def edge(before, listen, public_origin):
    require(urlsplit(public_origin).hostname in before, 'wallet hostname absent from installed edge')
    require(before.count('handle /v1/status/* {') == 1, 'expected one installed Status wallet handler')
    require('/provider/status/v1/' not in before, 'distribution route already exists')
    return before.replace('handle /v1/status/* {',
                          'handle /provider/status/v1/* {\n        reverse_proxy ' + listen
                          + '\n    }\n    handle /v1/status/* {', 1)


class Cutover:
    def __init__(self, inventory, executor, state_dir, request, *, lock=None, clock=time.monotonic, sleep=time.sleep):
        self.inventory, self.ex, self.state_dir, self.request = inventory, executor, state_dir, request
        self.clock, self.sleep = clock, sleep
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        self.roles = request['roles']
        self.host = self.roles['publisher']['host']
        require(inventory.lock == {'type': 'remote', 'host': self.host}, 'use the coordinator production lock')
        require(self.roles['worker']['host'] == self.roles['router']['host'], 'retain the existing colocated serving pair')
        require(all(host in inventory.hosts for host in request['machine_ids']), 'unknown machine')
        require(all(check['host'] in inventory.hosts for check in request['checks'].values()), 'unknown probe host')
        publisher = inventory.services.get('publisher', {})
        require(publisher.get('exact_check') == {'products': request['checks']}, 'checks must match the fleet publisher probes')

    def run(self, host, argv, timeout=30):
        code, output = self.ex.run(host, argv, timeout)
        require(code == 0, 'remote check failed on ' + host + ': ' + output[-1000:])
        return output.strip()

    def capture(self):
        files, roles = [], {}
        for host, machine in self.request['machine_ids'].items():
            self.ex.check_identity(host)
            require(self.run(host, ['cat', '/etc/machine-id']) == machine, 'machine identity changed: ' + host)
        for name, role in self.roles.items():
            host, unit = role['host'], role['unit']
            state = self.ex.probe_unit(host, unit)
            require(not state['need_daemon_reload'], 'reload pending before cutover: ' + unit)
            require(state['active_state'] == 'active', 'predecessor must be active: ' + unit)
            require(self.ex.sha256(host, role['binary']) == role['binary_sha256'], 'staged binary differs')
            before = self.ex.read(host, role['config_path'])
            require(before is not None and digest(before) == role['config_sha256'], 'configuration changed: ' + name)
            after = configuration(name, before, role['configuration'], self.request['public_origin'])
            effective = units.effective([state['fragment_text'], *[d['text'] for d in state['drop_ins']]])
            command = units.exec_start(effective)
            argv = shlex.split(command)
            at = argv.index('--config') if '--config' in argv else -1
            require(at >= 1 and argv[at + 1:] == [role['config_path']], 'unknown installed command: ' + name)
            previous_binary = argv[at - 1]
            require(self.ex.sha256(host, previous_binary) == state['exe_sha256'], 'rollback executable differs')
            require(command.count(previous_binary) == 1, 'ambiguous installed binary')
            replacement = command.replace(previous_binary, role['binary'], 1)
            overlay = '[Service]\nExecStart=\nExecStart=' + replacement + '\n'
            if name != 'publisher':
                path = role['configuration']['status']['settings']['state_dir']
                overlay += 'ReadWritePaths=' + path + '\n'
            else:
                path = None
            drop = DROP.format(unit=unit)
            files += [self.file(host, role['config_path'], after), self.file(host, drop, overlay)]
            roles[name] = {'host': host, 'unit': unit, 'before': state, 'binary': previous_binary,
                           'state_dir': path, 'user': effective.get('Service', {}).get('User', 'root'),
                           'group': effective.get('Service', {}).get('Group', effective.get('Service', {}).get('User', 'root'))}
        before_inventory = self.ex.read(self.host, INVENTORY)
        require(before_inventory is not None, 'installed fleet inventory absent')
        installed = json.loads(before_inventory)
        require(installed['services']['publisher']['exact_check'] == {'products': self.request['checks']},
                'installed fleet probes differ from the request')
        for name in ('worker', 'router'):
            placements = installed['services']['status']['roles'].get(name, [])
            require(len(placements) == 1 and placements[0]['host'] == self.roles[name]['host'],
                    'installed Status placement differs from the request')
            settings = dict(self.roles[name]['configuration']['status']['settings'])
            settings.pop('role')
            if 'limits' in placements[0].get('vars', {}):
                settings['limits'] = placements[0]['vars']['limits']
            placements[0]['vars'] = settings
        files.append(self.file(self.host, INVENTORY, json.dumps(installed, indent=2) + '\n'))
        listen = self.roles['publisher']['configuration']['status']['settings']['publisher']['private_listen']
        require(listen.startswith('127.0.0.1:'), 'distribution listener must stay loopback')
        caddy = self.ex.read(self.host, CADDY)
        require(caddy is not None, 'installed TLS configuration absent')
        files.append(self.file(self.host, CADDY, edge(caddy, listen, self.request['public_origin'])))
        # Resource checks are observations, not fields whose changing free-byte
        # counts make an otherwise identical reviewed plan unstable.
        for host in self.request['machine_ids']:
            require(self.ex.free_bytes(host, '/opt') >= 4 << 30, 'less than 4 GiB free: ' + host)
        return {'request_sha256': durable.digest(self.request), 'roles': roles, 'files': files}

    def file(self, host, path, after):
        before = self.ex.read(host, path)
        mode = 0o644
        if before is not None:
            stat = self.run(host, ['stat', '-c', '%a:%u', path]).split(':')
            require(len(stat) == 2 and stat[1] == '0' and stat[0] in ('600', '644'), 'unexpected managed file permissions')
            mode = int(stat[0], 8)
        return {'host': host, 'path': path, 'before': before, 'after': after, 'mode': mode}

    def plan(self):
        plan = self.capture()
        return {'plan_sha256': durable.digest(plan), 'source_sha': self.request['source_sha'],
                'targets': [{'host': role['host'], 'unit': role['unit'], 'binary_sha256': role['binary_sha256']}
                            for role in self.roles.values()], 'recovery_window_seconds': WINDOW}

    def preflight(self):
        result = self.plan()
        program = ('import json,subprocess,sys,tempfile;from pathlib import Path;'
                   'd=tempfile.TemporaryDirectory();p=Path(d.name)/"config.json";'
                   'p.write_text(sys.argv[2]);r=subprocess.run([sys.argv[1],"--config",str(p),"--check"],capture_output=True);'
                   'raise SystemExit(r.returncode)')
        for role in self.roles.values():
            self.run(role['host'], ['python3', '-c', program, role['binary'], json.dumps(role['configuration'])], 60)
        self.validate_edge(edge(self.ex.read(self.host, CADDY),
                                self.roles['publisher']['configuration']['status']['settings']['publisher']['private_listen'],
                                self.request['public_origin']))
        for role in self.capture()['roles'].values():
            if role['state_dir']:
                self.state_exists(role)
        self.checks()
        return result

    def state_exists(self, role):
        program = ('import os,pwd,grp,stat,sys;from pathlib import Path;'
                   'p=Path(sys.argv[1]);exists=os.path.lexists(p);'
                   'valid=not exists or (p.is_dir() and not p.is_symlink() and p.resolve()==p '
                   'and p.stat().st_uid==pwd.getpwnam(sys.argv[2]).pw_uid '
                   'and p.stat().st_gid==grp.getgrnam(sys.argv[3]).gr_gid '
                   'and not stat.S_IMODE(p.stat().st_mode)&0o077);'
                   'print("present" if exists else "absent");raise SystemExit(0 if valid else 1)')
        return self.run(role['host'], ['python3', '-c', program, role['state_dir'], role['user'], role['group']]) == 'present'

    def validate_edge(self, text):
        program = ('import subprocess,sys;'
                   'r=subprocess.run(["caddy","validate","--config","/dev/stdin","--adapter","caddyfile"],'
                   'input=sys.argv[1].encode(),capture_output=True);raise SystemExit(r.returncode)')
        self.run(self.host, ['python3', '-c', program, text])

    def checks(self, deadline=None):
        for name, check in self.request['checks'].items():
            timeout = check['timeout']
            if deadline is not None:
                require(self.clock() < deadline, 'twenty-minute recovery window expired')
                timeout = min(timeout, max(1, int(deadline - self.clock())))
            self.run(check['host'], check['argv'], timeout)

    def save(self, journal, status=None):
        self.lock.verify()
        if status:
            journal.set_status(status)
        journal.save()
        root = '/opt/status-pir/https-transactions/' + journal.id
        self.ex.mkdir(self.host, root, 0o700)
        self.ex.write(self.host, root + '/journal.json', durable.canonical(journal.data), 0o600)

    def effect(self, journal, label, action):
        journal.data['intent'] = label
        self.save(journal)
        self.lock.verify()
        action()
        journal.data['intent'] = None
        self.save(journal)

    def write(self, item, before=False):
        text = item['before'] if before else item['after']
        if text is None:
            self.ex.remove(item['host'], item['path'])
        else:
            self.ex.mkdir(item['host'], str(Path(item['path']).parent), 0o755)
            self.ex.write(item['host'], item['path'], text.encode(), item['mode'])

    def restart(self, journal, plan, restoring=False):
        for name in ('worker', 'router', 'publisher') if restoring else ('publisher', 'worker', 'router'):
            role = plan['roles'][name]
            self.effect(journal, 'start ' + name, lambda r=role: self.ex.systemctl(r['host'], 'start', r['unit']))

    def restore(self, journal):
        plan = journal.data['plan']
        for role in plan['roles'].values():
            observed = self.ex.probe_unit(role['host'], role['unit'])
            require(observed['fragment_text'] == role['before']['fragment_text'], 'foreign unit fragment change')
            previous = {item['path']: item['text'] for item in role['before']['drop_ins']}
            current = {item['path']: item['text'] for item in observed['drop_ins'] if item['path'] != DROP.format(unit=role['unit'])}
            require(current == {path: text for path, text in previous.items() if path != DROP.format(unit=role['unit'])}, 'foreign service overlay change')
        for item in plan['files']:
            require(self.ex.read(item['host'], item['path']) in (item['before'], item['after']), 'foreign managed-file change; restoration refused')
        for role in plan['roles'].values():
            require(self.ex.sha256(role['host'], role['binary']) == role['before']['exe_sha256'], 'rollback executable changed')
        self.save(journal, 'rolling-back')
        for name in ('publisher', 'router', 'worker'):
            role = plan['roles'][name]
            self.effect(journal, 'stop ' + name, lambda r=role: self.ex.systemctl(r['host'], 'stop', r['unit']))
        for item in reversed(plan['files']):
            self.effect(journal, 'restore ' + item['path'], lambda i=item: self.write(i, before=True))
        for host in self.request['machine_ids']:
            self.ex.systemctl(host, 'daemon-reload')
        self.ex.systemctl(self.host, 'reload', 'caddy.service')
        self.restart(journal, plan, restoring=True)
        self.wait_ready(plan, restoring=True)
        self.checks()
        self.save(journal, 'rolled-back')

    def wait_ready(self, plan, *, restoring=False, deadline=None):
        deadline = deadline or self.clock() + WINDOW
        while self.clock() < deadline:
            ready = True
            for name, role in self.roles.items():
                state = self.ex.probe_unit(role['host'], role['unit'])
                expected = plan['roles'][name]['before']['exe_sha256'] if restoring else role['binary_sha256']
                path = role['configuration']['status']['settings']['publisher']['private_listen'] + '/control/active' if name == 'publisher' else role['configuration']['status']['settings']['listen'] + '/control/health'
                status, body = self.ex.http_get(role['host'], 'http://' + path, timeout=min(5, max(1, int(deadline - self.clock()))))
                product_ready = status == 200 and (name == 'publisher' or json.loads(body).get('ready') is True)
                ready &= state['active_state'] == 'active' and state['exe_sha256'] == expected and product_ready
            if ready:
                return
            self.sleep(min(2, max(0, deadline - self.clock())))
        raise DeployError('Status HTTPS: service did not recover before the deadline')

    def deploy(self, expected):
        with self.lock_factory() as self.lock:
            mutation_fence(self.inventory, self.ex, self.lock)
            previous = Journal.load(self.state_dir, SERVICE)
            require(previous is None or previous.status in FINAL, 'recover the previous transaction first')
            self.preflight()
            plan = self.capture()
            require(durable.digest(plan) == expected, 'plan changed since review')
            journal = Journal.create(self.state_dir, SERVICE, self.roles['publisher']['binary_sha256'],
                                     {'source_sha': self.request['source_sha']}, [], {})
            journal.data.update(plan=plan, request=self.request)
            self.save(journal, 'preparing')
            try:
                for role in plan['roles'].values():
                    if role['state_dir'] and not self.state_exists(role):
                        self.effect(journal, 'create consumer state', lambda r=role: self.run(r['host'], ['install', '-d', '-o', r['user'], '-g', r['group'], '-m', '0700', r['state_dir']]))
                deadline = self.clock() + WINDOW
                journal.data['recovery_started_unix'] = int(time.time())
                for name in ('publisher', 'router', 'worker'):
                    role = self.roles[name]
                    self.effect(journal, 'stop ' + name, lambda r=role: self.ex.systemctl(r['host'], 'stop', r['unit']))
                for item in plan['files']:
                    self.effect(journal, 'install ' + item['path'], lambda i=item: self.write(i))
                for host in self.request['machine_ids']:
                    self.ex.systemctl(host, 'daemon-reload')
                self.ex.systemctl(self.host, 'reload', 'caddy.service')
                self.restart(journal, plan)
                self.wait_ready(plan, deadline=deadline)
                self.checks(deadline)
                self.save(journal, 'committed')
            except Exception:
                try:
                    self.restore(journal)
                except Exception:
                    self.save(journal, 'rollback-failed')
                    raise
                raise
            return {'transaction': journal.id, 'phase': journal.status}

    def rollback(self, identifier=None):
        journal = Journal.load(self.state_dir, SERVICE, identifier)
        require(journal is not None and journal.data['request'] == self.request, 'transaction/request mismatch')
        latest = Journal.load(self.state_dir, SERVICE)
        require(latest.id == journal.id, 'restore the latest transaction first')
        with self.lock_factory() as self.lock:
            mutation_fence(self.inventory, self.ex, self.lock)
            self.restore(journal)
        return {'transaction': journal.id, 'phase': journal.status}

    def status(self, identifier=None):
        journal = Journal.load(self.state_dir, SERVICE, identifier)
        require(journal is not None and journal.data['request'] == self.request, 'transaction/request mismatch')
        return {'transaction': journal.id, 'phase': journal.status,
                'units': {name: {'active_state': state['active_state'], 'binary_sha256': state['exe_sha256']}
                          for name, role in self.roles.items()
                          for state in [self.ex.probe_unit(role['host'], role['unit'])]}}
