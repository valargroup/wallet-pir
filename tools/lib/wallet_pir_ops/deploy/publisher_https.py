"""Switch the existing Transparent listeners to publisher-owned HTTPS updates.

The publisher transaction owns the lock and recovery journal. Consumer state is
separate from the fleet's state; rollback keeps it, including observed withdrawals.
Wallet origins, listener addresses, unit credentials and resource limits stay put.
"""
import base64
import json
from pathlib import PurePosixPath
import shlex
from urllib.parse import urlsplit

from ..role_config import directory, render, service_settings
from . import units
from .publisher import require, sha, stable_unit
from .publisher_serving import Serving

KINDS = ('history-archive', 'history-recent', 'transaction-details')

# The old workers have stopped before this runs. Only immutable bytes are
# reused: the provider still verifies every object against fresh HTTPS authority.
SEED_OBJECTS = r'''
import hashlib,json,os,pathlib,stat,sys
request=json.loads(sys.argv[1]); root=pathlib.Path(request['root'])
assert root.is_dir() and not root.is_symlink()
objects=root/'objects'; objects.mkdir(mode=0o700,exist_ok=True)
assert objects.is_dir() and not objects.is_symlink()
seen=set();count=0;size=0
for source in request['sources']:
    source=pathlib.Path(source)
    if not source.exists():continue
    assert source.is_dir() and not source.is_symlink()
    for directory,dirs,files in os.walk(source,followlinks=False):
        dirs[:]=[d for d in dirs if not (pathlib.Path(directory)/d).is_symlink()]
        for name in files:
            path=pathlib.Path(directory)/name; metadata=path.lstat()
            key=(metadata.st_dev,metadata.st_ino)
            if not stat.S_ISREG(metadata.st_mode) or key in seen:continue
            seen.add(key)
            with path.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
            target=objects/digest
            if not target.exists():
                os.link(path,target,follow_symlinks=False);count+=1;size+=metadata.st_size
print(json.dumps({'linked_objects':count,'reused_bytes':size}))
'''


def validate(request, inventory):
    settings = request['https']
    require(isinstance(settings, dict) and set(settings) == {'origin', 'publisher_budget_bytes', 'consumers', 'history_edge'},
            'HTTPS cutover needs an origin, publisher storage budget and every consumer')
    origin = urlsplit(settings['origin'])
    require(origin.scheme == 'https' and origin.hostname and origin.port in (None, 443)
            and not origin.username and not origin.password and origin.path in ('', '/')
            and not origin.query and not origin.fragment, 'publisher must use a trusted HTTPS origin')
    require(type(settings['publisher_budget_bytes']) is int and settings['publisher_budget_bytes'] > 0,
            'publisher preparation needs a positive additional disk allowance')
    consumers = settings['consumers']
    edge = settings['history_edge']
    require(isinstance(edge, dict) and set(edge) == {'host', 'origin', 'before_sha256'}
            and edge['host'] in inventory.hosts, 'history TLS edge must be pinned')
    archive = urlsplit(edge['origin'])
    require(archive.scheme == 'https' and archive.hostname and archive.port in (None, 443)
            and not archive.username and not archive.password and archive.path in ('', '/')
            and not archive.query and not archive.fragment, 'archive requires its existing HTTPS origin')
    require(isinstance(consumers, list) and len(consumers) == 3
            and [c.get('kind') for c in consumers if isinstance(c, dict)] == list(KINDS),
            'HTTPS cutover needs archive, recent and combined transaction-detail consumers')
    observed = set()
    for consumer in consumers:
        require(set(consumer) == {'kind', 'host', 'unit', 'configuration', 'config_path', 'budget_bytes'},
                'consumer has unknown or missing fields')
        host, unit = consumer['host'], consumer['unit']
        require(host in inventory.hosts and isinstance(unit, str) and unit.endswith('.service')
                and '/' not in unit and (host, unit) not in observed, 'invalid or duplicate HTTPS consumer')
        observed.add((host, unit))
        directory(consumer['config_path'])
        require(consumer['config_path'].startswith('/etc/wallet-pir/'), 'consumer configuration must be root-owned')
        config = consumer['configuration']
        section = 'transaction_details' if consumer['kind'] == 'transaction-details' else 'transparent_history'
        require(set(config) == {'management_listen', section}
                and config[section].get('enabled') is True, 'consumer must enable only its Transparent product')
        body = config[section]['settings']
        require(body.get('publisher') == settings['origin']
                and body.get('publisher_network') == 'main', 'consumer publisher or network differs')
        state = body.get('state_dir') if section == 'transaction_details' else body.get('shard_dir')
        directory(state)
        require(state.startswith('/srv/') and 'https' in PurePosixPath(state).name,
                'HTTPS consumer needs distinct state under /srv')
        require(not any(key in body for key in ('control_socket', 'active_record', 'assignment',
                                                'worker_id', 'runtime_cache_dir', 'publication_dir')),
                'HTTPS consumers cannot also receive fleet-controlled state')
        if section == 'transaction_details':
            require(body.get('role') == 'combined', 'display HTTPS consumer must hold both tiers')
        else:
            require(body.get('full_holdings') is (consumer['kind'] == 'history-archive'),
                    'history holdings must match its archive or recent role')
            if consumer['kind'] == 'history-recent':
                require(body.get('archive_upstream') == edge['origin'], 'recent consumer requires the terminal archive origin')
            else:
                require(type(body.get('cache_bytes')) is int and body['cache_bytes'] > 0,
                        'archive cache memory budget must be explicit')
                require(not body.get('archive_upstream') and host == edge['host'],
                        'terminal archive must be local to its TLS edge')
        require(type(consumer['budget_bytes']) is int and consumer['budget_bytes'] > 0,
                'consumer storage budget must be explicit')
        require(body.get('bootstrap_max_bytes') == consumer['budget_bytes'],
                'consumer transfer limit must match its storage budget')
    expected = set()
    listeners = {}
    for service in ('transparent-history', 'transparent-display'):
        entries = inventory.services[service]['roles'].get('worker', [])
        for entry in entries:
            default = ('transparent-shard-server.service' if service == 'transparent-history'
                       else 'transparent-txid-display-worker.service')
            key = (entry['host'], entry.get('unit', default))
            expected.add(key)
            listeners[key] = entry['vars']['listen']
    require(observed == expected, 'HTTPS consumers must replace every current Transparent worker exactly once')
    for consumer in consumers:
        section = 'transaction_details' if consumer['kind'] == 'transaction-details' else 'transparent_history'
        body = consumer['configuration'][section]['settings']
        require(body.get('listen') == listeners[(consumer['host'], consumer['unit'])],
                'HTTPS cutover must preserve every existing wallet listener')


class HttpsServing(Serving):
    def history_edge(self):
        owner = self.cutover
        specification = owner.request['https']['history_edge']
        host = specification['host']
        before = owner.ex.read(host, '/etc/caddy/Caddyfile')
        require(before is not None and sha(before) == specification['before_sha256'], 'history edge differs from request')
        origin = urlsplit(specification['origin']).hostname
        require('\n' + origin + ' {' in '\n' + before, 'archive origin is not served by the history edge')
        bodies = {c['kind']: c['configuration']['transparent_history']['settings']
                  for c in owner.request['https']['consumers'] if c['kind'] != 'transaction-details'}
        archive, recent = bodies['history-archive']['listen'], bodies['history-recent']['listen']
        require(recent.rsplit(':', 1)[0] == archive.rsplit(':', 1)[0], 'history consumers must share the current edge host')
        internal = 'http://' + archive.rsplit(':', 1)[0] + ':8080'
        require('\n' + internal + ' {' in '\n' + before, 'history private listener differs')
        def site(address, upstream):
            return (address + ' {\n\trequest_body {\n\t\tmax_size 1MB\n\t}\n'
                    '\t@wallet path /v1/ready /v2/*\n\thandle @wallet {\n'
                    '\t\treverse_proxy ' + upstream + '\n\t}\n'
                    '\thandle {\n\t\trespond 404\n\t}\n}\n')
        global_options = before.split(origin + ' {', 1)[0]
        after = global_options + site(origin, archive) + '\n' + site(internal, recent)
        # Validate on the host whose Caddy version and modules will load it.
        code, output = owner.ex.run(host, ['python3', '-c',
            'import base64,subprocess,sys;raise SystemExit(subprocess.run('
            '["caddy","validate","--config","/dev/stdin","--adapter","caddyfile"],'
            'input=base64.b64decode(sys.argv[1])).returncode)', base64.b64encode(after.encode()).decode()], 30)
        require(code == 0, 'history edge validation failed: ' + output[-1000:])
        state = stable_unit(owner.ex.probe_unit(host, 'caddy.service'))
        require(state['active_state'] == 'active' and not state['need_daemon_reload'], 'history edge is not active')
        return {**specification, 'before': before, 'after': after, 'unit': state}

    def edge(self, text):
        origin = urlsplit(self.cutover.request['https']['origin'])
        require('\n' + origin.hostname + ' {' in '\n' + text, 'publisher origin is not served by the installed TLS edge')
        history = self.cutover.request['configuration']['transparent_history']['settings']['listen']
        detail = self.cutover.request['configuration']['transaction_details']['settings']['status_listen']
        addition = (f'\thandle /provider/v1/* {{\n\t\treverse_proxy {history}\n\t}}\n'
                    f'\thandle /provider/txid/v1/* {{\n\t\treverse_proxy {detail}\n\t}}\n')
        require('/provider/v1/' not in text and '/provider/txid/v1/' not in text,
                'publisher distribution routes are already owned')
        marker = '\thandle {\n\t\trespond 404\n\t}'
        require(text.count(marker) == 1, 'publisher TLS edge has no unique final refusal route')
        return text.replace(marker, addition + marker)

    def capture(self):
        plan = super().capture()
        plan['edge_after'] = self.edge(plan['edge_before'])
        self.validate_edge(plan['edge_after'])
        plan['history_edge'] = self.history_edge()
        consumers = []
        roots = set()
        budgets = {}
        for specification in self.cutover.request['https']['consumers']:
            host, unit = specification['host'], specification['unit']
            state = stable_unit(self.cutover.ex.probe_unit(host, unit))
            require(state['active_state'] == 'active' and state['main_pid'] and not state['need_daemon_reload'],
                    'Transparent consumer must already be active: ' + unit)
            source = self.cutover.ex.probe_unit(host, unit)
            require(source['fragment_path'] == '/etc/systemd/system/' + unit,
                    'HTTPS cutover needs an operator-owned worker unit')
            text = source['fragment_text']
            effective = units.effective([text, *(d['text'] for d in source['drop_ins'])])
            command = units.exec_start(effective)
            old_binary = units.split_exec(command)[1]
            arguments = shlex.split(units.split_exec(command)[2])
            seed_paths = []
            legacy_active = None
            legacy_shards = None
            if specification['kind'] != 'transaction-details':
                for flag in ('--shard-dir', '--runtime-cache-dir'):
                    if flag in arguments:
                        seed_paths.append(arguments[arguments.index(flag) + 1])
                if '--shard-dir' in arguments:
                    legacy_shards = arguments[arguments.index('--shard-dir') + 1]
                if '--active-record' in arguments:
                    legacy_active = arguments[arguments.index('--active-record') + 1]
                require(all(path.startswith('/srv/') and '..' not in PurePosixPath(path).parts
                            for path in seed_paths), 'legacy object sources must stay under /srv')
            require(self.cutover.ex.sha256(host, old_binary) == state['exe_sha256'],
                    'consumer rollback executable differs: ' + unit)
            config = specification['configuration']
            section = 'transaction_details' if specification['kind'] == 'transaction-details' else 'transparent_history'
            body = config[section]['settings']
            root = body.get('state_dir') or body['shard_dir']
            require(not any(h == host and (PurePosixPath(root).is_relative_to(r)
                        or PurePosixPath(r).is_relative_to(root)) for h, r in roots),
                    'HTTPS consumers must have separate durable state')
            roots.add((host, root))
            resource = self.cutover.ex.publisher_resources(host, [root])
            require(resource['files'][root] is None, 'initial HTTPS state must not replace an existing directory')
            require(resource['memory_available'] >= resource['memory_total'] * .2,
                    'consumer has less than 20 percent available memory')
            for disk in resource['disks'].values():
                budgets[host] = budgets.get(host, 0) + specification['budget_bytes']
                require(disk['available'] - budgets[host] >= disk['total'] * .2,
                        'HTTPS consumer staging would leave less than 20 percent disk headroom')
            binary = '/opt/pir-query-server/releases/' + self.release['binary_sha256'] + '/pir-query-server'
            path = specification['config_path']
            require(effective.get('Service', {}).get('User', 'root') == 'root',
                    'HTTPS cutover supports only the existing root-owned workers')
            role = render(text, binary, ['--config', path], {path: config}, [root], [path])
            # The service itself may verify live state only after its old owner stops.
            candidate = service_settings(role.unit, {'ExecStartPre': '', 'TimeoutStartSec': '900'})
            for drop in source['drop_ins']:
                require(PurePosixPath(drop['path']).name == units.MANAGED_DROP_IN
                        or ('ExecStart' not in drop['text'] and 'ReadWritePaths' not in drop['text']),
                        'consumer has an unmanaged launch overlay')
            consumers.append({**specification, 'before': state, 'binary': binary,
                              'old_binary': old_binary, 'root': root,
                              'unit_text': candidate, 'config_before': self.cutover.ex.read(host, path),
                              'seed_paths': sorted(set(seed_paths)), 'legacy_active_record': legacy_active,
                              'legacy_shard_dir': legacy_shards})
        plan['https_consumers'] = consumers
        return plan

    def stage(self, parent, binary):
        super().stage(parent, binary)
        owner = self.cutover
        self.pin_production_snapshots(parent)
        from .publisher_warmup import SnapshotPreparation
        SnapshotPreparation(self).stage(parent)
        for consumer in parent['plan']['serving']['https_consumers']:
            host, target = consumer['host'], consumer['binary']
            owner.effect(parent, 'stage HTTPS executable ' + host, lambda host=host, target=target:
                         self.stage_binary(host, target, binary))
            config_path = '/opt/pir-query-server/transactions/' + parent['id'] + '/' + consumer['unit'] + '.json'
            owner.effect(parent, 'stage HTTPS configuration ' + consumer['unit'],
                         lambda host=host, path=config_path, config=consumer['configuration']:
                         self.write(host, path, json.dumps(config, indent=2, sort_keys=True) + '\n', 0o600))
            code, output = owner.ex.run(host, [target, '--config', config_path, '--check'], 60)
            require(code == 0, 'staged HTTPS consumer rejects its configuration: ' + output[-1000:])

    def pin_production_snapshots(self, parent):
        """Pin public inputs; the actual services validate their bytes at startup."""
        owner = self.cutover
        for section, root_key, map_name in (
                ('transparent_history', 'publication_root', 'shards.json'),
                ('transaction_details', 'root', 'txid-shards.json')):
            root = owner.request['configuration'][section]['settings'][root_key]
            snapshot = json.loads(owner.read(root + '/active.json'))
            source, digest = snapshot['directory'], snapshot['map_sha256']
            require(PurePosixPath(source).is_relative_to(root) and '..' not in PurePosixPath(source).parts,
                    'production snapshot is outside its publication root')
            map_path = source + '/' + map_name
            raw_digest = owner.ex.sha256(owner.host, map_path)
            require(raw_digest is not None, 'production snapshot map is missing')
            from .publisher_warmup import SnapshotPreparation
            pin = SnapshotPreparation(self).pin_snapshot(parent, section, source)
            map_path = pin + '/' + map_name
            require(owner.ex.sha256(owner.host, map_path) == raw_digest, 'pinned production snapshot differs')
            if section == 'transparent_history':
                code, output = owner.ex.run(owner.host,
                    [parent['plan']['binary'], 'verify-filters', '--shard-dir', pin], 120)
                try:
                    canonical = json.loads(output)
                except ValueError:
                    canonical = {}
                require(code == 0 and canonical.get('map_sha256') == digest,
                        'production history snapshot differs from its active record')
            else:
                require(raw_digest == digest, 'production detail snapshot differs from its active record')
            parent.setdefault('production_snapshots', {})[section] = {
                'directory': source, 'map_sha256': digest, 'metadata_verified': True,
                'pin': pin,
                'raw_map_sha256': raw_digest,
                'query_binary_sha256': self.release['binary_sha256']}
            if section == 'transparent_history':
                parent['production_snapshots'][section]['identity'] = canonical
            owner.save(parent)

    def stage_binary(self, host, target, binary):
        owner = self.cutover
        owner.ex.mkdir(host, str(PurePosixPath(target).parent), 0o755)
        current = owner.ex.sha256(host, target)
        require(current in (None, self.release['binary_sha256']), 'immutable query release differs')
        if current is None:
            owner.ex.upload(host, binary, target, 0o755)
        require(owner.ex.sha256(host, target) == self.release['binary_sha256'], 'query release upload differs')

    def write(self, host, path, text, mode):
        self.cutover.ex.mkdir(host, str(PurePosixPath(path).parent), 0o755)
        self.cutover.ex.write(host, path, text.encode(), mode)

    def activate(self, parent):
        super().activate(parent)
        from .publisher_warmup import SnapshotPreparation
        SnapshotPreparation(self).seed(parent)
        owner = self.cutover
        # The distribution prefixes can become reachable while consumers are stopped;
        # the publisher starts only after its old writer has released every lock.
        self.route(parent)
        for consumer in parent['plan']['serving']['https_consumers']:
            host, unit = consumer['host'], consumer['unit']
            parent.setdefault('https_started', []).append(unit)
            owner.save(parent)
            owner.effect(parent, 'stop HTTPS predecessor ' + unit,
                         lambda host=host, unit=unit: self.stop(host, unit))
            for drop in consumer['before']['drop_ins']:
                if PurePosixPath(drop['path']).name == units.MANAGED_DROP_IN:
                    owner.effect(parent, 'retire fleet overlay ' + unit,
                                 lambda host=host, path=drop['path']: owner.ex.remove(host, path))
            owner.effect(parent, 'create HTTPS state ' + unit,
                         lambda host=host, root=consumer['root']: owner.ex.mkdir(host, root, 0o700))
            owner.effect(parent, 'install HTTPS configuration ' + unit,
                         lambda c=consumer: self.write(c['host'], c['config_path'],
                             json.dumps(c['configuration'], indent=2, sort_keys=True) + '\n', 0o600))
            owner.effect(parent, 'install HTTPS unit ' + unit,
                         lambda c=consumer: self.write(c['host'], '/etc/systemd/system/' + c['unit'], c['unit_text'], 0o644))
            owner.effect(parent, 'reload HTTPS units ' + unit,
                         lambda host=host: owner.ex.systemctl(host, 'daemon-reload'))

    def stop(self, host, unit):
        self.cutover.ex.systemctl(host, 'stop', unit)
        state = self.cutover.ex.probe_unit(host, unit)
        require(not state['main_pid'] and state['active_state'] in ('inactive', 'failed'),
                'Transparent owner did not stop: ' + unit)

    def verify(self, parent, restored=False):
        super().verify(parent, restored)
        owner = self.cutover
        owner.resources()
        if not restored:
            deadline = owner.clock() + 900
            while True:
                owner.lock.verify()
                available = True
                for prefix in ('/provider/v1/current', '/provider/txid/v1/current'):
                    status, body = owner.ex.http_get(owner.host, owner.request['https']['origin'] + prefix)
                    try:
                        current = json.loads(body)
                        available &= status == 200 and current.get('eligibility') == 'canonical'
                        available &= current.get('network') == 'main' and len(current.get('inventory_sha256', '')) == 64
                    except (ValueError, TypeError, AttributeError):
                        available = False
                if available:
                    break
                require(owner.clock() < deadline, 'prepared HTTPS publication readiness deadline exceeded')
                owner.sleep(3)
            owner.resources()
        for consumer in parent['plan']['serving']['https_consumers']:
            host, unit = consumer['host'], consumer['unit']
            if not restored and consumer['seed_paths']:
                def seed(consumer=consumer):
                    sources = consumer['seed_paths']
                    if consumer['legacy_active_record']:
                        record = owner.ex.read(consumer['host'], consumer['legacy_active_record'])
                        if record:
                            active = json.loads(record)['directory']
                            require(active.startswith('/srv/') and '..' not in PurePosixPath(active).parts,
                                    'legacy active bytes are outside /srv')
                            sources = [p for p in sources if p != consumer['legacy_shard_dir']] + [active]
                    code, output = owner.ex.run(consumer['host'], ['python3', '-c', SEED_OBJECTS,
                        json.dumps({'root': consumer['root'], 'sources': sources})], 300)
                    require(code == 0, 'could not reuse legacy immutable objects: ' + output[-1000:])
                    parent.setdefault('reused_objects', {})[consumer['unit']] = json.loads(output)
                owner.effect(parent, 'reuse immutable objects ' + unit, seed)
            owner.effect(parent, ('restore start ' if restored else 'start HTTPS ') + unit,
                         lambda host=host, unit=unit: owner.ex.systemctl(host, 'start', unit))
            deadline = owner.clock() + 900
            url = 'http://' + consumer['configuration']['management_listen'] + '/ready'
            while True:
                owner.lock.verify()
                state = owner.ex.probe_unit(host, unit)
                expected = consumer['before']['exe_sha256'] if restored else self.release['binary_sha256']
                require(state['active_state'] != 'failed', 'consumer failed during HTTPS startup: ' + unit)
                if restored:
                    break  # Independent wallet probes verify the restored fleet publication below.
                status, body = owner.ex.http_get(host, url)
                try:
                    ready = status == 200 and json.loads(body).get('ready') is True
                except (ValueError, TypeError):
                    ready = False
                if ready and state['exe_sha256'] == expected:
                    break
                require(owner.clock() < deadline, 'HTTPS consumer readiness deadline exceeded: ' + unit)
                owner.sleep(3)
            if not restored and consumer['kind'] == 'history-archive':
                # Recent startup verifies the terminal archive through this TLS
                # origin, so expose the ready archive before starting recent.
                self.route_history(parent)
            resources = owner.ex.publisher_resources(host, [consumer['root'] + '/.capacity-check'])
            require(all(disk['available'] >= disk['total'] * .2 for disk in resources['disks'].values()),
                    'HTTPS synchronization used the reserved disk headroom')
            require(resources['memory_available'] >= resources['memory_total'] * .2,
                    'HTTPS synchronization used the reserved memory headroom')

    def route(self, parent):
        # Called early, before producer startup, then again by the common cutover.
        if self.cutover.read('/etc/caddy/Caddyfile') == parent['plan']['serving']['edge_after']:
            return
        super().route(parent)

    def route_history(self, parent):
        edge = parent['plan']['serving']['history_edge']
        owner = self.cutover
        owner.effect(parent, 'install autonomous history routes',
                     lambda: self.write(edge['host'], '/etc/caddy/Caddyfile', edge['after'], 0o644))
        owner.effect(parent, 'reload autonomous history routes',
                     lambda: owner.ex.systemctl(edge['host'], 'reload', 'caddy.service'))

    def restore_guard(self, parent):
        super().restore_guard(parent)
        from .publisher_warmup import SnapshotPreparation
        SnapshotPreparation(self).guard(parent)
        edge = parent['plan']['serving']['history_edge']
        require(self.cutover.ex.read(edge['host'], '/etc/caddy/Caddyfile') in (edge['before'], edge['after']),
                'history edge changed outside the HTTPS transaction')
        state = self.cutover.ex.probe_unit(edge['host'], 'caddy.service')
        require(all(state[key] == edge['unit'][key] for key in ('fragment_text', 'drop_ins')),
                'history edge service changed outside the HTTPS transaction')
        for consumer in parent['plan']['serving']['https_consumers']:
            owner = self.cutover
            state = owner.ex.probe_unit(consumer['host'], consumer['unit'])
            require(state['fragment_text'] in (consumer['before']['fragment_text'], consumer['unit_text']),
                    'consumer unit changed outside the HTTPS transaction')
            require(owner.ex.read(consumer['host'], consumer['config_path']) in
                    (consumer['config_before'], json.dumps(consumer['configuration'], indent=2, sort_keys=True) + '\n'),
                    'consumer configuration changed outside the HTTPS transaction')
            require(owner.ex.sha256(consumer['host'], consumer['old_binary']) == consumer['before']['exe_sha256'],
                    'consumer rollback executable changed')
            expected = consumer['before']['drop_ins']
            require(state['drop_ins'] in (expected, [d for d in expected
                    if PurePosixPath(d['path']).name != units.MANAGED_DROP_IN]),
                    'consumer overlays changed outside the HTTPS transaction')

    def restore(self, parent):
        owner = self.cutover
        for consumer in reversed(parent['plan']['serving']['https_consumers']):
            host, unit = consumer['host'], consumer['unit']
            if not self.touched(parent, unit):
                continue
            owner.effect(parent, 'restore stop HTTPS ' + unit, lambda host=host, unit=unit: self.stop(host, unit))
            owner.effect(parent, 'restore consumer unit ' + unit,
                         lambda c=consumer: self.write(c['host'], '/etc/systemd/system/' + c['unit'],
                                                       c['before']['fragment_text'], 0o644))
            if consumer['config_before'] is None:
                owner.effect(parent, 'restore remove HTTPS configuration ' + unit,
                             lambda c=consumer: owner.ex.remove(c['host'], c['config_path']))
            else:
                owner.effect(parent, 'restore consumer configuration ' + unit,
                             lambda c=consumer: self.write(c['host'], c['config_path'], c['config_before'], 0o600))
            for drop in consumer['before']['drop_ins']:
                if PurePosixPath(drop['path']).name == units.MANAGED_DROP_IN:
                    owner.effect(parent, 'restore fleet overlay ' + unit,
                                 lambda host=host, d=drop: self.write(host, d['path'], d['text'], 0o644))
            owner.effect(parent, 'restore reload consumer ' + unit,
                         lambda host=host: owner.ex.systemctl(host, 'daemon-reload'))
        edge = parent['plan']['serving']['history_edge']
        if owner.ex.read(edge['host'], '/etc/caddy/Caddyfile') == edge['after']:
            owner.effect(parent, 'restore history routes',
                         lambda: self.write(edge['host'], '/etc/caddy/Caddyfile', edge['before'], 0o644))
            owner.effect(parent, 'reload restored history routes',
                         lambda: owner.ex.systemctl(edge['host'], 'reload', 'caddy.service'))
        super().restore(parent)
        # The fleet-controlled publisher needs its old workers before it can
        # reactivate a current publication during recovery.
        for consumer in parent['plan']['serving']['https_consumers']:
            if not self.touched(parent, consumer['unit']):
                continue
            owner.effect(parent, 'restore start predecessor ' + consumer['unit'],
                         lambda c=consumer: owner.ex.systemctl(c['host'], 'start', c['unit']))

    @staticmethod
    def touched(parent, unit):
        return unit in parent.get('https_started', [])

    def cleanup_staging(self, parent):
        from .publisher_warmup import SnapshotPreparation
        SnapshotPreparation(self).cleanup(parent)

    def unchanged_during_staging(self, plan):
        super().unchanged_during_staging(plan)
        owner = self.cutover
        require(all(stable_unit(owner.ex.probe_unit(c['host'], c['unit'])) == c['before']
                    for c in plan['https_consumers']), 'Transparent consumer changed during staging')
        edge = plan['history_edge']
        require(owner.ex.read(edge['host'], '/etc/caddy/Caddyfile') == edge['before'] and
                stable_unit(owner.ex.probe_unit(edge['host'], 'caddy.service')) == edge['unit'],
                'history edge changed during staging')
