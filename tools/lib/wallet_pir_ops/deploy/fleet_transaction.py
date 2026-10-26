"""One durable fleet transaction over the existing role deployment engines.

Preparation stages immutable binaries and checks inactive configurations while
the predecessor serves. Activation installs all reviewed files while consumers
are stopped, then crosses the publisher/discovery barrier before starting them.
The coordinator keeps the parent journal before every effect. Recovery uses its
saved inventory, descriptors and child records, never a newly rendered old unit.
"""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
import copy
from dataclasses import asdict, replace
import json
from pathlib import Path
import re
import shlex
import threading
import time

from .. import contract, durable, paths
from . import descriptors, fleet_release, units
from .engine import Deployer, DeployError, file_sha256, mutation_fence
from .remote import production_lock
from .transaction import FLEET_ROOT, Journal, new_id

ROOT = FLEET_ROOT
FINAL = ('committed', 'rolled-back', 'failed')


def require(condition, message):
    if not condition:
        raise DeployError(message)


def configuration(executor, process, state):
    """Read the exact configuration the installed process starts with."""
    text = [state.get('fragment_text') or '', *(d['text'] for d in state.get('drop_ins', []))]
    command = units.exec_start(units.effective(text))
    arguments = shlex.split(command)
    require(arguments.count('--config') == 1, '%s must run with one --config FILE' % process['key'])
    index = arguments.index('--config')
    require(index + 1 < len(arguments), 'missing configuration path')
    path = arguments[index + 1]
    require(path.startswith('/'), 'configuration path must be absolute')
    body = executor.read(process['host'], path)
    require(body is not None, '%s configuration is unreadable' % process['key'])
    document = json.loads(body)
    require(isinstance(document, dict), 'configuration must be an object')
    return path, body, document


def stable_state(state):
    return {key: state.get(key) for key in ('load_state', 'active_state', 'main_pid', 'invocation_id',
            'need_daemon_reload', 'fragment_path', 'fragment_text', 'drop_ins', 'exe_sha256', 'unit_file_state')}


def storage_paths(document):
    """Persistent roots are retained across executable/configuration updates."""
    found = set()
    if isinstance(document, dict):
        for key, value in document.items():
            if key in ('state_dir', 'data_dir', 'root', 'journal', 'publication_root', 'shard_dir', 'publication_dir') \
                    and isinstance(value, str) and value.startswith('/'):
                found.add(value)
            else:
                found.update(storage_paths(value))
    return found


def peers(document, inside=False):
    """Private peer URLs, excluding publisher origins and unrelated HTTP settings."""
    found = set()
    if isinstance(document, dict):
        for key, value in document.items():
            found.update(peers(value, inside or key in ('workers', 'routers', 'worker_origin',
                                                       'control_origin', 'query_origin')))
    elif isinstance(document, list):
        for value in document:
            found.update(peers(value, inside))
    elif inside and isinstance(document, str) and document.startswith('http://'):
        from urllib.parse import urlsplit
        parsed = urlsplit(document)
        found.add(parsed.netloc)
    return found


def listeners(document):
    found = set()
    if isinstance(document, dict):
        for key, value in document.items():
            if key in ('listen', 'control_listen', 'public_listen', 'query_listen') and isinstance(value, str):
                found.add(value)
            else:
                found.update(listeners(value))
    return found


def dependencies(processes, documents):
    """Prefer configured peers; otherwise retain conservative per-product order."""
    result = {}
    for key, process in processes.items():
        candidates = [other for other in processes if other != key
                      and not processes[other]['publisher']
                      and processes[other]['rank'] < process['rank']
                      and set(process['products']) & set(processes[other]['products'])]
        references = peers(documents[key])
        matched = set()
        for other in candidates:
            sockets = listeners(documents[other])
            for reference in references & sockets:
                if reference.startswith(('127.', 'localhost:', '[::1]:')):
                    if process['host'] != processes[other]['host']:
                        continue
                matched.add(other)
        fallback = {other for other in candidates if processes[other]['role'] != 'combined'}
        result[key] = sorted(matched if references and matched else fallback)
    return result


def service_document(service):
    """Retain descriptor policy with the transaction for inventory-independent recovery."""
    result = asdict(service)
    for role in result['roles'].values():
        role['template'] = str(role['template']) if role['template'] else None
    return result


def restore_service(document):
    value = copy.deepcopy(document)
    value['roles'] = {name: descriptors.Role(**{**role, 'template': Path(role['template']) if role['template'] else None})
                      for name, role in value['roles'].items()}
    return descriptors.Service(**value)


class ChildJournal(Journal):
    """Child changes are durably part of the parent, including concurrent roles."""
    def __init__(self, owner, key):
        self.owner = owner
        super().__init__(owner.local_path, owner.data['children'][key])

    def save(self):
        self.owner.save()

    def event(self, message, **fields):
        with self.owner.mutex:
            self.data['events'].append({'unix': self.owner.now(), 'message': message, **fields})
            self.save()

    def set_phase(self, index, phase, **fields):
        with self.owner.mutex:
            super().set_phase(index, phase, **fields)


class FleetTransaction:
    def __init__(self, registry, inventory, inventory_sha256, request, executor, state_dir,
                 *, clock=time.monotonic, now=time.time, sleep=time.sleep):
        self.registry, self.inventory, self.request = registry, inventory, request
        self.inventory_sha256, self.ex = inventory_sha256, executor
        self.state_dir = Path(state_dir) / 'fleet-release'
        self.clock, self.now, self.sleep = clock, now, sleep
        self.mutex = threading.RLock()
        self.lock = None
        self.data = None
        self.adapters = {}
        self.backend_checks = set()
        self.coordinator = inventory.lock.get('host')
        if self.coordinator is None:
            self.coordinator = next((h for h, spec in inventory.hosts.items()
                                     if spec.get('machine_id') == inventory.lock.get('machine_id')), None)
        require(self.coordinator in inventory.hosts, 'fleet release requires its lock coordinator in the inventory')

    @property
    def local_path(self):
        return self.state_dir / (self.data['id'] + '.json')

    def save(self):
        with self.mutex:
            self.lock.verify()
            # The host helper writes atomically. Remote state is authoritative
            # after a lost runner; local state is a private inspection copy.
            self.ex.write(self.coordinator, ROOT + '/transactions/' + self.data['id'] + '.json',
                          durable.canonical(self.data), 0o600)
            durable.atomic_json(self.local_path, self.data, mode=0o600)

    def event(self, message, **fields):
        with self.mutex:
            self.data['events'].append({'unix': self.now(), 'message': message, **fields})
            self.save()

    def phase(self, phase):
        self.data['phase'] = phase
        if phase == 'committed':
            self.data.setdefault('accepted_unix', self.now())
            self.data.setdefault('acceptance_checks', copy.deepcopy(self.data['checks']))
        self.event('phase ' + phase)

    @contextmanager
    def measured(self, name, **fields):
        started = self.clock()
        try:
            yield
        finally:
            self.event('duration', operation=name, seconds=max(0, self.clock() - started), **fields)

    def parallel(self, keys, operation):
        """At most four hosts; serialize operations sharing a physical machine.

        Drain every future, including failures, before a recovery barrier. No
        worker can still be installing files when rollback begins.
        """
        groups = {}
        for key in keys:
            host = self.data['processes'][key]['host']
            machine = self.data['machines'][host]
            groups.setdefault(machine, []).append(key)

        def group(items):
            for key in items:
                operation(key)

        failures = []
        with ThreadPoolExecutor(max_workers=fleet_release.HOST_CONCURRENCY) as pool:
            futures = [pool.submit(group, items) for items in groups.values()]
            for future in futures:
                try:
                    future.result()
                except BaseException as error:
                    failures.append(error)
        if failures:
            raise failures[0]

    def adapter(self, process, *, recovery=False):
        key = process['key']
        service = self.registry[process['service']]
        target = next(t for t in descriptors.targets(service, self.inventory)
                      if (t.host, t.unit) == (process['host'], process['unit']))
        if process['publisher']:
            # A binary update preserves the installed sandbox, credentials and
            # resource policy, rather than replacing them with an example unit.
            role = replace(target.role, mode='exec-drop-in', owns_unit=False)
            service = replace(service, roles={**service.roles, role.name: role})
            target = replace(target, role=role)
        if not target.role.shared_lifecycle and process['executable'] == 'pir-query-server':
            # The ordinary provider check opens exclusive state. This adapter
            # only replaces the executable over byte-identical configuration
            # and uses the root host's offline --check before stopping it.
            role = replace(target.role, mode='exec-drop-in', owns_unit=False, shared_lifecycle=True)
            service = replace(service, roles={**service.roles, role.name: role})
            target = replace(target, role=role)
        if process['service'] in ('transparent-history', 'transparent-display') and target.role.name == 'worker':
            from tools.transparent.lib import binary_upgrade
            product_request = self.request.get('product_requests', {}).get(process['service'])
            require(product_request is not None, '%s needs its existing pinned Transparent upgrade request' % key)
            binary_upgrade.validate(product_request)
            upgrade = binary_upgrade.Upgrade(product_request, service, self.inventory, self.registry.get('publisher'))
            targets, service = upgrade.component(self.ex)
            require(any((t.host, t.unit) == (target.host, target.unit) for t in targets), 'product request omits ' + key)
            deployer = binary_upgrade.UpgradeDeployer(upgrade, 'rollback' if recovery else 'deploy', service,
                        self.inventory, self.ex, self.state_dir, out=lambda _line: None,
                        clock=self.clock, sleep=self.sleep)
            host_unit = (target.host, target.unit)
            target = next(t for t in targets if (t.host, t.unit) == host_unit)
        else:
            deployer = Deployer(service, self.inventory, self.ex, self.state_dir,
                                out=lambda _line: None, clock=self.clock, sleep=self.sleep)
        require(target.role.shared_lifecycle, '%s requires its exclusive-state product lifecycle' % key)
        deployer.targets = [target]
        deployer.coordinated = True
        deployer.lock = self.lock
        self.adapters[key] = deployer
        return deployer

    def inspect(self):
        """Capture stable units/configurations and actual executable identities."""
        require(self.request['source']['submitted_unix'] <= self.now(), 'source submission time is in the future')
        observations = fleet_release.observe(self.registry, self.inventory, self.ex, self.request)
        found, conflicts = fleet_release.processes(self.registry, self.inventory)
        machines = {}
        for host in sorted({p['host'] for p in found.values()}):
            self.ex.check_identity(host)
            identity = self.ex.publisher_resources(host, [])
            machines[host] = identity['machine_id']
            expected = self.inventory.hosts[host].get('machine_id')
            require(expected is None or expected == machines[host], host + ': machine identity differs from inventory')
        seen = set()
        activation, problems, loads, documents = {}, list(conflicts), {}, {}
        for load in self.request.get('loads', []):
            require(load['host'] in self.inventory.hosts, 'load host is outside inventory')
            require(not any(p['host'] == load['host'] and p['unit'] == load['unit'] for p in found.values()),
                    'load must not name a serving process')
            self.ex.check_identity(load['host'])
            loads[(load['host'], load['unit'])] = {**load, 'state': stable_state(
                self.ex.probe_unit(load['host'], load['unit']))}
        for key, process in found.items():
            identity = (machines[process['host']], process['unit'])
            require(identity not in seen, 'two declarations own the same verified machine/unit')
            seen.add(identity)
            if process['executable'] not in contract.EXECUTABLES:
                continue
            state = self.ex.probe_unit(process['host'], process['unit'])
            require(state.get('active_state') == 'active', key + ' must already run; bootstrap is a separate operation')
            config_path, config_text, document = configuration(self.ex, process, state)
            documents[key] = document
            self.ex.publisher_resources(process['host'], [config_path])
            enabled = sorted(p for p in fleet_release.QUERY_PRODUCTS if isinstance(document.get(p), dict)
                             and document[p].get('enabled') is True)
            observed = observations['processes'][key]
            observed.update(state=stable_state(state), configuration_sha256=fleet_release.digest(document),
                            reads=sorted(fleet_release.publisher_origins(document)) if not process['publisher'] else [],
                            products=enabled)
            if key not in self.request['select']:
                continue
            resources = self.ex.publisher_resources(process['host'],
                [root.rstrip('/') + '/.fleet-headroom' for root in storage_paths(document)])
            require(resources['memory_available'] >= .2 * resources['memory_total'],
                    key + ': less than 20 percent available memory')
            require(all(disk['available'] >= .2 * disk['total'] for disk in resources['disks'].values()),
                    key + ': less than 20 percent disk headroom')
            deployer = self.adapter(process)
            entry = self.request['executables'][process['variant']]
            planned = deployer.plan_target(deployer.targets[0], state, entry['sha256'])
            problems += [key + ': ' + reason for reason in planned.refusals]
            require(not planned.drift, key + ' changes its installed unit policy; reconcile the inventory first')
            if process['publisher'] or not deployer.targets[0].role.renderer:
                planned.check = ('--config', config_path, '--check')
            if process['publisher']:
                # Reuse the installed publisher's headroom and owner policy.
                from . import publisher
                owners = [name for name in (*publisher.OLD, *publisher.ADOPTED) if
                          self.ex.probe_unit(process['host'], name).get('main_pid')]
                require(not owners, 'competing publisher owners: ' + ', '.join(owners))
                safety = publisher.Cutover.__new__(publisher.Cutover)
                safety.host, safety.ex = process['host'], self.ex
                policy = self.request.get('publisher_policies', {}).get(key, {})
                safety.request = {'configuration': document, 'machine_id': machines[process['host']], **policy}
                if set(publisher.PRODUCTS) <= set(enabled):
                    safety.resources()
                load = self.ex.probe_unit(process['host'], publisher.LOAD)
                if load.get('load_state') != 'not-found':
                    if load['active_state'] != 'active':
                        safety.require_stopped_load(load)
                    else:
                        require(self.ex.sha256(process['host'], publisher.LOAD_LATCH) is None,
                                'running continuous checker has a critical latch')
                    loads[(process['host'], publisher.LOAD)] = {'host': process['host'], 'unit': publisher.LOAD,
                        'state': stable_state(load), 'latch_path': publisher.LOAD_LATCH,
                        'latch_sha256': self.ex.sha256(process['host'], publisher.LOAD_LATCH)}
                retained = self.ex.read(process['host'], publisher.ROOT_DIR + '/latest.json')
                if retained:
                    identifier = json.loads(retained)['id']
                    require(re.fullmatch('publisher-[A-Za-z0-9-]+', identifier), 'invalid predecessor publisher pointer')
                    journal = json.loads(self.ex.read(process['host'], publisher.ROOT_DIR +
                                                       '/transactions/' + identifier + '/journal.json'))
                    require(journal['phase'] in publisher.FINAL, 'unfinished publisher transaction; recover it first')
            record = deployer.record(planned)
            desired_document = document
            for change in record['changes']:
                if change['path'] == config_path and change['new'] is not None:
                    desired_document = json.loads(change['new'])
                    require(storage_paths(document) <= storage_paths(desired_document),
                            key + ': persistent data roots change; a data cutover is outside this lifecycle')
            final_enabled = sorted(p for p in fleet_release.QUERY_PRODUCTS
                                   if isinstance(desired_document.get(p), dict)
                                   and desired_document[p].get('enabled') is True)
            observed['final_products'] = final_enabled
            observed['final_reads'] = (sorted(fleet_release.publisher_origins(desired_document))
                                       if not process['publisher'] else [])
            record['configuration'] = {'path': config_path, 'text': config_text}
            previous_vars = dict(deployer.targets[0].vars)
            previous_vars.update({k: v for k, v in document.items() if k in ('management_listen', 'listen')})
            if len(enabled) == 1:
                previous_vars.update({k: v for k, v in document[enabled[0]].get('settings', {}).items()
                                      if k in ('listen', 'control_listen')})
            previous_target = replace(deployer.targets[0], vars=previous_vars)
            record['previous_verify'] = {**record['verify'],
                'health': previous_target.url(previous_target.role.health),
                'ready_url': previous_target.url(previous_target.role.ready_url)}
            record['previous_state'] = stable_state(state)
            record['fleet_key'] = key
            activation[key] = record
            # This observation is verified against the candidate's actual bytes
            # before stopping any process, not trusted because it was requested.
            candidate = deployer.release_binary(entry['sha256'])
            require(self.ex.sha256(process['host'], candidate) == entry['sha256'], key + ': candidate is not staged')
            code, output = self.ex.run(process['host'], [candidate, '--contract-info'], 60)
            require(code == 0, key + ': candidate cannot report contract info')
            actual = contract.parse_contract_info(output, executable=process['executable'], sha256=entry['sha256'])
            expected = contract.parse_contract_info(json.dumps(entry['contract']), executable=process['executable'],
                                                     sha256=entry['sha256'])
            require(actual == expected, key + ': candidate contract report differs from pinned request')
            require(not contract.missing_products(actual, final_enabled), key + ': configured products absent from candidate')
            if (len(enabled) > 1 or len(final_enabled) > 1) and not process['publisher']:
                probes = self.request.get('process_probes', {}).get(key, {})
                require(set(probes) == set(enabled) | set(final_enabled),
                        key + ': independent process probes must cover every enabled product before and after deployment')
            if hasattr(deployer, 'upgrade'):
                upgrade = deployer.upgrade
                observed_majors = {observations['publishers'].get(name, {}).get('major')
                                   for name in observed['reads']}
                require(len(observed_majors) == 1 and None not in observed_majors,
                        key + ': publisher contract is unknown or inconsistent')
                upgrade.discovery_major = next(iter(observed_majors))
                require(upgrade.request['release']['binary_sha256'] == entry['sha256'], 'product request candidate differs')
                require(upgrade.request['release']['git_sha'] == self.request['source']['revision'],
                        'product request source differs')
                upgrade.require_current('deploy')
                problems += upgrade.problems(self.ex) + upgrade.load_problems(self.ex, check_pin=True)
                if upgrade.request['load'] is not None:
                    host = upgrade.request['load']['host']
                    policy = upgrade.product['load']
                    unit, path = policy['unit'], policy['root'] + '/pins.json'
                    loads[(host, unit)] = {'host': host, 'unit': unit, 'pin_path': path,
                        'pin_text': self.ex.read(host, path), 'product_key': key,
                        'state': stable_state(self.ex.probe_unit(host, unit))}
                record['verify']['timeout'] = max(record['verify']['timeout'], upgrade.product['worker_warm_seconds'])
            if not process['publisher']:
                settings = desired_document.get('enhance', {}).get('settings', {}).get('matvec', {})
                backend = settings.get('matvec_backend', 'cpu')
                if backend == 'cuda' or 'cuda' in process['variant']:
                    require('cuda' in actual.backends, key + ': configured CUDA backend is not compiled')
                    effective = units.effective([state.get('fragment_text') or '',
                        *(d['text'] for d in state.get('drop_ins', []))])
                    library = effective.get('Service', {}).get('Environment', {}).get('LD_LIBRARY_PATH')
                    argv = ['env', 'LD_LIBRARY_PATH=' + library] if library is not None else []
                    argv += [candidate, 'check-backend', '--matvec-backend', 'cuda',
                             '--cuda-device', str(settings.get('cuda_device') or 0)]
                    identity = (machines[process['host']], entry['sha256'], tuple(argv))
                    if identity not in self.backend_checks:
                        code, _ = self.ex.run(process['host'], argv, 60)
                        require(code == 0, key + ': candidate CUDA backend check failed')
                        self.backend_checks.add(identity)
        fresh = fleet_release.plan(self.registry, self.inventory, self.inventory_sha256, self.request, observations)
        fresh['problems'] += problems
        fresh['activation'] = activation
        noops = {key for key, record in activation.items() if record['action'] == 'skip'}
        fresh['activation'] = {key: record for key, record in activation.items() if key not in noops}
        for steps in fresh['waves'].values():
            steps[:] = [remaining for step in steps if (remaining := [key for key in step if key not in noops])]
        for process in fresh['processes']:
            if process['key'] in noops:
                process['action'] = 'keep'
        fresh['machines'] = machines
        fresh['loads'] = list(loads.values())
        for probe in [*self.request['probes'].values(), *(p for group in self.request.get('process_probes', {}).values()
                                                        for p in group.values())]:
            require(probe['host'] in self.inventory.hosts, 'probe host is outside inventory')
            self.ex.check_identity(probe['host'])
            if probe['host'] not in machines:
                machines[probe['host']] = self.ex.publisher_resources(probe['host'], [])['machine_id']
            if fresh['kind'] == 'breaking':
                restore = probe.get('restore_argv')
                require(isinstance(restore, list) and restore and all(isinstance(a, str) and a for a in restore),
                        'breaking releases need explicit restore_argv for every encrypted probe')
        changing = {p['key']: p for p in fresh['processes'] if p['action'] == 'change'}
        desired = dict(documents)
        for key, record in activation.items():
            for change in record['changes']:
                if change['path'] == record['configuration']['path'] and change['new']:
                    desired[key] = json.loads(change['new'])
        fresh['dependencies'] = dependencies(changing, desired)
        fresh['previous_dependencies'] = dependencies(changing, documents)
        fresh['timing_path'] = {
            'preparation_allocation_seconds': 120, 'total_target_seconds': 180, 'interruption_target_seconds': 60,
            'startup_dependencies': fresh['dependencies'],
            'readiness_deadlines_seconds': {key: record['verify']['timeout']
                                          for key, record in fresh['activation'].items()},
            'encrypted_probe_deadlines_seconds': {name: probe.get('timeout', 120)
                                                  for name, probe in fresh['probes'].items()},
            'qualification': 'required service and hardware qualification; no duration reduction',
            'estimate': None, 'measured_target_evidence': None,
        }
        receipt = self.ex.read(self.coordinator, ROOT + '/preparation/' + fleet_release.digest(self.request) + '.json')
        fresh['preparation'] = json.loads(receipt) if receipt is not None else None
        fresh['plan_sha256'] = fleet_release.digest({k: v for k, v in fresh.items() if k != 'plan_sha256'})
        return fresh

    def save_preparation(self, seconds):
        """Keep staging and lock waits even though services continued serving."""
        self.lock.verify()
        path = ROOT + '/preparation'
        self.ex.mkdir(self.coordinator, path, 0o700)
        receipt = {'request_sha256': fleet_release.digest(self.request), 'seconds': seconds,
                   'reported': self.request.get('preparation'), 'finished_unix': self.now()}
        self.ex.write(self.coordinator, path + '/' + fleet_release.digest(self.request) + '.json',
                      durable.canonical(receipt), 0o600)

    def stage(self, binaries):
        """Upload each variant once per host using the existing immutable staging."""
        found, problems = fleet_release.processes(self.registry, self.inventory)
        require(not problems, '; '.join(problems))
        by_host = {}
        for key in self.request['select']:
            require(key in found, 'selected process is outside inventory: ' + key)
            process = found[key]
            refusals = fleet_release.candidate_problems(process, self.request['executables'].get(process['variant']))
            require(not refusals, '; '.join(refusals))
            self.ex.check_identity(process['host'])
            machine = self.ex.publisher_resources(process['host'], [])['machine_id']
            by_host.setdefault(machine, []).append(process)

        def prepare_host(items):
            staged = set()
            available = {}
            for process in items:
                deployer = self.adapter(process)
                sha = self.request['executables'][process['variant']]['sha256']
                binary = binaries.get(process['variant'])
                require(binary is None or file_sha256(binary) == sha, 'local artifact digest differs from request')
                if self.ex.sha256(process['host'], deployer.release_binary(sha)) is None:
                    size = Path(binary).stat().st_size if binary is not None else 0
                    require(self.ex.free_bytes(process['host'], deployer.service.root) >=
                            size + deployer.service.min_free_bytes, process['key'] + ': insufficient staging headroom')
                identity = (process['host'], deployer.service.root, process['executable'], sha)
                if identity not in staged:
                    shared = (process['executable'], sha)
                    destination = deployer.release_binary(sha)
                    if shared in available and self.ex.sha256(process['host'], destination) is None:
                        partial = deployer.service.root + '/releases/.partial-' + sha
                        self.ex.mkdir(process['host'], partial, 0o755)
                        self.lock.verify()
                        self.ex.release_copy(process['host'], available[shared],
                                             partial + '/' + process['executable'], sha)
                        self.ex.rename(process['host'], partial, deployer.service.release_dir(sha))
                    deployer.stage([process['host']], sha, binary)
                    available[shared] = destination
                    staged.add(identity)
        with ThreadPoolExecutor(max_workers=fleet_release.HOST_CONCURRENCY) as pool:
            futures = [pool.submit(prepare_host, items) for items in by_host.values()]
            # Drain uploads even after failure; immutable bytes never activate.
            failures = []
            for future in futures:
                try:
                    future.result()
                except BaseException as error:
                    failures.append(error)
            if failures:
                raise failures[0]

    def initialize(self, fresh):
        previous = self.latest()
        require(previous is None or previous['phase'] in FINAL, 'unfinished fleet release; verify or reconcile it first')
        processes = {p['key']: p for p in fresh['processes']}
        manifest = paths.ROOT / 'operations.json'
        self.data = {'schema': 'fleet-release-journal/1', 'id': new_id('fleet', self.request['source']['revision']),
                     'phase': 'preparing', 'created_unix': self.now(), 'request': self.request,
                     'inventory': asdict(self.inventory), 'plan': fresh, 'processes': processes,
                     'machines': fresh['machines'], 'children': {}, 'events': [], 'stopped': [], 'started': {},
                     'checks': {}, 'loads': [], 'errors': {},
                     'preparation': fresh['preparation'],
                     'operations': {'root': str(paths.ROOT), 'source_revision': self.request['source']['revision'],
                                    'manifest_sha256': file_sha256(manifest) if manifest.exists() else None},
                     'services': {name: service_document(service) for name, service in self.registry.items()}}
        for key, record in fresh['activation'].items():
            self.data['children'][key] = {'id': self.data['id'], 'service': processes[key]['service'],
                'status': 'preparing', 'hosts': [copy.deepcopy(record)], 'events': [], 'source': self.request['source']}
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.ex.mkdir(self.coordinator, ROOT + '/transactions', 0o700)
        self.save()
        self.ex.write(self.coordinator, ROOT + '/latest.json', durable.canonical({'id': self.data['id']}), 0o600)

    def latest(self):
        pointer = self.ex.read(self.coordinator, ROOT + '/latest.json')
        if pointer is None:
            return None
        identifier = json.loads(pointer)['id']
        require(isinstance(identifier, str) and re.fullmatch('fleet-[A-Za-z0-9-]+', identifier),
                'invalid fleet transaction pointer')
        data = json.loads(self.ex.read(self.coordinator, ROOT + '/transactions/' + identifier + '.json'))
        require(data['id'] == identifier and data['schema'] == 'fleet-release-journal/1', 'invalid fleet journal')
        return data

    def record(self, key):
        return self.data['children'][key]['hosts'][0]

    def pause_load(self):
        for load in self.data['plan']['loads']:
            state = self.ex.probe_unit(load['host'], load['unit'])
            require(stable_state(state) == load['state'], 'continuous checker changed during preparation')
            if load.get('latch_path'):
                require(self.ex.sha256(load['host'], load['latch_path']) == load['latch_sha256'], 'checker latch changed')
            if load.get('pin_path'):
                require(self.ex.read(load['host'], load['pin_path']) == load['pin_text'], 'checker pins changed')
            self.data['loads'].append({**load, 'was_active': state['active_state'] == 'active'})
            self.event('pause load intent', host=load['host'], unit=load['unit'])
            if state['active_state'] == 'active':
                self.ex.systemctl(load['host'], 'stop', load['unit'])

    def resume_load(self, *, restored=False):
        for load in self.data['loads']:
            self.check_load_ownership(load)
            if load.get('latch_path'):
                require(self.ex.sha256(load['host'], load['latch_path']) == load['latch_sha256'], 'checker latch changed')
            if load.get('pin_path'):
                current = self.ex.read(load['host'], load['pin_path'])
                allowed = [load['pin_text']]
                if 'candidate_pin_text' in load:
                    allowed.append(load['candidate_pin_text'])
                require(current in allowed, 'checker pins changed outside the transaction')
                if restored:
                    self.event('restore checker pins intent', host=load['host'])
                    if load['pin_text'] is None:
                        self.ex.remove(load['host'], load['pin_path'])
                    else:
                        self.ex.write(load['host'], load['pin_path'], load['pin_text'].encode(), 0o644)
                else:
                    deployer = self.adapters[load['product_key']]
                    upgrade = deployer.upgrade
                    targets = {t.key: t for t in upgrade.workers}
                    pins = {identifier: self.ex.probe_unit(targets[key].host, targets[key].unit)['exe_sha256']
                            for identifier, key in sorted(upgrade.worker_ids().items())}
                    load['candidate_pin_text'] = json.dumps(pins, indent=2) + '\n'
                    self.event('replace checker pins intent', host=load['host'])
                    self.ex.write(load['host'], load['pin_path'], load['candidate_pin_text'].encode(), 0o644)
            if load['was_active']:
                self.event('resume load intent', host=load['host'], unit=load['unit'])
                self.ex.systemctl(load['host'], 'start', load['unit'])

    def check_load_ownership(self, load):
        current = stable_state(self.ex.probe_unit(load['host'], load['unit']))
        for field in ('fragment_text', 'fragment_path', 'drop_ins', 'unit_file_state'):
            require(current.get(field) == load['state'].get(field), 'continuous checker unit changed')
        if load.get('pin_path'):
            require(self.ex.read(load['host'], load['pin_path']) in
                    (load['pin_text'], load.get('candidate_pin_text', load['pin_text'])),
                    'checker pins changed outside the transaction')

    def stop(self, key):
        process = self.data['processes'][key]
        with self.mutex:
            if key not in self.data['stopped']:
                self.data['stopped'].append(key)
            self.event('stop intent', target=key, phase=self.data['phase'])
        self.lock.verify()
        with self.measured('shutdown', target=key):
            self.ex.systemctl(process['host'], 'stop', process['unit'])
        state = self.ex.probe_unit(process['host'], process['unit'])
        require(not state.get('main_pid') and state['active_state'] in ('inactive', 'failed'), key + ' did not stop')

    def start(self, key, *, restored=False):
        deployer, record = self.adapters[key], self.record(key)
        journal = ChildJournal(self, key)
        with self.measured('recovery-startup' if restored else 'startup', target=key):
            if restored:
                if hasattr(deployer, 'upgrade'):
                    majors = {self.data['plan']['publishers'][origin]['current_major']
                              for origin in self.data['processes'][key].get('previous_reads', [])}
                    require(len(majors) == 1, key + ': predecessor publisher majors differ')
                    deployer.upgrade.discovery_major = next(iter(majors))
                guard = deployer.service.roles[record['role']].product_guard()
                if guard:
                    require(not guard.before_restore(deployer, journal, record), key + ': product refused restoration')
                deployer.start_restored(journal, 0)
            else:
                sha = self.request['executables'][self.data['processes'][key]['variant']]['sha256']
                deployer.start(journal, 0, sha, verify=False)
        state = self.ex.probe_unit(record['host'], record['unit'])
        with self.mutex:
            self.data['started'][key] = state.get('invocation_id')
            self.event('process started', target=key, restored=restored)

    def wait_ready(self, keys, *, restored=False):
        with self.measured('recovery-readiness' if restored else 'readiness'):
            self.observe_ready(keys, restored=restored)

    def observe_ready(self, keys, *, restored=False):
        """Observe every pending process without serial per-role polling loops."""
        deadlines = {key: self.clock() + self.record(key)['verify']['timeout'] for key in keys}
        pending = set(keys)
        while pending:
            results = {}
            def observe(key):
                record = self.record(key)
                sha = (record['previous']['exe_sha256'] if restored else
                       self.request['executables'][self.data['processes'][key]['variant']]['sha256'])
                if restored:
                    record = {**record, 'verify': record.get('previous_verify', record['verify'])}
                results[key] = self.adapters[key].check(record, sha)
            self.parallel(sorted(pending), observe)
            for key, (ok, reason) in results.items():
                if ok:
                    ChildJournal(self, key).set_phase(0, 'restored' if restored else 'verified')
                    pending.remove(key)
                else:
                    require(self.clock() < deadlines[key], key + ': readiness deadline: ' + reason)
            if pending:
                self.sleep(1)

    def discovery(self, *, restored=False):
        with self.measured('recovery-discovery' if restored else 'discovery'):
            self.observe_discovery(restored=restored)

    def observe_discovery(self, *, restored=False):
        for name, relationship in self.data['plan']['publishers'].items():
            expected = relationship['current_major'] if restored else relationship['final_major']
            for process in self.data['processes'].values():
                reads = process.get('previous_reads', process['reads']) if restored else process['reads']
                if name in reads or process['key'] == relationship['process']:
                    status, body = self.ex.http_get(process['host'], name + contract.DISCOVERY_PATH)
                    require(contract.publisher_document_major(status, body) == expected,
                            '%s discovery does not match expected major %s from %s' % (name, expected, process['host']))
        self.event('discovery verified', restored=restored)

    def reload_hosts(self, keys):
        representatives = {}
        for key in keys:
            host = self.data['processes'][key]['host']
            representatives.setdefault(self.data['machines'][host], key)
        def reload(key):
            host = self.data['processes'][key]['host']
            self.event('reload services intent', host=host)
            self.lock.verify()
            self.ex.systemctl(host, 'daemon-reload')
        self.parallel(list(representatives.values()), reload)

    def probe_checks(self, *, restored=False):
        checks = []
        previous = {product for key in self.data['children']
                    for product in self.data['processes'][key].get('previous_products', [])}
        for product, probe in self.data['plan']['probes'].items():
            if not restored or product in previous:
                checks.append((product, probe))
        for key, products in self.request.get('process_probes', {}).items():
            if key in self.data['children']:
                enabled = self.data['processes'][key]['previous_products' if restored else 'products']
                checks += [(key + '#' + product, probe) for product, probe in products.items() if product in enabled]
        return checks

    def run_probe(self, label, probe, *, restored=False):
        self.event('encrypted probe intent', product=label, restored=restored)
        command = probe.get('restore_argv', probe['argv']) if restored else probe['argv']
        argv = [arg.replace('{transaction}', self.data['id']) for arg in command]
        with self.measured('recovery-probe' if restored else 'encrypted-probe', product=label):
            code, _output = self.ex.run(probe['host'], argv, probe.get('timeout', 120))
        require(code == 0, label + ': independent encrypted-answer probe failed')
        with self.mutex:
            self.data['checks'][label] = {'unix': self.now(), 'restored': restored}
            self.event('encrypted probe passed', product=label, restored=restored)

    def probes(self, *, restored=False, skip_completed=False):
        # One native workload per physical host, four independent hosts maximum.
        groups = {}
        for label, probe in self.probe_checks(restored=restored):
            if skip_completed and label in self.data['checks']:
                continue
            groups.setdefault(self.data['machines'].get(probe['host'], probe['host']), []).append((label, probe))
        def run(items):
            for label, probe in items:
                self.run_probe(label, probe, restored=restored)
        failures = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            for future in [pool.submit(run, items) for items in groups.values()]:
                try:
                    future.result()
                except BaseException as error:
                    failures.append(error)
        if failures:
            raise failures[0]

    def start_consumers(self):
        """Release each dependent role as soon as its own peers are ready.

        The publisher barrier has already passed. Starts share one four-worker
        executor with native acceptance, one heavy operation per machine.
        Lightweight readiness checks run concurrently.
        """
        pending = {key for key in self.data['children'] if not self.data['processes'][key]['publisher']}
        ready, starting, checking = set(), set(), set()
        deadlines, poll_at, futures = {}, {}, {}
        readiness_started = {}
        busy = set()
        probe_pending = dict(self.probe_checks())

        def machine(host):
            return self.data['machines'][host]

        def probe_ready(label):
            if '#' in label:
                return label.rsplit('#', 1)[0] in ready
            return all(key in ready for key in self.data['children']
                       if not self.data['processes'][key]['publisher']
                       and label in self.data['processes'][key]['products'])

        def probe(label, specification):
            self.run_probe(label, specification)

        def start(key):
            self.start(key)

        def check(key):
            record = self.record(key)
            sha = self.request['executables'][self.data['processes'][key]['variant']]['sha256']
            return self.adapters[key].check(record, sha)

        with ThreadPoolExecutor(max_workers=4) as pool:
            while pending or starting or checking or probe_pending or futures:
                runnable = [key for key in pending if set(self.data['plan']['dependencies'][key]) <= ready]
                for key in sorted(runnable):
                    host = machine(self.data['processes'][key]['host'])
                    if host in busy:
                        continue
                    busy.add(host)
                    pending.remove(key)
                    starting.add(key)
                    futures[pool.submit(start, key)] = ('start', key)
                for label in sorted(probe_pending):
                    host = machine(probe_pending[label]['host'])
                    if host not in busy and probe_ready(label):
                        busy.add(host)
                        futures[pool.submit(probe, label, probe_pending.pop(label))] = ('probe', label)
                for key in sorted(checking):
                    if self.clock() >= poll_at[key] and not any(kind == 'check' and other == key
                                                               for kind, other in futures.values()):
                        futures[pool.submit(check, key)] = ('check', key)
                require(futures or checking or not pending, 'fleet dependency graph cannot make progress')
                if not futures:
                    self.sleep(1)
                    continue
                completed, _ = wait(futures, timeout=.1, return_when=FIRST_COMPLETED)
                for future in completed:
                    kind, key = futures.pop(future)
                    value = future.result()  # executor drains other operations before unwinding
                    if kind == 'start':
                        starting.remove(key)
                        checking.add(key)
                        deadlines[key] = self.clock() + self.record(key)['verify']['timeout']
                        readiness_started[key] = self.clock()
                        poll_at[key] = self.clock()
                    elif kind == 'check':
                        ok, reason = value
                        if ok:
                            checking.remove(key)
                            ready.add(key)
                            busy.remove(machine(self.data['processes'][key]['host']))
                            self.event('duration', operation='readiness', target=key,
                                       seconds=self.clock() - readiness_started[key])
                            ChildJournal(self, key).set_phase(0, 'verified')
                        else:
                            require(self.clock() < deadlines[key], key + ': readiness deadline: ' + reason)
                            poll_at[key] = self.clock() + 1
                    elif kind == 'probe':
                        specification = dict(self.probe_checks())[key]
                        busy.remove(machine(specification['host']))

    def deploy(self, expected):
        with production_lock(self.inventory, self.ex) as self.lock:
            mutation_fence(self.inventory, self.ex, self.lock)
            fresh = self.inspect()
            require(not fresh['problems'], '; '.join(fresh['problems']))
            require(fresh['plan_sha256'] == expected, 'fleet release plan is stale')
            self.initialize(fresh)
            try:
                def check_product(key):
                    deployer = self.adapters[key]
                    with self.measured('configuration-check', target=key):
                        deployer.product_checks(ChildJournal(self, key),
                            self.request['executables'][self.data['processes'][key]['variant']]['sha256'])
                    # Static --check must not open the publisher's live state.
                    record = self.record(key)
                    if self.data['processes'][key]['publisher']:
                        code, _ = self.ex.run(record['host'], [deployer.release_binary(
                            self.request['executables'][self.data['processes'][key]['variant']]['sha256']),
                            '--config', record['configuration']['path'], '--check'], 60)
                        require(code == 0, key + ': publisher configuration refused')
                self.parallel(list(self.data['children']), check_product)
                # Include staging-time file/process drift, not changing tips.
                check = self.inspect()
                require(check['plan_sha256'] == expected, 'fleet changed during preparation')
                self.pause_load()
                self.phase('activating')
                for wave in fresh['waves']['stop']:
                    self.parallel(wave, self.stop)
                from . import publisher
                for key in self.data['children']:
                    if self.data['processes'][key]['publisher']:
                        document = json.loads(self.record(key)['configuration']['text'])
                        # Join the old writers before installing any successor.
                        paths = []
                        history = document.get('transparent_history', {}).get('settings', {})
                        detail = document.get('transaction_details', {}).get('settings', {})
                        if history:
                            paths += [history['data_dir'] + '/writer.lock', history['publication_root'] + '/controller.lock']
                        if detail:
                            paths += [detail['journal'] + '/writer.lock', detail['root'] + '/controller.lock']
                        if 'enhance' in document and 'status' in document:
                            paths += publisher.publisher_products.locks(document)
                        self.ex.publisher_unlocked(self.data['processes'][key]['host'], paths)
                self.parallel(list(self.data['children']), lambda key: Deployer.activate(self.adapters[key],
                    ChildJournal(self, key), 0,
                    self.request['executables'][self.data['processes'][key]['variant']]['sha256'],
                    verify=False, start=False, reload=False))
                self.reload_hosts(list(self.data['children']))
                for wave in fresh['waves']['start'][:1]:
                    if not any(self.data['processes'][key]['publisher'] for key in wave):
                        break
                    self.parallel(wave, self.start)
                    self.wait_ready(wave)
                self.discovery()
                self.start_consumers()
                self.phase('verifying')
                self.accept(skip_completed=True)
            except BaseException as error:
                from .deployment_alerts import failed
                failed()
                self.data['errors']['deploy_error'] = str(error)
                self.event('deployment failed')
                if not self.data['stopped'] and not self.data['loads']:
                    self.phase('failed')
                else:
                    try:
                        self.restore()
                    except BaseException as restore_error:
                        self.data['errors']['rollback_error'] = str(restore_error)
                        self.phase('rollback-failed')
                        raise DeployError('deployment failed (%s); restoration failed (%s)' % (error, restore_error)) from error
                raise
            return self.report()

    def accept(self, *, skip_completed=False):
        keys = list(self.data['children'])
        self.wait_ready(keys)
        self.discovery()
        def product_acceptance(key):
            deployer = self.adapters[key]
            if not hasattr(deployer, 'upgrade'):
                return
            deployer.wait_for_controller()
            record = self.record(key)
            upgrade = deployer.upgrade
            problems = upgrade.ready_problems(self.ex, record['key'], record['host'],
                record['verify']['ready_url'] or record['verify']['health'])
            problems += upgrade.controller_problems(self.ex)
            require(not problems, key + ': publication identity changed: ' + '; '.join(problems))
        self.parallel(keys, product_acceptance)
        self.probes(skip_completed=skip_completed)
        self.check_ownership(candidate=True)
        self.resume_load()
        self.check_ownership(candidate=True)
        self.phase('committed')

    def check_ownership(self, *, candidate=False):
        """Refuse foreign file/process changes before touching any participant."""
        for key in self.data['stopped']:
            record, deployer = self.record(key), self.adapters[key]
            require(self.ex.sha256(record['host'], record['previous']['exec_binary']) ==
                    record['previous']['exe_sha256'], key + ': predecessor executable is unavailable')
            require(not deployer.restore_conflicts(record), key + ': managed files changed outside the transaction')
            config = record['configuration']
            expected = {config['text']}
            expected.update(change['new'] for change in record['changes'] if change['path'] == config['path'])
            require(self.ex.read(record['host'], config['path']) in expected, key + ': configuration changed')
            state = self.ex.probe_unit(record['host'], record['unit'])
            previous = record['previous_state']
            require(state['fragment_path'] == previous['fragment_path'] and
                    state.get('unit_file_state') == previous.get('unit_file_state'), key + ': unit ownership changed')
            before = {previous['fragment_path']: previous['fragment_text'],
                      **{item['path']: item['text'] for item in previous['drop_ins']}}
            allowed = {path: {text} for path, text in before.items()}
            for change in record['changes']:
                path = change['path']
                if path.startswith('/etc/systemd/system/'):
                    allowed[path] = {change['new']} if candidate else {change['previous'], change['new']}
                if candidate:
                    require(self.ex.sha256(record['host'], path) == change['new_sha256'],
                            key + ': activated files changed')
            current = {state['fragment_path']: state['fragment_text'],
                       **{item['path']: item['text'] for item in state['drop_ins']}}
            require(all(current.get(path) in allowed.get(path, {None}) for path in set(current) | set(allowed)),
                    key + ': service unit changed outside the transaction')
            if candidate:
                require(not state.get('need_daemon_reload'), key + ': unit has pending foreign changes')
            if state.get('main_pid'):
                proposed = self.request['executables'][self.data['processes'][key]['variant']]['sha256']
                require(state['exe_sha256'] in (proposed, record['previous']['exe_sha256']), key + ': foreign executable')
                receipt = self.data['started'].get(key)
                require(receipt is None or state.get('invocation_id') == receipt, key + ': process invocation changed')
                if receipt is None and not record.get('restart_issued'):
                    require(state.get('invocation_id') == record['previous_state']['invocation_id'],
                            key + ': predecessor invocation changed')
            guard = deployer.service.roles[record['role']].product_guard()
            if guard and self.data['plan']['kind'] != 'breaking':
                require(not guard.before_restore(deployer, ChildJournal(self, key), record), key + ': product refused recovery')

    def restore(self):
        self.check_ownership()
        for load in self.data['loads']:
            self.check_load_ownership(load)
        for load in self.data['loads']:
            # A lost reply may have resumed this checker immediately before
            # failure. Join it again before restoring any serving process.
            self.event('pause load for recovery intent', host=load['host'], unit=load['unit'])
            self.ex.systemctl(load['host'], 'stop', load['unit'])
        self.data.setdefault('recovery_started_unix', self.now())
        self.phase('rolling-back')
        touched = set(self.data['stopped'])
        running_predecessors = {key for key in touched if self.adapters[key].restored_process_running(
            ChildJournal(self, key), self.record(key), self.record(key)['previous']['exe_sha256'])}
        for wave in self.data['plan']['waves']['stop']:
            self.parallel([key for key in wave if key in touched - running_predecessors], self.stop)
        self.parallel(sorted(touched - running_predecessors), lambda key: self.adapters[key].restore(ChildJournal(self, key), 0,
                                                                            verify=False, start=False, reload=False))
        self.reload_hosts(sorted(touched - running_predecessors))
        for wave in self.data['plan']['waves']['start']:
            selected = [key for key in wave if key in touched]
            if any(not self.data['processes'][key]['publisher'] for key in selected):
                self.discovery(restored=True)
            self.parallel([key for key in selected if key not in running_predecessors],
                          lambda key: self.start(key, restored=True))
            self.wait_ready(selected, restored=True)
            if any(self.data['processes'][key]['publisher'] for key in selected):
                self.discovery(restored=True)
        self.probes(restored=True)
        self.resume_load(restored=True)
        self.phase('rolled-back')

    def recover(self, operation):
        self.data = self.latest()
        require(self.data is not None, 'no fleet release is recorded')
        require(self.data['request'] == self.request, 'request differs from recorded fleet transaction')
        from ..bundle import check_installed
        check_installed(mutation=True, revision=self.data['operations']['source_revision'])
        manifest = paths.ROOT / 'operations.json'
        if self.data['operations']['manifest_sha256'] is not None:
            require(manifest.exists() and file_sha256(manifest) == self.data['operations']['manifest_sha256'],
                    'use the recorded operations package for fleet recovery: ' + self.data['operations']['root'])
        # Recovery takes its immutable inventory and descriptor policies from
        # the journal; a changed operator inventory cannot retarget restoration.
        self.inventory = descriptors.Inventory(**self.data['inventory'])
        self.registry = {name: restore_service(value) for name, value in self.data['services'].items()}
        from .remote import SSHExecutor
        if isinstance(self.ex, SSHExecutor):
            self.ex = SSHExecutor(self.inventory)
        with production_lock(self.inventory, self.ex) as self.lock:
            for host, expected in self.data['machines'].items():
                require(self.ex.publisher_resources(host, [])['machine_id'] == expected, host + ': recovery machine changed')
            for key in self.data['children']:
                self.adapter(self.data['processes'][key], recovery=True)
            mutation_fence(self.inventory, self.ex, self.lock, fleet_id=self.data['id'])
            if operation == 'verify':
                require(self.data['phase'] in ('verifying', 'committed'), 'verify requires a fully activated fleet release')
                self.check_ownership(candidate=True)
                if self.data['phase'] == 'committed':
                    self.wait_ready(list(self.data['children']))
                    self.discovery()
                    self.probes()
                else:
                    self.accept()
            elif self.data['phase'] != 'rolled-back':
                self.restore()
        return self.report()

    def report(self):
        events = self.data['events']
        stop_events = [e for e in events if e['message'] == 'stop intent']
        if self.data['phase'] == 'rolled-back' and 'accepted_unix' in self.data:
            stop_events = [e for e in stop_events if e.get('phase') == 'rolling-back']
        stopped = [e['unix'] for e in stop_events]
        last = events[-1]['unix'] if self.data['phase'] in FINAL else None
        accepted = self.data.get('accepted_unix')
        checks_for_gap = (self.data.get('acceptance_checks', self.data['checks']) if self.data['phase'] == 'committed'
                          else self.data['checks'])
        gap_end = max((c['unix'] for c in checks_for_gap.values()), default=None) \
            if self.data['phase'] in ('committed', 'rolled-back') else None
        total = accepted - self.request['source']['submitted_unix'] if accepted is not None else None
        interruption = gap_end - min(stopped) if stopped and gap_end is not None else None
        products = {}
        for product in self.data['plan']['probes']:
            starts = [event['unix'] for event in stop_events
                      if product in self.data['processes'][event['target']]['products']]
            checks = [check['unix'] for name, check in checks_for_gap.items()
                      if name == product or name.endswith('#' + product)]
            if starts and checks:
                products[product] = max(checks) - min(starts)
        durations = {}
        for event in events:
            if event['message'] == 'duration':
                durations[event['operation']] = durations.get(event['operation'], 0.) + event['seconds']
        preparation = self.data.get('preparation') or {}
        durations.update(preparation.get('seconds', {}))
        reported = (preparation.get('reported') or {}).get('seconds', {})
        for operation, seconds in reported.items():
            durations['reported-' + operation] = seconds
        return {'transaction': self.data['id'], 'phase': self.data['phase'], 'errors': self.data['errors'],
                'evidence': 'recorded transaction; use verify for current encrypted acceptance',
                'operations': self.data['operations'],
                'source': self.request['source'], 'timing': {'source_to_accepted': total,
                    'interruption': interruption, 'product_gaps': products, 'operation_seconds': durations,
                    'preparation_seconds': preparation.get('seconds'),
                    'unmeasured_phases': [name for name in ('build', 'qualification') if name not in reported],
                    'recovery_seconds': (last - self.data['recovery_started_unix']
                        if last is not None and 'recovery_started_unix' in self.data else None),
                    'largest_contributors': sorted(durations, key=durations.get, reverse=True)[:3],
                    'under_180_seconds': total < 180 if total is not None else None,
                    'under_60_seconds': interruption < 60 if interruption is not None else None},
                'performance_qualified': False, 'processes': self.data['processes']}
