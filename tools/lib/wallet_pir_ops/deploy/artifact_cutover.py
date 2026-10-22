"""Coordinate prepared artifact metadata and the existing binary lifecycle.

Large objects are prepared while predecessors serve. One production lock owns
the final small-record conversion and publisher-first restart. Rollback converts
the latest records; saved pre-deployment authority snapshots are never restored.
"""
import contextlib
import json
from pathlib import Path, PurePosixPath
import shlex

from .. import durable
from . import artifact_preparation, descriptors, units
from .engine import Deployer, DeployError, file_sha256, mutation_fence, unit_files
from .remote import production_lock
from .transaction import Journal

NAME = 'artifact-checksum'
FENCE = '/var/lib/wallet-pir/artifact-checksum-fence.json'
SERVICES = {'publisher', 'enhance', 'status', 'transparent-history', 'transparent-display'}
KINDS = {'enhance-state', 'status-state', 'status-retained', 'transparent-state', 'transparent-restore'}
# Transparent History integrity failures, written by a serving publisher in
# its configured `publication_root`.
INCIDENTS = 'integrity-failures.json'
ROLL_FORWARD = ('keep the current release serving and roll forward: publish freshly verified replacement '
                'bytes so it clears the recorded failure, then retry the rollback')
# The predecessor's failure record: every field but `found_sha256` required,
# no other field accepted. Its parser refuses anything else at startup.
PREDECESSOR_FAILURE = {'path', 'expected_sha256', 'found_sha256', 'inventory_sha256', 'map_sha256', 'reason'}


def require(condition, message):
    if not condition:
        raise DeployError(message)


def load_request(path, expected):
    require(file_sha256(path) == expected, 'artifact cutover request pin differs')
    request = json.loads(Path(path).read_bytes())
    require(set(request) == {'schema', 'source_sha', 'groups', 'preparations', 'records', 'discovery'},
            'artifact cutover request fields differ')
    require(request['schema'] == 'artifact-checksum-cutover/1', 'unsupported artifact cutover schema')
    require(isinstance(request['source_sha'], str) and len(request['source_sha']) == 40
            and all(c in '0123456789abcdef' for c in request['source_sha']), 'invalid source revision')
    require(isinstance(request['groups'], list) and request['groups'], 'executable groups are required')
    for group in request['groups']:
        require(set(group) == {'service', 'targets', 'binary_sha256', 'binary_bytes', 'binary'},
                'executable group fields differ')
        require(group['service'] in SERVICES and isinstance(group['targets'], list) and group['targets'],
                'invalid executable group targets')
        require(isinstance(group['binary_sha256'], str) and len(group['binary_sha256']) == 64
                and all(c in '0123456789abcdef' for c in group['binary_sha256'])
                and type(group['binary_bytes']) is int and group['binary_bytes'] > 0,
                'invalid executable identity')
    for preparation in request['preparations']:
        fields = {'host', 'helper', 'helper_sha256', 'root', 'stage'}
        require(set(preparation) in (fields, fields | {'source'}),
                'preparation fields differ')
        if 'source' in preparation:
            artifact_preparation.validate(preparation['source'])
        for field in ('helper', 'root', 'stage'):
            require(descriptors.PLAIN_PATH.fullmatch(preparation[field]) is not None,
                    'preparation paths must be absolute plain paths')
    for record in request['records']:
        require(set(record) == {'host', 'helper', 'helper_sha256', 'kind', 'input', 'output', 'preparations'},
                'record conversion fields differ')
        require(record['kind'] in KINDS and record['preparations'], 'invalid record conversion')
        for field in ('helper', 'input', 'output'):
            require(descriptors.PLAIN_PATH.fullmatch(record[field]) is not None,
                    'record paths must be absolute plain paths')
        require(record['input'] != record['output'], 'record output cannot replace its live input')
        require(all(descriptors.PLAIN_PATH.fullmatch(path) is not None for path in record['preparations']),
                'record preparations must be absolute plain paths')
    require(set(request['discovery']) == {'host', 'url'}, 'publisher discovery must be explicit')
    return request


def predecessor_failure(failure):
    return (isinstance(failure, dict) and PREDECESSOR_FAILURE - {'found_sha256'} <= set(failure) <= PREDECESSOR_FAILURE
            and all(isinstance(value, str) or (key == 'found_sha256' and value is None)
                    for key, value in failure.items()))


class Cutover:
    def __init__(self, request, inventory, registry, executor, state_dir, lock=None, **options):
        self.request, self.inventory, self.ex = request, inventory, executor
        self.state_dir = Path(state_dir)
        self.custom_lock = lock
        self.held = None
        self.groups = []
        actual = set()
        for group in request['groups']:
            deployer = Deployer(registry[group['service']], inventory, executor, state_dir,
                                only=group['targets'], out=lambda _: None, **options)
            selected = {(group['service'], target.key) for target in deployer.targets}
            require(not actual.intersection(selected), 'cutover repeats a service target')
            actual.update(selected)
            self.groups.append((group, deployer))
        expected = {(name, target.key) for name in SERVICES
                    for target in descriptors.targets(registry[name], inventory)}
        require(actual == expected, 'artifact cutover must include every affected publisher and query target')
        require(request['groups'][0]['service'] == 'publisher'
                and sum(group['service'] == 'publisher' for group in request['groups']) == 1,
                'one complete publisher group must precede every query group')
        for entry in request['preparations'] + request['records'] + [request['discovery']]:
            require(entry['host'] in inventory.hosts, 'artifact cutover names an unknown host')
        for index, entry in enumerate(request['preparations']):
            source = entry.get('source', {})
            if 'publisher_host' in source:
                require(any(p['host'] == source['publisher_host'] and p['stage'] == source['publisher_stage']
                            and 'product' in p.get('source', {}) for p in request['preparations'][:index]),
                        'cache preparation must follow its declared publisher preparation')

    @contextlib.contextmanager
    def ownership(self, transaction=None):
        factory = self.custom_lock or (lambda: production_lock(self.inventory, self.ex,
                                                              artifact_transaction=transaction))
        with factory() as held:
            self.held = held
            for _, deployer in self.groups:
                deployer.lock = held
            try:
                yield held
            finally:
                self.held = None
                for _, deployer in self.groups:
                    deployer.lock = None

    def command(self, entry, *arguments):
        require(self.ex.sha256(entry['host'], entry['helper']) == entry['helper_sha256'],
                'artifact maintenance executable pin differs')
        if self.held is not None:
            self.held.verify()
        code, output = self.ex.run(entry['host'], [entry['helper'], 'artifact-checksum', *arguments], 7200)
        require(code == 0, 'artifact maintenance refused; inspect its retained output: ' + output[-1500:])
        return json.loads(output)

    def capture(self, allow_unprepared=False):
        mutation_fence(self.inventory, self.ex)
        groups = []
        for group, deployer in self.groups:
            deployer.check_identities()
            binary = group['binary']
            if binary:
                require(file_sha256(binary) == group['binary_sha256']
                        and Path(binary).stat().st_size == group['binary_bytes'], 'local candidate differs')
            plans, problems = deployer.assess(group['binary_sha256'], binary=binary,
                allow_drift=False, retire_historical=False, require_baseline=True)
            require(not problems, 'artifact cutover preflight refused: ' + '; '.join(problems))
            require(all(plan.action == 'restart' for plan in plans),
                    'artifact cutover requires every predecessor running on a different executable')
            require(descriptors.exact_check(deployer.service, self.inventory) is not None,
                    'every product needs its independent exact-answer check')
            groups.append([deployer.record(plan) for plan in plans])
        preparations = []
        for entry in self.request['preparations']:
            if 'source' in entry and self.ex.sha256(entry['host'], entry['stage'] + '/prepared.json') is None:
                require(allow_unprepared, 'artifact preparation is incomplete; run artifact-checksum-prepare first')
                preparations.append({'state':'not-prepared'})
            else:
                preparations.append(self.command(entry, 'verify-prepared', '--stage', entry['stage']))
        return {'request': self.request, 'groups': groups, 'preparations': preparations,
                'incidents': self.history_incidents(groups[0])}

    def plan(self):
        plan = self.capture(allow_unprepared=True)
        return {'plan': plan, 'plan_sha256': durable.digest(plan)}

    def preflight(self):
        plan = self.capture()
        return {'plan':plan, 'plan_sha256':durable.digest(plan)}

    def prepare(self, expected_plan):
        with self.ownership() as held:
            mutation_fence(self.inventory, self.ex, held)
            plan = self.capture(allow_unprepared=True)
            require(durable.digest(plan) == expected_plan, 'artifact preparation plan changed')
            journal = Journal.create(self.state_dir, NAME + '-preparation', durable.digest(self.request),
                                     self.request['source_sha'], [], None)
            journal.data['request'] = self.request
            journal.save()
            try:
                publisher, deployer = self.groups[0]
                entries = [entry for entry in self.request['preparations'] if 'source' in entry]
                require(all(entry['helper_sha256'] == publisher['binary_sha256']
                            and entry['helper'] == deployer.release_binary(publisher['binary_sha256'])
                            for entry in entries), 'preparation must use the pinned publisher maintenance executable')
                deployer.stage([entry['host'] for entry in entries], publisher['binary_sha256'],
                               publisher['binary'], journal)
                for entry in entries:
                    journal.event('preparing public artifacts', host=entry['host'], stage=entry['stage'])
                    result = artifact_preparation.run(self, entry)
                    journal.event('prepared public artifacts', host=entry['host'], stage=entry['stage'],
                                  inventory=result.get('inventory'))
                journal.set_status('prepared')
            except Exception as error:
                journal.set_status('failed', error=str(error))
                raise
            return journal.data

    def child(self, journal, index):
        child = Journal(self.state_dir / (journal.id + '-group-' + str(index) + '.json'),
                        journal.data['groups'][index])
        save = child.save

        def persist():
            save()
            journal.data['groups'][index] = child.data
            journal.save()
        child.save = persist
        return child

    def convert_record(self, entry, reverse=False):
        digest = self.ex.sha256(entry['host'], entry['input'])
        require(digest is not None, 'authority record disappeared')
        argv = ['prepare-record', '--kind', entry['kind'], '--input', entry['input'],
                '--input-sha256', digest, '--output', entry['output']]
        if reverse:
            argv.append('--reverse')
        for preparation in entry['preparations']:
            argv += ['--preparation', preparation]
        self.command(entry, *argv)
        output = self.ex.sha256(entry['host'], entry['output'])
        require(output is not None, 'record converter did not retain its output')
        return {'previous_sha256': digest, 'prepared_sha256': output}

    def install_record(self, entry, evidence):
        self.command(entry, 'install-record', '--input', entry['output'],
            '--input-sha256', evidence['prepared_sha256'], '--destination', entry['input'],
            '--destination-sha256', evidence['previous_sha256'])

    def stop(self, journal):
        for index, (_, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            for number, record in enumerate(child.hosts):
                # Mark touched before stop, including a lost stop reply. The
                # existing engine's restore then restarts the captured owner.
                child.set_phase(number, 'installing')
                deployer.lock.verify()
                self.ex.systemctl(record['host'], 'stop', record['unit'])
                state = self.ex.probe_unit(record['host'], record['unit'])
                require(not state['main_pid'] and state['active_state'] in ('inactive', 'failed'),
                        'predecessor did not release ownership')

    def deploy(self, expected_plan):
        with self.ownership() as held:
            mutation_fence(self.inventory, self.ex, held)
            prior = Journal.load(self.state_dir, NAME)
            require(prior is None or prior.status in ('committed', 'rolled-back'),
                    'an earlier artifact cutover needs recovery first')
            plan = self.capture()
            require(durable.digest(plan) == expected_plan, 'artifact cutover plan changed; review a fresh plan')
            journal = Journal.create(self.state_dir, NAME, durable.digest(self.request),
                                     self.request['source_sha'], [], None)
            journal.data.update(request=self.request, groups=[], records=[], incidents=plan['incidents'])
            for index, (group, deployer) in enumerate(self.groups):
                journal.data['groups'].append({'id':journal.id + '-' + str(index),
                    'service':group['service'], 'binary_sha256':group['binary_sha256'],
                    'source':self.request['source_sha'], 'status':'staging',
                    'hosts':plan['groups'][index], 'events':[], 'baseline_before':None})
            journal.save()
            try:
                for index, (group, deployer) in enumerate(self.groups):
                    child = self.child(journal, index)
                    deployer.stage([record['host'] for record in child.hosts], group['binary_sha256'],
                                   group['binary'], child)
                # New names only. No running owner's metadata changes here.
                for entry in self.request['preparations']:
                    held.verify()
                    self.command(entry, 'install-prepared', '--stage', entry['stage'], '--root', entry['root'])
                journal.set_status('quiescing')
                held.verify()
                host = self.inventory.lock.get('host')
                require(host is not None, 'artifact cutover needs the shared coordinator lock host')
                self.ex.mkdir(host, str(Path(FENCE).parent), 0o700)
                held.verify()
                self.ex.write(host, FENCE, json.dumps({'transaction':journal.id,
                    'source_sha':self.request['source_sha']}).encode(), 0o600)
                self.stop(journal)
                # Read the latest records only after every owner has joined.
                # A newly advanced unknown inventory refuses before replacement.
                evidence = [self.convert_record(entry) for entry in self.request['records']]
                journal.data['records'] = [{**item, 'installed':False} for item in evidence]
                journal.save()
                for index, (entry, item) in enumerate(zip(self.request['records'], evidence)):
                    held.verify()
                    journal.data['records'][index]['installed'] = True
                    journal.save()
                    self.install_record(entry, item)
                journal.set_status('activating')
                for index, (group, deployer) in enumerate(self.groups):
                    child = self.child(journal, index)
                    deployer.product_checks(child, group['binary_sha256'])
                    for number in range(len(child.hosts)):
                        deployer.activate(child, number, group['binary_sha256'], verify=False)
                    if index == 0:
                        # Publication availability and discovery precede consumers.
                        for record in child.hosts:
                            deployer.wait_verified(record, group['binary_sha256'])
                        discovery = self.request['discovery']
                        status, raw = self.ex.http_get(discovery['host'], discovery['url'])
                        document = json.loads(raw)
                        require(status == 200 and document.get('service') == 'pir-publisher'
                                and document.get('version', '').split('.')[0] == '2',
                                'publisher does not expose the new discovery contract')
                for index, (group, deployer) in enumerate(self.groups):
                    child = self.child(journal, index)
                    for number, record in enumerate(child.hosts):
                        deployer.wait_verified(record, group['binary_sha256'])
                        child.set_phase(number, 'verified')
                    deployer.run_exact_check(descriptors.exact_check(deployer.service, self.inventory),
                                             child, group['binary_sha256'])
                journal.set_status('committed')
                for _, deployer in self.groups:
                    deployer.refresh_baseline()
                self.clear_fence(journal)
                return journal.data
            except Exception as error:
                journal.event('forward failed', error=str(error))
                try:
                    self.restore(journal)
                except Exception as rollback_error:
                    journal.set_status('rollback-failed', error=str(rollback_error))
                    raise DeployError('artifact cutover failed and needs recovery with its recorded rollback: '
                                      + str(rollback_error)) from error
                raise

    def restore(self, journal):
        require(journal.data['request'] == self.request, 'rollback request differs from journal')
        if journal.status == 'rolled-back':
            self.clear_fence(journal)
            return
        # Keep every conflict and product-owned recovery gate from the shared
        # lifecycle before stopping any current process.
        problems = []
        for index, (_, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            for number in child.touched():
                record = child.hosts[number]
                problems.extend(deployer.restore_conflicts(record))
                guard = deployer.service.roles[record['role']].product_guard()
                if guard is not None:
                    problems.extend(guard.before_restore(deployer, child, record))
        require(not problems, 'artifact rollback refused before interruption: ' + '; '.join(problems))
        # The configured roots now, and those pinned at deployment.
        records = self.history_incidents(self.child(journal, 0).hosts)
        records += [entry for entry in journal.data['incidents'] if entry not in records]
        incidents = self.incident_problems(records)
        if incidents:
            if journal.data.get('rollback') is None:
                raise DeployError('artifact rollback refused before interruption: ' + '; '.join(incidents)
                                  + '; ' + ROLL_FORWARD)
            # An earlier attempt stopped writers; their record is now stable.
            self.resume_current(journal, incidents)
        # Prove reverse translations while the candidate can still serve. A
        # newer unknown publication is never replaced by a stale snapshot.
        for entry, item in zip(self.request['records'], journal.data['records']):
            if self.reverse_pending(journal, entry, item):
                self.convert_record(entry, reverse=True)
        if any(self.child(journal, index).touched() for index in range(len(self.groups))):
            for entry in self.request['preparations']:
                self.command(entry, 'verify-predecessors', '--stage', entry['stage'])
            if journal.data.get('rollback') is None:
                # Status to resume if a stopping writer records a failure.
                journal.data['rollback'] = {'status':journal.status}
            journal.set_status('rolling-back')
            self.stop(journal)
            # A validator can record a failure while it shuts down.
            incidents = self.incident_problems(records)
            if incidents:
                self.resume_current(journal, incidents)
            for entry in self.request['preparations']:
                self.command(entry, 'restore-predecessors', '--stage', entry['stage'])
        reverse = []
        for entry, item in zip(self.request['records'], journal.data['records']):
            if self.reverse_pending(journal, entry, item):
                reverse.append((entry, item, self.convert_record(entry, reverse=True)))
        for entry, item, evidence in reverse:
            # Journal the exact reverse output before its side effect so a
            # retry after a lost runner or reply recognizes the installed record.
            item['reverse'] = {**evidence, 'installed':False}
            journal.save()
            self.install_record(entry, evidence)
            item['reverse']['installed'] = True
            journal.save()
        # Publisher first on restoration too; defer cross-service acceptance.
        for index, (_, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            for number in child.touched():
                deployer.restore(child, number, verify=False)
        for index, (_, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            for number in child.touched():
                record = child.hosts[number]
                deployer.wait_verified(record, record['previous']['exe_sha256'])
                child.set_phase(number, 'restored')
        for index, (_, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            if child.touched():
                deployer.run_exact_check(descriptors.exact_check(deployer.service, self.inventory),
                    child, child.hosts[0]['previous']['exe_sha256'])
                deployer.refresh_baseline()
        self.clear_fence(journal)
        journal.set_status('rolled-back')

    def history_incidents(self, records):
        """Every History publisher's incident record, from its running configuration.

        Derived from each publisher unit's effective `--config` document on its
        own host, never from the request, so no preparation form can omit one.
        A file need not exist yet: a failure can be recorded after deployment.
        """
        incidents = []
        for record in records:
            prefix = record['key'] + ': cannot derive Transparent History incident records: '
            state = self.ex.probe_unit(record['host'], record['unit'])
            try:
                command = units.exec_start(units.effective(text for _, text in unit_files(state)))
                launcher = tuple(item['path'] for item in record.get('launcher', []))
                arguments = shlex.split(units.split_exec(command, launcher)[2])
            except (units.UnitError, ValueError, TypeError, AttributeError) as error:
                raise DeployError(prefix + str(error)) from error
            configs = [value for flag, value in zip(arguments, arguments[1:]) if flag == '--config']
            configs += [argument[len('--config='):] for argument in arguments if argument.startswith('--config=')]
            require(len(configs) == 1 and descriptors.PLAIN_PATH.fullmatch(configs[0]) is not None,
                    prefix + 'the unit must name one absolute --config')
            raw = self.ex.read(record['host'], configs[0])
            try:
                config = json.loads(raw)
            except (TypeError, ValueError):
                config = None
            require(isinstance(config, dict), prefix + configs[0] + ' is not a readable publisher configuration')
            history = config.get('transparent_history')
            if history is None:
                continue
            require(isinstance(history, dict) and type(history.get('enabled')) is bool,
                    prefix + 'transparent_history must state whether it is enabled')
            if not history['enabled']:
                continue
            root = (history.get('settings') or {}).get('publication_root')
            require(isinstance(root, str) and descriptors.PLAIN_PATH.fullmatch(root) is not None,
                    prefix + 'publication_root must be an absolute plain path')
            entry = {'host':record['host'], 'path':str(PurePosixPath(root) / INCIDENTS)}
            if entry not in incidents:
                incidents.append(entry)
        return incidents

    def incident_problems(self, incidents):
        """Incident records the predecessor could not read.

        A current-format failure has no predecessor form. It is never dropped
        or relabelled; rollback waits until the current release clears it.
        """
        problems = []
        for incident in incidents:
            raw = self.ex.read(incident['host'], incident['path'])
            if raw is None:
                continue
            try:
                failures = json.loads(raw)
            except ValueError:
                failures = None
            if not isinstance(failures, list) or not all(map(predecessor_failure, failures)):
                problems.append(incident['host'] + ':' + incident['path']
                                + ' holds integrity failures the predecessor cannot read')
        return problems

    def resumed_process(self, journal, record, expected):
        """The exact process this resume started, if it still runs.

        Only an acknowledged restart is reused after a lost runner; anything
        else, including a dead or different executable, is restarted again.
        """
        receipt = next((event for event in reversed(journal.data['events'])
                        if event['message'] in ('resume process started', 'resume process reset')
                        and event.get('target') == record['key']), None)
        if receipt is None or receipt['message'] != 'resume process started':
            return False
        state = self.ex.probe_unit(record['host'], record['unit'])
        return (state['active_state'] == 'active' and state['main_pid'] and state['exe_sha256'] == expected
                and not state.get('need_daemon_reload') and state.get('invocation_id')
                and state['invocation_id'] == receipt['invocation_id'])

    def resume_current(self, journal, incidents):
        """Restart the current executables on the current records, then refuse.

        Only before any reverse record is installed: writers were merely
        stopped, so the latest records, withdrawals and incidents are intact.
        Each target must run its exact current executable from the current
        unit and answer health. Readiness is not awaited: a retained History
        incident holds the publisher's aggregate readiness false by design.
        """
        message = '; '.join(incidents) + '; ' + ROLL_FORWARD
        require(not any('reverse' in item for item in journal.data['records']),
                'artifact rollback cannot resume the current release after reverse record installation: ' + message)
        journal.event('rollback resuming current release', incidents=incidents)
        for index, (group, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            expected = group['binary_sha256']
            for number, record in enumerate(child.hosts):
                if not record.get('restart_issued'):
                    deployer.activate(child, number, expected, verify=False)
                elif not self.resumed_process(child, record, expected):
                    require(all(self.ex.sha256(record['host'], change['path']) == change['new_sha256']
                                for change in record['changes']),
                            record['key'] + ': current unit configuration changed; inspect before resuming')
                    # A lost restart reply stays uncertain until a new receipt.
                    child.event('resume process reset', target=record['key'])
                    deployer.lock.verify()
                    self.ex.systemctl(record['host'], 'restart', record['unit'])
                    child.set_phase(number, 'verifying')
                state = self.ex.probe_unit(record['host'], record['unit'])
                if state['active_state'] == 'active' and state['exe_sha256'] == expected and state.get('invocation_id'):
                    child.event('resume process started', target=record['key'], exe_sha256=expected,
                                invocation_id=state['invocation_id'])
            if index == 0:
                # Publisher first, as in the forward cutover.
                for record in child.hosts:
                    deployer.wait_verified(record, expected, live_only=True)
        for index, (group, deployer) in enumerate(self.groups):
            child = self.child(journal, index)
            for number, record in enumerate(child.hosts):
                deployer.wait_verified(record, group['binary_sha256'], live_only=True)
                require(self.resumed_process(child, record, group['binary_sha256']),
                        record['key'] + ': the resumed process is not the one this rollback started')
                child.set_phase(number, 'verified')
        journal.set_status(journal.data.pop('rollback')['status'])
        raise DeployError('artifact rollback resumed the current release: ' + message)

    def reverse_pending(self, journal, entry, item):
        """Whether the live record still needs its reverse translation.

        Only digest identity proves completion: the untouched forward input, or
        the journaled reverse output. A record merely in predecessor format is
        neither, so the reverse converter refuses it rather than guessing.
        """
        if not item['installed']:
            return False
        reverse = item.get('reverse')
        if reverse is not None and reverse['installed']:
            return False
        current = self.ex.sha256(entry['host'], entry['input'])
        if current == item['previous_sha256']:
            return False
        if reverse is not None and current == reverse['prepared_sha256']:
            reverse['installed'] = True
            journal.save()
            return False
        return True

    def clear_fence(self, journal):
        host = self.inventory.lock.get('host')
        if host is None:
            return
        raw = self.ex.read(host, FENCE)
        if raw is not None:
            require(json.loads(raw).get('transaction') == journal.id,
                    'artifact fence belongs to another transaction')
            self.held.verify()
            self.ex.remove(host, FENCE)

    def rollback(self, transaction=None):
        journal = Journal.load(self.state_dir, NAME, transaction)
        require(journal is not None, 'no artifact cutover transaction')
        latest = Journal.load(self.state_dir, NAME)
        require(latest.id == journal.id, 'a newer artifact cutover exists; inspect it first')
        with self.ownership(journal.id) as held:
            mutation_fence(self.inventory, self.ex, held, artifact_transaction=journal.id)
            self.restore(journal)
        return journal.data


def register(commands):
    for operation in ('plan', 'preflight', 'prepare', 'deploy', 'status', 'rollback'):
        parser = commands.add_parser(NAME + '-' + operation, allow_abbrev=False)
        parser.add_argument('--request', type=Path, required=True)
        parser.add_argument('--request-sha256', required=True)
        if operation in ('prepare', 'deploy'):
            parser.add_argument('--expect-plan-sha256', required=True)
        if operation in ('status', 'rollback'):
            parser.add_argument('--transaction')
