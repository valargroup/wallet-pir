"""Prepare separate Transparent roots; activation remains an ordinary cutover.

A reviewed manifest binds the candidate, configuration and verified destination.
Preparation never stops services or opens journals. The stopped-owner handoff
checks for drift and carries revision high-water marks forward in both directions.
"""
import json
from pathlib import Path, PurePosixPath
import re
import tempfile

from .. import durable, release
from . import publisher, publisher_products
from .engine import file_sha256
from .remote import production_lock

ROOT = publisher.ROOT_DIR + '/storage-preparations'
PRODUCTS = {'transparent_history': ('publication_root', 'history'),
            'transaction_details': ('root', 'transaction-details')}
require = publisher.require

# Runs only under the deployment lock. A snapshot records identities, never
# follows links, and never reads large bodies. Cleanup refuses changed trees.
TREE_PROGRAM = r'''
import hashlib,json,os,pathlib,shutil,stat,sys
operation,raw=sys.argv[1:]
request=json.loads(raw)
root=pathlib.Path(request['root'])
def require(value, message):
    if not value: raise ValueError(message)
require(root.is_absolute() and str(root)==os.path.normpath(root), 'non-canonical root')
if not (operation=='remove' and not root.exists() and not root.is_symlink()):
    require(stat.S_ISDIR(root.lstat().st_mode) and not root.is_symlink(), 'root is not a real directory')
def snapshot():
    result={}
    def walk(path):
        for entry in sorted(path.iterdir()):
            s=entry.lstat()
            relative=str(entry.relative_to(root))
            if relative=='controller.lock': continue
            require(stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode) or stat.S_ISLNK(s.st_mode), 'special file in storage')
            result[relative]=[s.st_dev,s.st_ino,s.st_mode,s.st_size,s.st_mtime_ns,s.st_ctime_ns,
                              os.readlink(entry) if stat.S_ISLNK(s.st_mode) else None]
            if stat.S_ISDIR(s.st_mode):
                require(s.st_dev==root.lstat().st_dev and not os.path.ismount(entry), 'nested mount in retained storage')
                walk(entry)
    s=root.lstat()
    walk(root)
    # The remote command transport retains only 4 KiB of output. Bind every
    # entry with one digest instead of returning a potentially truncated tree.
    encoded=json.dumps(result,sort_keys=True,separators=(',',':')).encode()
    return {'root':[s.st_dev,s.st_ino], 'entries_sha256':hashlib.sha256(encoded).hexdigest(),
            'entry_count':len(result)}
if operation=='remove' and not root.exists():
    print(json.dumps({'absent':True})); sys.exit(0)
observed=snapshot()
if operation=='remove':
    require(observed==request['expected'], 'retained storage changed')
    # Descent is confined to this exact real root; symlinks are unlinked.
    shutil.rmtree(root)
elif operation!='snapshot': raise ValueError('unknown storage operation')
print(json.dumps(observed,sort_keys=True))
'''


def validate_reference(value):
    require(isinstance(value, dict) and set(value) == {'id', 'manifest_sha256'}
            and isinstance(value['id'], str) and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9-]{0,79}', value['id'])
            and isinstance(value['manifest_sha256'], str) and re.fullmatch('[a-f0-9]{64}', value['manifest_sha256']),
            'prepared_storage requires a safe id and the reviewed manifest checksum')


def work_path(identifier):
    validate_reference({'id': identifier, 'manifest_sha256': '0' * 64})
    return ROOT + '/' + identifier


def configuration(installed, candidate, identifier):
    """Allow precisely two sibling root moves; every journal/setting stays exact."""
    expected = publisher_products.configuration_for_update(installed)
    roots = {}
    for product, (field, _) in PRODUCTS.items():
        old = expected[product]['settings'][field]
        new = candidate[product]['settings'][field]
        require(isinstance(old, str) and PurePosixPath(old).is_absolute()
                and str(PurePosixPath(old)) == old and '..' not in PurePosixPath(old).parts
                and new == old + '.prepared-' + identifier,
                'prepared storage must use the reviewed separate sibling roots')
        expected[product]['settings'][field] = new
        if product == 'transparent_history':
            initial = PurePosixPath(expected[product]['settings']['initial_publication'])
            require(initial.is_relative_to(old), 'initial History publication is outside its root')
            expected[product]['settings']['initial_publication'] = str(PurePosixPath(new) / initial.relative_to(old))
        roots[product] = {'source': old, 'target': new}
    require(expected == candidate, 'storage preparation must preserve every other publisher setting')
    all_roots = [PurePosixPath(path) for pair in roots.values() for path in pair.values()]
    journals = [PurePosixPath(candidate['transparent_history']['settings']['data_dir']),
                PurePosixPath(candidate['transaction_details']['settings']['journal'])]
    require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
                for a in all_roots for b in journals), 'derived storage overlaps a journal')
    require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
                for i, a in enumerate(all_roots) for b in all_roots[i + 1:]), 'derived storage roots overlap')
    return roots


def tree(owner, root, *, remove=None):
    request = {'root': root}
    if remove is not None:
        request['expected'] = remove
    code, output = owner.ex.run(owner.host, ['python3', '-c', TREE_PROGRAM,
                                           'remove' if remove is not None else 'snapshot', json.dumps(request)], 300)
    require(code == 0, 'retained storage inspection refused: ' + output[-1000:])
    return json.loads(output)


def counter(owner, root):
    raw = owner.read(root + '/distribution-revision.json')
    value = 0 if raw is None else json.loads(raw)
    require(type(value) is int and 0 <= value < (1 << 64) - 1, 'invalid or exhausted distribution revision')
    return value


def advance_floors(owner, roots, destination):
    for pair in roots.values():
        floor = max(counter(owner, pair['source']), counter(owner, pair['target']))
        owner.write(pair[destination] + '/distribution-revision.json', json.dumps(floor) + '\n')


def source_resolved(owner, pair, receipt):
    """Source may advance, but cannot withdraw or move behind our selected anchor.

    The native receipt verifier independently rechecks that anchor with the node.
    File identities bind the prepared snapshot, not the moving source pointer.
    """
    active = json.loads(owner.read(pair['source'] + '/active.json'))
    height = active.get('height', active.get('tip'))
    require(type(height) is int and height >= receipt['anchor_height'],
            'source moved behind the prepared anchor; prepare a current snapshot')
    for name in ('activation.json', 'withdrawn.json', 'halted.json'):
        require(owner.read(pair['source'] + '/' + name) is None, 'source has unresolved publication intent')
    for name, empty in (('invalidate.json', {'rollback': None}), ('integrity-failures.json', [])):
        raw = owner.read(pair['source'] + '/' + name)
        value = None if raw is None else json.loads(raw)
        retired_empty = name == 'invalidate.json' and value == {'pending': {}, 'rollback': None}
        require(raw is None or value == empty or retired_empty, 'source has unresolved integrity or rollback state')


class Preparation:
    def __init__(self, owner, identifier):
        self.owner, self.id = owner, identifier
        self.work = work_path(identifier)
        self.config_path = self.work + '/publisher.json'
        self.manifest_path = self.work + '/manifest.json'

    def plan(self):
        o = self.owner
        require('prepared_storage' not in o.request, 'preparation request must not consume a manifest')
        installed = o.read(publisher.CONFIG)
        roots = configuration(json.loads(installed), o.request['configuration'], self.id)
        state = publisher.stable_unit(o.ex.probe_unit(o.host, publisher.UNIT))
        require(state['active_state'] == 'active' and state['main_pid'], 'predecessor publisher must keep serving')
        resource = o.resources(o.request['release']['binary_bytes'])
        require(resource['machine_id'] == o.request['machine_id'], 'coordinator identity differs')
        previous = o.latest()
        require(previous is None or previous['phase'] in publisher.FINAL, 'unfinished cutover owns production')
        result = {'id': self.id, 'request_sha256': durable.digest(o.request),
                  'source_configuration_sha256': publisher.sha(installed), 'unit': state, 'roots': roots}
        return {'plan': result, 'plan_sha256': durable.digest(result)}

    def prepare(self, binary, expected):
        o = self.owner
        require(file_sha256(binary) == o.request['release']['binary_sha256']
                and Path(binary).stat().st_size == o.request['release']['binary_bytes'], 'preparation executable differs')
        with production_lock(o.inventory, o.ex) as o.lock:
            plan = self.plan()['plan']
            require(durable.digest(plan) == expected, 'storage preparation plan changed')
            o.ex.mkdir(o.host, self.work, 0o700)
            ownership = durable.canonical(plan)
            owner_path = self.work + '/owner.json'
            previous = o.read(owner_path)
            if previous is not None:
                recorded = json.loads(previous)
                # A completed rollback may restart the exact same predecessor.
                # Reverify its prepared bytes and canonical anchors below; only
                # the process identity may differ from the original preparation.
                for field in ('main_pid', 'invocation_id'):
                    recorded['unit'][field] = plan['unit'][field]
                require(recorded == plan, 'preparation id belongs to another request')
            o.ex.write(o.host, owner_path, ownership, 0o600)
            binary_path = publisher.ROOT_DIR + '/releases/' + o.request['release']['binary_sha256'] + '/pir-publisher'
            o.stage(binary, binary_path)
            config = json.dumps(o.request['configuration'], indent=2) + '\n'
            o.write(self.config_path, config)
            code, output = o.ex.run(o.host, [binary_path, '--config', self.config_path, '--check'], 60)
            require(code == 0, 'candidate configuration refused: ' + output[-1000:])
            records = {}
            for product, pair in plan['roots'].items():
                o.lock.verify()
                o.ex.write(o.host, self.work + '/progress.json', durable.canonical({'phase': 'preparing', 'product': product}), 0o600)
                self.run(binary_path, product, pair)
                path = pair['target'] + '/prepared-storage.json'
                receipt = json.loads(o.read(path))
                require(receipt['configuration_sha256'] == publisher.sha(config)
                        and receipt['ownership'] == {'schema': 'transparent-storage-preparation/1', 'product': product, **pair}
                        and receipt['schema'] == 'transparent-prepared-storage/1', 'preparation returned another receipt')
                source_resolved(o, pair, receipt)
                records[product] = {'receipt_sha256': o.ex.sha256(o.host, path), 'receipt': receipt}
            require(publisher.sha(o.read(publisher.CONFIG)) == plan['source_configuration_sha256']
                    and publisher.stable_unit(o.ex.probe_unit(o.host, publisher.UNIT)) == plan['unit'],
                    'publisher changed during storage preparation')
            for product, pair in plan['roots'].items():
                source_resolved(o, pair, records[product]['receipt'])
            manifest = {'schema': 'publisher-storage-preparation/1', 'plan': plan,
                        'configuration_sha256': publisher.sha(config), 'release': o.request['release'], 'products': records}
            o.ex.write(o.host, self.manifest_path, durable.canonical(manifest), 0o600)
            o.ex.write(o.host, self.work + '/progress.json', durable.canonical({'phase': 'prepared'}), 0o600)
            return {'prepared_storage': {'id': self.id, 'manifest_sha256': durable.digest(manifest)}, 'roots': plan['roots']}

    def run(self, binary, product, pair, *, verify=False):
        argv = [binary, 'prepare-transparent-storage', '--config', self.config_path, '--product', PRODUCTS[product][1],
                '--source-root', pair['source'], '--target-root', pair['target']]
        if verify:
            argv.append('--verify-only')
        code, output = self.owner.ex.run(self.owner.host, argv, 300 if verify else 7200)
        require(code == 0, 'storage preparation refused; predecessor roots stay intact: ' + output[-1000:])

    def status(self):
        return {'progress': self.owner.read(self.work + '/progress.json'),
                'manifest_sha256': self.owner.ex.sha256(self.owner.host, self.manifest_path)}


class Handoff:
    def __init__(self, owner):
        self.owner = owner
        self.reference = owner.request['prepared_storage']
        validate_reference(self.reference)
        self.preparation = Preparation(owner, self.reference['id'])

    def manifest(self):
        raw = self.owner.read(self.preparation.manifest_path)
        require(publisher.sha(raw) == self.reference['manifest_sha256'], 'reviewed storage manifest changed')
        manifest = json.loads(raw)
        require(manifest['schema'] == 'publisher-storage-preparation/1'
                and manifest['release'] == self.owner.request['release'], 'prepared storage names another release')
        require(publisher.sha(self.owner.read(self.preparation.config_path)) == manifest['configuration_sha256'],
                'prepared configuration changed')
        return manifest

    def capture(self, installed):
        m = self.manifest()
        roots = configuration(installed, self.owner.request['configuration'], self.reference['id'])
        require(publisher.stable_unit(self.owner.ex.probe_unit(self.owner.host, publisher.UNIT)) == m['plan']['unit'],
                'predecessor owner changed since preparation')
        require(m['plan']['roots'] == roots
                and m['plan']['source_configuration_sha256'] == publisher.sha(self.owner.read(publisher.CONFIG)),
                'installed publisher differs from prepared source')
        config = json.loads(self.owner.read(self.preparation.config_path))
        require(config == self.owner.request['configuration'], 'candidate differs from prepared configuration')
        self.verify(m)
        return self.reference

    def verify(self, manifest):
        o = self.owner
        binary = publisher.ROOT_DIR + '/releases/' + o.request['release']['binary_sha256'] + '/pir-publisher'
        require(o.ex.sha256(o.host, binary) == o.request['release']['binary_sha256'], 'preparation executable changed')
        for product, pair in manifest['plan']['roots'].items():
            require(o.ex.sha256(o.host, pair['target'] + '/prepared-storage.json')
                    == manifest['products'][product]['receipt_sha256'], 'prepared storage receipt changed')
            self.preparation.run(binary, product, pair, verify=True)
            source_resolved(o, pair, manifest['products'][product]['receipt'])

    def install(self, journal):
        o = self.owner
        manifest = self.manifest()
        self.verify(manifest)
        # Save ownership before any revision write. Recovery always rolls back
        # from this point, even when a reply was lost halfway through seeding.
        journal['prepared_storage'] = {'reference': self.reference, 'roots': manifest['plan']['roots'],
                                      'retained': {p: tree(o, pair['source'])
                                                   for p, pair in manifest['plan']['roots'].items()}}
        o.save(journal)
        o.effect(journal, 'seed prepared storage revisions', lambda: advance_floors(o, manifest['plan']['roots'], 'target'))

    def rollback(self, journal):
        evidence = journal.get('prepared_storage')
        if evidence:
            self.owner.effect(journal, 'restore storage revision floors',
                              lambda: advance_floors(self.owner, evidence['roots'], 'source'))

    def cleanup_plan(self):
        o = self.owner
        journal = o.latest()
        require(journal is not None and journal['request'] == o.request and journal['phase'] == 'committed'
                and set(journal.get('checks', {})) == set(o.request['checks'])
                and all(v.get('passed') is True for v in journal['checks'].values()),
                'storage cleanup requires the accepted committed cutover')
        evidence = journal.get('prepared_storage')
        require(evidence is not None and evidence['reference'] == self.reference, 'cutover did not consume this preparation')
        require(json.loads(o.read(publisher.CONFIG)) == o.request['configuration'], 'installed roots changed')
        state = o.ex.probe_unit(o.host, publisher.UNIT)
        require(state['active_state'] == 'active' and state['main_pid']
                and state['exe_sha256'] == o.request['release']['binary_sha256'], 'candidate publisher is not serving')
        # Cleanup retires rollback; never silently do it during deployment.
        for product, pair in evidence['roots'].items():
            require(tree(o, pair['source']) == evidence['retained'][product], 'retained source changed after handoff')
        return {'transaction': journal['id'], 'reference': self.reference, 'roots': evidence['roots'],
                'retained': evidence['retained'], 'unit': publisher.stable_unit(state)}

    def cleanup(self, expected):
        o = self.owner
        with production_lock(o.inventory, o.ex) as o.lock:
            journal = o.latest()
            require(journal is not None and journal['request'] == o.request and journal['phase'] == 'committed',
                    'cleanup requires this committed cutover')
            existing = journal.get('storage_cleanup')
            plan = existing['plan'] if existing else self.cleanup_plan()
            require(durable.digest(plan) == expected, 'cleanup plan changed')
            require(json.loads(o.read(publisher.CONFIG)) == o.request['configuration']
                    and publisher.stable_unit(o.ex.probe_unit(o.host, publisher.UNIT)) == plan['unit'],
                    'publisher changed before storage cleanup')
            if existing and existing['phase'] == 'removed':
                return {'removed': [p['source'] for p in plan['roots'].values()], 'rollback_retired': True}
            o.ready(True)
            o.exact(journal)
            o.ex.publisher_unlocked(o.host, [pair['source'] + '/controller.lock' for pair in plan['roots'].values()])
            # A durable intent makes rollback refuse after even partial cleanup.
            journal['storage_cleanup'] = existing or {'phase': 'removing', 'plan': plan, 'completed': []}
            o.save(journal)
            for product, pair in plan['roots'].items():
                o.lock.verify()
                if product not in journal['storage_cleanup']['completed']:
                    tree(o, pair['source'], remove=plan['retained'][product])
                    journal['storage_cleanup']['completed'].append(product)
                    o.save(journal)
            journal['storage_cleanup']['phase'] = 'removed'
            o.save(journal)
            return {'removed': [p['source'] for p in plan['roots'].values()], 'rollback_retired': True}


def register(commands):
    for operation in ('plan', 'prepare', 'status', 'cleanup-plan', 'cleanup'):
        command = commands.add_parser('publisher-storage-' + operation, allow_abbrev=False)
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        command.add_argument('--preparation', required=True)
        if operation == 'prepare':
            command.add_argument('--archive', required=True)
        if operation in ('prepare', 'cleanup'):
            command.add_argument('--expect-plan-sha256', required=True)


def dispatch(args, executor=None, **_options):
    from .remote import SSHExecutor
    request, inventory = publisher.load_request(args.request, args.request_sha256)
    owner = publisher.Cutover(request, inventory, executor or SSHExecutor(inventory), args.state_dir)
    operation = args.command.removeprefix('publisher-storage-')
    preparation = Preparation(owner, args.preparation)
    if operation in ('cleanup', 'cleanup-plan'):
        require(request.get('prepared_storage', {}).get('id') == args.preparation, 'cleanup names another preparation')
        handoff = Handoff(owner)
        if operation == 'cleanup':
            return handoff.cleanup(args.expect_plan_sha256)
        plan = handoff.cleanup_plan()
        return {'plan': plan, 'plan_sha256': durable.digest(plan)}
    if operation == 'status':
        return preparation.status()
    if operation == 'plan':
        return preparation.plan()
    with tempfile.TemporaryDirectory(prefix='publisher-storage-') as directory:
        bundle = Path(directory) / 'bundle'
        release.extract(Path(args.archive), bundle, request['release']['git_sha'], 'pir-publisher')
        require(json.loads((bundle / 'candidate.json').read_text()).get('source_dirty') is False,
                'storage preparation requires a clean release')
        return preparation.prepare(bundle / 'pir-publisher', args.expect_plan_sha256)
