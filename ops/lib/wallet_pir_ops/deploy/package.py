"""Install, select and roll back the operations package on the coordinator.

The package is the unpacked operations release (`bundle`), kept on the
coordinator as root-owned `/opt/wallet-pir-ops/releases/<full-sha>`; the
`current` link selects one. The maintained operator inventory,
`/etc/wallet-pir/fleet.json`, lives outside the releases. Installing a package
and adopting an inventory change only those paths: no unit is written, reloaded
or restarted, so a package transaction never adopts or changes a service.

Every install is planned first. The plan binds the archive and manifest
digests, the source revision, the full-CI run that produced the archive, the
coordinator's machine id, the candidate inventory, and what the coordinator
holds now (selection, inventory, release directory, latest package
transaction). Install takes the production lock, re-plans and refuses unless
the plan digest equals the reviewed one. It then runs fixed steps. Before each
step the runner journal (`transaction.Journal`) and, once it exists, the
coordinator's transaction directory record the intent; afterwards both record
completion. A transport failure leaves the outcome unknown: nothing is guessed
or undone until `reconcile` (finish) or `rollback` (undo) observes the host.
Reconcile continues only where each step's observed state is exactly its
recorded before or after state; anything else is refused. Rollback restores the
previous selection and inventory, including their absence on a first install,
and keeps the release directory and the transaction's records.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from .. import bundle, durable, release
from .engine import DeployError, file_sha256, mutation_fence
from .remote import LockHeld, RemoteError, production_lock
from .transaction import FINAL, Journal

SERVICE = 'ops-package'
PLAN_SCHEMA = 'wallet-pir-ops-package-plan-v1'
RECORD_SCHEMA = 'wallet-pir-ops-package-record-v1'
REVISION = re.compile('[0-9a-f]{40}')
TRANSACTION = re.compile(r'ops-package-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}-[0-9a-f]{6}')
SELECTION = re.compile(r'releases/[0-9a-f]{40}')
HEADROOM = 64 << 20
SELF_CHECK_SECONDS = 60
# Failures that leave a remote effect's outcome unknown.
TRANSPORT = (RemoteError, LockHeld, subprocess.TimeoutExpired, OSError)


class PackageError(DeployError):
    pass


@dataclass(frozen=True)
class Layout:
    root: str = '/opt/wallet-pir-ops'
    fleet: str = '/etc/wallet-pir/fleet.json'
    machine_id: str = '/etc/machine-id'
    owner_uid: int = 0

    @property
    def releases(self):
        return self.root + '/releases'

    @property
    def staging(self):
        return self.root + '/staging'

    @property
    def transactions(self):
        return self.root + '/transactions'

    @property
    def current(self):
        return self.root + '/current'

    @property
    def pointer(self):
        return self.transactions + '/latest.json'

    def transaction(self, identifier):
        return '%s/%s' % (self.transactions, identifier)


def qualification(run, revision):
    """The full-CI provenance of an archive, from the GitHub run record that produced it."""
    if run is None:
        return {'provenance': 'absent'}
    fields = ('id', 'head_sha', 'head_branch', 'event', 'status', 'conclusion', 'path')
    if not isinstance(run, dict) or not all(k in run for k in fields):
        raise PackageError('the CI run record must be the GitHub run object (gh api .../actions/runs/ID)')
    record = {k: run[k] for k in fields}
    if not release.qualified(run, revision):
        return {'provenance': 'not-qualified', 'run': record}
    return {'provenance': 'full-ci-main', 'run': record,
            'artifact': 'wallet-pir-ops-%s' % revision}


def tree_problems(tree, files, owner):
    """Why a directory listing is not exactly the planned package (files, modes, owner)."""
    root = tree['root']
    if root is None or root['kind'] != 'dir':
        return ['missing']
    problems = []
    if root['uid'] != owner or root['mode'] != 0o755:
        problems.append('release directory must be owned by uid %d with mode 0755' % owner)
    expected_dirs = {str(Path(n).parent) for n in files} - {'.'}
    expected_dirs |= {str(p) for d in list(expected_dirs) for p in Path(d).parents if str(p) != '.'}
    for name, entry in sorted(tree['entries'].items()):
        if entry['kind'] == 'dir' and name in expected_dirs:
            if entry['uid'] != owner or entry['mode'] != 0o755:
                problems.append('%s: directory must be uid %d mode 0755' % (name, owner))
        elif entry['kind'] == 'file' and name in files:
            digest, mode = files[name]
            if entry['sha256'] != digest:
                problems.append('%s: content differs' % name)
            if entry['mode'] != mode or entry['uid'] != owner:
                problems.append('%s: must be uid %d mode %o' % (name, owner, mode))
        else:
            problems.append('%s: unplanned %s' % (name, entry['kind']))
    missing = sorted(set(files) - {n for n, e in tree['entries'].items() if e['kind'] == 'file'})
    if missing:
        problems.append('missing files: %s' % missing[:5])
    return problems


class Package:
    """Plan, preflight, install, status, reconcile and rollback of the package on one coordinator host."""

    def __init__(self, inventory, executor, state_dir, host=None, layout=None, lock=None, out=print,
                 inventory_path='<inventory>'):
        self.inventory = inventory
        self.ex = executor
        self.state_dir = Path(state_dir)
        self.layout = layout or Layout()
        if host is None:
            if inventory.lock.get('type') != 'remote':
                raise PackageError('name the coordinator with --host when the lock is pinned to this runner')
            host = inventory.lock['host']
        if host not in inventory.hosts:
            raise PackageError('unknown coordinator host %r' % host)
        self.host = host
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        self.out = out
        self.inventory_path = inventory_path

    def command(self, name, *arguments):
        return ' '.join(['python3', 'ops/scripts/wallet-pir-deploy.py', '--inventory', str(self.inventory_path),
                         name, *arguments])

    # ------------------------------------------------------------- planning

    def candidate_problems(self, archive, revision, fleet):
        """Run the target package's own complete-fleet validation of the candidate inventory."""
        with tempfile.TemporaryDirectory(prefix='wallet-pir-ops-plan-') as directory:
            unpacked = Path(directory) / 'release'
            bundle.extract(archive, unpacked, revision)
            result = subprocess.run([sys.executable, '-B', str(unpacked / 'ops/scripts/wallet-pir-deploy.py'),
                                     '--inventory', str(fleet), 'fleet', '--complete'],
                                    capture_output=True, text=True, timeout=60, cwd=directory)
        if result.returncode:
            return ['candidate inventory is not a complete fleet for %s: %s'
                    % (revision[:12], (result.stdout + result.stderr).strip()[-2000:])]
        return []

    def observe(self, revision):
        layout = self.layout
        return self.ex.package_observe(self.host, layout.root, layout.fleet, revision, layout.machine_id)

    def remote_record(self, identifier):
        text = self.ex.read(self.host, self.layout.transaction(identifier) + '/record.json')
        return None if text is None else json.loads(text)

    def remote_final(self, identifier):
        text = self.ex.read(self.host, self.layout.transaction(identifier) + '/final.json')
        return None if text is None else json.loads(text)

    def build_plan(self, archive, revision, fleet, ci_run=None):
        """`(plan, problems)`; reads the archive, the candidate inventory and the coordinator only."""
        if not REVISION.fullmatch(revision or ''):
            raise PackageError('--sha must be the full 40-hex source revision')
        archive = Path(archive).resolve()
        manifest, files, size = bundle.planned_files(archive, revision)
        fleet_bytes = Path(fleet).read_bytes()
        json.loads(fleet_bytes)
        layout = self.layout
        observed = self.observe(revision)
        problems = []
        problems += self.candidate_problems(archive, revision, fleet)
        if manifest['source_dirty']:
            problems.append('the archive was built from a dirty source tree; only a clean full-CI archive installs')
        provenance = qualification(ci_run, revision)
        if provenance['provenance'] != 'full-ci-main':
            problems.append('no successful full-CI run on main for %s binds this archive (%s)'
                            % (revision, provenance['provenance']))
        machine_id = observed['machine_id']
        expected = [v for v in (self.inventory.hosts[self.host].get('machine_id'),
                                self.inventory.lock.get('machine_id')) if v]
        if any(v != machine_id for v in expected):
            problems.append('%s answers as machine %s, not the inventory\'s %s' % (self.host, machine_id, expected))
        for path, state in observed['paths'].items():
            if state is None:
                continue
            if state['kind'] != 'dir' or state['uid'] != layout.owner_uid or state['mode'] & 0o022:
                problems.append('%s must be a directory owned by uid %d and writable only by it'
                                % (path, layout.owner_uid))
        current = observed['current']
        prior_selection = None
        if current is not None:
            if current['kind'] != 'link' or not SELECTION.fullmatch(current.get('target', '')):
                problems.append('%s is not a link to releases/<revision>' % layout.current)
            else:
                prior_selection = current['target']
        fleet_state = observed['fleet']
        prior_fleet = None
        if fleet_state is not None:
            if fleet_state['kind'] != 'file' or fleet_state['uid'] != layout.owner_uid or fleet_state['mode'] & 0o022:
                problems.append('%s must be a regular file owned by uid %d and writable only by it'
                                % (layout.fleet, layout.owner_uid))
            else:
                prior_fleet = fleet_state['sha256']
        prior_transaction = None
        if observed['pointer'] is not None:
            pointer = json.loads(observed['pointer'])
            prior_transaction = pointer.get('id') if isinstance(pointer, dict) else None
            if set(pointer) != {'id'} or not TRANSACTION.fullmatch(prior_transaction or ''):
                problems.append('invalid package transaction pointer')
            else:
                final = self.remote_final(prior_transaction)
                if final is None:
                    problems.append('package transaction %s is unfinished; run %s' % (
                        prior_transaction, self.command('ops-package-status')))
        if observed['staging']:
            problems.append('%s holds %s; reconcile or roll back their transactions first'
                            % (layout.staging, observed['staging']))
        release_state = 'absent'
        if observed['release'] is not None:
            tree = self.ex.package_tree(self.host, '%s/%s' % (layout.releases, revision))
            collision = tree_problems(tree, files, layout.owner_uid)
            if collision:
                problems.append('%s/%s exists and differs from this archive (%s); a release directory is never '
                                'overwritten' % (layout.releases, revision, '; '.join(collision[:3])))
            release_state = 'present-verified'
        required = HEADROOM + (2 * size if release_state == 'absent' else 0)
        if observed['free_bytes'] < required:
            problems.append('%s has %d bytes free; the package needs %d' % (self.host, observed['free_bytes'], required))
        target_selection = 'releases/' + revision
        candidate_fleet = hashlib.sha256(fleet_bytes).hexdigest()
        changes = []
        if release_state == 'absent':
            changes.append('stage, verify and add %s/%s' % (layout.releases, revision))
        if candidate_fleet != prior_fleet:
            changes.append('%s %s' % ('replace' if prior_fleet else 'install', layout.fleet))
        if target_selection != prior_selection:
            changes.append('point %s at %s' % (layout.current, target_selection))
        plan = {
            'schema': PLAN_SCHEMA,
            'coordinator': {'host': self.host, 'machine_id': machine_id},
            'layout': {'root': layout.root, 'fleet': layout.fleet},
            'package': {'revision': revision, 'archive_sha256': file_sha256(archive),
                        'manifest_sha256': files['operations.json'][0],
                        'source_dirty': manifest['source_dirty'], 'files': files, 'bytes': size},
            # The archive's own `qualification` label describes an integrity artifact;
            # provenance below is the separate full-CI record.
            'integrity': {'manifest_qualification': manifest['qualification'], 'checksums': 'verified'},
            'qualification': provenance,
            'prior': {'selection': prior_selection, 'fleet_sha256': prior_fleet,
                      'transaction': prior_transaction, 'release': release_state},
            'target': {'selection': target_selection, 'fleet_sha256': candidate_fleet},
            'changes': changes,
        }
        return plan, problems

    def describe(self, plan, problems):
        package = plan['package']
        self.out('ops package %s on %s (machine %s)' % (package['revision'], plan['coordinator']['host'],
                                                        plan['coordinator']['machine_id']))
        self.out('  integrity: archive %s, %d files, manifest label %r (integrity only)'
                 % (package['archive_sha256'], len(package['files']), plan['integrity']['manifest_qualification']))
        self.out('  provenance: %s' % json.dumps(plan['qualification'], sort_keys=True))
        self.out('  prior: selection %s, inventory %s, release %s'
                 % (plan['prior']['selection'], plan['prior']['fleet_sha256'], plan['prior']['release']))
        for change in plan['changes'] or ['no change']:
            self.out('  change: ' + change)
        self.out('  services: unchanged; a package transaction restarts and reloads nothing')
        for problem in problems:
            self.out('  refused: ' + problem)

    def preflight(self, archive, revision, fleet, ci_run=None):
        """The plan plus the read-only mutation fences an install would apply."""
        self.ex.check_identity(self.host)
        plan, problems = self.build_plan(archive, revision, fleet, ci_run)
        try:
            mutation_fence(self.inventory, self.ex)
        except ValueError as error:
            problems.append(str(error))
        latest = Journal.load(self.state_dir, SERVICE)
        if latest is not None and latest.status not in FINAL:
            problems.append('runner transaction %s is %s' % (latest.id, latest.status))
        return plan, problems

    # ---------------------------------------------------------------- steps

    def steps(self, plan):
        result = ['record']
        if plan['prior']['release'] == 'absent':
            result += ['stage', 'verify', 'promote']
        else:
            result += ['verify']
        if plan['target']['fleet_sha256'] != plan['prior']['fleet_sha256']:
            result.append('fleet')
        if plan['target']['selection'] != plan['prior']['selection']:
            result.append('select')
        return result + ['commit']

    def record_bytes(self, identifier, plan):
        return (json.dumps({'schema': RECORD_SCHEMA, 'transaction': identifier, 'plan': plan,
                            'plan_sha256': durable.digest(plan), 'steps': self.steps(plan)},
                           sort_keys=True, indent=2) + '\n').encode()

    def pointer_bytes(self, identifier):
        return (json.dumps({'id': identifier}, sort_keys=True) + '\n').encode()

    def marker(self, identifier, index, name, kind):
        return '%s/steps/%02d-%s.%s.json' % (self.layout.transaction(identifier), index, name, kind)

    def markers(self, identifier):
        tree = self.ex.package_tree(self.host, self.layout.transaction(identifier) + '/steps')
        return {name for name in tree['entries']}

    def staged_path(self, identifier):
        return '%s/%s' % (self.layout.staging, identifier)

    def state(self, identifier, plan, name):
        """'before', 'after', 'partial' (owned, discardable) or a conflict description."""
        layout, revision = self.layout, plan['package']['revision']
        files = plan['package']['files']
        observed = self.observe(revision)
        if name == 'record':
            record = self.ex.package_tree(self.host, layout.transaction(identifier))
            pointer = observed['pointer']
            expected = hashlib.sha256(self.record_bytes(identifier, plan)).hexdigest()
            if record['root'] is None:
                prior = plan['prior']['transaction']
                if pointer is None and prior is None or pointer and json.loads(pointer).get('id') == prior:
                    return 'before'
                return 'the package transaction pointer moved since the plan'
            entries = record['entries']
            complete = (entries.get('record.json', {}).get('sha256') == expected and
                        (plan['prior']['fleet_sha256'] is None or
                         entries.get('previous-fleet.json', {}).get('sha256') == plan['prior']['fleet_sha256']) and
                        pointer is not None and pointer.encode() == self.pointer_bytes(identifier))
            return 'after' if complete else 'partial'
        if name == 'stage':
            staged = self.ex.package_tree(self.host, self.staged_path(identifier))
            if staged['root'] is None:
                return 'before' if observed['release'] is None else 'the release directory appeared since the plan'
            return 'after' if not tree_problems(staged, files, layout.owner_uid) else 'partial'
        if name == 'verify':
            return 'before'
        if name == 'promote':
            staged = self.ex.package_tree(self.host, self.staged_path(identifier))
            final = self.ex.package_tree(self.host, '%s/%s' % (layout.releases, revision))
            if staged['root'] is not None and final['root'] is None:
                return 'before'
            if staged['root'] is None and not tree_problems(final, files, layout.owner_uid):
                return 'after'
            return 'neither the staged nor the final release directory is exactly the planned package'
        if name == 'fleet':
            current = (observed['fleet'] or {}).get('sha256')
            if current == plan['prior']['fleet_sha256']:
                return 'before'
            if current == plan['target']['fleet_sha256']:
                return 'after'
            return '%s is %s, neither the prior nor the planned inventory' % (layout.fleet, current)
        if name == 'select':
            current = (observed['current'] or {}).get('target')
            if current == plan['prior']['selection']:
                return 'before'
            if current == plan['target']['selection']:
                return 'after'
            return '%s points at %s, neither the prior nor the planned selection' % (layout.current, current)
        if name == 'commit':
            final = self.remote_final(identifier)
            return 'after' if final and final.get('status') == 'committed' else 'before'
        raise AssertionError(name)

    def verify_entrypoints(self, path, plan):
        """The staged or final release's entrypoints run from where they are and check their own files."""
        revision = plan['package']['revision']
        for argv in ([path + '/ops/scripts/wallet-pir-bundle.py', 'self-check', '--sha', revision],
                     [path + '/ops/scripts/wallet-pir-deploy.py', '--help']):
            code, output = self.ex.run(self.host, argv, SELF_CHECK_SECONDS)
            if code:
                raise PackageError('%s failed on %s (exit %d): %s' % (' '.join(argv[:2]), self.host, code, output[-500:]))

    def apply(self, identifier, plan, name, archive):
        layout, host = self.layout, self.host
        revision = plan['package']['revision']
        directory = layout.transaction(identifier)
        if name == 'record':
            for path, mode in ((layout.root, 0o755), (layout.releases, 0o755), (layout.staging, 0o755),
                               (layout.transactions, 0o700), (directory + '/steps', 0o700)):
                self.ex.mkdir(host, path, mode)
            self.ex.write(host, directory + '/record.json', self.record_bytes(identifier, plan), 0o600)
            if plan['prior']['fleet_sha256'] is not None:
                # The previous inventory, kept immutable for rollback.
                self.ex.package_copy(host, layout.fleet, directory + '/previous-fleet.json',
                                     plan['prior']['fleet_sha256'], 0o600)
            self.ex.write(host, layout.pointer, self.pointer_bytes(identifier), 0o600)
        elif name == 'stage':
            if archive is None:
                raise PackageError('staging needs the planned archive; pass --archive')
            if file_sha256(archive) != plan['package']['archive_sha256']:
                raise PackageError('%s is not the planned archive' % archive)
            self.ex.package_unpack(host, archive, self.staged_path(identifier), plan['package']['archive_sha256'],
                                   plan['package']['manifest_sha256'])
        elif name == 'verify':
            if plan['prior']['release'] == 'absent':
                path = self.staged_path(identifier)
            else:
                path = '%s/%s' % (layout.releases, revision)
            problems = tree_problems(self.ex.package_tree(host, path), plan['package']['files'], layout.owner_uid)
            if problems:
                raise PackageError('%s is not exactly the planned package: %s' % (path, '; '.join(problems[:5])))
            self.verify_entrypoints(path, plan)
        elif name == 'promote':
            self.ex.rename(host, self.staged_path(identifier), '%s/%s' % (layout.releases, revision))
        elif name == 'fleet':
            self.ex.mkdir(host, str(Path(layout.fleet).parent), 0o755)
            self.ex.write(host, layout.fleet, self.candidate, 0o600)
        elif name == 'select':
            self.ex.replace_link(host, layout.current, plan['target']['selection'])
        elif name == 'commit':
            self.ex.write(host, directory + '/final.json',
                          (json.dumps({'status': 'committed', 'transaction': identifier}) + '\n').encode(), 0o600)
        else:
            raise AssertionError(name)

    def run_steps(self, journal, plan, archive):
        identifier = journal.id
        steps = self.steps(plan)
        done = set()
        if self.remote_record(identifier) is not None:
            done = self.markers(identifier)
        for index, name in enumerate(steps):
            self.lock.verify()
            if ('%02d-%s.done.json' % (index, name)) in done:
                continue
            state = self.state(identifier, plan, name)
            if state == 'partial' and name == 'stage':
                journal.event('intent', step='discard-staging')
                self.ex.remove_tree(self.host, self.staged_path(identifier), self.layout.staging)
                state = 'before'
            elif state == 'partial' and name == 'record':
                state = 'before'  # every record write is the same bytes into this transaction's own directory
            if state not in ('before', 'after'):
                raise PackageError('%s: %s; the recorded intent no longer matches the coordinator' % (name, state))
            if state == 'before':
                journal.event('intent', step=name)
                if name != 'record':
                    self.ex.write(self.host, self.marker(identifier, index, name, 'intent'), b'{}\n', 0o600)
                self.apply(identifier, plan, name, archive)
            self.ex.write(self.host, self.marker(identifier, index, name, 'done'), b'{}\n', 0o600)
            journal.event('done', step=name, observed=state)
        journal.set_status('committed')

    # --------------------------------------------------------------- install

    def locked(self, action):
        self.ex.check_identity(self.host)
        if self.inventory.lock.get('host'):
            self.ex.check_identity(self.inventory.lock['host'])
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                mutation_fence(self.inventory, self.ex, lock)
                return action()
            finally:
                self.lock = None

    def install(self, archive, revision, fleet, expect, ci_run=None):
        """Returns the committed journal, or None when the coordinator already matches."""
        if not re.fullmatch('[0-9a-f]{64}', expect or ''):
            raise PackageError('install needs the reviewed plan digest (--expect-plan-sha256)')
        def action():
            latest = Journal.load(self.state_dir, SERVICE)
            if latest is not None and latest.status not in FINAL:
                raise PackageError('transaction %s is %s; run %s' % (latest.id, latest.status, self.next_command(latest.id, latest.status)))
            plan, problems = self.build_plan(archive, revision, fleet, ci_run)
            digest = durable.digest(plan)
            if digest != expect:
                raise PackageError('the coordinator or inputs changed since the reviewed plan (%s, now %s); '
                                   'plan again and review' % (expect, digest))
            if problems:
                raise PackageError('refused before any change:\n  ' + '\n  '.join(problems))
            if not plan['changes']:
                self.out('no-op: %s already selects %s with this inventory' % (self.host, revision))
                return None
            self.candidate = Path(fleet).read_bytes()
            journal = Journal.create(self.state_dir, SERVICE, revision, {'plan_sha256': digest}, [], None)
            journal.data['plan'] = plan
            journal.set_status('installing')
            self.out('transaction %s (%s)' % (journal.id, journal.path))
            self.execute(journal, plan, archive)
            return journal
        return self.locked(action)

    def next_command(self, identifier, status):
        if status in ('unknown', 'installing'):
            return '%s or %s' % (self.command('ops-package-reconcile', '--transaction', identifier, '--archive', '<archive>'),
                                 self.command('ops-package-rollback', '--transaction', identifier))
        if status in ('refused', 'rolling-back', 'rollback-unknown'):
            return self.command('ops-package-rollback', '--transaction', identifier)
        return self.command('ops-package-status')

    def execute(self, journal, plan, archive):
        try:
            self.run_steps(journal, plan, archive)
        except TRANSPORT as error:
            journal.set_status('unknown', error='%s: %s' % (type(error).__name__, error))
            raise PackageError('transport failed (%s); the last step\'s outcome is unknown until reconciled; run %s'
                               % (error, self.next_command(journal.id, 'unknown'))) from error
        except PackageError as error:
            journal.set_status('refused', error=str(error))
            raise PackageError('%s; nothing further was changed; run %s'
                               % (error, self.next_command(journal.id, 'refused'))) from error
        self.out('committed %s: %s selects %s; services unchanged'
                 % (journal.id, self.host, plan['target']['selection']))

    # ------------------------------------------------------------- recovery

    def load(self, identifier):
        """The transaction's plan and runner journal, from this runner or the coordinator's record."""
        if identifier is None:
            text = self.observe('0' * 40)['pointer']
            identifier = json.loads(text)['id'] if text else None
            if identifier is None:
                latest = Journal.load(self.state_dir, SERVICE)
                identifier = latest.id if latest else None
        if not TRANSACTION.fullmatch(identifier or ''):
            raise PackageError('no package transaction is recorded on %s or this runner' % self.host)
        path = self.state_dir / (identifier + '.json')
        journal = Journal(path, json.loads(path.read_text())) if path.exists() else None
        record = self.remote_record(identifier)
        if record is not None:
            if record.get('schema') != RECORD_SCHEMA or durable.digest(record['plan']) != record['plan_sha256']:
                raise PackageError('package record %s is not intact' % identifier)
            if journal is not None and journal.data['source']['plan_sha256'] != record['plan_sha256']:
                raise PackageError('runner journal and coordinator record of %s disagree' % identifier)
            plan = record['plan']
        elif journal is not None:
            plan = journal.data['plan']
        else:
            raise PackageError('transaction %s is not recorded here or on %s' % (identifier, self.host))
        if journal is None:
            # A receipt for this runner, created from the coordinator's record.
            self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            journal = Journal(path, {'id': identifier, 'service': SERVICE,
                                     'binary_sha256': plan['package']['revision'],
                                     'source': {'plan_sha256': durable.digest(plan)}, 'status': 'recovered',
                                     'created_unix': int(time.time()), 'hosts': [], 'baseline_before': None,
                                     'events': [], 'plan': plan})
            journal.save()
        return journal, plan

    def reconcile(self, identifier=None, archive=None, fleet=None):
        """Finish an unfinished install whose every observed step state matches its record."""
        def action():
            journal, plan = self.load(identifier)
            final = self.remote_final(journal.id)
            if final is not None:
                # The last write landed but its reply was lost: record what the coordinator says.
                journal.set_status(final['status'], observed='final record')
                self.out('%s is already %s' % (journal.id, final['status']))
                return journal
            if 'fleet' in self.steps(plan):
                self.candidate = Path(fleet).read_bytes() if fleet else None
                if self.candidate is None or hashlib.sha256(self.candidate).hexdigest() != plan['target']['fleet_sha256']:
                    if self.state(journal.id, plan, 'fleet') != 'after':
                        raise PackageError('finishing needs the planned inventory; pass --fleet')
            journal.set_status('installing')
            self.execute(journal, plan, archive)
            return journal
        return self.locked(action)

    def rollback(self, identifier=None):
        """Restore the prior selection and inventory; keep the release and the records."""
        def action():
            journal, plan = self.load(identifier)
            layout, host = self.layout, self.host
            pointer = self.observe(plan['package']['revision'])['pointer']
            latest = json.loads(pointer)['id'] if pointer else None
            if latest not in (journal.id, plan['prior']['transaction']):
                raise PackageError('later package transaction %s exists; roll it back first' % latest)
            final = self.remote_final(journal.id)
            if final is not None and final.get('status') == 'rolled-back':
                journal.set_status('rolled-back')
                self.out('%s is already rolled back' % journal.id)
                return journal
            observed = self.observe(plan['package']['revision'])
            selection = (observed['current'] or {}).get('target')
            fleet = (observed['fleet'] or {}).get('sha256')
            conflicts = []
            if selection not in (plan['prior']['selection'], plan['target']['selection']):
                conflicts.append('%s points at %s, changed outside this transaction' % (layout.current, selection))
            if fleet not in (plan['prior']['fleet_sha256'], plan['target']['fleet_sha256']):
                conflicts.append('%s is %s, changed outside this transaction' % (layout.fleet, fleet))
            if conflicts:
                journal.event('rollback refused', conflicts=conflicts)
                raise PackageError('rollback refused; nothing was changed:\n  ' + '\n  '.join(conflicts))
            journal.set_status('rolling-back')
            directory = layout.transaction(journal.id)
            try:
                if selection != plan['prior']['selection']:
                    journal.event('intent', step='restore-selection')
                    if plan['prior']['selection'] is None:
                        self.ex.remove(host, layout.current)
                    else:
                        self.ex.replace_link(host, layout.current, plan['prior']['selection'])
                    journal.event('done', step='restore-selection')
                if fleet != plan['prior']['fleet_sha256']:
                    journal.event('intent', step='restore-inventory')
                    if plan['prior']['fleet_sha256'] is None:
                        self.ex.remove(host, layout.fleet)
                    else:
                        previous = (self.ex.read(host, directory + '/previous-fleet.json') or '').encode()
                        if hashlib.sha256(previous).hexdigest() != plan['prior']['fleet_sha256']:
                            raise PackageError('the retained previous inventory of %s is missing or altered' % journal.id)
                        self.ex.write(host, layout.fleet, previous, 0o600)
                    journal.event('done', step='restore-inventory')
                if self.ex.package_tree(host, self.staged_path(journal.id))['root'] is not None:
                    journal.event('intent', step='discard-staging')
                    self.ex.remove_tree(host, self.staged_path(journal.id), layout.staging)
                    journal.event('done', step='discard-staging')
                if self.ex.package_tree(host, directory)['root'] is not None:
                    journal.event('intent', step='final')
                    self.ex.write(host, directory + '/final.json',
                                  (json.dumps({'status': 'rolled-back', 'transaction': journal.id}) + '\n').encode(),
                                  0o600)
            except TRANSPORT as error:
                journal.set_status('rollback-unknown', error='%s: %s' % (type(error).__name__, error))
                raise PackageError('transport failed during rollback (%s); re-run %s'
                                   % (error, self.next_command(journal.id, 'rollback-unknown'))) from error
            journal.set_status('rolled-back')
            self.out('rolled back %s: selection %s, inventory %s; release %s and the transaction records are kept'
                     % (journal.id, plan['prior']['selection'] or 'absent', plan['prior']['fleet_sha256'] or 'absent',
                        plan['package']['revision']))
            return journal
        return self.locked(action)

    # ---------------------------------------------------------------- status

    def status(self):
        """What the coordinator selects, whether it is intact, and the next command if a transaction is open."""
        self.ex.check_identity(self.host)
        layout = self.layout
        observed = self.observe('0' * 40)
        selection = (observed['current'] or {}).get('target')
        result = {'operation': 'ops-package-status', 'host': self.host, 'machine_id': observed['machine_id'],
                  'selection': selection, 'revision': None, 'integrity': None, 'qualification': None,
                  'fleet_sha256': (observed['fleet'] or {}).get('sha256'), 'transaction': None,
                  'phase': None, 'next': None,
                  'services': 'not changed by package transactions; service adoption is a product deployment'}
        if selection and SELECTION.fullmatch(selection):
            revision = selection.split('/')[1]
            result['revision'] = revision
            path = '%s/%s' % (layout.root, selection)
            manifest = self.ex.read(self.host, path + '/operations.json')
            if manifest is None:
                result['integrity'] = 'missing manifest'
            else:
                files = {name: [digest, bundle.mode(name)] for name, digest in json.loads(manifest)['files'].items()}
                files['operations.json'] = [hashlib.sha256(manifest.encode()).hexdigest(), 0o644]
                problems = tree_problems(self.ex.package_tree(self.host, path), files, layout.owner_uid)
                result['integrity'] = 'verified' if not problems else 'changed: ' + '; '.join(problems[:3])
        if observed['pointer']:
            identifier = json.loads(observed['pointer'])['id']
            record = self.remote_record(identifier)
            final = self.remote_final(identifier)
            result['transaction'] = identifier
            result['phase'] = final['status'] if final else 'unfinished'
            if record:
                if record['plan']['target']['selection'] == selection:
                    result['qualification'] = record['plan']['qualification']
                if not final:
                    result['next'] = self.next_command(identifier, 'unknown')
        latest = Journal.load(self.state_dir, SERVICE)
        if latest is not None:
            result['runner'] = {'transaction': latest.id, 'status': latest.status}
            if latest.status not in FINAL and result['next'] is None:
                result['next'] = self.next_command(latest.id, latest.status)
        for key in ('selection', 'revision', 'integrity', 'qualification', 'fleet_sha256', 'transaction', 'phase',
                    'services', 'next'):
            self.out('%s: %s' % (key, json.dumps(result[key]) if isinstance(result[key], dict) else result[key]))
        return result
