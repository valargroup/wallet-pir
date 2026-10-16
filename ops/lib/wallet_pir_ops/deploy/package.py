"""Install, select and roll back the operations package on the coordinator.

The package is the unpacked operations release (`bundle`), kept on the
coordinator as root-owned `/opt/wallet-pir-ops/releases/<full-sha>`; the
`current` link selects one. Two managed files live outside the releases: the
operator inventory `/etc/wallet-pir/fleet.json` and, when given, the reviewed
public host-key pins `/etc/wallet-pir/known_hosts`. A package transaction
changes only those paths: no unit is written, reloaded or restarted, so it
never adopts or changes a service.

The archive's bytes come from the exact full-CI artifact on GitHub: the run
must be a successful full CI on main for the revision, the artifact
`wallet-pir-ops-<revision>` is downloaded and checked against GitHub's own
digest when it reports one, and the archive inside it is what installs. A
local `--archive` is accepted only when its digest equals those bytes. There
is no other way to qualify an archive.

Every install is planned first. The plan binds the archive and manifest
digests, the source revision, the run and artifact identities, the
coordinator's machine id, the candidate managed files, and what the
coordinator holds now (selection, managed files, release directory, latest
package transaction). Install takes the production lock, re-plans and refuses
unless the plan digest equals the reviewed one. It then runs fixed steps.
Before each step the runner journal (`transaction.Journal`) and, once it
exists, the coordinator's transaction directory record the intent; afterwards
both record completion. Every remote mutation verifies lock ownership again;
losing the lock stops effects and leaves the outcome unknown. Completed steps
are observed again during recovery, and the selected package and managed files
are verified before commit. A transport failure leaves the outcome unknown:
nothing is guessed or undone until `reconcile` (finish) or `rollback` (undo)
observes the host. Reconcile continues only where each step's observed state
is exactly its recorded before or after state; anything else is refused.
Rollback restores the previous selection and managed files, including their
absence on a first install, and keeps the release directory and the
transaction's records. Before it changes anything, the release it selects
again must be exactly the files, owners and modes recorded by the transaction
that promoted it, and must pass its own self-check where it is; a missing,
changed or unproven release refuses and the current selection stays. A later plan may select that retained release again:
a release directory is reused only when a recorded package transaction
promoted it and its files still match, and is never overwritten.
"""
from dataclasses import dataclass
import base64
import binascii
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import zipfile

from .. import bundle, durable, release
from .engine import DeployError, file_sha256, mutation_fence
from .remote import LockHeld, RemoteError, production_lock
from .transaction import FINAL, Journal

SERVICE = 'ops-package'
PLAN_SCHEMA = 'wallet-pir-ops-package-plan-v2'
RECORD_SCHEMA = 'wallet-pir-ops-package-record-v1'
REVISION = re.compile('[0-9a-f]{40}')
TRANSACTION = re.compile(r'ops-package-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}-[0-9a-f]{6}')
SELECTION = re.compile(r'releases/[0-9a-f]{40}')
ARCHIVE = 'wallet-pir-ops.tar.gz'
HEADROOM = 64 << 20
SELF_CHECK_SECONDS = 60
KEY_TYPES = {'ssh-ed25519', 'ssh-rsa', 'ecdsa-sha2-nistp256', 'ecdsa-sha2-nistp384', 'ecdsa-sha2-nistp521',
             'sk-ssh-ed25519@openssh.com', 'sk-ecdsa-sha2-nistp256@openssh.com'}
# Failures that leave a remote effect's outcome unknown.
TRANSPORT = (RemoteError, LockHeld, subprocess.TimeoutExpired, OSError)


class PackageError(DeployError):
    pass


@dataclass(frozen=True)
class Layout:
    root: str = '/opt/wallet-pir-ops'
    fleet: str = '/etc/wallet-pir/fleet.json'
    known_hosts: str = '/etc/wallet-pir/known_hosts'
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

    def managed(self):
        """`(step, path, retained copy name)` of each managed file, in install order."""
        return (('known-hosts', self.known_hosts, 'previous-known_hosts'),
                ('fleet', self.fleet, 'previous-fleet.json'))


class GitHub:
    """Run and artifact records, and artifact bytes, from the repository's Actions API."""

    def __init__(self, repository=None):
        self.repository = repository or os.environ.get('GITHUB_REPOSITORY') or 'valargroup/wallet-pir'

    def run(self, run_id):
        return release.api('repos/%s/actions/runs/%d' % (self.repository, run_id))

    def artifacts(self, run_id):
        return release.api('repos/%s/actions/runs/%d/artifacts?per_page=100' % (self.repository, run_id))['artifacts']

    def download(self, artifact_id):
        # gh follows the redirect to storage without forwarding the token.
        return subprocess.run(['gh', 'api', 'repos/%s/actions/artifacts/%d/zip' % (self.repository, artifact_id)],
                              capture_output=True, check=True, timeout=900).stdout


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def known_hosts_problems(data):
    """Why `data` is not a plain file of public host keys."""
    try:
        text = data.decode()
    except UnicodeDecodeError:
        return ['known-hosts file is not text']
    if 'PRIVATE KEY' in text:
        return ['known-hosts file contains private key material']
    problems = []
    for number, line in enumerate(text.splitlines(), 1):
        fields = line.split()
        if not fields or fields[0].startswith('#'):
            continue
        if fields[0] in ('@cert-authority', '@revoked'):
            fields = fields[1:]
        if len(fields) < 3 or fields[1] not in KEY_TYPES:
            problems.append('known-hosts line %d is not "hosts key-type key"' % number)
            continue
        try:
            base64.b64decode(fields[2], validate=True)
        except (binascii.Error, ValueError):
            problems.append('known-hosts line %d has an invalid key' % number)
    if not any(line.split() and not line.startswith('#') for line in text.splitlines()):
        problems.append('known-hosts file pins no host')
    return problems


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
                 inventory_path='<inventory>', github=None):
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
        self.github = github or GitHub()
        self.candidates = {}

    def command(self, name, *arguments):
        return ' '.join(['python3', 'ops/scripts/wallet-pir-deploy.py', '--inventory', str(self.inventory_path),
                         name, *arguments])

    # ----------------------------------------------------------- provenance

    def artifact_dir(self, artifact_id):
        return self.state_dir / 'artifacts' / str(int(artifact_id))

    def acquire(self, run_id, revision):
        """`(provenance, archive path)` for the exact full-CI artifact of `revision` in run `run_id`."""
        run = self.github.run(run_id)
        if run.get('id') != run_id or not release.qualified(run, revision):
            raise PackageError('run %s is not a successful full CI on main for %s' % (run_id, revision))
        name = 'wallet-pir-ops-' + revision
        matches = [a for a in self.github.artifacts(run_id) if a.get('name') == name]
        if len(matches) != 1 or matches[0].get('expired') or not isinstance(matches[0].get('id'), int):
            raise PackageError('run %s has no single unexpired %s artifact' % (run_id, name))
        artifact = matches[0]
        reported = artifact.get('digest')
        if reported is not None and not re.fullmatch('sha256:[0-9a-f]{64}', reported):
            raise PackageError('GitHub reports an unrecognized digest for %s' % name)
        expected = reported and reported[len('sha256:'):]
        cache = self.artifact_dir(artifact['id'])
        zipped = cache / 'artifact.zip'
        data = zipped.read_bytes() if zipped.is_file() else None
        if data is None or expected and sha256(data) != expected:
            data = self.github.download(artifact['id'])
            if expected and sha256(data) != expected:
                raise PackageError('the downloaded %s differs from the digest GitHub reports' % name)
            cache.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.write_private(zipped, data)
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = archive.infolist()
            if [m.filename for m in members] != [ARCHIVE] or members[0].is_dir():
                raise PackageError('%s must hold exactly %s' % (name, ARCHIVE))
            payload = archive.read(ARCHIVE)
        path = cache / ARCHIVE
        if not path.is_file() or path.read_bytes() != payload:
            self.write_private(path, payload)
        provenance = {
            'provenance': 'full-ci-main', 'repository': self.github.repository,
            'run': {k: run[k] for k in ('id', 'head_sha', 'head_branch', 'event', 'status', 'conclusion', 'path')},
            'artifact': {'id': artifact['id'], 'name': name, 'zip_sha256': sha256(data),
                         'digest_source': 'github' if expected else 'downloaded'},
            'archive_sha256': sha256(payload),
        }
        return provenance, path

    @staticmethod
    def write_private(path, data):
        temporary = path.with_name('.' + path.name + '.tmp')
        with open(temporary, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fchmod(handle.fileno(), 0o600)
            os.fsync(handle.fileno())
        os.replace(temporary, path)

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

    def pin_problems(self, fleet_bytes, known_hosts_sha256):
        """Inventory entries that name the managed pin file must pin its planned digest."""
        document = json.loads(fleet_bytes)
        problems = []
        entries = [('ssh', document.get('ssh', {}))] + list(document.get('hosts', {}).items())
        for name, entry in entries:
            if isinstance(entry, dict) and entry.get('known_hosts') == self.layout.known_hosts:
                if known_hosts_sha256 is None:
                    problems.append('%s names %s, which this plan leaves absent' % (name, self.layout.known_hosts))
                elif entry.get('known_hosts_sha256') != known_hosts_sha256:
                    problems.append('%s pins %s to %s, not the planned %s'
                                    % (name, self.layout.known_hosts, entry.get('known_hosts_sha256'),
                                       known_hosts_sha256))
        return problems

    def observe(self, revision):
        layout = self.layout
        return self.ex.package_observe(self.host, layout.root, [path for _, path, _ in layout.managed()],
                                       revision, layout.machine_id)

    def remote_record(self, identifier):
        text = self.ex.read(self.host, self.layout.transaction(identifier) + '/record.json')
        return None if text is None else json.loads(text)

    def remote_final(self, identifier):
        text = self.ex.read(self.host, self.layout.transaction(identifier) + '/final.json')
        return None if text is None else json.loads(text)

    def promoters(self, revision):
        """`[(transaction, plan)]` of the recorded package transactions that promoted `revision`."""
        tree = self.ex.package_tree(self.host, self.layout.transactions)
        result = []
        for name in sorted(tree['entries']):
            parts = name.split('/')
            if (len(parts) == 3 and parts[1] == 'steps' and TRANSACTION.fullmatch(parts[0])
                    and re.fullmatch(r'[0-9]{2}-promote\.done\.json', parts[2])):
                record = self.remote_record(parts[0]) or {}
                plan = record.get('plan', {})
                if (plan.get('package', {}).get('revision') == revision and record.get('schema') == RECORD_SCHEMA
                        and durable.digest(plan) == record.get('plan_sha256')):
                    result.append((parts[0], plan))
        return result

    def promoted_by(self, revision, manifest_sha256):
        """The recorded package transaction that promoted this exact release, or None."""
        return next((identifier for identifier, plan in self.promoters(revision)
                     if plan['package']['manifest_sha256'] == manifest_sha256), None)

    def path_problems(self, observed):
        """The package root and the managed files' directories must be root's alone."""
        return ['%s must be a directory owned by uid %d and writable only by it' % (path, self.layout.owner_uid)
                for path, state in observed['paths'].items()
                if state is not None and (state['kind'] != 'dir' or state['uid'] != self.layout.owner_uid
                                          or state['mode'] & 0o022)]

    def release_problems(self, selection):
        """Why the release `selection` names is not exactly, and working as, what a recorded transaction promoted.

        A rollback selects that release again, so it must still be the files,
        owners and modes in the record of the transaction that promoted it,
        and its entrypoints must check themselves from where they are.
        """
        revision = selection.split('/')[1]
        owners = self.promoters(revision)
        if len({durable.digest(plan['package']['files']) for _, plan in owners}) != 1:
            return ['%s has no single recorded package transaction that promoted it' % selection]
        plan = owners[0][1]
        path = '%s/%s' % (self.layout.root, selection)
        problems = tree_problems(self.ex.package_tree(self.host, path), plan['package']['files'],
                                 self.layout.owner_uid)
        if problems:
            return ['%s is not the package %s promoted: %s' % (path, owners[0][0], '; '.join(problems[:5]))]
        try:
            self.verify_entrypoints(path, plan)
        except PackageError as error:
            return [str(error)]
        return []

    def build_plan(self, revision, fleet, run_id=None, archive=None, known_hosts=None):
        """`(plan, problems, archive path)`; reads GitHub, the inputs and the coordinator, changing none."""
        if not REVISION.fullmatch(revision or ''):
            raise PackageError('--sha must be the full 40-hex source revision')
        problems = []
        if run_id is not None:
            provenance, trusted = self.acquire(run_id, revision)
            if archive is not None and file_sha256(archive) != provenance['archive_sha256']:
                problems.append('%s is not the archive of artifact %s' % (archive, provenance['artifact']['name']))
            archive = trusted
        elif archive is not None:
            provenance = {'provenance': 'absent'}
            problems.append('no full-CI run qualifies this archive; pass --ci-run-id (local bytes alone never install)')
        else:
            raise PackageError('give --ci-run-id (or --archive to inspect an unqualified archive)')
        archive = Path(archive).resolve()
        manifest, files, size = bundle.planned_files(archive, revision)
        layout = self.layout
        candidates = {'fleet': Path(fleet).read_bytes(),
                      'known-hosts': Path(known_hosts).read_bytes() if known_hosts else None}
        json.loads(candidates['fleet'])
        if candidates['known-hosts'] is not None:
            problems += known_hosts_problems(candidates['known-hosts'])
        observed = self.observe(revision)
        problems += self.candidate_problems(archive, revision, fleet)
        if manifest['source_dirty']:
            problems.append('the archive was built from a dirty source tree; only a clean full-CI archive installs')
        machine_id = observed['machine_id']
        expected = [v for v in (self.inventory.hosts[self.host].get('machine_id'),
                                self.inventory.lock.get('machine_id')) if v]
        if any(v != machine_id for v in expected):
            problems.append('%s answers as machine %s, not the inventory\'s %s' % (self.host, machine_id, expected))
        problems += self.path_problems(observed)
        current = observed['current']
        prior_selection = None
        if current is not None:
            if current['kind'] != 'link' or not SELECTION.fullmatch(current.get('target', '')):
                problems.append('%s is not a link to releases/<revision>' % layout.current)
            else:
                prior_selection = current['target']
        prior_files, target_files = {}, {}
        for step, path, _ in layout.managed():
            state = observed['files'][path]
            prior_files[step] = None
            if state is not None:
                if state['kind'] != 'file' or state['uid'] != layout.owner_uid or state['mode'] & 0o022:
                    problems.append('%s must be a regular file owned by uid %d and writable only by it'
                                    % (path, layout.owner_uid))
                else:
                    prior_files[step] = state['sha256']
            data = candidates[step]
            # An absent candidate leaves the file as it is.
            target_files[step] = prior_files[step] if data is None else sha256(data)
        problems += self.pin_problems(candidates['fleet'], target_files['known-hosts'])
        prior_transaction = None
        if observed['pointer'] is not None:
            pointer = json.loads(observed['pointer'])
            prior_transaction = pointer.get('id') if isinstance(pointer, dict) else None
            if set(pointer) != {'id'} or not TRANSACTION.fullmatch(prior_transaction or ''):
                problems.append('invalid package transaction pointer')
            elif self.remote_final(prior_transaction) is None:
                problems.append('package transaction %s is unfinished; run %s' % (
                    prior_transaction, self.command('ops-package-status')))
        if observed['staging']:
            problems.append('%s holds %s; reconcile or roll back their transactions first'
                            % (layout.staging, observed['staging']))
        release_state = 'absent'
        if observed['release'] is not None:
            release_state = 'retained'
            path = '%s/%s' % (layout.releases, revision)
            differences = tree_problems(self.ex.package_tree(self.host, path), files, layout.owner_uid)
            if differences:
                problems.append('%s exists and differs from this archive (%s); a release directory is never '
                                'overwritten' % (path, '; '.join(differences[:3])))
            elif self.promoted_by(revision, files['operations.json'][0]) is None:
                problems.append('%s matches this archive but no recorded package transaction created it; '
                                'it is not reused' % path)
        required = HEADROOM + (2 * size if release_state == 'absent' else 0)
        if observed['free_bytes'] < required:
            problems.append('%s has %d bytes free; the package needs %d' % (self.host, observed['free_bytes'], required))
        target_selection = 'releases/' + revision
        changes = []
        if release_state == 'absent':
            changes.append('stage, verify and add %s/%s' % (layout.releases, revision))
        for step, path, _ in layout.managed():
            if target_files[step] != prior_files[step]:
                changes.append('%s %s' % ('replace' if prior_files[step] else 'install', path))
        if target_selection != prior_selection:
            changes.append('point %s at %s%s' % (layout.current, target_selection,
                                                 ' (retained release)' if release_state == 'retained' else ''))
        plan = {
            'schema': PLAN_SCHEMA,
            'coordinator': {'host': self.host, 'machine_id': machine_id},
            'layout': {'root': layout.root, 'files': {step: path for step, path, _ in layout.managed()}},
            'package': {'revision': revision, 'archive_sha256': file_sha256(archive),
                        'manifest_sha256': files['operations.json'][0],
                        'source_dirty': manifest['source_dirty'], 'files': files, 'bytes': size},
            # The archive's own `qualification` label describes an integrity artifact;
            # provenance below is the separate full-CI record.
            'integrity': {'manifest_qualification': manifest['qualification'], 'checksums': 'verified'},
            'qualification': provenance,
            'prior': {'selection': prior_selection, 'files': prior_files,
                      'transaction': prior_transaction, 'release': release_state},
            'target': {'selection': target_selection, 'files': target_files},
            'changes': changes,
        }
        self.candidates = candidates
        return plan, problems, archive

    def describe(self, plan, problems):
        package = plan['package']
        self.out('ops package %s on %s (machine %s)' % (package['revision'], plan['coordinator']['host'],
                                                        plan['coordinator']['machine_id']))
        self.out('  integrity: archive %s, %d files, manifest label %r (integrity only)'
                 % (package['archive_sha256'], len(package['files']), plan['integrity']['manifest_qualification']))
        self.out('  provenance: %s' % json.dumps(plan['qualification'], sort_keys=True))
        self.out('  prior: selection %s, files %s, release %s'
                 % (plan['prior']['selection'], json.dumps(plan['prior']['files'], sort_keys=True),
                    plan['prior']['release']))
        for change in plan['changes'] or ['no change']:
            self.out('  change: ' + change)
        self.out('  services: unchanged; a package transaction restarts and reloads nothing')
        for problem in problems:
            self.out('  refused: ' + problem)

    def preflight(self, revision, fleet, run_id=None, archive=None, known_hosts=None):
        """The plan plus the read-only mutation fences an install would apply."""
        self.ex.check_identity(self.host)
        plan, problems, archive = self.build_plan(revision, fleet, run_id, archive, known_hosts)
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
        for step, _, _ in self.layout.managed():
            if plan['target']['files'][step] != plan['prior']['files'][step]:
                result.append(step)
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

    def managed_path(self, step):
        return next(path for name, path, _ in self.layout.managed() if name == step)

    def state(self, identifier, plan, name):
        """'before', 'after', 'partial' (owned, discardable) or a conflict description."""
        layout, revision = self.layout, plan['package']['revision']
        files = plan['package']['files']
        observed = self.observe(revision)
        if name == 'record':
            record = self.ex.package_tree(self.host, layout.transaction(identifier))
            pointer = observed['pointer']
            expected = sha256(self.record_bytes(identifier, plan))
            if record['root'] is None:
                prior = plan['prior']['transaction']
                if pointer is None and prior is None or pointer and json.loads(pointer).get('id') == prior:
                    return 'before'
                return 'the package transaction pointer moved since the plan'
            entries = record['entries']
            complete = (entries.get('record.json', {}).get('sha256') == expected and
                        all(plan['prior']['files'][step] is None or
                            entries.get(copy, {}).get('sha256') == plan['prior']['files'][step]
                            for step, _, copy in layout.managed()) and
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
        if name in plan['target']['files']:
            path = self.managed_path(name)
            current = (observed['files'][path] or {}).get('sha256')
            if current == plan['prior']['files'][name]:
                return 'before'
            if current == plan['target']['files'][name]:
                entry = observed['files'][path] or {}
                if (entry.get('kind') != 'file' or entry.get('uid') != layout.owner_uid
                        or entry.get('mode') != (0o600 if name == 'fleet' else 0o644)):
                    return '%s metadata no longer matches the planned file' % path
                return 'after'
            return '%s is %s, neither the prior nor the planned file' % (path, current)
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

    def locked_effect(self, operation, *arguments):
        """Each effect uses its own connection; the lock connection must still own the lock."""
        self.lock.verify()
        return operation(*arguments)

    def verify_package(self, path, plan):
        """Check all files, owners and modes before running any package entrypoint."""
        problems = tree_problems(self.ex.package_tree(self.host, path), plan['package']['files'],
                                 self.layout.owner_uid)
        if problems:
            raise PackageError('%s is not exactly the planned package: %s' % (path, '; '.join(problems[:5])))
        self.verify_entrypoints(path, plan)

    def verify_target(self, identifier, plan):
        """A completion marker is a receipt, not proof that its selected files remain unchanged."""
        observed = self.observe(plan['package']['revision'])
        problems = self.path_problems(observed)
        selection = observed['current'] or {}
        if (selection.get('kind') != 'link' or selection.get('uid') != self.layout.owner_uid
                or selection.get('target') != plan['target']['selection']):
            problems.append('the selected release no longer matches the planned selection')
        for step, path, _ in self.layout.managed():
            state = observed['files'][path]
            expected = plan['target']['files'][step]
            if expected is None:
                if state is not None:
                    problems.append('%s appeared outside the recorded plan' % path)
            elif (state is None or state.get('kind') != 'file' or state.get('sha256') != expected
                  or state.get('uid') != self.layout.owner_uid or state.get('mode', 0) & 0o022):
                problems.append('%s no longer matches the planned file' % path)
        if problems:
            raise PackageError('; '.join(problems))
        self.verify_package('%s/%s' % (self.layout.releases, plan['package']['revision']), plan)

    def apply(self, identifier, plan, name, archive):
        layout, host = self.layout, self.host
        revision = plan['package']['revision']
        directory = layout.transaction(identifier)
        if name == 'record':
            for path, mode in ((layout.root, 0o755), (layout.releases, 0o755), (layout.staging, 0o755),
                               (layout.transactions, 0o700), (directory + '/steps', 0o700)):
                self.locked_effect(self.ex.mkdir, host, path, mode)
            self.locked_effect(self.ex.write, host, directory + '/record.json', self.record_bytes(identifier, plan), 0o600)
            for step, path, copy in layout.managed():
                if plan['prior']['files'][step] is not None:
                    # The previous file, kept immutable for rollback.
                    self.locked_effect(self.ex.package_copy, host, path, '%s/%s' % (directory, copy), plan['prior']['files'][step], 0o600)
            self.locked_effect(self.ex.write, host, layout.pointer, self.pointer_bytes(identifier), 0o600)
        elif name == 'stage':
            if archive is None:
                raise PackageError('staging needs the planned archive; pass --archive')
            if file_sha256(archive) != plan['package']['archive_sha256']:
                raise PackageError('%s is not the planned archive' % archive)
            self.locked_effect(self.ex.package_unpack, host, archive, self.staged_path(identifier), plan['package']['archive_sha256'],
                                   plan['package']['manifest_sha256'])
        elif name == 'verify':
            if plan['prior']['release'] == 'absent':
                path = self.staged_path(identifier)
            else:
                path = '%s/%s' % (layout.releases, revision)
            self.verify_package(path, plan)
        elif name == 'promote':
            self.locked_effect(self.ex.rename, host, self.staged_path(identifier), '%s/%s' % (layout.releases, revision))
        elif name in plan['target']['files']:
            data = self.candidates.get(name)
            if data is None or sha256(data) != plan['target']['files'][name]:
                raise PackageError('finishing needs the planned %s file; pass it again' % name)
            path = self.managed_path(name)
            self.locked_effect(self.ex.mkdir, host, str(Path(path).parent), 0o755)
            self.locked_effect(self.ex.write, host, path, data, 0o600 if name == 'fleet' else 0o644)
        elif name == 'select':
            self.locked_effect(self.ex.replace_link, host, layout.current, plan['target']['selection'])
        elif name == 'commit':
            self.locked_effect(self.ex.write, host, directory + '/final.json',
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
                if name == 'verify':
                    final = '%s/%s' % (self.layout.releases, plan['package']['revision'])
                    path = final if self.observe(plan['package']['revision'])['release'] is not None else self.staged_path(identifier)
                    self.verify_package(path, plan)
                elif name != 'stage' and self.state(identifier, plan, name) != 'after':
                    raise PackageError('%s: completed effect no longer matches the recorded plan' % name)
                # Promotion consumes staging; verification above checks its current location.
                continue
            if name == 'commit':
                self.verify_target(identifier, plan)
            state = self.state(identifier, plan, name)
            if state == 'partial' and name == 'stage':
                journal.event('intent', step='discard-staging')
                self.locked_effect(self.ex.remove_tree, self.host, self.staged_path(identifier), self.layout.staging)
                state = 'before'
            elif state == 'partial' and name == 'record':
                state = 'before'  # every record write is the same bytes into this transaction's own directory
            if state not in ('before', 'after'):
                raise PackageError('%s: %s; the recorded intent no longer matches the coordinator' % (name, state))
            if state == 'before':
                journal.event('intent', step=name)
                if name != 'record':
                    self.locked_effect(self.ex.write, self.host, self.marker(identifier, index, name, 'intent'), b'{}\n', 0o600)
                self.apply(identifier, plan, name, archive)
            self.locked_effect(self.ex.write, self.host, self.marker(identifier, index, name, 'done'), b'{}\n', 0o600)
            journal.event('done', step=name, observed=state)
        self.lock.verify()
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

    def install(self, revision, fleet, expect, run_id=None, archive=None, known_hosts=None):
        """Returns the committed journal, or None when the coordinator already matches."""
        if not re.fullmatch('[0-9a-f]{64}', expect or ''):
            raise PackageError('install needs the reviewed plan digest (--expect-plan-sha256)')
        def action():
            latest = Journal.load(self.state_dir, SERVICE)
            if latest is not None and latest.status not in FINAL:
                raise PackageError('transaction %s is %s; run %s'
                                   % (latest.id, latest.status, self.next_command(latest.id, latest.status)))
            plan, problems, trusted = self.build_plan(revision, fleet, run_id, archive, known_hosts)
            digest = durable.digest(plan)
            if digest != expect:
                raise PackageError('the coordinator or inputs changed since the reviewed plan (%s, now %s); '
                                   'plan again and review' % (expect, digest))
            if problems:
                raise PackageError('refused before any change:\n  ' + '\n  '.join(problems))
            if not plan['changes']:
                self.out('no-op: %s already selects %s with these files' % (self.host, revision))
                return None
            journal = Journal.create(self.state_dir, SERVICE, revision, {'plan_sha256': digest}, [], None)
            journal.data['plan'] = plan
            journal.set_status('installing')
            self.out('transaction %s (%s)' % (journal.id, journal.path))
            self.execute(journal, plan, trusted)
            return journal
        return self.locked(action)

    def next_command(self, identifier, status):
        if status in ('unknown', 'installing'):
            return '%s or %s' % (self.command('ops-package-reconcile', '--transaction', identifier,
                                              '--fleet', '<fleet.json>', '[--known-hosts <file>]'),
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

    def reconcile(self, identifier=None, archive=None, fleet=None, known_hosts=None):
        """Finish an unfinished install whose every observed step state matches its record."""
        def action():
            journal, plan = self.load(identifier)
            final = self.remote_final(journal.id)
            if final is not None:
                # The last write landed but its reply was lost: record what the coordinator says.
                journal.set_status(final['status'], observed='final record')
                self.out('%s is already %s' % (journal.id, final['status']))
                return journal
            self.candidates = {'fleet': Path(fleet).read_bytes() if fleet else None,
                               'known-hosts': Path(known_hosts).read_bytes() if known_hosts else None}
            source = archive
            artifact = plan['qualification'].get('artifact')
            if source is None and artifact:
                # The bytes this runner acquired from the qualified artifact when it planned.
                cached = self.artifact_dir(artifact['id']) / ARCHIVE
                source = cached if cached.is_file() else None
            journal.set_status('installing')
            self.execute(journal, plan, source)
            return journal
        return self.locked(action)

    def rollback(self, identifier=None):
        """Restore the prior selection and managed files; keep the release and the records."""
        def action():
            journal, plan = self.load(identifier)
            layout, host = self.layout, self.host
            pointer = self.observe(plan['package']['revision'])['pointer']
            latest = json.loads(pointer)['id'] if pointer else None
            # Every later transaction must already be rolled back.
            later = latest
            while later not in (journal.id, plan['prior']['transaction']):
                final, record = self.remote_final(later), self.remote_record(later)
                if not final or final.get('status') != 'rolled-back' or record is None:
                    raise PackageError('later package transaction %s is not rolled back; roll it back first' % later)
                later = record['plan']['prior']['transaction']
            final = self.remote_final(journal.id)
            if final is not None and final.get('status') == 'rolled-back':
                journal.set_status('rolled-back')
                self.out('%s is already rolled back' % journal.id)
                return journal
            observed = self.observe(plan['package']['revision'])
            selection = (observed['current'] or {}).get('target')
            current = {step: (observed['files'][path] or {}).get('sha256') for step, path, _ in layout.managed()}
            conflicts = []
            if selection not in (plan['prior']['selection'], plan['target']['selection']):
                conflicts.append('%s points at %s, changed outside this transaction' % (layout.current, selection))
            for step, path, _ in layout.managed():
                if current[step] not in (plan['prior']['files'][step], plan['target']['files'][step]):
                    conflicts.append('%s is %s, changed outside this transaction' % (path, current[step]))
            # The release rollback selects again is proven before anything changes.
            conflicts += self.path_problems(observed)
            if plan['prior']['selection'] is not None:
                conflicts += self.release_problems(plan['prior']['selection'])
            if conflicts:
                journal.event('rollback refused', conflicts=conflicts)
                raise PackageError('rollback refused; nothing was changed:\n  ' + '\n  '.join(conflicts))
            journal.set_status('rolling-back')
            directory = layout.transaction(journal.id)
            try:
                if selection != plan['prior']['selection']:
                    journal.event('intent', step='restore-selection')
                    if plan['prior']['selection'] is None:
                        self.locked_effect(self.ex.remove, host, layout.current)
                    else:
                        self.locked_effect(self.ex.replace_link, host, layout.current, plan['prior']['selection'])
                    journal.event('done', step='restore-selection')
                # The inventory first, then the pins it may name.
                for step, path, copy in reversed(layout.managed()):
                    prior = plan['prior']['files'][step]
                    if current[step] == prior:
                        continue
                    journal.event('intent', step='restore-' + step)
                    if prior is None:
                        self.locked_effect(self.ex.remove, host, path)
                    else:
                        previous = (self.ex.read(host, '%s/%s' % (directory, copy)) or '').encode()
                        if sha256(previous) != prior:
                            raise PackageError('the retained previous %s of %s is missing or altered'
                                               % (path, journal.id))
                        self.locked_effect(self.ex.write, host, path, previous, 0o600 if step == 'fleet' else 0o644)
                    journal.event('done', step='restore-' + step)
                if self.ex.package_tree(host, self.staged_path(journal.id))['root'] is not None:
                    journal.event('intent', step='discard-staging')
                    self.locked_effect(self.ex.remove_tree, host, self.staged_path(journal.id), layout.staging)
                    journal.event('done', step='discard-staging')
                if self.ex.package_tree(host, directory)['root'] is not None:
                    journal.event('intent', step='final')
                    self.locked_effect(self.ex.write, host, directory + '/final.json',
                                  (json.dumps({'status': 'rolled-back', 'transaction': journal.id}) + '\n').encode(),
                                  0o600)
                self.lock.verify()
            except TRANSPORT as error:
                journal.set_status('rollback-unknown', error='%s: %s' % (type(error).__name__, error))
                raise PackageError('transport failed during rollback (%s); re-run %s'
                                   % (error, self.next_command(journal.id, 'rollback-unknown'))) from error
            journal.set_status('rolled-back')
            self.out('rolled back %s: selection %s, files %s; release %s and the transaction records are kept'
                     % (journal.id, plan['prior']['selection'] or 'absent',
                        json.dumps(plan['prior']['files'], sort_keys=True), plan['package']['revision']))
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
                  'files': {step: (observed['files'][path] or {}).get('sha256') for step, path, _ in layout.managed()},
                  'transaction': None, 'phase': None, 'next': None,
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
                files['operations.json'] = [sha256(manifest.encode()), 0o644]
                problems = tree_problems(self.ex.package_tree(self.host, path), files, layout.owner_uid)
                result['integrity'] = 'verified' if not problems else 'changed: ' + '; '.join(problems[:3])
                # Provenance is the record of the transaction that promoted these exact files.
                owner = self.promoted_by(revision, files['operations.json'][0])
                record = owner and self.remote_record(owner)
                result['qualification'] = record['plan']['qualification'] if record else 'unrecorded'
        if observed['pointer']:
            identifier = json.loads(observed['pointer'])['id']
            final = self.remote_final(identifier)
            result['transaction'] = identifier
            result['phase'] = final['status'] if final else 'unfinished'
            if not final:
                result['next'] = self.next_command(identifier, 'unknown')
        latest = Journal.load(self.state_dir, SERVICE)
        if latest is not None:
            result['runner'] = {'transaction': latest.id, 'status': latest.status}
            if latest.status not in FINAL and result['next'] is None:
                result['next'] = self.next_command(latest.id, latest.status)
        for key in ('selection', 'revision', 'integrity', 'qualification', 'files', 'transaction', 'phase',
                    'services', 'next'):
            value = result[key]
            self.out('%s: %s' % (key, json.dumps(value, sort_keys=True) if isinstance(value, dict) else value))
        return result
