"""Run the transparent elastic Terraform root for the fleet actuator.

The root, ops/infra/digitalocean/transparent-elastic/, holds one droplet and
one project entry per elastic recent replica. This module plans it from a
member map, applies only the saved plan whose digest the caller validated,
reads the members back from state and compares them with DigitalOcean.

Writes (plan, apply) hold the root's pinned-host lock, and Terraform inherits
its descriptor, so killing the actuator mid-apply cannot let a second writer
in. Credentials come from the runtime credential that
ops/scripts/wallet-pir-runtime.py injects; Terraform receives only what it
needs. Saved plans, their JSON and the variables file contain secrets.
"""
import contextlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile

# The shared primitives live in the same checkout; this module is never shipped alone.
LIB = str(Path(__file__).resolve().parents[3] / 'ops/lib')
if LIB not in sys.path:
    sys.path.insert(0, LIB)
from wallet_pir_ops import hostlock, terraform as shared_terraform  # noqa: E402

LOCK_PATH = '/run/lock/transparent-elastic-terraform.lock'
BACKEND = {'type': 's3', 'bucket': 'enhance-pir-terraform', 'key': 'transparent-elastic/terraform.tfstate'}
WORKER_TAG = 'transparent-pir-worker'
PREFIX = 'transparent-pir-recent-'
NAME = re.compile(PREFIX + '[0-9]{2,3}')
DROPLET_ID = re.compile('[1-9][0-9]*')
# Runtime credential names (wallet-pir-runtime.py) and what Terraform calls them.
CREDENTIALS = {'DO_TOKEN_NEW_ORG': 'TF_VAR_digitalocean_token',
               'WALLET_PIR_TF_STATE_ACCESS_KEY': 'AWS_ACCESS_KEY_ID',
               'WALLET_PIR_TF_STATE_SECRET_KEY': 'AWS_SECRET_ACCESS_KEY'}
# The rest of the runtime credential, which Terraform never needs.
UNUSED_SECRETS = ('CF_API_TOKEN', 'WALLET_PIR_DEPLOY_SSH_KEY', 'PIR_APM_SLACK_WEBHOOK_URL')
VARIABLES = frozenset({'region', 'vpc_uuid', 'project_id', 'ssh_key_ids', 'worker_tag', 'deploy_public_key'})


def terraform_environment(source):
    """Terraform's whole environment: the source minus ambient overrides, plus mapped credentials."""
    missing = [name for name in CREDENTIALS if not source.get(name)]
    if missing:
        raise ValueError('elastic root requires runtime credentials: ' + ', '.join(missing))
    # Ambient AWS_* (a profile, a session token, an endpoint) must not steer the state backend.
    env = shared_terraform.clean_environment(source, strip=('TF_', 'DIGITALOCEAN_', 'AWS_'))
    for name in (*CREDENTIALS, *UNUSED_SECRETS):
        env.pop(name, None)
    env.update({target: source[name] for name, target in CREDENTIALS.items()})
    env['AWS_EC2_METADATA_DISABLED'] = 'true'
    return env


def check_members(members):
    """{name: {"size", "image"}} for elastic recent replicas only."""
    if not isinstance(members, dict):
        raise ValueError('members must map names to size and image')
    result = {}
    for name, member in members.items():
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise ValueError(f'{name!r} is not an elastic recent replica name')
        if (not isinstance(member, dict) or set(member) != {'size', 'image'}
                or not all(isinstance(member[k], str) and member[k] for k in member)):
            raise ValueError(f'{name}: a member has exactly a size and an image')
        result[name] = dict(member)
    return result


def check_host_keys(host_keys, members):
    if not isinstance(host_keys, dict) or not set(host_keys) <= set(members):
        raise ValueError('host keys must belong to listed members')
    for name, key in host_keys.items():
        if (not isinstance(key, dict) or set(key) != {'private', 'public'}
                or not isinstance(key['public'], str) or not key['public'].startswith('ssh-ed25519 ')
                or not isinstance(key['private'], str)
                or not key['private'].strip().startswith('-----BEGIN OPENSSH PRIVATE KEY-----')):
            raise ValueError(f'{name}: host key must be an OpenSSH ed25519 key pair')
    return {name: dict(key) for name, key in host_keys.items()}


def private_work_dir(path):
    """A directory only this user can enter; plans and variables hold secrets."""
    if path is None:
        return Path(tempfile.mkdtemp(prefix='transparent-elastic-'))
    path = Path(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('elastic work directory must be private to this user')
    return path.resolve()


class ElasticRoot:
    """The elastic Terraform root, run by the actuator on the pinned coordinator.

    `env` is the source environment (default `os.environ`), carrying the
    runtime credential. `machine_id` pins the only host allowed to write; it
    is required for `plan` and `apply`. `work_dir` holds saved plans and the
    transient variables file (default: a fresh private temporary directory).
    """

    def __init__(self, root_dir, lock_path=LOCK_PATH, terraform='terraform', env=None,
                 machine_id=None, work_dir=None):
        self.root = Path(root_dir).resolve()
        self.lock_path = Path(lock_path)
        self.machine_id = machine_id
        self.runner = shared_terraform.Terraform(
            self.root, env=terraform_environment(os.environ if env is None else env), executable=terraform)
        self.work_dir = private_work_dir(work_dir)

    def verify(self):
        """Refuse any root but the elastic one: its backend key, default workspace, no stray inputs."""
        try:
            backend = json.loads((self.root / '.terraform/terraform.tfstate').read_text())['backend']
        except (OSError, ValueError, KeyError, TypeError):
            raise ValueError('elastic root is not initialized') from None
        config = backend.get('config') or {}
        if (backend.get('type') != BACKEND['type'] or config.get('bucket') != BACKEND['bucket']
                or config.get('key') != BACKEND['key']):
            raise ValueError('Terraform backend is not the elastic root state')
        if config.get('use_lockfile') is not False:
            raise ValueError('elastic root backend must disable unsupported Spaces locking')
        environment = self.root / '.terraform/environment'
        if environment.exists() and environment.read_text().strip() != 'default':
            raise ValueError('elastic root requires the default workspace')
        # Auto-loaded variables and override files would change the plan behind the caller's inputs.
        for path in self.root.iterdir():
            if (path.name.endswith(('.tfvars', '.tfvars.json'))
                    or re.fullmatch(r'(.*_)?override\.tf(\.json)?', path.name)):
                raise ValueError(f'unexpected Terraform input in the elastic root: {path.name}')

    @contextlib.contextmanager
    def locked(self):
        """Hold the pinned-host lock; nested use and every Terraform child share it."""
        if self.runner.lock is not None:
            yield self.runner.lock
            return
        if self.machine_id is None:
            raise ValueError('elastic root writes require the pinned coordinator machine id')
        config = {'type': 'pinned_host', 'machine_id': self.machine_id}
        with hostlock.PinnedHostLock(config, path=self.lock_path) as lock:
            self.runner.lock = lock
            try:
                yield lock
            finally:
                self.runner.lock = None

    def plan(self, members, host_keys, extra_vars):
        """Save a plan for exactly `members`; return (plan_path, sha256).

        `host_keys` covers members being created: a new member without a
        pinned key would be reachable only by trusting its first key.
        """
        members = check_members(members)
        host_keys = check_host_keys(host_keys, members)
        if not isinstance(extra_vars, dict) or not set(extra_vars) <= VARIABLES:
            raise ValueError('extra variables may only be: ' + ', '.join(sorted(VARIABLES)))
        variables = {**extra_vars, 'members': members, 'host_keys': host_keys}
        with self.locked():
            self.verify()
            unpinned = set(members) - set(self.state_members()) - set(host_keys)
            if unpinned:
                raise ValueError('new members need pinned host keys: ' + ', '.join(sorted(unpinned)))
            descriptor, plan_path = tempfile.mkstemp(prefix='elastic-', suffix='.tfplan', dir=self.work_dir)
            os.close(descriptor)
            descriptor, inputs = tempfile.mkstemp(prefix='.elastic-', suffix='.tfvars.json', dir=self.work_dir)
            try:
                with os.fdopen(descriptor, 'w') as handle:
                    json.dump(variables, handle, sort_keys=True)
                digest = self.runner.save_plan(plan_path, ['-var-file=' + inputs])
            except BaseException:
                os.unlink(plan_path)
                raise
            finally:
                os.unlink(inputs)
        return Path(plan_path), digest

    def show(self, plan_path):
        """`terraform show -json` of a saved plan, for the plan validator."""
        return self.runner.show_json(plan_path)

    def apply(self, plan_path, sha256):
        """Apply exactly the validated saved plan under the inherited lock."""
        with self.locked():
            self.verify()
            self.runner.apply(plan_path, sha256)

    def state_members(self):
        """{name: {id, ipv4_private, urn, size, image}} from the root's `members` output."""
        self.verify()
        outputs = self.runner.output_json()
        if not outputs:
            return {}
        members = (outputs.get('members') or {}).get('value')
        if not isinstance(members, dict):
            raise ValueError('elastic root state lacks the members output')
        result = {}
        for name, member in members.items():
            identity = str(member.get('id', ''))
            if (not NAME.fullmatch(name) or not DROPLET_ID.fullmatch(identity)
                    or member.get('urn') != 'do:droplet:' + identity):
                raise ValueError(f'elastic root state has an invalid member: {name}')
            address = member.get('ipv4_private')
            if not address or not ipaddress.IPv4Address(address).is_private:
                raise ValueError(f'{name}: state lacks a private IPv4 address')
            result[name] = {'id': identity, 'ipv4_private': address, 'urn': member['urn'],
                            'size': member.get('size'), 'image': member.get('image')}
        return result

    def reconcile(self, do_client, tag=WORKER_TAG, static_ids=()):
        """Compare state with DigitalOcean's tagged recent droplets; report, never act.

        `static_ids` are the production root's recent replicas, which share the
        tag and name prefix. Reported: `missing` (in state, gone from
        DigitalOcean), `mismatched` (in state, but untagged, renamed, moved or
        also static), `unknown` (tagged recent droplets in neither state nor
        `static_ids`, such as an interrupted apply's orphan) and `duplicates`.
        """
        state = self.state_members()
        static = {str(identity) for identity in static_ids}
        droplets = [d for d in do_client.droplets(tag=tag) if str(d.get('name', '')).startswith(PREFIX)]
        by_id = {str(d['id']): d for d in droplets}
        report = {'members': {}, 'missing': [], 'mismatched': [], 'unknown': [], 'duplicates': []}
        for name, member in sorted(state.items()):
            droplet = by_id.get(member['id'])
            if droplet is None:
                # Not tagged any more, or gone: only the id lookup tells which.
                gone = do_client.droplet(member['id']) is None
                report['missing' if gone else 'mismatched'].append(name)
                continue
            private = [n.get('ip_address') for n in (droplet.get('networks') or {}).get('v4', []) if n.get('type') == 'private']
            if droplet.get('name') != name or private != [member['ipv4_private']] or member['id'] in static:
                report['mismatched'].append(name)
            report['members'][name] = {'id': member['id'], 'status': droplet.get('status')}
        known = {member['id'] for member in state.values()} | static
        for droplet in droplets:
            if str(droplet['id']) not in known:
                report['unknown'].append({'id': str(droplet['id']), 'name': droplet.get('name'),
                                          'status': droplet.get('status')})
        names = [d.get('name') for d in droplets]
        report['duplicates'] = sorted({name for name in names if names.count(name) > 1})
        report['consistent'] = not any(report[k] for k in ('missing', 'mismatched', 'unknown', 'duplicates'))
        return report
