#!/usr/bin/env python3
"""Execute an isolated v4 expansion using a pinned Terraform root and journal.

Credentials are runtime environment inputs. Private plans/state never reach
stdout. This command provisions resources only; it never registers workers.
"""
import argparse
import hashlib
import fcntl
import stat
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.parse
import urllib.request


def sibling(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


journal_module = sibling('v4-expansion-journal')
plan_guard = sibling('v4-infra-plan')
MODULE_FILES = ('main.tf', 'variables.tf', 'outputs.tf', 'cloud-init-worker.yaml', '.terraform.lock.hcl')


def module_digest(root):
    expected = {'main.tf', 'variables.tf', 'outputs.tf'}
    actual = {p.name for p in root.iterdir() if p.name.endswith(('.tf', '.tf.json', '.tfvars', '.tfvars.json'))}
    if actual != expected or any((root / name).is_symlink() for name in MODULE_FILES):
        raise ValueError('unexpected Terraform root inputs')
    return journal_module.digest({name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in MODULE_FILES})


def state_workers(state):
    """Read identities from Terraform's raw state, rejecting foreign resources."""
    result = {}
    for resource in state.get('resources', []):
        kind = resource['type']
        if (resource.get('module') or resource.get('mode') != 'managed' or resource['name'] != 'worker'
                or kind not in ('digitalocean_droplet', 'digitalocean_project_resources', 'digitalocean_tag', 'digitalocean_firewall')):
            raise ValueError('unexpected resource in isolated state')
        for instance in resource.get('instances', []):
            if instance.get('deposed') or instance.get('status') == 'tainted':
                raise ValueError('reconcile deposed or tainted infrastructure first')
            if kind == 'digitalocean_droplet':
                index = instance['index_key']
                resource_id = str(instance['attributes']['id'])
                address = f'digitalocean_droplet.worker[{index}]'
                if type(index) is not int or not 0 <= index < 8 or address in result or not re.fullmatch('[1-9][0-9]*', resource_id):
                    raise ValueError('invalid worker identity in state')
                result[address] = resource_id
    return result


def reconcile_workers(state, droplets, policy, operation, inventory):
    """Bind state, journal, provider identities and established private origins.

    A matching name outside Terraform state is an orphan, not authorization to
    adopt or recreate it. Leave the operation fenced until it has been resolved.
    """
    if state.get('lineage') != policy['state_lineage']:
        raise ValueError('Terraform state lineage changed')
    workers = state_workers(state)
    expected = {f'digitalocean_droplet.worker[{i}]': f'enhance-pir-v4-g{i // 2 + 1:02d}-r{i % 2 + 1}'
                for i in range(operation['target_groups'] * 2)}
    if not set(workers) <= set(expected):
        raise ValueError('Terraform state exceeds requested fleet')
    found = {}
    for droplet in droplets:
        name = droplet['name']
        if name.startswith('enhance-pir-v4-') or 'enhance-pir-v4-worker' in droplet.get('tags', []):
            if name not in expected.values() or name in found:
                raise ValueError('unexpected or duplicate v4 provider resource')
            found[name] = droplet
    for address, name in expected.items():
        droplet = found.get(name)
        if droplet is not None and address not in workers:
            raise ValueError('provider orphan requires explicit recovery before another apply')
        if address not in workers:
            continue
        if droplet is None or str(droplet['id']) != workers[address]:
            raise ValueError('provider and Terraform worker identities disagree')
        if (droplet.get('size_slug') != 'c-4' or droplet.get('region', {}).get('slug') != policy['region']
                or droplet.get('vpc_uuid') != policy['vpc_id'] or droplet.get('status') != 'active'
                or set(droplet.get('tags', [])) != {'enhance-pir-v4-worker'}):
            raise ValueError('provider worker profile differs from policy')
    required = {f'digitalocean_droplet.worker[{i}]' for i in range(operation['before'] * 2)}
    if not required <= set(workers):
        raise ValueError('established fleet is missing from state')
    for address, identity in operation['resources'].items():
        if workers.get(address) != identity:
            raise ValueError('recorded worker was removed or replaced')
    for group_index, group in enumerate(inventory['groups']):
        for replica_index, replica in enumerate(group['replicas']):
            address = f'digitalocean_droplet.worker[{group_index * 2 + replica_index}]'
            droplet = found[expected[address]]
            private = [network['ip_address'] for network in droplet.get('networks', {}).get('v4', []) if network['type'] == 'private']
            if replica['name'] != droplet['name'] or len(private) != 1 or replica['url'].rstrip('/') != f'http://{private[0]}:8291':
                raise ValueError('serving inventory does not match established Terraform workers')
    return workers


class DigitalOcean:
    def __init__(self, token):
        self.token = token

    def get(self, path):
        url = 'https://api.digitalocean.com' + path
        request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + self.token})
        # Do not follow redirects while carrying credentials.
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *_args, **_kwargs):
                return None
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            data = response.read(16 * 1024 * 1024 + 1)
        if len(data) > 16 * 1024 * 1024:
            raise ValueError('oversized provider response')
        return json.loads(data)

    def droplets(self):
        result = []
        path = '/v2/droplets?per_page=200'
        visited = set()
        while path:
            if path in visited or len(visited) >= 100:
                raise ValueError('invalid provider pagination')
            visited.add(path)
            page = self.get(path)
            result.extend(page['droplets'])
            following = page.get('links', {}).get('pages', {}).get('next')
            path = None
            if following:
                parsed = urllib.parse.urlsplit(following)
                if parsed.scheme != 'https' or parsed.netloc != 'api.digitalocean.com' or parsed.path != '/v2/droplets' or parsed.fragment:
                    raise ValueError('unexpected provider pagination origin')
                path = parsed.path + '?' + parsed.query
        if len({d['id'] for d in result}) != len(result):
            raise ValueError('inconsistent provider pagination')
        return result


class StateLock:
    """Serialize Spaces state writers on one pinned Linux host.

    All manual writers must use the same host and lock. This is an operational
    single-writer boundary, not a distributed lock or protection from root.
    Terraform inherits the descriptor so an interrupted controller cannot release
    the lock while its child is still applying a saved plan.
    """
    PATH = Path('/run/lock/enhance-pir-v4-terraform.lock')
    MACHINE_ID = Path('/etc/machine-id')

    def __init__(self, policy):
        self.config = policy.get('state_lock')
        self.fd = None

    def __enter__(self):
        if self.config is None:
            return self
        if (set(self.config) != {'type', 'machine_id'} or self.config['type'] != 'pinned_host'
                or not re.fullmatch('[0-9a-f]{32}', self.config['machine_id'])
                or self.MACHINE_ID.read_text().strip() != self.config['machine_id']
                or os.geteuid() != 0):
            raise ValueError('state writer must run as root on the pinned host')
        fd = os.open(self.PATH, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
                raise ValueError('state lock requires a private root-owned regular file')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.fd = fd
            self.verify()
        except BaseException:
            self.fd = None
            os.close(fd)
            raise
        return self

    def verify(self):
        if self.config is not None:
            if self.fd is None:
                raise ValueError('pinned-host state lock is not held')
            opened, current = os.fstat(self.fd), self.PATH.lstat()
            if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                raise ValueError('state lock file was replaced')

    def __exit__(self, *_args):
        if self.fd is not None:
            # Do not LOCK_UN: a surviving Terraform child shares this lock.
            os.close(self.fd)
            self.fd = None


class Terraform:
    def __init__(self, root, directory, policy, target, token, state_lock=None):
        self.root = Path(root).resolve()
        self.directory = directory
        self.policy = policy
        self.target = target
        self.state_lock = state_lock
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(('TF_', 'DIGITALOCEAN_'))}
        self.env.update(TF_IN_AUTOMATION='1', TF_INPUT='0', TF_VAR_digitalocean_token=token)
        self.variables = directory / 'terraform-inputs.json'
        journal_module.atomic(self.variables, {key: policy[key] for key in (
            'region', 'project_id', 'vpc_id', 'ssh_key_ids', 'coordinator_private_ipv4', 'operator_ssh_cidrs')})

    def run(self, arguments, timeout=120):
        descriptors = ()
        if self.policy.get('state_lock') is not None:
            if self.state_lock is None or self.state_lock.config != self.policy['state_lock']:
                raise ValueError('pinned-host state lock is not held')
            self.state_lock.verify()
            descriptors = (self.state_lock.fd,)
        result = subprocess.run(['terraform', f'-chdir={self.root}', *arguments], env=self.env,
                                capture_output=True, timeout=timeout, pass_fds=descriptors)
        if result.returncode:
            raise RuntimeError('Terraform command failed; operation remains fenced')
        return result.stdout

    def verify(self):
        if module_digest(self.root) != self.policy['module_sha256']:
            raise ValueError('Terraform module differs from pinned source')
        backend = json.loads((self.root / '.terraform/terraform.tfstate').read_text())['backend']
        if backend['type'] != 's3' or any(backend['config'].get(k) != v for k, v in self.policy['backend'].items()):
            raise ValueError('Terraform backend differs from isolated policy')
        if not {'bucket', 'key', 'region'} <= self.policy['backend'].keys():
            raise ValueError('backend identity must include bucket, key and region')
        if self.policy.get('state_lock') is not None:
            if backend['config'].get('use_lockfile') is not False:
                raise ValueError('pinned-host backend must explicitly disable unsupported S3 locking')
            if self.state_lock is None or self.state_lock.config != self.policy['state_lock']:
                raise ValueError('pinned-host state lock is not held')
            self.state_lock.verify()
        else:
            endpoints = backend['config'].get('endpoints') or {}
            endpoint = endpoints.get('s3') or backend['config'].get('endpoint') or ''
            hostname = urllib.parse.urlsplit(endpoint).hostname or ''
            if hostname == 'digitaloceanspaces.com' or hostname.endswith('.digitaloceanspaces.com'):
                raise ValueError('Spaces requires pinned-host state locking')
            if backend['config'].get('use_lockfile') is not True:
                raise ValueError('isolated S3 backend requires native state locking')
        if self.run(['workspace', 'show']).strip() != b'default':
            raise ValueError('isolated v4 root requires the default workspace')

    def state(self):
        return json.loads(self.run(['state', 'pull']))

    def plan(self, label):
        self.verify()
        path = self.directory / (label + '.tfplan')
        self.run(['plan', '-input=false', '-lock-timeout=60s', '-out=' + str(path),
                  '-var-file=' + str(self.variables), '-var=group_count=' + str(self.target)], timeout=300)
        os.chmod(path, 0o600)
        plan = json.loads(self.run(['show', '-json', str(path)]))
        journal_module.atomic(self.directory / (label + '.plan.json'), plan)
        return path, plan

    def apply(self, path, expected_digest):
        self.verify()
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_digest:
            raise ValueError('saved plan changed before apply')
        self.run(['apply', '-input=false', '-lock-timeout=60s', str(path)], timeout=900)


def inspect_fleet(terraform, provider, policy, operation, inventory):
    """Verify the live provider/state binding before provisioning or bootstrap."""
    terraform.verify()
    account = provider.get('/v2/account')['account']
    if account.get('uuid') != policy['account_uuid'] or account.get('status') != 'active':
        raise ValueError('provider account differs from pinned active account')
    for field in ('project_id', 'vpc_id'):
        if not re.fullmatch('[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', policy[field]):
            raise ValueError('project and VPC require explicit UUIDs')
    project = provider.get('/v2/projects/' + policy['project_id'])['project']
    vpc = provider.get('/v2/vpcs/' + policy['vpc_id'])['vpc']
    if project.get('id') != policy['project_id'] or project.get('owner_uuid') != account['uuid']:
        raise ValueError('project differs from the pinned account')
    if (vpc.get('id') != policy['vpc_id'] or vpc.get('region') != policy['region']
            or ipaddress.ip_address(policy['coordinator_private_ipv4']) not in ipaddress.ip_network(vpc['ip_range'])):
        raise ValueError('VPC does not contain the pinned coordinator in the selected region')
    state, droplets = terraform.state(), provider.droplets()
    workers = reconcile_workers(state, droplets, policy, operation, inventory)
    return {'account': account, 'state': state, 'droplets': droplets, 'workers': workers}


def provision(journal, terraform, provider, policy, inventory):
    """Recover complete fleets; only unfinished project membership is retryable."""
    operation = journal.state['operation']
    if operation is None:
        return
    observed = inspect_fleet(terraform, provider, policy, operation, inventory)
    account, state, droplets, workers = (observed[k] for k in ('account', 'state', 'droplets', 'workers'))
    journal.record_resources(workers)
    if operation['phase'] in ('provisioned', 'bootstrapping', 'bootstrapped'):
        return
    if operation['phase'] == 'applying':
        if len(workers) != operation['target_groups'] * 2:
            raise ValueError('interrupted partial apply requires explicit provider recovery')
        _, plan = terraform.plan('recovery')
        result = plan_guard.validate(plan, policy, operation['before'], workers)
        membership_only = bool(result['creates']) and all(
            re.fullmatch(r'digitalocean_project_resources\.worker\[[0-7]\]', address)
            for address in result['creates'])
        if result['creates'] and not membership_only:
            raise ValueError('interrupted apply has unfinished resources; recovery required')
        evidence = {'state': state, 'droplets': droplets, 'plan_sha256': journal_module.digest(plan),
                    'account_uuid': account['uuid']}
        journal_module.atomic(terraform.directory / 'reconciliation.json', evidence)
        # Project association is idempotent for the already verified droplet IDs.
        # Resolve the prior attempt first; a later invocation must obtain and
        # validate a fresh plan before retrying any mutation. Never recreate a
        # droplet, tag or firewall merely because an interrupted apply timed out.
        journal.resolve_apply(resources=workers, evidence_sha256=journal_module.digest(evidence),
                              complete=not membership_only)
        return
    if operation['phase'] not in ('requested', 'retryable'):
        raise ValueError('unexpected infrastructure phase')
    path, plan = terraform.plan('attempt-' + str(len(operation['attempts']) + 1))
    result = plan_guard.validate(plan, policy, operation['before'], workers)
    if len(workers) == operation['target_groups'] * 2 and any(
            not re.fullmatch(r'digitalocean_project_resources\.worker\[[0-7]\]', address)
            for address in result['creates']):
        raise ValueError('complete fleet retry may only finish project membership')
    plan_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    journal.start_apply(plan_hash)
    terraform.apply(path, plan_hash)
    state, droplets = terraform.state(), provider.droplets()
    workers = reconcile_workers(state, droplets, policy, operation, inventory)
    _, after = terraform.plan('verification')
    result = plan_guard.validate(after, policy, operation['before'], workers)
    if result['creates']:
        raise ValueError('applied fleet remains incomplete')
    evidence = {'state': state, 'droplets': droplets, 'plan_sha256': journal_module.digest(after),
                'account_uuid': account['uuid']}
    journal_module.atomic(terraform.directory / 'reconciliation.json', evidence)
    journal.resolve_apply(resources=workers, evidence_sha256=journal_module.digest(evidence), complete=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--terraform-dir', type=Path, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--token-env', default='DIGITALOCEAN_ACCESS_TOKEN')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        policy = json.loads(args.policy.read_text())
        inventory = json.loads(args.inventory.read_text())
        token = os.environ[args.token_env]
        with StateLock(policy) as state_lock, journal_module.Journal(args.state_dir.resolve()) as journal:
            operation = journal.state['operation']
            if operation is None or operation['policy_digest'] != journal_module.digest(policy) or operation['inventory_digest'] != journal_module.digest(inventory):
                raise ValueError('observe matching demand and inputs before provisioning')
            if not re.fullmatch('successor-[0-9]+-pair-[2-4]', operation['id']):
                raise ValueError('invalid persisted operation identity')
            directory = journal.directory / operation['id']
            directory.mkdir(mode=0o700, exist_ok=True)
            terraform = Terraform(args.terraform_dir, directory, policy, operation['target_groups'], token, state_lock)
            provision(journal, terraform, DigitalOcean(token), policy, inventory)
            print(json.dumps({'operation': operation['id'], 'phase': operation['phase'], 'target_groups': operation['target_groups']}))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError):
        raise SystemExit('v4 provisioning stopped; inspect private operation state and reconcile before retry') from None


if __name__ == '__main__':
    main()
