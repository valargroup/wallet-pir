#!/usr/bin/env python3
"""Append-only Enhance capacity controller. Credentials are runtime environment only.

Run through the production Infisical identity. All state and plan files are private;
only allowlisted summaries may leave the host. No command logs credentials or plans.
"""
import argparse
import base64
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import urllib.request

GROUP_POSITIONS = 16 * 73728
MAX_GROUPS = 4


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix('.next')
    with open(temporary, 'w', encoding='utf8') as handle:
        os.chmod(temporary, 0o600)
        json.dump(value, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run(args, *, timeout=60, data=None, env=None):
    result = subprocess.run(args, input=data, capture_output=True, text=True,
                            timeout=timeout, env=env)
    if result.returncode:
        # Terraform output can include sensitive variables; do not echo it.
        raise RuntimeError(f'{Path(args[0]).name} failed (exit {result.returncode}); inspect private operation evidence')
    return result.stdout


def http_json(url, token=None):
    headers = {'Authorization': 'Bearer ' + token} if token else {}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as response:
            return json.load(response)
    except Exception:
        raise RuntimeError('HTTP probe failed') from None


def control(path, request):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(10)
        stream.connect(path)
        stream.sendall(json.dumps(request).encode() + b'\n')
        with stream.makefile('rb') as reader:
            line = reader.readline(65537)
        if len(line) > 65536:
            raise RuntimeError('oversized control response')
        response = json.loads(line)
    if not response.get('ok'):
        raise RuntimeError(response.get('error', 'control request failed'))
    return response['status']


def validate_plan(plan, current_groups, existing_workers=()):
    """Only the next pair and a strictly additive project membership update."""
    expected = {f'digitalocean_droplet.worker[{i}]' for i in range(current_groups * 2, current_groups * 2 + 2)}
    created = set(existing_workers)
    if not created.issubset(expected):
        raise ValueError("partial operation contains unexpected workers")
    for drift in plan.get('resource_drift', []):
        if drift['change']['actions'] == ['no-op']:
            continue
        before = drift['change'].get('before') or {}
        after = drift['change'].get('after') or {}
        changed = {key for key in before.keys() | after.keys() if before.get(key) != after.get(key)}
        # DO refreshes these computed counters after creating a tagged worker.
        # They are observations, not edits to a tag or to another resource.
        if (drift.get('address') == 'digitalocean_tag.worker'
                and before.get('name') == after.get('name') == 'enhance-pir-worker'
                and changed <= {'total_resource_count', 'droplets_count'}):
            continue
        raise ValueError('infrastructure drift must be reconciled before expansion')
    for resource in plan.get('resource_changes', []):
        change = resource['change']
        actions = change['actions']
        address = resource['address']
        if actions in (['no-op'], ['read']):
            continue
        if address in expected and actions == ['create']:
            after = change.get('after') or {}
            index = int(address.split('[')[1].rstrip(']'))
            if after.get('size') != 'c-4' or after.get('name') != f'enhance-pir-worker-{index + 1:02d}':
                raise ValueError('unexpected worker name or hardware')
            if after.get('region') != 'ams3':
                raise ValueError('worker region must remain ams3')
            created.add(address)
            continue
        if re.fullmatch(r'digitalocean_project_resources.enhance_added_workers\[\d+\]', address) and actions == ['create']:
            index = int(address.split('[')[1].rstrip(']'))
            if index + 2 in range(current_groups * 2, current_groups * 2 + 2):
                continue
        raise ValueError(f'expansion refuses changes to {address}')
    if created != expected:
        raise ValueError('plan must create exactly the next two workers')


def has_serving_snapshot(health):
    phase = health.get('phase', {}).get('phase')
    return phase == 'serving' or (phase == 'building' and (health.get('generation') or 0) > 0)


def observe(state, topology, health, now):
    groups = len(topology['groups'])
    fresh = has_serving_snapshot(health)
    fresh &= health.get('ironwood_tree_size') is not None
    fresh &= health.get('tables', {}).get('enhance', {}).get('workers') == groups * 2
    previous = state.get('sample', {})
    continuous = 240 <= now - previous.get('time', 0) <= 600
    positions = health.get('ironwood_tree_size', 0)
    same_topology = previous.get('revision') == topology['revision']
    increasing = positions >= previous.get('positions', positions)
    over = fresh and positions >= 0.8 * GROUP_POSITIONS * groups
    state['trigger_samples'] = (state.get('trigger_samples', 0) + 1 if continuous and same_topology and increasing else 1) if over else 0
    state['healthy_since'] = state.get('healthy_since', now) if continuous and fresh and increasing else now
    state['sample'] = {'time': now, 'positions': positions, 'revision': topology['revision'], 'healthy': fresh}
    return fresh and state['trigger_samples'] >= 3 and groups < MAX_GROUPS and now - state.get('last_success', 0) >= 3600


def operator_acceptance(receipt, revision):
    """An explicit, release-bound waiver never represents completed qualification."""
    return (
        receipt.get('acceptance') == 'operator'
        and receipt.get('revision') == revision
        and receipt.get('worker_size') == 'c-4'
        and receipt.get('shards_per_group') == 16
        and receipt.get('waive_qualification') is True
        and receipt.get('waive_initial_observation') is True
        and all(isinstance(receipt.get(key), str) and receipt[key].strip()
                for key in ('authorized_by', 'authorized_at', 'reason'))
    )


class Controller:
    def __init__(self, config):
        self.config = config
        self.directory = Path(config['state_dir'])
        self.path = self.directory / 'state.json'
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {'outbox': {}, 'desired_groups': 1}
        if not 1 <= self.state['desired_groups'] <= MAX_GROUPS:
            raise RuntimeError('invalid desired group count')

    def save(self):
        atomic(self.path, self.state)

    def topology(self):
        return control(self.config['control_socket'], {'command': 'status'})

    def notify(self, event, message):
        operation = self.state.get('operation', {}).get('id', 'capacity')
        key = f'{operation}:{event}'
        self.state['outbox'].setdefault(key, {'text': f'Enhance PIR [{key}] {message}', 'sent': False})
        self.save()
        self.flush_notifications()
        return self.state['outbox'][key]['sent']

    def flush_notifications(self):
        webhook = os.environ.get('PIR_APM_SLACK_WEBHOOK_URL')
        if not webhook:
            return
        for message in self.state['outbox'].values():
            if message['sent']:
                continue
            try:
                request = urllib.request.Request(webhook, data=json.dumps({'text': message['text']}).encode(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status != 200:
                        continue
                message['sent'] = True
                self.save()
            except Exception:
                # Webhook URLs contain credentials. Never log the exception.
                pass

    def ssh(self, host, command, timeout=60):
        return run(['ssh', '-i', self.config['ssh_key'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no',
                    '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=yes',
                    '-o', f"UserKnownHostsFile={self.config['known_hosts']}",
                    f'root@{host}', command], timeout=timeout)

    def terraform(self, args, timeout=60):
        env = os.environ.copy()
        env['TF_VAR_enhance_group_count'] = str(self.state['desired_groups'])
        # Explicit tfvars override TF_VAR values. Persisted capacity must win
        # over the initial deployment's count in those files on every plan.
        if args and args[0] == 'plan':
            args = [*args, f"-var=enhance_group_count={self.state['desired_groups']}"]
        return run(['terraform', f"-chdir={self.config['terraform_dir']}", *args], timeout=timeout, env=env)

    def verify_release(self):
        if int(self.terraform(['output', '-json', 'enhance_legacy_worker_count'])) != 0:
            raise RuntimeError('retire accepted legacy workers before enabling automatic expansion')
        artifacts = Path(self.config['artifact_dir'])
        revision = (artifacts / 'revision').read_text().strip()
        if revision != Path(self.config['current_release_file']).read_text().strip():
            raise RuntimeError('artifact release differs from running coordinator')
        if not re.fullmatch('[0-9a-f]{40}', revision):
            raise RuntimeError('invalid release revision')
        checksums = {}
        for line in (artifacts / 'SHA256SUMS').read_text().splitlines():
            digest, name = line.split(maxsplit=1)
            name = name.removeprefix('./')
            if '/' in name or not re.fullmatch('[0-9a-f]{64}', digest):
                raise RuntimeError('invalid artifact checksum manifest')
            checksums[name] = digest
        for name in ('enhance-pir-worker', 'enhance-pir-qualify', 'enhance-pir-cli', 'enhance-pir-worker.service'):
            if hashlib.sha256((artifacts / name).read_bytes()).hexdigest() != checksums.get(name):
                raise RuntimeError('artifact checksum mismatch')
        receipt = json.loads(Path(self.config['qualification_receipt']).read_text())
        if not operator_acceptance(receipt, revision) and not (receipt.get('passed') is True and receipt.get('revision') == revision and receipt.get('worker_size') == 'c-4'
                and receipt.get('shards_per_group') == 16 and receipt.get('full_capacity') is True
                and receipt.get('failover') is True and receipt.get('online_append') is True
                and receipt.get('memory') is True and receipt.get('seconds', 0) >= 21600
                and receipt.get('publications', 0) >= 300):
            raise RuntimeError('matching qualification or explicit operator acceptance is required')
        return revision

    def check_existing(self, topology):
        health = http_json(self.config['coordinator_url'].rstrip('/') + '/v1/health')
        if not has_serving_snapshot(health):
            raise RuntimeError('coordinator is not serving')
        cookie = Path(self.config['zakura_cookie']).read_text().strip()
        request = urllib.request.Request(self.config['zakura_rpc_url'],
            data=json.dumps({'jsonrpc': '2.0', 'id': 'capacity', 'method': 'getblockchaininfo', 'params': []}).encode(),
            headers={'Content-Type': 'application/json', 'Authorization': 'Basic ' + base64.b64encode(cookie.encode()).decode()})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                chain = json.load(response)['result']
        except Exception:
            raise RuntimeError('chain freshness probe failed') from None
        if chain.get('chain') != 'main' or chain['blocks'] - health['anchor_height'] not in (0, 1):
            raise RuntimeError('published coverage is not at the current chain tip')

        for i, group in enumerate(topology['groups']):
            assigned = health['ironwood_tree_size'] > i * GROUP_POSITIONS
            for replica in group['replicas']:
                status = http_json(replica['url'] + '/internal/health')
                if status.get('status') != 'ok' or (assigned and (status.get('generation') or 0) < health['generation']):
                    raise RuntimeError('existing replica is not current and healthy')
        return health

    def tick(self):
        self.flush_notifications()
        topology = self.topology()
        try:
            health = self.check_existing(topology)
        except Exception:
            if self.state.get('operation', {}).get('step') == 'observing':
                self.state['operation']['observe_since'] = time.time()
            self.state['trigger_samples'] = 0
            self.save()
            raise
        now = time.time()
        trigger = observe(self.state, topology, health, now)
        self.state['desired_groups'] = max(self.state['desired_groups'], len(topology['groups']))
        self.save()
        if self.state.get('paused'):
            return
        operation = self.state.get('operation')
        if not operation:
            if len(topology['groups']) == MAX_GROUPS and health['ironwood_tree_size'] >= .8 * MAX_GROUPS * GROUP_POSITIONS:
                self.notify('ceiling', 'Eight-worker ceiling reached; operator action required.')
            if not self.config.get('enabled', False) or not trigger:
                return
            revision = self.verify_release()
            receipt = json.loads(Path(self.config['qualification_receipt']).read_text())
            if now - self.state['healthy_since'] < 86400 and not operator_acceptance(receipt, revision):
                raise RuntimeError('24 hours of healthy observation required before automatic provisioning')
            operation = {'id': f"expand-{topology['revision']}-{len(topology['groups']) + 1}",
                         'step': 'planned', 'old_groups': topology['groups'], 'revision': topology['revision'],
                         'release': revision, 'attempts': 0, 'created': now, 'positions': health['ironwood_tree_size']}
            self.state['operation'] = operation
            self.save()
        if not self.config.get('enabled', False):
            return
        # Reconcile a crash after coordinator commit before touching any worker.
        last = topology.get('last_operation') or {}
        if last.get('operation_id') == operation.get('activation_id', operation['id']) and not topology.get('error'):
            if operation['step'] not in ('observing', 'complete'):
                operation.update(step='observing', observe_since=now)
                self.save()
        if now < operation.get('retry_after', 0):
            return
        try:
            self.advance(operation, topology, now)
        except Exception as error:
            operation['attempts'] += 1
            operation['retry_after'] = time.time() + 300
            operation['last_error'] = str(error)
            if operation['attempts'] >= 3:
                self.state['paused'] = True
                self.notify('failed', f"Expansion paused at {operation['step']}: {error}. Existing topology retained; no resources deleted.")
            self.save()
            raise

    def advance(self, op, topology, now):
        if self.verify_release() != op['release']:
            raise RuntimeError('running release changed during expansion; restore the pinned release before resuming')
        if op['step'] == 'planned':
            before = len(op['old_groups'])
            if not self.notify('provisioning', f"Provisioning pair {before + 1}: {op['positions']} positions; capacity {before * GROUP_POSITIONS} -> {(before + 1) * GROUP_POSITIONS}; +$168/month; release {op['release']}."):
                raise RuntimeError('provisioning notification has not been delivered')
            token = os.environ.get('TF_VAR_digitalocean_token')
            if not token:
                raise RuntimeError('runtime DigitalOcean token missing')
            fleet = http_json('https://api.digitalocean.com/v2/droplets?tag_name=enhance-pir-worker&per_page=200', token)['droplets']
            if len(fleet) != before * 2 or len(fleet) + 2 > 8:
                raise RuntimeError('unexpected tagged workers or eight-worker ceiling; reconcile fleet before provisioning')
            self.state['desired_groups'] = before + 1
            self.save()
            plan_path = self.directory / f"{op['id']}.tfplan"
            self.terraform(['plan', '-input=false', '-lock-timeout=60s', f"-var-file={self.config['tfvars_file']}", f'-out={plan_path}'], 300)
            validate_plan(json.loads(self.terraform(['show', '-json', str(plan_path)])), before)
            op.update(step='applying', plan=str(plan_path), deadline=time.time() + 1800)
            self.save()
        if op['step'] == 'applying':
            if time.time() >= op['deadline']:
                raise RuntimeError('provisioning deadline exceeded')
            # The saved plan is applied at most once. Interrupted/partial applies are
            # reconciled from state, never blindly regenerated into another pair.
            if not op.get('apply_started'):
                op['apply_started'] = True
                op['apply_failed'] = True
                self.save()
                self.terraform(['apply', '-input=false', '-lock-timeout=60s', op['plan']], max(1, int(op['deadline'] - time.time())))
                op['apply_failed'] = False
                self.save()
            if op.get('apply_failed'):
                state = json.loads(self.terraform(['show', '-json']))
                resources = state.get('values', {}).get('root_module', {}).get('resources', [])
                expected_names = {f'enhance-pir-worker-{i+1:02d}' for i in range(len(op['old_groups']) * 2, len(op['old_groups']) * 2 + 2)}
                known = [r for r in resources if r.get('type') == 'digitalocean_droplet' and r.get('values', {}).get('name') in expected_names]
                token = os.environ.get('TF_VAR_digitalocean_token')
                if not token:
                    raise RuntimeError('runtime DigitalOcean token missing')
                actual = http_json('https://api.digitalocean.com/v2/droplets?per_page=200', token)['droplets']
                actual = [d for d in actual if d['name'] in expected_names]
                if {str(d['id']) for d in actual} != {str(r['values']['id']) for r in known}:
                    raise RuntimeError('unrecorded or missing Droplet after interrupted apply; import/reconcile before retry')
                self.terraform(['plan', '-input=false', '-lock-timeout=60s', f"-var-file={self.config['tfvars_file']}", f"-out={op['plan']}"], 300)
                validate_plan(json.loads(self.terraform(['show', '-json', op['plan']])), len(op['old_groups']), [r['address'] for r in known])
                self.terraform(['apply', '-input=false', '-lock-timeout=60s', op['plan']], max(1, int(op['deadline'] - time.time())))
                op['apply_failed'] = False
                self.save()
            outputs = json.loads(self.terraform(['output', '-json', 'worker_groups']))
            if len(outputs) != self.state['desired_groups']:
                raise RuntimeError('partial Terraform apply; reconcile private state before resuming')
            workers = outputs[-1]['replicas']
            if len(workers) != 2:
                raise RuntimeError('new group must have two workers')
            token = os.environ.get('TF_VAR_digitalocean_token')
            if not token:
                raise RuntimeError('runtime DigitalOcean token missing')
            droplets = http_json('https://api.digitalocean.com/v2/droplets?per_page=200', token)['droplets']
            for worker in workers:
                found = [d for d in droplets if d['name'] == worker['name']]
                if len(found) != 1 or found[0]['size_slug'] != 'c-4' or found[0]['region']['slug'] != 'ams3':
                    raise RuntimeError('provisioned worker identity mismatch')
                if worker['private_ipv4'] not in [n['ip_address'] for n in found[0]['networks']['v4'] if n['type'] == 'private']:
                    raise RuntimeError('worker private address mismatch')
                worker['droplet_id'] = found[0]['id']
            op.update(step='bootstrap', workers=workers, qualification_deadline=time.time() + 900)
            self.save()
            self.notify('provisioned', 'Provisioned ' + ', '.join(w['name'] for w in workers))
        if op['step'] == 'bootstrap':
            if time.time() > op['deadline']:
                raise RuntimeError('provisioning deadline exceeded')
            artifacts = Path(self.config['artifact_dir'])
            for worker in op['workers']:
                if worker.get('qualified'):
                    continue
                host = worker['private_ipv4']
                # Pin the first VPC-observed host key only for a newly created,
                # API-verified Droplet. Never replace a previously pinned key.
                if not worker.get('host_key'):
                    key = run(['ssh-keyscan', '-T', '10', '-t', 'ed25519', host])
                    if len([line for line in key.splitlines() if not line.startswith('#')]) != 1:
                        raise RuntimeError('could not pin new worker host key')
                    worker['host_key'] = key
                    self.save()
                with open(self.config['known_hosts'], 'a', encoding='utf8') as handle:
                    handle.write(worker['host_key'])
                self.ssh(host, 'cloud-init status --wait', timeout=max(1, int(op['deadline'] - time.time())))
                self.ssh(host, 'install -d -m 700 /srv/enhance-pir/artifacts-v7 /opt/enhance-pir; test -e /swapfile; swapon --show --noheadings | grep -q /swapfile')
                for name in ('enhance-pir-worker', 'enhance-pir-worker.service'):
                    # Stream bytes through authenticated SSH without shell interpolation.
                    destination = '/usr/local/bin/enhance-pir-worker' if name == 'enhance-pir-worker' else '/etc/systemd/system/enhance-pir-worker.service'
                    digest = hashlib.sha256((artifacts / name).read_bytes()).hexdigest()
                    ssh = ['ssh', '-i', self.config['ssh_key'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'StrictHostKeyChecking=yes', '-o', f"UserKnownHostsFile={self.config['known_hosts']}", f'root@{host}', f'cat > {destination}.next && echo {digest} {destination}.next | sha256sum -c --status && chmod 755 {destination}.next && mv {destination}.next {destination}']
                    result = subprocess.run(ssh, input=(artifacts / name).read_bytes(), capture_output=True, timeout=60)
                    if result.returncode:
                        raise RuntimeError('worker artifact installation failed')
                self.ssh(host, 'systemctl daemon-reload && systemctl enable --now enhance-pir-worker')
                work = self.directory / f"{op['id']}-{worker['name']}-{time.time_ns()}"
                report = work.with_suffix('.report.json')
                run([str(artifacts / 'enhance-pir-qualify'), '--isolated-workers', '--worker-url', f'http://{host}:8091', '--shards', '1', '--seconds', '10', '--work-dir', str(work), '--output', str(report)], timeout=max(1, int(op['qualification_deadline'] - time.time())))
                if json.loads(report.read_text()).get('passed') is not True:
                    raise RuntimeError('worker qualification failed')
                # Only these newly provisioned workers contain disposable fixtures.
                self.ssh(host, 'systemctl stop enhance-pir-worker && rm -rf /srv/enhance-pir/artifacts-v7/enhance && systemctl start enhance-pir-worker')
                worker['qualified'] = True
                self.save()
            op.update(step='activate', activation_deadline=time.time() + 600)
            self.save()
        if op['step'] == 'activate':
            if time.time() > op['activation_deadline']:
                raise RuntimeError('topology activation deadline exceeded')
            group = {'name': f"shard-group-{len(op['old_groups']) + 1:02d}", 'replicas': [
                {'name': w['name'], 'url': f"http://{w['private_ipv4']}:8091"} for w in op['workers']]}
            current = self.topology()
            activation_id = op.get('activation_id', op['id'])
            if current.get('error') and (current.get('last_operation') or {}).get('operation_id') == activation_id:
                activation_id = f"{op['id']}-retry-{op['attempts']}"
                op['activation_id'] = activation_id
                self.save()
            control(self.config['control_socket'], {'command': 'append-group', 'request': {
                'operation_id': activation_id, 'expected_revision': op['revision'], 'groups': op['old_groups'] + [group]}})
            status = self.topology()
            if status.get('error'):
                raise RuntimeError('coordinator rejected candidate publication')
            if status.get('pending') is None and (status.get('last_operation') or {}).get('operation_id') == op.get('activation_id', op['id']):
                op.update(step='observing', observe_since=time.time())
                self.save()
                self.notify('activated', 'Topology committed; existing processes remained running.')
        if op['step'] == 'observing':
            try:
                run([str(Path(self.config['artifact_dir']) / 'enhance-pir-cli'), '--server', self.config['public_url'], 'dummy'], timeout=30)
            except Exception:
                op['observe_since'] = time.time()
                self.save()
                raise
            if now - op['observe_since'] >= 1800:
                self.notify('complete', f"Expansion complete: {self.state['desired_groups']} groups, {self.state['desired_groups'] * GROUP_POSITIONS} positions, ${self.state['desired_groups'] * 168}/month worker compute.")
                self.state['last_success'] = now
                self.state['last_operation'] = op
                self.state.pop('operation')
                self.save()


@contextlib.contextmanager
def locked(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'a', encoding='utf8') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('command', choices=['tick', 'status', 'plan', 'resume'])
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    allowed_aliases = {'TF_VAR_digitalocean_token', 'TF_VAR_cloudflare_api_token', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'PIR_APM_SLACK_WEBHOOK_URL'}
    for destination, source in config.get('runtime_env_aliases', {}).items():
        if destination not in allowed_aliases or not re.fullmatch('[A-Za-z_][A-Za-z_0-9]*', source):
            raise RuntimeError('invalid runtime credential alias')
        if source in os.environ:
            os.environ[destination] = os.environ[source]
    with tempfile.TemporaryDirectory(prefix='enhance-autoscale-key-') as key_directory:
        private_key = os.environ.pop('ENHANCE_DEPLOY_SSH_KEY', None)
        if private_key:
            key_path = Path(key_directory) / 'key'
            key_path.write_text(private_key.rstrip() + '\n')
            key_path.chmod(0o600)
            config['ssh_key'] = str(key_path)
        execute(args, config)


def execute(args, config):
    with locked(config.get('lock_file', '/run/lock/enhance-production.lock')):
        controller = Controller(config)
        if args.command == 'status':
            print(json.dumps(controller.state, indent=2))
        elif args.command == 'resume':
            controller.verify_release()
            controller.state['paused'] = False
            if controller.state.get('operation'):
                op = controller.state['operation']
                op.update(attempts=0, retry_after=0, deadline=time.time()+1800,
                          qualification_deadline=time.time()+900, activation_deadline=time.time()+600)
                if op.get('activation_id'):
                    op['activation_id'] = f"{op['id']}-resume-{int(time.time())}"
            controller.save()
            print('Controller resumed; the next timer tick reconciles the existing operation.')
        elif args.command == 'plan':
            controller.state['desired_groups'] = max(controller.state['desired_groups'], len(controller.topology()['groups']))
            controller.terraform(['plan', '-input=false', '-lock-timeout=60s', f"-var-file={config['tfvars_file']}"], 300)
            print('Terraform plan completed; output suppressed to protect sensitive infrastructure values.')
        else:
            controller.tick()


if __name__ == '__main__':
    main()
