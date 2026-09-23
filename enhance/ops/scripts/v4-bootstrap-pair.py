#!/usr/bin/env python3
"""Bootstrap a provisioned replica pair, preserving per-host receipts on retry.

SSH host keys must be verified out of band and pinned before this command runs.
No private key is copied, no inventory is registered, and no qualification is
claimed. Run from the coordinator network with access to the worker private IPs.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

# Reuse the source/backend/provider binding used by the provisioning adapter.
import importlib.util
spec = importlib.util.spec_from_file_location('v4_provision', Path(__file__).with_name('v4-provision.py'))
provisioning = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provisioning)
journal_module = provisioning.journal_module
installer = provisioning.sibling('bootstrap-v4-worker')


def targets(operation, observed):
    result = []
    by_id = {str(d['id']): d for d in observed['droplets']}
    for index in range(operation['before'] * 2, operation['target_groups'] * 2):
        address = f'digitalocean_droplet.worker[{index}]'
        identity = operation['resources'][address]
        if observed['workers'].get(address) != identity:
            raise ValueError('new worker differs from the provisioned identity')
        droplet = by_id[identity]
        private = [n['ip_address'] for n in droplet['networks']['v4'] if n['type'] == 'private']
        if len(private) != 1 or not ipaddress.IPv4Address(private[0]).is_private:
            raise ValueError('new worker needs one private IPv4 origin')
        result.append({'address': address, 'resource_id': identity, 'name': droplet['name'], 'private_ipv4': private[0]})
    if len(result) != 2 or result[0]['private_ipv4'] == result[1]['private_ipv4']:
        raise ValueError('bootstrap requires two distinct provisioned replicas')
    return result


class Remote:
    def __init__(self, target, key, known_hosts, known_hosts_sha256):
        self.host = str(ipaddress.IPv4Address(target['private_ipv4']))
        self.key = Path(key).resolve()
        self.known_hosts = Path(known_hosts).resolve()
        self.known_hosts_sha256 = known_hosts_sha256
        if installer.sha256(self.known_hosts.read_bytes()) != known_hosts_sha256:
            raise ValueError('SSH host-key inventory differs from the verified pin')
        self.options = ['-F', '/dev/null', '-i', str(self.key), '-o', 'BatchMode=yes',
                        '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=10',
                        '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(self.known_hosts),
                        '-o', 'GlobalKnownHostsFile=/dev/null']

    def check_host_keys(self):
        if installer.sha256(self.known_hosts.read_bytes()) != self.known_hosts_sha256:
            raise ValueError('SSH host-key inventory changed during bootstrap')

    def command(self, arguments, timeout=120):
        self.check_host_keys()
        result = subprocess.run(['ssh', *self.options, 'root@' + self.host, shlex.join(arguments)],
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('remote bootstrap command failed')
        return result.stdout

    def copy(self, paths, destination):
        self.check_host_keys()
        result = subprocess.run(['scp', *self.options, *[str(p) for p in paths], 'root@' + self.host + ':' + destination],
                                capture_output=True, timeout=300)
        if result.returncode:
            raise RuntimeError('candidate transfer failed')

    def bootstrap(self, target, bundle, config):
        base = '/opt/enhance-pir-v4/bootstrap/' + config['manifest_sha256']
        remote_bundle = base + '/bundle'
        self.command(['cloud-init', 'status', '--wait'], timeout=600)
        self.command(['install', '-d', '-m', '0700', remote_bundle])
        self.copy(sorted(bundle.iterdir()), remote_bundle + '/')
        # Verify before executing any code from the transferred bundle.
        actual = self.command(['sha256sum', remote_bundle + '/SHA256SUMS']).decode().split()
        if not actual or actual[0] != config['manifest_sha256']:
            raise ValueError('transferred candidate manifest differs from its trusted pin')
        self.command(['sh', '-c', 'cd ' + shlex.quote(remote_bundle) + ' && sha256sum --check --strict SHA256SUMS'])
        limits = config['limits'][target['name']]
        with tempfile.TemporaryDirectory(prefix='v4-bootstrap-limits-') as directory:
            path = Path(directory) / 'limits.json'
            journal_module.atomic(path, limits)
            self.copy([path], base + '/limits.json')
        output = self.command(['python3', remote_bundle + '/bootstrap-v4-worker.py', 'install',
                               '--bundle', remote_bundle, '--revision', config['revision'],
                               '--manifest-sha256', config['manifest_sha256'], '--worker-name', target['name'],
                               '--private-ipv4', target['private_ipv4'], '--limits', base + '/limits.json'], timeout=180)
        if len(output) > 1024 * 1024:
            raise ValueError('oversized bootstrap receipt')
        return json.loads(output)


def validate_receipt(receipt, target, config, identity):
    limits = config['limits'][target['name']]
    expected = {**identity, 'worker_name': target['name'], 'private_ipv4': target['private_ipv4'],
                'limits': limits, 'phase': 'bootstrapped', 'qualification': 'unqualified',
                'unit_sha256': installer.sha256(installer.unit(identity['binary_sha256'], target['private_ipv4'], limits))}
    if any(receipt.get(k) != v for k, v in expected.items()):
        raise ValueError('remote bootstrap receipt differs from the requested installation')
    host, health, runtime = (receipt[k] for k in ('host', 'health', 'runtime'))
    installer.validate_limits(host, limits)
    if host.get('hostname') != target['name'] or not isinstance(host.get('boot_id'), str) or not host['boot_id']:
        raise ValueError('bootstrap receipt lacks matching host/boot identity')
    if (health.get('protocol') != installer.PROTOCOL or type(health.get('epoch')) is not int or health['epoch'] != 0
            or type(health.get('revision')) is not int or health['revision'] != 0
            or health.get('published') != [] or 'candidate' not in health or health['candidate'] is not None
            or not isinstance(health.get('incarnation'), str) or not health['incarnation']):
        raise ValueError('bootstrap receipt must identify a fresh idle v4 process')
    if (type(runtime.get('main_pid')) is not int or runtime['main_pid'] <= 0
            or runtime.get('cgroup') != '/system.slice/' + installer.SERVICE):
        raise ValueError('bootstrap receipt lacks the expected running service')


def bootstrap_pair(journal, pair, config, bundle, remote_factory):
    operation = journal.state['operation']
    if operation['phase'] not in ('provisioned', 'bootstrapping', 'bootstrapped'):
        raise ValueError('complete provisioning before worker bootstrap')
    identity = installer.verify_bundle(bundle, config['revision'], config['manifest_sha256'])
    if (set(config['limits']) != {target['name'] for target in pair}
            or not re.fullmatch('[0-9a-f]{64}', config['known_hosts_sha256'])):
        raise ValueError('pin SSH host keys and limits for exactly the new replica pair')
    binding = {'config_digest': journal_module.digest(config), 'targets_digest': journal_module.digest(pair)}
    state = operation.get('bootstrap')
    if state is None:
        state = {**binding, 'receipts': {}}
        operation['bootstrap'] = state
    elif any(state.get(k) != v for k, v in binding.items()):
        raise ValueError('bootstrap inputs or provisioned identities changed during recovery')
    operation['phase'] = 'bootstrapping'
    journal.save()
    for target in pair:
        receipt = remote_factory(target).bootstrap(target, bundle, config)
        validate_receipt(receipt, target, config, identity)
        if any(other['health']['incarnation'] == receipt['health']['incarnation']
               for address, other in state['receipts'].items() if address != target['address']):
            raise ValueError('replicas reported the same worker process')
        state['receipts'][target['address']] = receipt
        journal.save()
    if set(state['receipts']) != {target['address'] for target in pair}:
        raise ValueError('bootstrap receipts do not cover exactly the requested pair')
    operation['phase'] = 'bootstrapped'
    journal.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('state-dir', 'terraform-dir', 'policy', 'inventory', 'bootstrap-policy', 'bundle', 'ssh-key', 'known-hosts'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--token-env', default='DIGITALOCEAN_ACCESS_TOKEN')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        policy = json.loads(args.policy.read_text())
        inventory = json.loads(args.inventory.read_text())
        config = json.loads(args.bootstrap_policy.read_text())
        with provisioning.StateLock(policy) as state_lock, journal_module.Journal(args.state_dir.resolve()) as journal:
            operation = journal.state['operation']
            if (operation is None or operation['policy_digest'] != journal_module.digest(policy)
                    or operation['inventory_digest'] != journal_module.digest(inventory)
                    or not re.fullmatch('successor-[0-9]+-pair-[2-4]', operation['id'])):
                raise ValueError('provisioning inputs changed')
            token = os.environ[args.token_env]
            terraform = provisioning.Terraform(args.terraform_dir, journal.directory / operation['id'],
                                                policy, operation['target_groups'], token, state_lock)
            observed = provisioning.inspect_fleet(terraform, provisioning.DigitalOcean(token), policy, operation, inventory)
            pair = targets(operation, observed)
            bootstrap_pair(journal, pair, config, args.bundle.resolve(),
                           lambda target: Remote(target, args.ssh_key, args.known_hosts, config['known_hosts_sha256']))
            print(json.dumps({'operation': operation['id'], 'phase': operation['phase'], 'qualification': 'unqualified'}))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError):
        raise SystemExit('v4 pair bootstrap stopped; receipts are retained for reconciliation') from None


if __name__ == '__main__':
    main()
