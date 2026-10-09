"""The receiver Droplet's Caddyfile, unit and cloud-init must agree on its private serving contract.

Caddy syntax validation accepts a catch-all proxy, which would put the
receiver's operator routes (`/v1/receiver/health`, `/metrics`) on the public
edge. These checks pin the edge to the six wallet routes, the service to a
private listener with its hardening and its NEAR key to the required file the
inventory names, and cloud-init to the matching account, directories and
firewall rule. Each check takes the file's text, so the
negative cases run it against mutated copies. The unit is the template
`wallet-pir-deploy.py` renders, so these checks also cover every deployed unit.
The example inventory's `exact_check` must probe this edge, listener and nodes
from the receiver's own host before a deploy commits, with the probe built into
the deployed binary, and wait for the restarted process to read both NEAR feeds. The Terraform firewall must admit
the coordinator's SSH, which every locked operation needs. The runbook's monitor
probe config must be the array `pir-monitor` reads, and its merge must keep the
monitor's other probes.
"""
import copy
import ipaddress
import json
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]
DIR = ROOT / 'receiver/ops/digitalocean'
WALLET_ROUTES = {'/v1/receiver/init', '/v1/receiver/query', '/v1/receiver/public/*',
                 '/v1/receiver/rows/*', '/v1/receiver/witness/*', '/v1/receiver/filters/*'}
PRIVATE_NETWORK = ipaddress.ip_network('10.70.0.0/16')
DEPLOY = ROOT / 'enhance/ops/deploy/deploy.toml'
INVENTORY = ROOT / 'enhance/ops/deploy/deploy-inventory.example.json'
INFRA = ROOT / 'ops/infra/digitalocean/production'
PROBE = ['{release_dir}/receiver-directory', 'probe']
USER = 'receiver-pir'
STATE = '/srv/receiver-pir'
# The only key source: required, and named by the inventory's NEAR_KEY id.
NEAR_KEY_FILE = '/etc/receiver-pir/near-@NEAR_KEY@.env'
HARDENING = {'NoNewPrivileges': 'true', 'ProtectSystem': 'strict', 'ProtectHome': 'true',
             'PrivateTmp': 'true', 'ReadWritePaths': STATE, 'Restart': 'on-failure'}


def caddy_tree(text):
    """Parse a Caddyfile into nested `(line, children)` blocks, dropping comments."""
    root, stack = [], []
    current = root
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line == '}':
            current = stack.pop()
        elif line.endswith('{'):
            node = (line[:-1].strip(), [])
            current.append(node)
            stack.append(current)
            current = node[1]
        else:
            current.append((line, []))
    assert not stack, 'unbalanced Caddyfile braces'
    return root


def check_caddyfile(text):
    """Return the upstream the wallet routes proxy to; fail on any other public route."""
    (site, body), = caddy_tree(text)
    assert site == 'receiver-pir.valargroup.dev', site
    names = sorted(line.split()[0] for line, _ in body)
    assert names == ['@wallet', 'handle', 'handle', 'request_body'], names
    matcher = next(line for line, _ in body if line.startswith('@wallet '))
    kind, *paths = matcher.split()[1:]
    assert kind == 'path' and set(paths) == WALLET_ROUTES and len(paths) == len(WALLET_ROUTES), paths
    handles = {line: children for line, children in body if line.split()[0] == 'handle'}
    assert set(handles) == {'handle @wallet', 'handle'}, sorted(handles)
    assert handles['handle'] == [('respond 404', [])], handles['handle']
    (proxy, nested), = handles['handle @wallet']
    directive, upstream = proxy.split()
    assert directive == 'reverse_proxy' and not nested, proxy
    return upstream


def systemd_sections(text):
    """Parse a unit into `{section: {key: [values]}}`."""
    sections, current = {}, None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('['):
            current = sections.setdefault(line.strip('[]'), {})
        else:
            key, value = line.split('=', 1)
            current.setdefault(key, []).append(value)
    return sections


def check_unit(text):
    """Return `(bind, data_dir)` after checking the template's release binary, listener and hardening."""
    service = systemd_sections(text)['Service']
    single = {key: values[0] for key, values in service.items() if len(values) == 1}
    assert single.get('User') == USER and single.get('Group') == USER, (single.get('User'), single.get('Group'))
    for key, value in HARDENING.items():
        assert single.get(key) == value, (key, service.get(key))
    assert int(single.get('RestartSec', '0')) > 0, single.get('RestartSec')
    assert service.get('EnvironmentFile') == [NEAR_KEY_FILE], service.get('EnvironmentFile')
    assert 'NEAR_INTENTS_EXPLORER' not in ' '.join(service.get('Environment', [])), service.get('Environment')
    argv = shlex.split(single['ExecStart'])
    assert argv[0] == '@RELEASE@/receiver-directory', argv[0]
    assert '--serve' in argv, argv
    option = lambda name: argv[argv.index(name) + 1]
    bind, data_dir = option('--bind'), option('--data-dir')
    host, port = bind.rsplit(':', 1)
    assert ipaddress.ip_address(host) in PRIVATE_NETWORK and port == '18380', bind
    assert data_dir.startswith(STATE + '/'), data_dir
    return bind, data_dir


def runcmd(text):
    """The cloud-init `runcmd` argument lists (flow sequences of plain or quoted words)."""
    commands = []
    for raw in text.split('\nruncmd:\n', 1)[1].splitlines():
        line = raw.strip()
        if line.startswith('- ['):
            commands.append([word.strip().strip("'\"") for word in line[3:].rstrip(']').split(',')])
    return commands


def check_cloud_init(text, bind):
    """Check the service account, its directories and that only the private network reaches `bind`."""
    packages = next(line for line in text.splitlines() if line.startswith('packages:'))
    assert 'caddy' in packages and 'ufw' in packages, packages
    commands = runcmd(text)
    assert ['useradd', '--system', '--home-dir', STATE, '--shell', '/usr/sbin/nologin', USER] in commands
    assert ['install', '-d', '-o', USER, '-g', USER, '-m', '0750', STATE] in commands
    assert any(c[:2] == ['install', '-d'] and '/opt/receiver-pir/releases' in c for c in commands)
    port = bind.rsplit(':', 1)[1]
    rules = [c for c in commands if c[:2] == ['ufw', 'allow'] and any(port in word for word in c)]
    assert rules == [['ufw', 'allow', 'from', str(PRIVATE_NETWORK), 'to', 'any', 'port', port, 'proto', 'tcp']], rules
    assert ['ufw', '--force', 'enable'] in commands


def check_exact_check(service, unit, caddy, descriptor):
    """Check the receiver's deploy gate: the deployed binary's own probe, run on the server's host.

    The coordinator cannot reach the private health route. The probe and its
    fixture are built into the release's binary, so they match the server.
    """
    (server,) = service['roles']['server']
    check = service['exact_check']
    bind, _ = check_unit(unit)
    assert server['vars']['listen'] == bind, server
    assert check['host'] == server['host'], check['host']
    # An unchanged deploy (a hand-provisioned host) still runs the check.
    assert descriptor.get('verify_unchanged') is True, descriptor.get('verify_unchanged')
    argv = check['argv']
    assert argv[:2] == PROBE and not any('{' in word for word in argv[2:]), argv
    assert not any(word.startswith('--fixture') for word in argv), argv
    option = lambda name: argv[argv.index(name) + 1]
    (site, _), = caddy_tree(caddy)
    assert option('--origin') == 'https://' + site, argv
    assert option('--health-url') == 'http://%s/v1/receiver/health' % bind, argv
    # The nodes and authentication the service itself uses, the only ones the host reaches.
    served = shlex.split(systemd_sections(unit)['Service']['ExecStart'][0])
    nodes = lambda words: [words[i + 1] for i, word in enumerate(words) if word == '--rpc-url']
    assert nodes(argv) and nodes(argv) == nodes(served), (nodes(argv), nodes(served))
    assert ('--no-auth' in argv) == ('--no-auth' in served), argv
    # A service that serves witness files has them checked against the nodes' root.
    assert ('--witnesses' in argv) == ('--witnesses' in served), argv
    # The probe first waits for the restarted process to read both NEAR feeds, which
    # proves the key the unit names; the timeout leaves 120 to 300 seconds for the rest.
    assert '--await-feed-reads' in argv, argv
    wait = int(option('--await-feed-reads'))
    timeout = check.get('timeout')
    assert wait > 0 and isinstance(timeout, int) and wait + 120 <= timeout <= wait + 300, (wait, timeout)


def ssh_sources(tf, firewall):
    """The `source_addresses` expression of `firewall`'s port-22 inbound rule in Terraform `tf`."""
    body = tf.split('resource "digitalocean_firewall" "%s" {' % firewall, 1)[1].split('\n}\n', 1)[0]
    (sources,) = re.findall(r'port_range\s*=\s*"22"\s*\n\s*source_addresses\s*=\s*(.+)', body)
    return sources.strip()


def check_firewall(tf, monitor_tf):
    """The receiver's SSH rule admits the coordinator's public /32, as the monitor's does.

    The coordinator is in another region's private network, so only its public
    address reaches the Droplet.
    """
    expected = 'concat(var.allowed_ssh_cidrs, ["${var.wallet_pir_coordinator_dns_ipv4}/32"])'
    assert ssh_sources(monitor_tf, 'pir_monitor') == expected, ssh_sources(monitor_tf, 'pir_monitor')
    assert ssh_sources(tf, 'receiver_pir') == expected, ssh_sources(tf, 'receiver_pir')


def fenced(text, language):
    """The bodies of `text`'s fenced code blocks in `language`."""
    return re.findall(r'^```%s\n(.*?)^```$' % language, text, re.S | re.M)


def check_probe_config(configs):
    """Check a `service-probes.json` holding only the receiver's record, as `pir-monitor` parses it."""
    assert isinstance(configs, list) and len(configs) == 1, configs
    (config,) = configs
    assert sorted(config) == ['command', 'service', 'timeout_seconds'], sorted(config)
    assert config['service'] == 'receiver' and 1 <= config['timeout_seconds'] <= 45, config
    argv = config['command']
    assert argv[0] == '/opt/pir-monitor/receiver-probe', argv
    # The probe's fixture is built in.
    assert not any(word.startswith('--fixture') for word in argv), argv
    # The service serves witness files (see check_exact_check), so the monitor checks them too.
    assert '--witnesses' in argv, argv


def monitor_merge(readme):
    """The runbook's two `jq` programs for an existing monitor: `(merge, check)`."""
    (block,) = [b for b in fenced(readme, 'sh') if '--slurpfile new receiver-probe.json' in b]
    (merge,) = re.findall(r"jq --slurpfile new receiver-probe\.json '(.*?)' service-probes\.json\.live", block, re.S)
    (check,) = re.findall(r"jq -e '(.*?)' service-probes\.json\.new", block, re.S)
    return merge, check


class ReceiverOpsContract(unittest.TestCase):
    def setUp(self):
        self.caddy = (DIR / 'Caddyfile').read_text()
        self.unit = (DIR / 'receiver-pir.service.in').read_text()
        self.cloud = (DIR / 'cloud-init.yaml').read_text()
        self.service = json.loads(INVENTORY.read_text())['services']['receiver']
        with open(DEPLOY, 'rb') as handle:
            self.descriptor = tomllib.load(handle)['services']['receiver']

    def test_repository_files_satisfy_the_contract(self):
        bind, _ = check_unit(self.unit)
        self.assertEqual(check_caddyfile(self.caddy), bind)
        check_cloud_init(self.cloud, bind)

    def test_the_deploy_tool_renders_this_whole_unit(self):
        with open(DEPLOY, 'rb') as handle:
            role = tomllib.load(handle)['services']['receiver']['roles']['server']
        self.assertEqual((role['unit'], role['mode']), ('receiver-pir.service', 'template'))
        # No drop-in may stay beside it, so the key file the unit names is the only key.
        self.assertIs(role.get('owns_unit'), True)
        self.assertEqual((DEPLOY.parent / role['template']).resolve(), DIR / 'receiver-pir.service.in')
        self.assertFalse((DIR / 'receiver-pir.service').exists(), 'the template is the only unit source')

    def test_the_inventory_names_the_key_file_the_unit_requires(self):
        self.assertEqual(self.descriptor.get('template_vars'), ['NEAR_KEY'])
        self.assertIsInstance(self.service.get('template_vars', {}).get('NEAR_KEY'), str)

    def test_the_deploy_runs_the_probe_on_the_receiver_before_commit(self):
        check_exact_check(self.service, self.unit, self.caddy, self.descriptor)
        self.assertNotIn('--skip-exact-check', (DIR / 'README.md').read_text())

    def test_misdirected_or_unbounded_probe_fails(self):
        def mutated(edit):
            service = copy.deepcopy(self.service)
            edit(service['exact_check'], service['exact_check']['argv'])
            return service
        swap = lambda old, new: lambda check, argv: argv.__setitem__(argv.index(old), new)
        for service in [
            mutated(lambda check, argv: check.update(host='coordinator')),
            mutated(lambda check, argv: check.pop('timeout')),
            mutated(lambda check, argv: check.update(timeout=900)),
            mutated(swap(PROBE[0], '/opt/receiver-pir/tools/receiver-directory')),
            mutated(swap('https://receiver-pir.valargroup.dev', 'https://receiver.example')),
            mutated(swap('http://10.70.0.11:18380/v1/receiver/health', 'http://127.0.0.1:18380/v1/receiver/health')),
            mutated(swap('http://10.70.0.6:8232', 'http://127.0.0.1:8232')),
            mutated(lambda check, argv: argv.remove('--no-auth')),
            mutated(lambda check, argv: argv.__delitem__(slice(argv.index('--await-feed-reads'), None))),
            mutated(lambda check, argv: check.update(timeout=300)),
        ]:
            self.assertNotEqual(service, self.service)
            with self.assertRaises(AssertionError):
                check_exact_check(service, self.unit, self.caddy, self.descriptor)

    def test_exposed_operator_routes_or_catch_all_proxy_fail(self):
        matcher = '/v1/receiver/filters/*'
        for mutated in [
            self.caddy.replace(matcher, matcher + ' /metrics'),
            self.caddy.replace(matcher, matcher + ' /v1/receiver/health'),
            self.caddy.replace(matcher, matcher + ' /v1/receiver/*'),
            self.caddy.replace('\thandle {', '\thandle /metrics {\n\t\treverse_proxy 10.70.0.11:18380\n\t}\n\thandle {'),
            self.caddy.replace('respond 404', 'reverse_proxy 10.70.0.11:18380'),
        ]:
            self.assertNotEqual(mutated, self.caddy)
            with self.assertRaises(AssertionError):
                check_caddyfile(mutated)

    def test_dropped_wallet_route_fails(self):
        for route in WALLET_ROUTES:
            mutated = self.caddy.replace(' ' + route, '', 1)
            self.assertNotEqual(mutated, self.caddy, route)
            with self.assertRaises(AssertionError):
                check_caddyfile(mutated)

    def test_public_bind_missing_serve_or_weakened_hardening_fail(self):
        for mutated in [
            self.unit.replace('--bind 10.70.0.11:18380', '--bind 0.0.0.0:18380'),
            self.unit.replace('--bind 10.70.0.11:18380', '--bind 159.203.10.20:18380'),
            self.unit.replace('--serve ', ''),
            self.unit.replace('ProtectSystem=strict', 'ProtectSystem=full'),
            self.unit.replace('ReadWritePaths=/srv/receiver-pir', 'ReadWritePaths=/'),
            self.unit.replace('Restart=on-failure', 'Restart=no'),
            self.unit.replace('User=receiver-pir', 'User=root'),
            self.unit.replace('@RELEASE@/receiver-directory', '/opt/receiver-pir/current/receiver-directory'),
            self.unit.replace('=' + NEAR_KEY_FILE, '=-' + NEAR_KEY_FILE),
            self.unit.replace(NEAR_KEY_FILE, '/etc/receiver-pir/near.env'),
            self.unit.replace(NEAR_KEY_FILE, NEAR_KEY_FILE + '\nEnvironmentFile=-/etc/receiver-pir/near.env'),
            self.unit.replace('Environment=RUST_LOG=info', 'Environment=RUST_LOG=info NEAR_INTENTS_EXPLORER=x'),
        ]:
            self.assertNotEqual(mutated, self.unit)
            with self.assertRaises(AssertionError):
                check_unit(mutated)

    def test_the_firewall_admits_the_coordinators_ssh(self):
        tf, monitor_tf = (INFRA / 'receiver.tf').read_text(), (INFRA / 'monitor.tf').read_text()
        check_firewall(tf, monitor_tf)
        expression = 'concat(var.allowed_ssh_cidrs, ["${var.wallet_pir_coordinator_dns_ipv4}/32"])'
        for mutated in [tf.replace(expression, 'var.allowed_ssh_cidrs'),
                        tf.replace('${var.wallet_pir_coordinator_dns_ipv4}/32', '10.142.0.0/16')]:
            self.assertNotEqual(mutated, tf)
            with self.assertRaises(AssertionError):
                check_firewall(mutated, monitor_tf)

    def test_the_monitor_probe_config_is_an_array(self):
        (block,) = [b for b in fenced((DIR / 'README.md').read_text(), 'json') if 'receiver-probe' in b]
        configs = json.loads(block)
        check_probe_config(configs)
        for mutated in [configs[0], configs * 2, [dict(configs[0], extra=1)]]:
            with self.assertRaises(AssertionError):
                check_probe_config(mutated)

    def test_the_monitor_probe_merge_keeps_one_receiver_and_the_other_probes(self):
        readme = (DIR / 'README.md').read_text()
        (block,) = [b for b in fenced(readme, 'json') if 'receiver-probe' in b]
        entry = json.loads(block)
        merge, check = monitor_merge(readme)
        jq = lambda program, data, *args: subprocess.run(
            ['jq', *args, program], input=json.dumps(data), capture_output=True, text=True)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        new = Path(directory.name) / 'receiver-probe.json'
        new.write_text(block)
        others = [{'service': name, 'command': ['/opt/pir-monitor/%s-probe' % name], 'timeout_seconds': 30}
                  for name in ('status', 'transparent')]
        stale = dict(entry[0], timeout_seconds=10)
        for live, kept in [([], []), (others, others), (others[:1] + [stale] + others[1:], others),
                           ([stale, stale], [])]:
            run = jq(merge, live, '--slurpfile', 'new', str(new))
            self.assertEqual(run.returncode, 0, run.stderr)
            merged = json.loads(run.stdout)
            self.assertEqual([c for c in merged if c['service'] != 'receiver'], kept)
            self.assertEqual([c for c in merged if c['service'] == 'receiver'], entry)
            self.assertEqual(jq(check, merged, '-e').returncode, 0, merged)
        self.assertNotEqual(jq(merge, {'service': 'status'}, '--slurpfile', 'new', str(new)).returncode, 0)
        for bad in [others + others, others + entry * 2, others + [dict(entry[0], extra=1)], {'0': entry[0]}]:
            self.assertNotEqual(jq(check, bad, '-e').returncode, 0, bad)

    def test_cloud_init_must_create_the_units_account_and_keep_the_port_private(self):
        bind, _ = check_unit(self.unit)
        for mutated in [
            self.cloud.replace("'0750', /srv/receiver-pir", "'0750', /srv/receiver"),
            self.cloud.replace('-o, receiver-pir, -g, receiver-pir', '-o, root, -g, root'),
            self.cloud.replace('--system, --home-dir', '--home-dir'),
            self.cloud.replace('from, 10.70.0.0/16, to, any, port', 'from, any, to, any, port'),
            self.cloud.replace("  - [ufw, --force, enable]", "  - [ufw, allow, '18380/tcp']\n  - [ufw, --force, enable]"),
        ]:
            self.assertNotEqual(mutated, self.cloud)
            with self.assertRaises(AssertionError):
                check_cloud_init(mutated, bind)


if __name__ == '__main__':
    unittest.main()
