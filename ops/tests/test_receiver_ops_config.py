"""The receiver Droplet's Caddyfile, unit and cloud-init must agree on its private serving contract.

Caddy syntax validation accepts a catch-all proxy, which would put the
receiver's operator routes (`/v1/receiver/health`, `/metrics`) on the public
edge. These checks pin the edge to the six wallet routes, the service to a
private listener with its hardening, and cloud-init to the matching account,
directories and firewall rule. Each check takes the file's text, so the
negative cases run it against mutated copies. The unit is the template
`wallet-pir-deploy.py` renders, so these checks also cover every deployed unit.
The example inventory's `exact_check` must probe this edge, listener, fixture and
nodes from the receiver's own host before a deploy commits, running the probe and
fixture the deploy stages from the same bundle. The Terraform firewall must admit
the coordinator's SSH, which every locked operation needs.
"""
import copy
import hashlib
import importlib.util
import ipaddress
import json
from pathlib import Path
import re
import shlex
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
PROBE = '{release_dir}/receiver-probe'
PROBE_FIXTURE = '{release_dir}/probe-fixture.json'
COMPANIONS = [{'name': 'receiver-probe', 'mode': 0o755}, {'name': 'probe-fixture.json', 'mode': 0o644}]
USER = 'receiver-pir'
STATE = '/srv/receiver-pir'
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


def release_bundle_files():
    """Every file name of a `receiver-pir` release bundle, from `tools/ci/release.py`."""
    spec = importlib.util.spec_from_file_location('release', ROOT / 'tools/ci/release.py')
    release = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(release)
    return set(release.BINARIES['receiver-pir']) | {Path(path).name for path in release.FILES['receiver-pir']}


def check_exact_check(service, unit, caddy, fixture, descriptor):
    """Check the receiver's deploy gate: the release's own probe, run on the server's host.

    The coordinator cannot reach the private health route. The descriptor stages
    the bundle's probe and fixture with the binary, so the check runs them from
    `{release_dir}`, never a separately installed copy.
    """
    (server,) = service['roles']['server']
    check = service['exact_check']
    bind, _ = check_unit(unit)
    assert server['vars']['listen'] == bind, server
    assert check['host'] == server['host'], check['host']
    assert descriptor.get('companions') == COMPANIONS, descriptor.get('companions')
    assert {c['name'] for c in COMPANIONS} <= release_bundle_files()
    argv = check['argv']
    assert argv[0] == PROBE and not any('{' in word for word in argv[1:] if word != PROBE_FIXTURE), argv
    option = lambda name: argv[argv.index(name) + 1]
    (site, _), = caddy_tree(caddy)
    assert option('--origin') == 'https://' + site, argv
    assert option('--health-url') == 'http://%s/v1/receiver/health' % bind, argv
    assert option('--fixture') == PROBE_FIXTURE, argv
    assert option('--fixture-sha256') == hashlib.sha256(fixture).hexdigest(), argv
    # The nodes and authentication the service itself uses, the only ones the host reaches.
    served = shlex.split(systemd_sections(unit)['Service']['ExecStart'][0])
    nodes = lambda words: [words[i + 1] for i, word in enumerate(words) if word == '--rpc-url']
    assert nodes(argv) and nodes(argv) == nodes(served), (nodes(argv), nodes(served))
    assert ('--no-auth' in argv) == ('--no-auth' in served), argv
    assert isinstance(check.get('timeout'), int) and 0 < check['timeout'] <= 300, check.get('timeout')


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


class ReceiverOpsContract(unittest.TestCase):
    def setUp(self):
        self.caddy = (DIR / 'Caddyfile').read_text()
        self.unit = (DIR / 'receiver-pir.service.in').read_text()
        self.cloud = (DIR / 'cloud-init.yaml').read_text()
        self.service = json.loads(INVENTORY.read_text())['services']['receiver']
        self.fixture = (DIR / 'probe-fixture.json').read_bytes()
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
        self.assertEqual((DEPLOY.parent / role['template']).resolve(), DIR / 'receiver-pir.service.in')
        self.assertFalse((DIR / 'receiver-pir.service').exists(), 'the template is the only unit source')

    def test_the_deploy_runs_the_probe_on_the_receiver_before_commit(self):
        check_exact_check(self.service, self.unit, self.caddy, self.fixture, self.descriptor)
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
            mutated(swap(PROBE, '/opt/receiver-pir/tools/receiver-probe')),
            mutated(swap('https://receiver-pir.valargroup.dev', 'https://receiver.example')),
            mutated(swap('http://10.70.0.11:18380/v1/receiver/health', 'http://127.0.0.1:18380/v1/receiver/health')),
            mutated(swap(PROBE_FIXTURE, '/opt/receiver-pir/tools/receiver-probe-fixture.json')),
            mutated(swap(PROBE_FIXTURE, '{release_dir}/receiver-probe-fixture.json')),
            mutated(swap(hashlib.sha256(self.fixture).hexdigest(), '0' * 64)),
            mutated(swap('http://10.70.0.6:8232', 'http://127.0.0.1:8232')),
            mutated(lambda check, argv: argv.remove('--no-auth')),
        ]:
            self.assertNotEqual(service, self.service)
            with self.assertRaises(AssertionError):
                check_exact_check(service, self.unit, self.caddy, self.fixture, self.descriptor)

    def test_unstaged_or_misnamed_companions_fail(self):
        for companions in [COMPANIONS[:1], COMPANIONS[1:], [],
                           [COMPANIONS[0], {'name': 'probe-fixture.json', 'mode': 0o755}],
                           [{'name': 'receiver-probe', 'mode': 0o644}, COMPANIONS[1]],
                           [COMPANIONS[0], {'name': 'receiver-probe-fixture.json', 'mode': 0o644}]]:
            descriptor = dict(self.descriptor, companions=companions)
            with self.assertRaises(AssertionError):
                check_exact_check(self.service, self.unit, self.caddy, self.fixture, descriptor)

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
