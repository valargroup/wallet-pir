"""Service descriptors from the repository and the fleet from an operator inventory.

The descriptors (`enhance/ops/deploy/deploy.toml`) say what a service is: its
roles in rollout order, each role's unit, how its unit is changed and how its
readiness is checked. They never name hosts. The inventory is a local JSON file
outside the repository that says where each role runs and how to reach it;
`enhance/ops/deploy/deploy-inventory.example.json` shows its shape.
"""
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import string
import tomllib

MODES = ('exec-drop-in', 'template')
UNIT = re.compile(r'^[A-Za-z0-9@._-]+\.service$')
PLAIN_PATH = re.compile(r'^/[A-Za-z0-9._/-]+$')
PLAIN_NAME = re.compile(r'^[A-Za-z0-9._-]+$')
HOST_NAME = re.compile(r'^[A-Za-z0-9._-]+$')
# A template value: no whitespace, `=`, `/` or quotes, so it cannot add a unit
# line or a path segment wherever a template puts it. NEAR key ids use it too.
TEMPLATE_VALUE = re.compile(r'[A-Za-z0-9._-]+')
LOCK_PATH = '/run/lock/wallet-pir-production.lock'


class DescriptorError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise DescriptorError(message)


@dataclass(frozen=True)
class Role:
    name: str
    unit: str
    mode: str
    health: str
    template: Path = None
    ready: dict = None
    ready_url: str = None
    ready_timeout: int = 180
    adoptable_drop_ins: tuple = ()
    # A template role whose rendered unit is all of its configuration: a drop-in
    # the tool would keep is refused, so none can add settings the deploy did not
    # review, such as another EnvironmentFile.
    owns_unit: bool = False


@dataclass(frozen=True)
class Service:
    name: str
    root: str
    binary: str
    order: tuple
    roles: dict
    artifact_kinds: tuple = ()
    self_check: tuple = ('--help',)
    min_free_bytes: int = 1 << 30
    template_vars: tuple = ()

    def release_dir(self, sha):
        return '%s/releases/%s' % (self.root, sha)

    def release_binary(self, sha):
        return '%s/%s' % (self.release_dir(sha), self.binary)

    def transaction_dir(self, transaction):
        return '%s/transactions/%s' % (self.root, transaction)


@dataclass(frozen=True)
class Target:
    """One role instance: a unit on a host."""
    role: Role
    host: str
    unit: str
    vars: dict = field(default_factory=dict)
    health_equals: dict = field(default_factory=dict)

    @property
    def key(self):
        return '%s@%s' % (self.role.name, self.host)

    def url(self, template):
        return template.format_map(self.vars) if template else None


def placeholders(template):
    return {name for _, name, _, _ in string.Formatter().parse(template or '') if name}


def load_descriptors(path):
    path = Path(path)
    with open(path, 'rb') as handle:
        document = tomllib.load(handle)
    services = {}
    for name, raw in document.get('services', {}).items():
        roles = {}
        for role_name, spec in raw.get('roles', {}).items():
            mode = spec.get('mode')
            require(mode in MODES, '%s.%s: mode must be one of %s' % (name, role_name, MODES))
            require(UNIT.match(spec.get('unit', '')), '%s.%s: invalid unit name' % (name, role_name))
            require(isinstance(spec.get('health'), str) and spec['health'].startswith('http://'),
                    '%s.%s: health must be an http:// URL checked from the host' % (name, role_name))
            template = None
            # Required to render a template role; an exec-drop-in role may name
            # the template its base unit was installed from.
            if mode == 'template' or spec.get('template'):
                template = path.parent / spec.get('template', '')
                require(spec.get('template') and template.is_file(), '%s.%s: template file missing' % (name, role_name))
            ready = spec.get('ready')
            require(ready is None or (set(ready) in ({'field', 'equals'}, {'field', 'nonempty'})),
                    '%s.%s: ready is {field, equals} or {field, nonempty}' % (name, role_name))
            owns_unit = spec.get('owns_unit', False)
            require(isinstance(owns_unit, bool) and (not owns_unit or mode == 'template'),
                    '%s.%s: owns_unit is a boolean, true only for a template role' % (name, role_name))
            roles[role_name] = Role(role_name, spec['unit'], mode, spec['health'], template, ready,
                                    spec.get('ready_url'), int(spec.get('ready_timeout', 180)),
                                    tuple(spec.get('adoptable_drop_ins', ())), owns_unit)
        order = tuple(raw.get('order', ()))
        require(order and sorted(order) == sorted(roles), '%s: order must list every role once' % name)
        require(PLAIN_PATH.match(raw.get('root', '')), '%s: root must be a plain absolute path' % name)
        require(PLAIN_NAME.match(raw.get('binary', '')), '%s: binary must be a plain file name' % name)
        services[name] = Service(name, raw['root'], raw['binary'], order, roles,
                                 tuple(raw.get('artifact_kinds', ())), tuple(raw.get('self_check', ('--help',))),
                                 int(raw.get('min_free_bytes', 1 << 30)), tuple(raw.get('template_vars', ())))
    return services


@dataclass(frozen=True)
class Inventory:
    hosts: dict
    ssh: dict
    lock: dict
    services: dict


def load_inventory(path):
    document = json.loads(Path(path).read_text())
    hosts = document.get('hosts', {})
    require(hosts and all(HOST_NAME.match(name) for name in hosts), 'inventory: hosts must be named')
    lock = document.get('lock')
    require(isinstance(lock, dict) and (
        (lock.get('type') == 'pinned_host' and set(lock) == {'type', 'machine_id'})
        or (lock.get('type') == 'remote' and set(lock) == {'type', 'host'} and lock['host'] in hosts)),
        'inventory: lock is {"type": "pinned_host", "machine_id"} on the runner or {"type": "remote", "host"}')
    ssh = document.get('ssh', {})
    require(ssh.get('mode') in ('pinned', 'config'), 'inventory: ssh.mode is "pinned" or "config"')
    if ssh['mode'] == 'pinned':
        require({'key', 'known_hosts', 'known_hosts_sha256'} <= set(ssh)
                and all(isinstance(entry, dict) and entry.get('address') for entry in hosts.values()),
                'inventory: pinned SSH needs ssh.key, known_hosts, known_hosts_sha256 and an address per host')
    for name, entry in hosts.items():
        require(isinstance(entry, dict), 'inventory: host entry must be an object')
        require(not entry.get('jump') or (entry['jump'] in hosts and entry['jump'] != name
                and not hosts[entry['jump']].get('jump')), 'inventory: jump must name a direct host')
        require(isinstance(entry.get('sudo', False), bool), 'inventory: sudo must be boolean')
        require(PLAIN_NAME.fullmatch(entry.get('user', ssh.get('user', 'root'))), 'inventory: invalid SSH user')
    return Inventory(hosts, ssh, lock, document.get('services', {}))


def select(targets, only):
    """The targets named by `only` ("role" or "role@host" selectors), in rollout order.

    Lets a replicated role roll alone while single-instance roles are left
    running; every selector must match something.
    """
    if not only:
        return targets
    chosen = []
    for selector in only:
        matched = [t for t in targets if selector in (t.role.name, t.key)]
        require(matched, 'no target matches %r; targets are %s' % (selector, [t.key for t in targets]))
        chosen += matched
    return [t for t in targets if t in chosen]


def targets(service, inventory):
    """Every role instance of `service`, in rollout order: roles by descriptor, hosts by inventory."""
    spec = inventory.services.get(service.name)
    require(spec is not None, 'inventory has no service %r' % service.name)
    unknown = set(spec.get('roles', {})) - set(service.roles)
    require(not unknown, 'inventory names unknown %s roles: %s' % (service.name, sorted(unknown)))
    result, seen = [], set()
    for role_name in service.order:
        role = service.roles[role_name]
        for entry in spec.get('roles', {}).get(role_name, []):
            host = entry.get('host')
            require(host in inventory.hosts, '%s.%s: unknown host %r' % (service.name, role_name, host))
            unit = entry.get('unit', role.unit)
            require(UNIT.match(unit), '%s.%s: invalid unit override' % (service.name, role_name))
            require((host, unit) not in seen, '%s: %s appears twice on %s' % (service.name, unit, host))
            seen.add((host, unit))
            values = {**spec.get('vars', {}), **entry.get('vars', {})}
            missing = (placeholders(role.health) | placeholders(role.ready_url)) - set(values)
            require(not missing, '%s.%s@%s: inventory vars missing %s' % (service.name, role_name, host, sorted(missing)))
            expected = entry.get('health_equals', {})
            require(isinstance(expected, dict) and all(
                isinstance(key, str) and re.fullmatch(r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*', key)
                and isinstance(value, (str, int, bool)) for key, value in expected.items()),
                'inventory: health_equals maps dotted fields to scalar expectations')
            result.append(Target(role, host, unit, values, expected))
    require(result, 'inventory lists no %s hosts' % service.name)
    return result


def template_values(service, inventory, sha):
    """`@NAME@` substitutions for a service's unit templates."""
    values = {'RELEASE': service.release_dir(sha)}
    configured = inventory.services.get(service.name, {}).get('template_vars', {})
    for name in service.template_vars:
        require(isinstance(configured.get(name), str) and configured[name],
                'inventory: %s.template_vars.%s is required' % (service.name, name))
        require(TEMPLATE_VALUE.fullmatch(configured[name]),
                'inventory: %s.template_vars.%s must be letters, digits, ".", "_" or "-"'
                % (service.name, name))
        values[name] = configured[name]
    return values


def exact_check(service, inventory):
    """The post-deploy exact-answer command, `{"host", "argv", "timeout"}`, or None."""
    check = inventory.services.get(service.name, {}).get('exact_check')
    if check is not None:
        require(check.get('host') in inventory.hosts and isinstance(check.get('argv'), list) and check['argv'],
                '%s.exact_check needs a known host and a non-empty argv' % service.name)
    return check
