"""Service descriptors from the repository and the fleet from an operator inventory.

The shared registry (`tools/deploy/services.toml`) includes product descriptors: its
roles in rollout order, each role's unit, how its unit is changed and how its
readiness is checked. They never name hosts. The inventory is a local JSON file
outside the repository that says where each role runs and how to reach it;
`tools/enhance/deploy/deploy-inventory.example.json` shows its shape.
"""
import dataclasses
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import string
import tomllib

MODES = ('exec-drop-in', 'template', 'role')
RENDERER = re.compile(r'^[a-z_][a-z0-9_.]*:[a-z_][a-z0-9_]*$')
GUARD = re.compile(r'^[a-z_][a-z0-9_.]*:[A-Z][A-Za-z0-9_]*$')
UNIT = re.compile(r'^[A-Za-z0-9@._-]+\.service$')
PLAIN_PATH = re.compile(r'^/[A-Za-z0-9._/-]+$')
PLAIN_NAME = re.compile(r'^[A-Za-z0-9._-]+$')
HOST_NAME = re.compile(r'^[A-Za-z0-9._-]+$')
# A template value: no whitespace, `=`, `/` or quotes, so it cannot add a unit
# line or a path segment wherever a template puts it. NEAR key ids use it too.
TEMPLATE_VALUE = re.compile(r'[A-Za-z0-9._-]+')
LOCK_PATH = '/run/lock/wallet-pir-production.lock'
RESPONSIBILITIES = ('journal-writer',)
ANCILLARY_KINDS = ('routing', 'supervision')
# What the shared CLI may do to each lifecycle driver's roles. Product drivers
# keep their own recipe families (`schema-*`, `txid-display-*`); ancillary
# routing and supervision units are inspected only, never changed here.
OPERATIONS = {
    'binary': ('plan', 'preflight', 'deploy', 'rollback', 'status', 'capture-baseline'),
    'transparent-history': ('status', 'schema-*'),
    'txid-display': ('status', 'txid-display-*'),
}
ANCILLARY_OPERATIONS = ('inspect',)


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
    # Duties exactly one instance of the role must hold in a complete fleet,
    # such as the journal writer on a publication controller. They are
    # attributes of that instance, never a unit of their own.
    responsibilities: tuple = ()
    # A `role` role's product adapter, `module:function`: renders the whole
    # unit, its configuration files and its preflight check from the
    # inventory's settings for one instance.
    renderer: str = None
    # Not required in a complete fleet: an external worker an operator may add.
    optional: bool = False
    # Some rendered roles own an exclusive writable root and must preflight
    # only as their unit user after stopping. The generic staged loader cannot
    # execute that lifecycle; rendering and private observations still work.
    shared_lifecycle: bool = True
    # A rendered role whose state and data updates the product owns, as
    # `module:Class`: the engine asks it before changing, checking, restarting
    # or restoring an instance (see `wallet_pir_ops.deploy.engine.Deployer`).
    guard: str = None
    # Executable file name in the release directory. None uses the service binary.
    # A deploy runs one transaction per binary, and the release directory is
    # named by the sha256 of that file.
    binary: str = None
    # Explicit product opt-in: recognized old launch overlays may include their
    # writable state paths when the complete role replaces both directives.
    adopt_command_write_paths: bool = False
    # An interpreter/credential loader that execs the real service executable.
    # Every prefix file is pinned; only the following executable may change.
    launcher: tuple = ()

    def render(self, settings, release, unit=None):
        """The product adapter's `RenderedRole` for one instance.

        `unit` is the inventory's unit name when this host runs the role more
        than once. Adapters that take `unit` can give that process its own
        files; the descriptor's own unit stays the single-instance rendering.
        """
        import importlib
        import inspect
        module, function = self.renderer.split(':')
        renderer = getattr(importlib.import_module(module), function)
        kwargs = {'release': release, 'template': self.template.read_text()}
        if unit not in (None, self.unit) and 'unit' in inspect.signature(renderer).parameters:
            kwargs['unit'] = unit
        return renderer(settings, **kwargs)

    def product_guard(self):
        """The product's lifecycle guard for this role, or None."""
        if self.guard is None:
            return None
        import importlib
        module, name = self.guard.split(':')
        return getattr(importlib.import_module(module), name)()


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
    driver: str = 'binary'
    # Kinds of shared ancillary unit (routing, supervision) a complete fleet
    # must name this service as a consumer of.
    ancillary: tuple = ()

    def release_dir(self, sha):
        return '%s/releases/%s' % (self.root, sha)

    def release_binary(self, sha, name=None):
        """`/opt/<svc>/releases/<sha256>/<file>`. `name` defaults to the service binary."""
        return '%s/%s' % (self.release_dir(sha), name or self.binary)

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
    # A release artifact kind other than the transaction's, e.g. a CUDA build
    # for a GPU worker. None means the binary given to the transaction.
    artifact_kind: str = None
    responsibilities: tuple = ()
    # Set when one host runs the role more than once (history archive and
    # recent workers): the key then names the unit too.
    qualified: bool = False
    # A `role` role's inventory settings: paths, addresses, budgets and
    # credential file references for this instance.
    settings: dict = field(default_factory=dict)
    # Whether a deploy may install this role where its unit is absent. Off by
    # default, so a mistyped host never receives a new service.
    install: bool = False

    @property
    def key(self):
        key = '%s@%s' % (self.role.name, self.host)
        return '%s:%s' % (key, self.unit) if self.qualified else key

    def url(self, template):
        return template.format_map(self.vars) if template else None


def placeholders(template):
    return {name for _, name, _, _ in string.Formatter().parse(template or '') if name}


def load_descriptors(path, _parents=()):
    path = Path(path).resolve()
    require(path not in _parents, 'cyclic service descriptor include: %s' % path)
    with open(path, 'rb') as handle:
        document = tomllib.load(handle)
    services = {}
    includes = document.get('include', [])
    require(isinstance(includes, list) and all(isinstance(p, str) and p for p in includes),
            'descriptor include must be a list of paths')
    for included in includes:
        additions = load_descriptors(path.parent / included, (*_parents, path))
        require(not services.keys() & additions.keys(), 'duplicate included service descriptor')
        services.update(additions)
    for name, raw in document.get('services', {}).items():
        require(name not in services, 'duplicate service descriptor: %s' % name)
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
            if mode in ('template', 'role') or spec.get('template'):
                template = path.parent / spec.get('template', '')
                require(spec.get('template') and template.is_file(), '%s.%s: template file missing' % (name, role_name))
            ready = spec.get('ready')
            require(ready is None or (set(ready) in ({'field', 'equals'}, {'field', 'nonempty'})),
                    '%s.%s: ready is {field, equals} or {field, nonempty}' % (name, role_name))
            owns_unit = spec.get('owns_unit', False)
            require(isinstance(owns_unit, bool) and (not owns_unit or mode == 'template'),
                    '%s.%s: owns_unit is a boolean, true only for a template role' % (name, role_name))
            duties = spec.get('responsibilities', [])
            require(isinstance(duties, list) and all(d in RESPONSIBILITIES for d in duties),
                    '%s.%s: responsibilities are among %s' % (name, role_name, RESPONSIBILITIES))
            renderer = spec.get('renderer')
            require((mode == 'role') == (renderer is not None) and (renderer is None or RENDERER.match(renderer)),
                    '%s.%s: a role-mode role, and only one, names its renderer as module:function' % (name, role_name))
            optional = spec.get('optional', False)
            require(isinstance(optional, bool) and (not optional or mode == 'role'),
                    '%s.%s: only a role-mode role may be optional' % (name, role_name))
            shared_lifecycle = spec.get('shared_lifecycle', True)
            require(type(shared_lifecycle) is bool and (shared_lifecycle or mode == 'role'),
                    '%s.%s: shared_lifecycle is boolean, false only for a rendered role' % (name, role_name))
            guard = spec.get('guard')
            require(guard is None or (mode == 'role' and shared_lifecycle and GUARD.match(guard)),
                    '%s.%s: guard names a rendered shared-lifecycle role\'s module:Class' % (name, role_name))
            binary = spec.get('binary')
            require(binary is None or (isinstance(binary, str) and PLAIN_NAME.match(binary)),
                    '%s.%s: binary must be a plain file name' % (name, role_name))
            adopt_paths = spec.get('adopt_command_write_paths', False)
            require(type(adopt_paths) is bool and (not adopt_paths or mode == 'role'),
                    '%s.%s: command/path adoption is boolean, true only for a rendered role' % (name, role_name))
            launcher = spec.get('launcher', [])
            require(isinstance(launcher, list) and len(launcher) <= 4 and (not launcher or mode == 'exec-drop-in')
                    and all(isinstance(p, dict) and set(p) == {'path', 'sha256'}
                            and isinstance(p['path'], str) and PLAIN_PATH.fullmatch(p['path'])
                            and isinstance(p['sha256'], str) and re.fullmatch('[a-f0-9]{64}', p['sha256'])
                            for p in launcher), 'launcher requires pinned absolute paths on an exec-drop-in role')
            roles[role_name] = Role(role_name, spec['unit'], mode, spec['health'], template, ready,
                                    spec.get('ready_url'), int(spec.get('ready_timeout', 180)),
                                    tuple(spec.get('adoptable_drop_ins', ())), owns_unit or mode == 'role',
                                    tuple(duties), renderer, optional, shared_lifecycle, guard, binary, adopt_paths,
                                    tuple((p['path'], p['sha256']) for p in launcher))
        order = tuple(raw.get('order', ()))
        require(order and sorted(order) == sorted(roles), '%s: order must list every role once' % name)
        require(PLAIN_PATH.match(raw.get('root', '')), '%s: root must be a plain absolute path' % name)
        require(PLAIN_NAME.match(raw.get('binary', '')), '%s: binary must be a plain file name' % name)
        driver = raw.get('driver', 'binary')
        require(driver in OPERATIONS, 'unknown service lifecycle driver')
        ancillary = raw.get('ancillary', [])
        require(isinstance(ancillary, list) and all(k in ANCILLARY_KINDS for k in ancillary),
                '%s: ancillary kinds are among %s' % (name, ANCILLARY_KINDS))
        services[name] = Service(name, raw['root'], raw['binary'], order, roles,
                                 tuple(raw.get('artifact_kinds', ())), tuple(raw.get('self_check', ('--help',))),
                                 int(raw.get('min_free_bytes', 1 << 30)), tuple(raw.get('template_vars', ())), driver,
                                 tuple(ancillary))
    return services


@dataclass(frozen=True)
class Inventory:
    hosts: dict
    ssh: dict
    lock: dict
    services: dict
    # Shared routing and supervision units, each listed once with the services
    # that consume it; inspected, never changed by the shared CLI.
    ancillary: dict = field(default_factory=dict)
    # Hosts that no longer exist, kept so a stale reference names the retirement.
    retired_hosts: dict = field(default_factory=dict)


PUBLISHER_PRODUCTS = {
    'enhance': ('enhance', 'coordinator'),
    'status': ('status', 'controller'),
    'transparent_history': ('transparent-history', 'controller'),
    'transaction_details': ('transparent-display', 'controller'),
    'receiver': ('receiver', 'server'),
}


def publisher_ownership(services):
    """A unified process owns the configured products once, across all hosts."""
    if 'publisher' not in services:
        return
    publisher = services['publisher']
    products = publisher.get('managed_products')
    require(isinstance(products, list) and products and all(isinstance(p, str) for p in products)
            and len(products) == len(set(products)) and set(products) <= PUBLISHER_PRODUCTS.keys(),
            'publisher: managed_products must name distinct supported products')
    require(isinstance(publisher.get('exact_check'), dict),
            'publisher: an independent exact_check covering every managed product is required')
    for product in products:
        name, role = PUBLISHER_PRODUCTS[product]
        require(not services.get(name, {}).get('roles', {}).get(role),
                'publisher and standalone %s both own publishing; remove the old owner before cutover' % name)
    servers = publisher.get('roles', {}).get('server', [])
    require(len(servers) == 1, 'publisher: exactly one server owns these publishing products')


def load_inventory(path):
    document = json.loads(Path(path).read_text())
    hosts = document.get('hosts', {})
    require(hosts and all(HOST_NAME.match(name) for name in hosts), 'inventory: hosts must be named')
    retired = document.get('retired_hosts', {})
    require(isinstance(retired, dict) and all(HOST_NAME.match(n) and isinstance(v, dict) for n, v in retired.items()),
            'inventory: retired_hosts maps host names to a retirement record')
    require(not retired.keys() & hosts.keys(), 'inventory: a host cannot be both live and retired: %s'
            % sorted(retired.keys() & hosts.keys()))
    for name, entry in hosts.items():
        require(not isinstance(entry, dict) or entry.get('jump') not in retired,
                'inventory: %s jumps through retired host %s' % (name, isinstance(entry, dict) and entry.get('jump')))
    lock = document.get('lock')
    require(not isinstance(lock, dict) or lock.get('host') not in retired, 'inventory: the lock names a retired host')
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
        # A host reached with its own existing identity names it; the global
        # ssh.key and known-hosts pin stay the fallback. A known-hosts file is
        # always pinned by the digest beside it.
        require(not ({'known_hosts', 'known_hosts_sha256'} & set(entry)) or
                (isinstance(entry.get('known_hosts'), str) and
                 re.fullmatch('[0-9a-f]{64}', str(entry.get('known_hosts_sha256')))),
                'inventory: %s overrides known_hosts only together with its known_hosts_sha256' % name)
        require(entry.get('key') is None or isinstance(entry['key'], str) and entry['key'],
                'inventory: %s.key must be a path' % name)
        require(not entry.get('jump') or (entry['jump'] in hosts and entry['jump'] != name
                and not hosts[entry['jump']].get('jump')), 'inventory: jump must name a direct host')
        require(isinstance(entry.get('sudo', False), bool), 'inventory: sudo must be boolean')
        require(PLAIN_NAME.fullmatch(entry.get('user', ssh.get('user', 'root'))), 'inventory: invalid SSH user')
    ancillary = document.get('ancillary', {})
    require(isinstance(ancillary, dict) and all(PLAIN_NAME.fullmatch(n) and isinstance(v, dict)
                                                for n, v in ancillary.items()),
            'inventory: ancillary maps names to unit records')
    services = document.get('services', {})
    publisher_ownership(services)
    return Inventory(hosts, ssh, lock, services, ancillary, retired)


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
            require(host not in inventory.retired_hosts, '%s.%s: host %r is retired (%s)'
                    % (service.name, role_name, host, inventory.retired_hosts.get(host, {}).get('retired', 'no date')))
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
            kind = entry.get('artifact_kind')
            require(kind is None or kind in service.artifact_kinds,
                    '%s.%s@%s: artifact_kind must be one of %s'
                    % (service.name, role_name, host, list(service.artifact_kinds)))
            duties = entry.get('responsibilities', [])
            require(isinstance(duties, list) and set(duties) <= set(role.responsibilities),
                    '%s.%s@%s: responsibilities must be among %s'
                    % (service.name, role_name, host, list(role.responsibilities)))
            settings = entry.get('settings', {})
            require(isinstance(settings, dict) and (not settings or role.mode == 'role'),
                    '%s.%s@%s: settings belong to role-mode roles' % (service.name, role_name, host))
            install = entry.get('install', False)
            require(isinstance(install, bool) and (not install or role.mode == 'role'),
                    '%s.%s@%s: install is a boolean, true only for a role-mode role' % (service.name, role_name, host))
            result.append(Target(role, host, unit, values, expected, kind, tuple(duties),
                                 settings=settings, install=install))
    require(result, 'inventory lists no %s hosts' % service.name)
    repeated = {(t.role.name, t.host) for t in result
                if sum((u.role.name, u.host) == (t.role.name, t.host) for u in result) > 1}
    return [dataclasses.replace(t, qualified=True) if (t.role.name, t.host) in repeated else t for t in result]


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
    """Validate independent acceptance commands before deployment can mutate hosts."""
    check = inventory.services.get(service.name, {}).get('exact_check')
    if service.name == 'query-server' and check is not None:
        require(isinstance(check, dict) and 'products' in check,
                'query-server exact checks must cover every configured product')
    if service.name in ('publisher', 'query-server') and isinstance(check, dict) and 'products' in check:
        if service.name == 'query-server':
            from ..query_role import PRODUCTS
            products = PRODUCTS
        else:
            products = inventory.services['publisher']['managed_products']
        require(set(check) == {'products'} and isinstance(check['products'], dict)
                and set(check['products']) == set(products),
                service.name + ' exact checks must cover every ' +
                ('managed' if service.name == 'publisher' else 'configured') + ' product')
        for product, command in check['products'].items():
            require(isinstance(command, dict) and set(command) == {'host', 'argv', 'timeout'}
                    and isinstance(command['host'], str)
                    and command['host'] in inventory.hosts and command['host'] not in inventory.retired_hosts
                    and isinstance(command['argv'], list) and command['argv']
                    and all(isinstance(a, str) and a for a in command['argv'])
                    and type(command['timeout']) is int and 1 <= command['timeout'] <= 900,
                    'invalid ' + service.name + ' exact check: ' + product)
        return check
    if check is not None:
        require(check.get('host') not in inventory.retired_hosts, '%s.exact_check names a retired host' % service.name)
        require(check.get('host') in inventory.hosts and isinstance(check.get('argv'), list) and check['argv'],
                '%s.exact_check needs a known host and a non-empty argv' % service.name)
    return check


def exact_check_hosts(check):
    """All acceptance hosts must accept the deploy identity before any side effect."""
    if check is None:
        return []
    commands = check['products'].values() if 'products' in check else [check]
    return list(dict.fromkeys(command['host'] for command in commands))
