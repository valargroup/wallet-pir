"""Render a process and its filesystem permissions as one reviewable role.

The role owns configuration files and the unit that starts the process. This
module renders bytes only; deployment and rollback remain in the deploy engine.
"""
from dataclasses import dataclass
import json
from pathlib import PurePosixPath
import re
import shlex

from .deploy import units


def directory(value):
    if (not isinstance(value, str) or not value.startswith('/') or value.startswith('//')
            or str(PurePosixPath(value)) == '/'):
        raise ValueError('role paths must be absolute non-root paths')
    if not re.fullmatch(r'/[A-Za-z0-9_./ -]+', value) or '..' in PurePosixPath(value).parts:
        raise ValueError('role path contains control characters, traversal or systemd specifiers')
    return str(PurePosixPath(value))


def service_settings(template, settings):
    """Replace only owned Service directives; preserve all other unit settings."""
    units.entries(template)
    lines, section, skip = [], None, False
    for line in template.splitlines():
        if skip:
            if line.strip().startswith(('#', ';')):
                continue
            skip = line.rstrip().endswith('\\')
            continue
        if line.strip().startswith('['):
            section = line.strip()[1:-1]
        if section == 'Service' and line.strip().split('=', 1)[0] in settings:
            skip = line.rstrip().endswith('\\')
            continue
        lines.append(line)
    if lines.count('[Service]') != 1:
        raise ValueError('role template must have one Service section')
    at = lines.index('[Service]') + 1
    lines[at:at] = [key + '=' + value for key, value in settings.items()]
    return '\n'.join(lines) + '\n'


@dataclass(frozen=True)
class RenderedRole:
    unit: str
    files: dict
    # Writable directories the role needs before it starts: its state and
    # caches, never its inputs. Created by an install; never removed by a
    # rollback, since they hold no authority and may hold an operator's data.
    directories: tuple = ()
    # Arguments, after the release binary, of the product's own check of this
    # configuration and its staged inputs: the loader the service runs at
    # start, exiting without serving. Empty when the product has none.
    check: tuple = ()

    def describe(self):
        return {'unit': self.unit, 'files': self.files, 'directories': list(self.directories),
                'check': list(self.check)}


def render(template, executable, arguments, configurations, writable=(), readonly=()):
    """Render complete unit/configuration bytes without touching a host.

    Secrets remain references to installed credential files. Configuration
    contents are the caller's public service configuration, not an environment.
    """
    executable = directory(executable)
    if ' ' in executable:
        raise ValueError('role executable paths cannot contain spaces')
    if not isinstance(arguments, (list, tuple)) or any(
            not isinstance(arg, str) or any(c in arg for c in ('%', '$')) or any(ord(c) < 32 for c in arg)
            for arg in arguments):
        raise ValueError('role arguments must be strings without systemd specifiers or control characters')
    writes = tuple(dict.fromkeys(directory(p) for p in writable))
    reads = tuple(dict.fromkeys(directory(p) for p in readonly))
    if set(writes) & set(reads):
        raise ValueError('role path cannot be both writable and read-only')
    files = {directory(p): json.dumps(value, sort_keys=True, indent=2) + '\n'
             for p, value in configurations.items()}
    settings = {'ExecStart': shlex.join([executable, *arguments]),
                'ReadWritePaths': ' '.join(json.dumps(p) for p in writes),
                'ReadOnlyPaths': ' '.join(json.dumps(p) for p in reads)}
    unit = service_settings(template, settings)
    units.exec_start(units.effective([unit]))
    return RenderedRole(unit, files)


# Inventory-owned budgets a worker template may carry, and their syntax.
LIMITS = {
    'MemoryMax': r'[0-9]+[KMGT]?|infinity',
    'MemoryHigh': r'[0-9]+[KMGT]?|infinity',
    'MemorySwapMax': r'[0-9]+[KMGT]?|infinity',
    'CPUQuota': r'[0-9]+%',
    'CPUWeight': r'[0-9]+',
    'IOWeight': r'[0-9]+',
    'Nice': r'-?[0-9]+',
    'User': r'[a-z_][a-z0-9_-]*',
    'Group': r'[a-z_][a-z0-9_-]*',
}
# Settings every worker role accepts besides its product's own.
COMMON = {'limits', 'credentials'}


def settings_of(settings, required, optional=()):
    """The inventory's settings for one role instance, refusing anything unknown or missing.

    Worker defaults carry no fleet addresses and no optional provider
    credentials: everything host-specific comes from here.
    """
    if not isinstance(settings, dict):
        raise ValueError('role settings must be an object')
    unknown = set(settings) - set(required) - set(optional) - COMMON
    if unknown:
        raise ValueError('unknown role settings: %s' % sorted(unknown))
    missing = [name for name in required if settings.get(name) in (None, '', [])]
    if missing:
        raise ValueError('missing role settings: %s' % missing)
    return settings


def limits_of(settings):
    limits = settings.get('limits', {})
    if not isinstance(limits, dict):
        raise ValueError('limits must be an object')
    for key, value in limits.items():
        if key not in LIMITS or not isinstance(value, str) or not re.fullmatch(LIMITS[key], value):
            raise ValueError('limit %s=%r is not one of %s in its syntax' % (key, value, sorted(LIMITS)))
    return dict(limits)


def credentials_of(settings):
    """Credential file references: `EnvironmentFile=` paths the operator installed.

    The files' contents are never read or rendered; a missing file fails the
    start, as systemd does for an unprefixed EnvironmentFile.
    """
    credentials = settings.get('credentials', [])
    if not isinstance(credentials, list):
        raise ValueError('credentials must be a list of installed credential file paths')
    return [directory(path) for path in credentials]


def disjoint(writable, readonly):
    """Inputs are read-only and never inside or around a writable path."""
    for read in readonly:
        for write in writable:
            if PurePosixPath(read).is_relative_to(write) or PurePosixPath(write).is_relative_to(read):
                raise ValueError('input %s overlaps writable %s; keep inputs and state apart' % (read, write))


def worker(template, executable, arguments, *, configurations=None, writable=(), readonly=(),
           settings=None, check=()):
    """Render a worker unit: its command, sandbox, budgets and credential references.

    Inputs (publications, configuration, credentials) are read-only; state and
    caches are separately writable and created by an install.
    """
    settings = settings or {}
    limits = limits_of(settings)
    credentials = credentials_of(settings)
    writes = [directory(p) for p in writable]
    reads = [directory(p) for p in readonly] + credentials
    disjoint(writes, reads)
    if any(not isinstance(a, str) or any(c in a for c in ('%', '$')) for a in check):
        raise ValueError('check arguments must be plain strings')
    rendered = render(template, executable, arguments, configurations or {}, writes, reads)
    extra = dict(limits)
    if credentials:
        extra['EnvironmentFile'] = credentials
    unit = set_service(rendered.unit, extra) if extra else rendered.unit
    units.exec_start(units.effective([unit]))
    return RenderedRole(unit, rendered.files, tuple(writes), tuple(check))


def set_service(unit, settings):
    """Replace or add Service directives; a list value becomes repeated lines."""
    lines, section = [], None
    for line in unit.splitlines():
        if line.strip().startswith('['):
            section = line.strip()[1:-1]
        if section == 'Service' and line.strip().split('=', 1)[0] in settings:
            continue
        lines.append(line)
    at = lines.index('[Service]') + 1
    lines[at:at] = ['%s=%s' % (key, item) for key, value in settings.items()
                    for item in (value if isinstance(value, list) else [value])]
    return '\n'.join(lines) + '\n'


ADDRESS = re.compile(r'(?:[0-9.]+|\[[0-9a-fA-F:]+\]|[A-Za-z0-9.-]+):[0-9]{1,5}')


def address(value, name):
    """A `host:port` listener or origin authority, refused if anything else."""
    if not isinstance(value, str) or not ADDRESS.fullmatch(value):
        raise ValueError('%s must be host:port' % name)
    return value


def number(value, name, low=0):
    if type(value) is not int or value < low:
        raise ValueError('%s must be an integer of at least %d' % (name, low))
    return str(value)


def sizes(s, names):
    """`--name value` for each positive size setting present, in the given order."""
    arguments = []
    for name in names:
        if name in s:
            arguments += ['--' + name.replace('_', '-'), number(s[name], name, 1)]
    return arguments
