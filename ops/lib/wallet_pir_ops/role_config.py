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

    def describe(self):
        return {'unit': self.unit, 'files': self.files}


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
