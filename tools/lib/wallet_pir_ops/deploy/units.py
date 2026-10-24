"""Systemd unit text: parsing, effective configuration and rendering.

A unit's effective configuration is its fragment followed by its drop-ins in
the order systemd applies them. Comparisons use that configuration, not file
bytes, so a comment or a setting moved between files is not drift, while a
drop-in that changes `MemoryMax` is.
"""
import fnmatch
import os
import re
import shlex

# Sorts after every drop-in the manual rollouts stacked (the longest seen on
# 2026-09-30 began with 17 z's), so its ExecStart is the effective one. A plan
# refuses a unit with an ExecStart drop-in that would sort after it.
MANAGED_DROP_IN = 'z' * 32 + '-wallet-pir-release.conf'
BINARY = '@BINARY@'
MANAGED_HEADER = ('# Managed by tools/scripts/wallet-pir-deploy.py; a deploy or rollback replaces\n'
                  '# this file. Record any other change in its own drop-in.\n')

# Settings that accumulate across assignments, one command or file per line.
LINE_LISTS = frozenset({
    'ExecStart', 'ExecStartPre', 'ExecStartPost', 'ExecStop', 'ExecStopPost', 'ExecReload',
    'EnvironmentFile', 'LoadCredential', 'LoadCredentialEncrypted', 'SetCredential',
})
# Settings that accumulate, several space-separated words per assignment.
WORD_LISTS = frozenset({
    'After', 'Before', 'Wants', 'Requires', 'Requisite', 'BindsTo', 'PartOf', 'Conflicts',
    'WantedBy', 'RequiredBy', 'Also', 'ReadWritePaths', 'ReadOnlyPaths', 'InaccessiblePaths',
    'BindPaths', 'BindReadOnlyPaths', 'SupplementaryGroups', 'DeviceAllow',
})
EXEC_PREFIX = re.compile(r'^[-@:+!|]*')
PLACEHOLDER = re.compile(r'@[A-Z][A-Z0-9_]*@')


class UnitError(ValueError):
    pass


def entries(text):
    """`(section, key, value)` in file order; comments dropped, continuations joined."""
    section, result, pending = None, [], None
    lines = text.splitlines()
    for raw in lines + ([''] if lines and lines[-1].rstrip().endswith('\\') else []):
        line = raw.strip()
        if line[:1] in ('#', ';'):
            continue
        if pending is not None:
            line = (pending + ' ' + line).strip()
            pending = None
        if line.endswith('\\'):
            pending = line[:-1].rstrip()
            continue
        if not line:
            continue
        if line.startswith('[') and line.endswith(']'):
            section = line[1:-1]
            continue
        key, separator, value = line.partition('=')
        if not separator or section is None:
            raise UnitError('unparseable unit line: %r' % raw)
        result.append((section, key.strip(), value.strip()))
    return result


def effective(texts):
    """The configuration systemd derives from files applied in order.

    An empty assignment resets a list (the `ExecStart=` idiom); otherwise
    lists accumulate and every other setting is last-assignment-wins.
    `Environment` is a map, so reassigning one variable replaces only it.
    """
    config = {}
    for text in texts:
        for section, key, value in entries(text):
            values = config.setdefault(section, {})
            if key == 'Environment':
                environment = values.setdefault(key, {})
                if not value:
                    environment.clear()
                for word in shlex.split(value):
                    name, _, setting = word.partition('=')
                    environment[name] = setting
            elif key in LINE_LISTS or key in WORD_LISTS:
                items = values.setdefault(key, [])
                if not value:
                    items.clear()
                elif key in WORD_LISTS:
                    items.extend(shlex.split(value))
                else:
                    items.append(value)
            else:
                values[key] = value
    return config


def exec_start(config):
    """The single effective `ExecStart` line; a service with several is refused."""
    lines = config.get('Service', {}).get('ExecStart', [])
    if len(lines) != 1:
        raise UnitError('expected exactly one effective ExecStart, found %d' % len(lines))
    return lines[0]


def split_exec(line, launcher=()):
    """Return systemd/launcher prefixes, the service executable and its unchanged arguments."""
    prefix = EXEC_PREFIX.match(line).group(0)
    command = line[len(prefix):].strip()
    if launcher:
        matched = re.match(r'\s+'.join(re.escape(path) for path in launcher) + r'\s+', command)
        if not matched:
            raise UnitError('ExecStart does not use the pinned credential launcher')
        prefix += matched[0]
        command = command[matched.end():]
    binary, _, tail = command.partition(' ')
    if not binary.startswith('/'):
        raise UnitError('ExecStart binary must be an absolute path: %r' % line)
    return prefix, binary, tail.strip()


def uses_launcher(line, launcher):
    """Whether an ExecStart line runs through exactly these launcher paths."""
    try:
        split_exec(line, launcher)
    except UnitError:
        return False
    return bool(launcher)


def normalized(config, launcher=()):
    """Comparable form: empty settings dropped, whitespace collapsed, the ExecStart binary abstracted.

    The binary is compared separately, by the digest of what runs. A start
    check running that same binary (`ExecStartPre`) is abstracted with it.
    """
    result = {}
    started = [split_exec(item, launcher)[1] for item in config.get('Service', {}).get('ExecStart', [])]
    for section, values in config.items():
        for key, value in values.items():
            if isinstance(value, dict):
                value = dict(sorted(value.items())) or None
            elif isinstance(value, list):
                value = [' '.join(item.split()) for item in value] or None
                if value and (key == 'ExecStart' or key == 'ExecStartPre' and started
                              and all(split_exec(item)[1] == started[-1] for item in value)):
                    value = [' '.join(filter(None, (split_exec(item, launcher)[0] + BINARY, split_exec(item, launcher)[2])))
                             for item in value]
            else:
                value = ' '.join(value.split()) or None
            if value is not None:
                result.setdefault(section, {})[key] = value
    return result


def differences(current, desired):
    """Human-readable `Section.Key: current -> desired` lines for two normalized configurations."""
    lines = []
    for section in sorted(set(current) | set(desired)):
        left, right = current.get(section, {}), desired.get(section, {})
        for key in sorted(set(left) | set(right)):
            if left.get(key) != right.get(key):
                lines.append('%s.%s: %r -> %r' % (section, key, left.get(key), right.get(key)))
    return lines


def sets_exec_start(text):
    return any(section == 'Service' and key == 'ExecStart' for section, key, _ in entries(text))


def exec_start_only(text):
    """Whether a drop-in does nothing but (re)set ExecStart, so retiring it changes nothing else."""
    found = entries(text)
    return bool(found) and all(section == 'Service' and key == 'ExecStart' for section, key, _ in found)


def command_and_write_paths_only(text):
    """Historical launch overlays may also grant their old state directory.

    A complete rendered role replaces both directives. Never discard credentials,
    resource limits or other service settings through this classification.
    """
    found = entries(text)
    return (sets_exec_start(text) and all(section == 'Service' and key in ('ExecStart', 'ReadWritePaths')
                                         for section, key, _ in found))


def adoptable(path, patterns):
    return any(fnmatch.fnmatchcase(os.path.basename(path), pattern) for pattern in patterns)


def managed_drop_in(prefix, binary, tail, previous=None):
    command = prefix + binary + (' ' + tail if tail else '')
    preserved = ''
    for section, key, value in entries(previous or ''):
        if (section, key) != ('Service', 'ExecStart'):
            preserved += '[%s]\n%s=%s\n' % (section, key, value)
    return MANAGED_HEADER + preserved + '[Service]\nExecStart=\nExecStart=' + command + '\n'


def render(template, values):
    """Substitute `@NAME@` placeholders; a placeholder left unset is an error, not an empty string."""
    for name, value in values.items():
        template = template.replace('@%s@' % name, value)
    left = sorted(set(PLACEHOLDER.findall(template)))
    if left:
        raise UnitError('unit template has unset placeholders: ' + ', '.join(left))
    return template
