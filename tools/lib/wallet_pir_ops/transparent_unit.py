"""The transparent shard worker's systemd unit: rewriting a serving unit.

New workers are never rendered from a template alone. Their unit is a serving
peer's unit with this worker's identity, publication and budgets substituted,
so every flag nobody meant to change stays exactly the running fleet's.

The worker runs `pir-query-server --config /etc/transparent-pir/shard-server.json`,
a generated `transparent_history` section whose settings are the retired
`transparent-query-server` executable's flags (`--name` -> `name`). A host
installed before that change still runs the retired executable with flags.
Both are read as the same flag list (`server_flags`), rewritten as flags, and
written in whichever form the installed release's executable takes
(`worker_exec`), so a rollback to a retired release keeps its own unit form.
"""
import json
from pathlib import Path
import shlex

# The unit's soft memory limit per role. Recent replicas reclaim file cache
# before transient admission reaches their 7 GiB hard limit (measured with a
# 5.5 GiB threshold); archive owners otherwise retain ~10 GiB of file cache on
# top of their ~46 GiB anonymous working set, exceeding the cgroup headroom gate.
MEMORY_HIGH = {'recent-replica': 5905580032, 'archive-owner': 51539607552}


def exec_args(unit):
    """The ExecStart argument vector of `unit`, or ValueError."""
    lines = [line for line in unit.splitlines() if line.startswith('ExecStart=')]
    if len(lines) != 1:
        raise ValueError(f'unit has {len(lines)} ExecStart lines, expected exactly 1')
    return shlex.split(lines[0][len('ExecStart='):])


# The serving executable and its generated configuration; the management
# listener is loopback and distinct from every other process on the host.
SERVER = '/usr/local/bin/pir-query-server'
CONFIG = '/etc/transparent-pir/shard-server.json'
MANAGEMENT = '127.0.0.1:18190'
SERVER_NAME = 'pir-query-server'
# Executables of releases before pir-query-server; they take flags.
RETIRED_NAMES = ('transparent-query-server', 'transparent-shard-server')
_SWITCHES = {'full_holdings', 'prune_excess', 'pilot_cold'}
_INTEGERS = {'bootstrap_max_bytes', 'cache_bytes', 'runtime_cache_max_bytes', 'runtime_restore_slots',
             'build_slots', 'query_slots', 'retain_revisions', 'query_waiters', 'body_bytes',
             'upload_deadline_secs', 'query_deadline_secs', 'retain_bytes'}
_REPEATED = {'runtime_cache_prune_set'}
_STRINGS = {'publisher', 'archive_upstream', 'archive_ca', 'publisher_network', 'publisher_genesis',
            'publisher_ca', 'listen', 'shard_dir', 'runtime_cache_dir', 'assignment', 'worker_id',
            'control_socket', 'active_record'}


def settings_from_flags(flags):
    """The `transparent_history` settings of the retired executable's `flags`.

    Switches become true, numbers integers and the repeated prune set an
    array; `--verify-only` and unknown flags are refused (a serving
    configuration never carries `verify_only`)."""
    settings, index = {}, 0
    while index < len(flags):
        flag = flags[index]
        if not flag.startswith('--'):
            raise ValueError(f'unexpected server argument {flag!r}')
        flag, has_value, value = flag.partition('=')
        name = flag[2:].replace('-', '_')
        if name in _SWITCHES and not has_value:
            if name in settings:
                raise ValueError(f'server arguments repeat {flag}')
            settings[name] = True
            index += 1
            continue
        if name not in _INTEGERS | _REPEATED | _STRINGS:
            raise ValueError(f'unknown server argument {flag}')
        if not has_value:
            if index + 1 >= len(flags):
                raise ValueError(f'server arguments have an incomplete {flag}')
            value = flags[index + 1]
            index += 1
        index += 1
        if name in _REPEATED:
            settings.setdefault(name, []).append(value)
            continue
        if name in settings:
            raise ValueError(f'server arguments repeat {flag}')
        if name in _INTEGERS:
            if not value.isdigit():
                raise ValueError(f'{flag} must be an integer, not {value!r}')
            settings[name] = int(value)
        else:
            settings[name] = value
    return settings


def flags_from_settings(settings):
    """The retired executable's flags for `transparent_history` `settings`."""
    flags = []
    for name, value in settings.items():
        flag = '--' + name.replace('_', '-')
        if name in _SWITCHES and value is True:
            flags.append(flag)
        elif name in _REPEATED and isinstance(value, list):
            for item in value:
                flags += [flag, str(item)]
        elif name in _INTEGERS and type(value) is int or name in _STRINGS and isinstance(value, str):
            flags += [flag, str(value)]
        elif not (name in _SWITCHES and value is False):
            raise ValueError(f'unsupported shard server setting {name}={value!r}')
    return flags


def config_text(flags):
    """The generated configuration for the retired executable's `flags`."""
    document = {'management_listen': MANAGEMENT,
                'transparent_history': {'enabled': True, 'settings': settings_from_flags(flags)}}
    return json.dumps(document, indent=2) + '\n'


def configured(unit):
    """Whether `unit` runs pir-query-server over the generated configuration."""
    return exec_args(unit) == [SERVER, '--config', CONFIG]


def server_flags(unit, config=None):
    """The server's settings as flags: from the generated configuration (its
    text, `config`) when `unit` runs pir-query-server, else the unit's own."""
    if configured(unit):
        if config is None:
            raise ValueError(f'the unit runs {CONFIG}, which was not read')
        document = json.loads(config)
        if (not isinstance(document, dict) or set(document) != {'management_listen', 'transparent_history'}
                or document['transparent_history'].get('enabled') is not True):
            raise ValueError(f'{CONFIG} must enable only transparent_history')
        return flags_from_settings(document['transparent_history'].get('settings') or {})
    return exec_args(unit)[1:]


def worker_exec(unit, flags, executable=SERVER):
    """(unit, configuration text or None): `unit` running `executable` with
    `flags`. pir-query-server takes them as its generated configuration; a
    retired executable takes them on its command line, as before."""
    if Path(executable).name == SERVER_NAME:
        return with_exec(unit, [executable, '--config', CONFIG]), config_text(flags)
    if Path(executable).name not in RETIRED_NAMES:
        raise ValueError(f'{executable} is not a Transparent history server')
    return with_exec(unit, [executable, *flags]), None


def with_exec(unit, argv):
    """`unit` with its one ExecStart set to `argv`."""
    exec_args(unit)
    line = 'ExecStart=' + shlex.join(argv)
    return '\n'.join(line if existing.startswith('ExecStart=') else existing for existing in unit.splitlines()) + '\n'


def rewrite_flags(args, values, append=False):
    """(args, flags found): `args` with each `--flag` in `values` set to its
    value, in either CLI spelling. A flag `args` lacks is appended only with
    `append`; the caller decides whether a missing flag is an error."""
    args, out, found, index = list(args), [], set(), 0
    while index < len(args):
        arg = args[index]
        flag = arg.split('=', 1)[0]
        if flag in values and flag.startswith('--'):
            if flag in found:
                raise ValueError(f'unit repeats {flag}')
            found.add(flag)
            if '=' in arg:
                out.append(flag + '=' + str(values[flag]))
                index += 1
            else:
                if index + 1 >= len(args):
                    raise ValueError(f'unit has an incomplete {flag}')
                out += [flag, str(values[flag])]
                index += 2
            continue
        out.append(arg)
        index += 1
    if append:
        for flag, value in values.items():
            if flag not in found:
                out += [flag, str(value)]
    return out, found


def rewrite_exec(unit, values, append=False):
    """(unit, flags found): `unit` with each `--flag` in `values` set to its
    value, in either CLI spelling. A flag the unit lacks is appended only with
    `append`; the caller decides whether a missing flag is an error."""
    out, found = rewrite_flags(exec_args(unit), values, append)
    return with_exec(unit, out), found


def set_service(unit, directives):
    """`unit` with each `[Service]` directive in `directives` set exactly once."""
    lines = [line for line in unit.splitlines() if line.split('=', 1)[0] not in directives]
    if '[Service]' not in lines:
        raise ValueError('unit has no [Service] section')
    at = lines.index('[Service]') + 1
    lines[at:at] = [f'{key}={value}' for key, value in directives.items()]
    return '\n'.join(lines) + '\n'


def set_memory_high(unit, role):
    return set_service(unit, {'MemoryHigh': MEMORY_HIGH[role]})


def unit_directories(unit, config=None):
    """Directories the worker expects to exist before its first start; a unit
    running pir-query-server names them in its configuration text `config`."""
    args = server_flags(unit, config)
    for flag in ('--runtime-cache-dir', '--active-record'):
        if flag in args:
            value = args[args.index(flag) + 1]
            yield value if flag == '--runtime-cache-dir' else str(Path(value).parent)
