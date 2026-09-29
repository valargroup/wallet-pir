"""The transparent shard worker's systemd unit: rewriting a serving unit.

New workers are never rendered from a template alone. Their unit is a serving
peer's unit with this worker's identity, publication and budgets substituted,
so every flag nobody meant to change stays exactly the running fleet's.
"""
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


def rewrite_exec(unit, values, append=False):
    """(unit, flags found): `unit` with each `--flag` in `values` set to its
    value, in either CLI spelling. A flag the unit lacks is appended only with
    `append`; the caller decides whether a missing flag is an error."""
    args, out, found, index = exec_args(unit), [], set(), 0
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
    line = 'ExecStart=' + shlex.join(out)
    lines = [line if existing.startswith('ExecStart=') else existing for existing in unit.splitlines()]
    return '\n'.join(lines) + '\n', found


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


def unit_directories(unit):
    """Directories the worker expects to exist before its first start."""
    for line in unit.splitlines():
        if line.startswith('ExecStart='):
            args = shlex.split(line[len('ExecStart='):])
            for flag in ('--runtime-cache-dir', '--active-record'):
                if flag in args:
                    value = args[args.index(flag) + 1]
                    yield value if flag == '--runtime-cache-dir' else str(Path(value).parent)
