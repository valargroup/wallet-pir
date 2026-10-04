"""Read-only, bounded survey of retained owner records and live processes.

One host's answer to "does any retained production operation, or anything it
started, still run here?". The caller passes every input explicitly: the owner
namespaces, the bounds, the operational executable classes, the baseline
services allowed to run them, the lock path, the allowed lock holder and a
binding (nonce, request digest, host) that is echoed into the raw result. It
never writes, signals or follows a symlink. The module is stdlib-only and has
no package imports, so a source bootstrap can embed its exact text, as it does
`hostlock` and `schema_fence`.

A survey refuses (`reasons` non-empty) on any of:

- a bound overflow, unreadable record, symlink or special file in a namespace;
- a process it cannot read (root only; unprivileged fixture observers skip
  other users' processes);
- a production lock holder other than the allowed one;
- a process carrying the caller's launch marker or running its receiver;
- a live process associated with any retained record (`associate`);
- a live process of an operational executable class that is not identity
  bound to a baseline service (`operational`).

Association is a set of fixed rules, not proof that nothing survives: a
detached process that matches no rule and no operational class is invisible.

Arguments and environment values are inspected in memory only. A process
summary, and every refusal reason, keeps the command line's SHA-256 and byte
count, the executable path, identity, the matched operational class entries and
boolean flags; never an argument value or a marker value (only its SHA-256), so
credentials, URL credentials or inline script text in another process's
command line never reach retained evidence.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

VERSION = 1
KIND = 'owner-process-survey'
BOUNDS = {'entries': 100000, 'file_bytes': 16 << 20, 'json_bytes': 512 << 20, 'references': 100000,
          'listed': 64, 'selected': 10000, 'argv_bytes': 4096, 'hashed_executables': 16, 'hashed_bytes': 1 << 30}
# Clock granularity between kernel start times (btime is whole seconds) and file mtimes.
TOLERANCE_SECONDS = 2
PF_KTHREAD = 0x00200000
PID_KEY = re.compile('(?:^|_)pid$')
BOOT_ID = Path('/proc/sys/kernel/random/boot_id')


class Refused(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON key')
        value[key] = item
    return value


def number(item):
    return type(item) in (int, float) and item == item and item not in (float('inf'), float('-inf'))


def boot_id():
    return BOOT_ID.read_text().strip()


def booted_unix():
    for line in Path('/proc/stat').read_text().splitlines():
        if line.startswith('btime '):
            return int(line.split()[1])
    raise ValueError('kernel boot time unavailable')


def started_unix(start_ticks, booted=None):
    """Wall-clock start of a process from its kernel start ticks."""
    return (booted_unix() if booted is None else booted)+start_ticks/os.sysconf('SC_CLK_TCK')


def process(pid):
    """Kernel state, name, parent, group, session and start ticks; None when absent."""
    try:
        raw = Path('/proc/%d/stat' % pid).read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    head, tail = raw.rsplit(')', 1)
    fields = tail.split()
    return {'pid': pid, 'comm': head.split('(', 1)[1][:32], 'state': fields[0], 'ppid': int(fields[1]),
            'pgid': int(fields[2]), 'session': int(fields[3]), 'start_ticks': int(fields[19]),
            'kernel': bool(int(fields[6]) & PF_KTHREAD)}


def cgroup(pid):
    try:
        return Path('/proc/%d/cgroup' % pid).read_text().strip()[:512]
    except (FileNotFoundError, ProcessLookupError):
        return None


def ancestors(pid=None):
    found, pid = set(), pid or os.getpid()
    while pid > 1 and pid not in found:
        found.add(pid)
        current = process(pid)
        pid = current['ppid'] if current else 0
    return found


def scan(lock_path=None, marker=None, receiver=None, argv_bytes=BOUNDS['argv_bytes']):
    """Every live user process with executable, argv, marker, lock use and receiver role.

    `argv` and `token` are for in-memory decisions; `summary` never emits them.

    Kernel threads are never listed. Root reads every process; one it cannot
    read is listed unreadable. Unprivileged observers (fixtures only) skip
    other users' processes and processes they cannot read.
    """
    target = None
    if lock_path is not None:
        try:
            info = Path(lock_path).lstat()
            target = (info.st_dev, info.st_ino)
        except FileNotFoundError:
            pass
    privileged = os.geteuid() == 0
    found = []
    for entry in os.scandir('/proc'):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        current = process(pid)
        if current is None or current['state'] == 'Z' or current['kernel']:
            continue
        try:
            owner = os.stat('/proc/%d' % pid).st_uid
        except FileNotFoundError:
            continue
        if not privileged and owner != os.geteuid():
            continue
        current.update(uid=owner, token=None, holds=False, receiver=False, unreadable=False, cgroup=cgroup(pid),
                       exe=None, argv=[], command_sha256=None, command_bytes=None)
        try:
            with open('/proc/%d/cmdline' % pid, 'rb') as stream:
                raw = stream.read(argv_bytes+1)
            current['command_sha256'], current['command_bytes'] = hashlib.sha256(raw[:argv_bytes]).hexdigest(), len(raw)
            current['argv'] = [a.decode(errors='replace') for a in raw[:argv_bytes].split(b'\0') if a][:64]
            try:
                current['exe'] = os.readlink('/proc/%d/exe' % pid).removesuffix(' (deleted)')
            except FileNotFoundError:
                pass
            if marker is not None:
                for item in Path('/proc/%d/environ' % pid).read_bytes().split(b'\0'):
                    if item.startswith(marker.encode()+b'='):
                        current['token'] = item.split(b'=', 1)[1].decode(errors='replace')[:200]
            if receiver is not None:
                current['receiver'] = (receiver.decode() in current['argv'] and '--action' in current['argv'] and
                                       any(a in ('stage', 'reconcile') for a in current['argv']))
            if target is not None:
                for fd in os.listdir('/proc/%d/fd' % pid):
                    try:
                        held = os.stat('/proc/%d/fd/%s' % (pid, fd))
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    if (held.st_dev, held.st_ino) == target:
                        current['holds'] = True
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            if not privileged:
                continue
            current['unreadable'] = True
        found.append(current)
    return found


# The only process fields retained: identity, executable, digests, class and flags.
SUMMARY = ('pid', 'start_ticks', 'session', 'pgid', 'ppid', 'uid', 'comm', 'exe', 'command_sha256', 'command_bytes',
           'holds', 'receiver', 'unreadable', 'cgroup', 'association', 'record', 'key', 'unit', 'exe_sha256', 'class')


def summary(items, listed=BOUNDS['listed']):
    """Retained view of processes: never an argument or a marker value.

    `command_bytes` above the argv bound means the digest covers only the
    bounded prefix. A marker is kept as its SHA-256 only.
    """
    found = []
    for item in items[:listed]:
        kept = {k: item[k] for k in SUMMARY if k in item}
        if 'token' in item:
            kept['token_sha256'] = (hashlib.sha256(item['token'].encode()).hexdigest()
                                    if isinstance(item['token'], str) else None)
        found.append(kept)
    return found


def label(item):
    """A process for a refusal reason: PID and executable, or kernel name; never an argument."""
    return '%d %s' % (item['pid'], item.get('exe') or item.get('comm') or '?')


def recorded(record, name, mtime):
    """Every process a retained record names, with whatever identity it kept.

    Any integer `pid` or `*_pid` value counts. Start ticks (`start_ticks`, or
    `process_start` of the upload owner), boot ID and cgroup bind only a `pid`
    in the same object. The start window is that object's, else the record's
    `started_unix`/`started`; the record's mtime closes it.
    """
    found = []

    def window(value, default=None):
        return next((value[k] for k in ('started_unix', 'started') if number(value.get(k))), default)
    top = window(record) if isinstance(record, dict) else None
    stack = [record]
    while stack:
        value = stack.pop()
        if isinstance(value, list):
            stack.extend(value)
            continue
        if not isinstance(value, dict):
            continue
        start = window(value, top)
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                stack.append(item)
            elif type(item) is int and item > 0 and PID_KEY.search(key):
                own = key == 'pid'
                found.append({'record': name, 'key': key, 'pid': item, 'window': start, 'mtime': mtime,
                              'start_ticks': next((value[k] for k in ('start_ticks', 'process_start')
                                                   if own and type(value.get(k)) is int), None),
                              'boot_id': value.get('boot_id') if own and isinstance(value.get('boot_id'), str) else None,
                              'cgroup': value.get('cgroup') if own and isinstance(value.get('cgroup'), str) else None})
    return found


def owner_inventory(roots, tick=lambda: None, bounds=BOUNDS, select=None):
    """Every entry of the retained owner namespaces, bounded and complete.

    Every regular `.json` file is parsed, latest or not and of any kind, and
    yields its recorded processes; other files are listed. Symlinks, special
    files, unreadable or oversized records and any bound overflow are reasons
    to refuse, never skipped silently. `select(label, name, record, sha256)`
    may return a summary of a namespace-root record for the caller.
    """
    reasons, references, selected, listing = [], [], [], []
    counts = {'entries': 0, 'json_files': 0, 'json_bytes': 0}
    digests = {}

    def walk():
        for label, root in roots:
            root = Path(root)
            if root.is_symlink():
                reasons.append('owner namespace is a symlink: '+label)
                continue
            if not root.exists():
                listing.append([label, 'absent'])
                continue
            pending = [root]
            while pending:
                directory = pending.pop()
                try:
                    entries = sorted(os.scandir(directory), key=lambda e: e.name)
                except OSError as error:
                    reasons.append('owner namespace unreadable: %s %s' % (label, type(error).__name__))
                    continue
                for entry in entries:
                    tick()
                    counts['entries'] += 1
                    if counts['entries'] > bounds['entries']:
                        reasons.append('owner namespaces exceed %d entries' % bounds['entries'])
                        return
                    relative = label+'/'+os.path.relpath(entry.path, root)
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError as error:
                        reasons.append('owner entry unreadable: %s %s' % (relative, type(error).__name__))
                        continue
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(Path(entry.path))
                        listing.append([relative, 'directory'])
                        continue
                    if not stat.S_ISREG(info.st_mode):
                        reasons.append('owner namespace holds a link or special file: '+relative)
                        continue
                    if not entry.name.endswith('.json'):
                        listing.append([relative, 'file', info.st_size, info.st_mtime_ns])
                        continue
                    if info.st_size > bounds['file_bytes']:
                        reasons.append('owner record exceeds bound: '+relative)
                        continue
                    counts['json_files'] += 1
                    counts['json_bytes'] += info.st_size
                    if counts['json_bytes'] > bounds['json_bytes']:
                        reasons.append('owner records exceed %d bytes' % bounds['json_bytes'])
                        return
                    try:
                        fd = os.open(entry.path, os.O_RDONLY | os.O_NOFOLLOW)
                        with os.fdopen(fd, 'rb') as stream:
                            raw = stream.read(bounds['file_bytes']+1)
                        if len(raw) > bounds['file_bytes']:
                            raise ValueError('owner record exceeds bound')
                        record = json.loads(raw, object_pairs_hook=unique)
                    except (OSError, ValueError) as error:
                        reasons.append('owner record unreadable: %s %s' % (relative, type(error).__name__))
                        continue
                    sha = hashlib.sha256(raw).hexdigest()
                    digests[relative] = [len(raw), info.st_mtime_ns, sha]
                    listing.append([relative, 'json', len(raw), info.st_mtime_ns, sha])
                    references.extend(recorded(record, relative, info.st_mtime_ns/1e9))
                    if len(references) > bounds['references']:
                        reasons.append('owner records name more than %d processes' % bounds['references'])
                        return
                    if select is not None and directory == root:
                        item = select(label, entry.name, record, sha)
                        if item is not None:
                            selected.append(item)
                            if len(selected) > bounds['selected']:
                                reasons.append('selected owner records exceed %d' % bounds['selected'])
                                return
    walk()
    inventory = dict(counts, references=len(references), sha256=hashlib.sha256(canonical(listing)).hexdigest())
    return inventory, references, selected, reasons, digests


def unit_of(text, baseline):
    """The baseline unit whose systemd cgroup holds a process, if any."""
    for line in (text or '').splitlines():
        hierarchy, _, path = line.partition(':')[2].partition(':')
        if hierarchy not in ('', 'name=systemd'):
            continue
        for unit in baseline:
            prefix = '/system.slice/'+unit
            if path == prefix or path.startswith(prefix+'/'):
                return unit
    return None


def leaf_service(text):
    for line in (text or '').splitlines():
        hierarchy, _, path = line.partition(':')[2].partition(':')
        if hierarchy in ('', 'name=systemd'):
            return path if path.endswith('.service') else None
    return None


def associate(references, processes, excluded, observer_cgroup, booted, everyone=None):
    """Live processes a retained record may still own; read-only.

    - recorded process: the PID is live and matches the kept start ticks or,
      without them, started no later than the record was last written;
    - session or process group: when the recorded process is gone or still its
      owner, every live member of a session or group it led (Linux never reuses
      a PID while such a session or group exists);
    - cgroup: likewise, every live process in a kept cgroup other than the
      observer's own;
    - orphan window: a live process whose session or group leader is gone and
      that started within the record's start window and last write;
    - reparented window: a live process started within that window whose
      parent is PID 1, a `systemd` manager, unscanned or younger than itself, unless it is
      the oldest process of its own `.service` cgroup (a service's main
      process); this covers a detached child that made its own session;
    - descendants: every live descendant, by parent PID, of anything above.

    Records last written before this boot or naming another boot are skipped.
    `everyone` is every scanned process including excluded ones, for parent
    and leader checks; it defaults to `processes`.
    """
    current = boot_id()
    everyone = everyone if everyone is not None else processes
    byid = {p['pid']: p for p in everyone}
    live = {p['pid']: p for p in processes if p['pid'] not in excluded}
    present = set(byid)
    started = {pid: started_unix(p['start_ticks'], booted) for pid, p in byid.items()}
    groups, children, oldest = {}, {}, {}
    for p in live.values():
        for value in {p['session'], p['pgid']}:
            groups.setdefault(value, []).append(p)
        if p.get('cgroup') and p['cgroup'] != observer_cgroup:
            groups.setdefault(('cgroup', p['cgroup']), []).append(p)
    for p in byid.values():
        children.setdefault(p['ppid'], []).append(p['pid'])
        if p.get('cgroup') and (p['cgroup'] not in oldest or started[p['pid']] < started[oldest[p['cgroup']]]):
            oldest[p['cgroup']] = p['pid']
    # Kernel-spawned helpers have session and group 0: never an operation's orphan.
    orphans = [p for p in live.values() if p['session'] > 0 and p['pgid'] > 0 and
               (p['session'] not in present or p['pgid'] not in present)]

    def reparented(p):
        # As root every live user parent is scanned, so an unscanned parent is a
        # kernel thread (whose helpers have session 0) or, for an unprivileged
        # fixture observer, another user's process: counted as an adopter.
        parent = byid.get(p['ppid'])
        moved = (p['ppid'] == 1 or parent is None or parent.get('comm') == 'systemd' or
                 parent['start_ticks'] > p['start_ticks'])
        main = leaf_service(p.get('cgroup')) is not None and oldest.get(p['cgroup']) == p['pid']
        return p['session'] > 0 and moved and not main
    adopted = [p for p in live.values() if reparented(p)]
    found = {}

    def flag(item, reason, ref):
        found.setdefault(item['pid'], dict(item, association=reason, record=ref['record'], key=ref['key']))
    for ref in references:
        if ref['mtime'] < booted-TOLERANCE_SECONDS or ref['boot_id'] not in (None, current) or ref['pid'] in excluded:
            continue
        item = live.get(ref['pid'])
        owner = item is not None and (item['start_ticks'] == ref['start_ticks'] if ref['start_ticks'] is not None
                                      else started[item['pid']] <= ref['mtime']+TOLERANCE_SECONDS)
        if owner:
            flag(item, 'recorded-process', ref)
        if owner or ref['pid'] not in present:
            for member in groups.get(ref['pid'], []):
                flag(member, 'recorded-session-or-group', ref)
            if ref['cgroup'] and ref['cgroup'] != observer_cgroup:
                for member in groups.get(('cgroup', ref['cgroup']), []):
                    flag(member, 'recorded-cgroup', ref)
        if ref['window'] is not None:
            low, high = ref['window']-TOLERANCE_SECONDS, ref['mtime']+TOLERANCE_SECONDS
            for member in orphans:
                if low <= started[member['pid']] <= high:
                    flag(member, 'orphan-in-recorded-window', ref)
            for member in adopted:
                if low <= started[member['pid']] <= high:
                    flag(member, 'reparented-in-recorded-window', ref)
    pending = list(found)
    while pending:
        parent = found[pending.pop()]
        for pid in children.get(parent['pid'], []):
            if pid in live and pid not in found:
                found[pid] = dict(live[pid], association='descendant-of-associated', record=parent['record'],
                                  key=parent['key'])
                pending.append(pid)
    return sorted(found.values(), key=lambda p: p['pid'])


def matched(item, classes):
    """The closed class entries a process matches by executable or argv basename, or root prefix.

    Only entries of `classes` are returned, never the argument that matched.
    """
    paths = [x for x in (item.get('exe'), *item.get('argv', [])) if x]
    found = {os.path.basename(x) for x in paths if os.path.basename(x) in classes['names']}
    found |= {root for x in paths for root in classes['roots'] if x.startswith(root)}
    return sorted(found)


def matches(item, classes):
    """True for a process of a closed operational class."""
    return bool(matched(item, classes))


def operational(processes, excluded, classes, baseline, bounds=BOUNDS):
    """Operational-class processes: bound to a baseline service, or unattributed.

    A match is bound only when systemd placed it in the cgroup of an exact
    baseline unit; the binding records the unit, PID, start ticks, executable
    and the executable's SHA-256 read through /proc. Any other match is
    unattributed and refuses, whatever its session, parent or marker.
    """
    bound, unattributed, reasons, hashed = [], [], [], {}
    budget = bounds['hashed_bytes']
    for item in processes:
        found = matched(item, classes) if item['pid'] not in excluded else []
        if not found:
            continue
        item = dict(item, **{'class': found})
        unit = unit_of(item.get('cgroup'), baseline)
        if unit is None:
            unattributed.append(item)
            continue
        key = item.get('exe')
        if key not in hashed:
            if len(hashed) >= bounds['hashed_executables']:
                reasons.append('baseline executables exceed %d' % bounds['hashed_executables'])
                break
            sha = hashlib.sha256()
            try:
                with open('/proc/%d/exe' % item['pid'], 'rb') as stream:
                    while True:
                        data = stream.read(8 << 20)
                        if not data:
                            break
                        budget -= len(data)
                        if budget < 0:
                            raise Refused('baseline executable hashing exceeds %d bytes' % bounds['hashed_bytes'])
                        sha.update(data)
                hashed[key] = sha.hexdigest()
            except (OSError, Refused) as error:
                reasons.append('baseline executable unreadable: %d %s' % (item['pid'], str(error)[:120]))
                hashed[key] = None
        bound.append(dict(item, unit=unit, exe_sha256=hashed[key]))
    return bound, unattributed, reasons


def observe(namespaces, *, classes, baseline, binding, lock_path=None, holder=None, marker=None, receiver=None,
            excluded=None, observer_cgroup=None, tick=lambda: None, bounds=BOUNDS, select=None, refuse=None):
    """One complete read-only survey of this host; the raw dict is the evidence.

    `namespaces` is [(label, path)], `classes` {'names': [...], 'roots': [...]},
    `baseline` a list of exact systemd unit names, `binding` any JSON value the
    caller binds (nonce, request digest, host). `holder` is the only process
    allowed to hold the lock, or None for none. `refuse(item)` returns a reason
    or None for every record `select` chose, before the listed display is cut
    to `bounds['listed']`; more than `bounds['selected']` refuses outright.
    """
    listed = bounds['listed']
    inventory, references, selected, reasons, digests = owner_inventory(namespaces, tick, bounds, select)
    # Decisions use every selected record; only the display below is compacted.
    for item in selected if refuse is not None else ():
        reason = refuse(item)
        if reason:
            reasons.append(reason)
    processes = scan(lock_path, marker, receiver, bounds['argv_bytes'])
    tick()
    excluded = set(ancestors() if excluded is None else excluded)
    holders = [p for p in processes if p['holds']]
    allowed = {holder['pid']} if holder else set()
    if any(p['pid'] not in allowed for p in holders) or holder and holder['pid'] not in {p['pid'] for p in holders}:
        reasons.append('production lock holder differs from the expected owner')
    marked = [p for p in processes if p['pid'] not in excluded and (p['token'] or p['receiver'] or p['unreadable'])]
    if marked:
        reasons.append('live, unreadable or foreign candidate execution process')
    associated = associate(references, processes, excluded,
                           cgroup(os.getpid()) if observer_cgroup is None else observer_cgroup, booted_unix())
    tick()
    if associated:
        reasons.append('live process associated with a retained owner record: '+', '.join(
            '%d %s %s' % (p['pid'], p['association'], p['record']) for p in associated[:4]))
    bound, unattributed, problems = operational(processes, excluded, classes, baseline, bounds)
    reasons.extend(problems)
    if unattributed:
        reasons.append('live operational process not bound to a baseline service: '+', '.join(
            label(p) for p in unattributed[:4]))
    records = sorted({p['record'] for p in associated if p['record'] in digests})
    return {'version': VERSION, 'kind': KIND, 'binding': binding, 'bounds': bounds,
            'classes': classes, 'classes_sha256': hashlib.sha256(canonical(classes)).hexdigest(),
            'baseline': list(baseline), 'boot_id': boot_id(), 'booted_unix': booted_unix(), 'euid': os.geteuid(),
            'namespaces': [[label, str(path)] for label, path in namespaces], 'inventory': inventory,
            'selected': selected[-listed:], 'selected_count': len(selected),
            'selected_sha256': hashlib.sha256(canonical(selected)).hexdigest(), 'scanned': len(processes),
            'lock': {'path': str(lock_path) if lock_path is not None else None, 'holders': summary(holders, listed)},
            'processes': summary(marked, listed),
            'associated': summary(associated, listed), 'associated_count': len(associated),
            'associated_records': {name: digests[name] for name in records[:listed]},
            'baseline_bound': summary(bound, listed), 'baseline_bound_count': len(bound),
            'unattributed': summary(unattributed, listed), 'unattributed_count': len(unattributed),
            'blocked': [reason[:300] for reason in reasons[:listed]], 'blocked_count': len(reasons)}
