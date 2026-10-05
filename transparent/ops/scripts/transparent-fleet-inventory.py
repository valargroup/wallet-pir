#!/usr/bin/env python3
"""Durable fleet intent for transparent PIR: who should serve, not who does.

The inventory (`state/inventory.json`) is written only here, under one lock,
by compare-and-swap on its revision. Every write keeps the revision on disk,
appends to a log and regenerates the roster the fleet adapter and planner
read, so the two can never disagree. See transparent/docs/elastic-recent.md.
"""
import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request

SCHEMA = 'transparent-fleet-inventory-v1'
INTENTS = ('enrolled', 'draining', 'retired', 'quarantined')
ROUTABLE = ('enrolled', 'draining')
ROLES = ('recent-replica', 'archive-owner')
RECENT_GROUP = 'recent'
# Facts a member keeps for its whole life; changing a host means a new member.
IMMUTABLE = ('role', 'group', 'origin', 'ssh_host', 'upstream', 'cache_bytes', 'memory_max')
TRANSITIONS = {
    ('enrolled', 'draining'), ('draining', 'enrolled'), ('draining', 'retired'),
    ('enrolled', 'quarantined'), ('draining', 'quarantined'), ('quarantined', 'retired'),
}
DRAINED_SECONDS = 120
MIN_OTHER_SERVING = 2
NAME = re.compile(r'[A-Za-z0-9_.:-]+')


class InventoryError(ValueError):
    pass


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, indent=1, sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def validate(inv):
    """Raise InventoryError unless `inv` is a complete, consistent inventory."""
    def fail(message):
        raise InventoryError(message)
    if not isinstance(inv, dict) or inv.get('schema') != SCHEMA:
        fail('not a ' + SCHEMA + ' inventory')
    if type(inv.get('revision')) is not int or inv['revision'] < 1:
        fail('revision must be a positive integer')
    ranges = inv.get('partition', {}).get('ranges')
    if not isinstance(ranges, list):
        fail('partition.ranges must be a list')
    def well_formed(r, seen):
        return (isinstance(r, dict) and isinstance(r.get('id'), str) and r['id'] not in seen
                and r['id'] != RECENT_GROUP and type(r.get('first')) is int and type(r.get('last')) is int)
    next_shard, range_ids = 0, set()
    for r in ranges:
        if not well_formed(r, range_ids):
            fail('invalid archive range')
        if r['first'] != next_shard or r['last'] < r['first']:
            fail(f"archive range {r['id']} must start at shard {next_shard}")
        next_shard = r['last'] + 1
        range_ids.add(r['id'])
    # Ranges a repartition replaced. Their ids stay reserved, so a retired
    # owner's group keeps meaning the shards it held.
    history = inv.get('partition_history', [])
    if not isinstance(history, list):
        fail('partition_history must be a list')
    history_ids = set()
    for r in history:
        if not well_formed(r, range_ids | history_ids) or not 0 <= r['first'] <= r['last']:
            fail('invalid or reused archive range in partition_history')
        history_ids.add(r['id'])
    members = inv.get('members')
    if not isinstance(members, list) or not members:
        fail('members must be a non-empty list')
    ids, live_hosts, live_upstreams = set(), set(), set()
    owners = {r: 0 for r in range_ids}
    recent = 0
    for m in members:
        if not isinstance(m, dict):
            fail('member must be an object')
        for field in ('id', 'ssh_host', 'upstream'):
            if not isinstance(m.get(field), str) or not NAME.fullmatch(m[field]):
                fail(f'member {m.get("id")!r} has an invalid {field}')
        if m['id'] in ids:
            fail(f"member {m['id']} appears twice")
        ids.add(m['id'])
        if m.get('role') not in ROLES or m.get('intent') not in INTENTS or m.get('origin') not in ('static', 'elastic'):
            fail(f"member {m['id']} has an invalid role, intent or origin")
        if type(m.get('cache_bytes')) is not int or m['cache_bytes'] <= 0:
            fail(f"member {m['id']} needs a positive cache_bytes")
        if m['role'] == 'recent-replica':
            if m.get('group') != RECENT_GROUP:
                fail(f"recent replica {m['id']} must be in group {RECENT_GROUP}")
        else:
            # Only a retired owner may name a range a repartition replaced.
            if m.get('group') not in range_ids and not (m['intent'] == 'retired' and m.get('group') in history_ids):
                fail(f"archive owner {m['id']} names no current archive range")
            if m.get('origin') != 'static':
                fail(f"archive owner {m['id']} must be static")
        if m['origin'] == 'elastic' and m['role'] != 'recent-replica':
            fail('only recent replicas can be elastic')
        if m['intent'] in ROUTABLE:
            if m['ssh_host'] in live_hosts or m['upstream'] in live_upstreams:
                fail(f"member {m['id']} shares a host with another live member")
            live_hosts.add(m['ssh_host'])
            live_upstreams.add(m['upstream'])
            if m['role'] == 'archive-owner':
                owners[m['group']] += 1
            else:
                recent += 1
    for range_id, count in owners.items():
        if count != 1:
            fail(f'archive range {range_id} has {count} live owners; it needs exactly one')
    if recent < 1:
        fail('the inventory has no live recent replica')


def check_transition(old, new, archive=False, restore=False):
    """Refuse changes a writer may not make: lost ids, rewritten host facts,
    skipped lifecycle steps, and archive changes without `archive`. `restore`
    lets archive owners a repartition retired come back, and nothing else."""
    if old is None:
        return
    before = {m['id']: m for m in old['members']}
    after = {m['id']: m for m in new['members']}
    if set(before) - set(after):
        raise InventoryError('member ids are never removed; retire them instead')
    if new['partition'] != old['partition'] and not archive:
        raise InventoryError('the archive partition changes only through an operator archive command')
    for mid, m in after.items():
        prior = before.get(mid)
        if prior is None:
            if m['intent'] != 'enrolled':
                raise InventoryError(f'new member {mid} must be enrolled')
            if m['role'] == 'archive-owner' and not archive:
                raise InventoryError('archive owners are added only through an operator archive command')
            continue
        for field in IMMUTABLE:
            if prior.get(field) != m.get(field):
                raise InventoryError(f'{field} of {mid} never changes; enroll a new member instead')
        if prior['intent'] != m['intent']:
            if prior['intent'] == 'retired' and not (restore and archive and m['role'] == 'archive-owner'):
                raise InventoryError(f'{mid} is retired; ids are never reused')
            if (prior['intent'], m['intent']) not in TRANSITIONS and not archive:
                raise InventoryError(f"{mid} cannot go from {prior['intent']} to {m['intent']}")
            if m['role'] == 'archive-owner' and not archive:
                raise InventoryError('archive owners change only through an operator archive command')


def roster_view(inv):
    """The roster the fleet adapter and planner read: every routable member."""
    ranges = {r['id']: [r['first'], r['last']] for r in inv['partition']['ranges']}
    roster = []
    for m in inv['members']:
        if m['intent'] not in ROUTABLE:
            continue
        entry = {k: m[k] for k in ('id', 'role', 'ssh_host', 'upstream', 'cache_bytes')}
        entry['intent'] = m['intent']
        entry['origin'] = m['origin']
        for optional in ('memory_max', 'build_slots'):
            if optional in m:
                entry[optional] = m[optional]
        if m['role'] == 'recent-replica':
            entry['replica_group'] = RECENT_GROUP
        else:
            entry['archive_range'] = ranges[m['group']]
        roster.append(entry)
    return roster


def known_hosts_lines(inv):
    """Pinned keys of live members that carry one: every elastic replica and
    any archive owner a repartition enrolled. Other static hosts' keys are
    managed outside the inventory."""
    return [m['ssh_host'] + ' ' + m['ssh_host_key'] for m in inv['members']
            if m['intent'] in ROUTABLE and m.get('ssh_host_key')]


def managed_key_hosts(inv):
    return {m['ssh_host'] for m in inv['members'] if m['origin'] == 'elastic' or m.get('ssh_host_key')}


class Inventory:
    def __init__(self, state_dir, roster_path=None, known_hosts=None):
        self.root = Path(state_dir)
        self.path = self.root/'inventory.json'
        self.roster_path = Path(roster_path) if roster_path else None
        self.known_hosts = Path(known_hosts) if known_hosts else None

    def load(self):
        value = json.loads(self.path.read_text())
        validate(value)
        return value

    def last_good(self):
        """The newest revision that validates; readers never act on a bad file."""
        try:
            return self.load()
        except (OSError, ValueError):
            pass
        for path in sorted((self.root/'inventory.d').glob('*.json'), key=lambda p: int(p.stem), reverse=True):
            try:
                value = json.loads(path.read_text())
                validate(value)
                return value
            except (OSError, ValueError):
                continue
        raise InventoryError('no valid inventory revision')

    def write(self, expected_revision, mutate, updated_by, archive=False, restore=False):
        """Apply `mutate` to a copy of the current inventory under the lock.

        `expected_revision` is the revision the caller read (0 to create);
        a concurrent writer makes this fail rather than silently merge.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root/'inventory.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            current = self.load() if self.path.exists() else None
            revision = current['revision'] if current else 0
            if revision != expected_revision:
                raise InventoryError(f'inventory is at revision {revision}, not {expected_revision}; re-read and retry')
            new = mutate(copy.deepcopy(current))
            new.update(schema=SCHEMA, revision=revision + 1, updated_by=updated_by, updated_unix=time.time())
            validate(new)
            check_transition(current, new, archive, restore)
            atomic_json(self.root/'inventory.d'/f'{new["revision"]}.json', new)
            atomic_json(self.path, new)
            with (self.root/'inventory.log.jsonl').open('a') as log:
                log.write(json.dumps({'revision': new['revision'], 'updated_by': updated_by,
                                      'updated_unix': new['updated_unix'], 'summary': summarize(current, new)}) + '\n')
            self.project(new)
            return new

    def project(self, inv):
        """Regenerate the roster and elastic known-hosts from `inv`."""
        if self.roster_path:
            atomic_json(self.roster_path, roster_view(inv))
        if self.known_hosts and self.known_hosts.exists():
            managed = managed_key_hosts(inv)
            kept = [line for line in self.known_hosts.read_text().splitlines()
                    if line.strip() and line.split()[0] not in managed]
            text = '\n'.join(kept + known_hosts_lines(inv)) + '\n'
            tmp = self.known_hosts.with_name(self.known_hosts.name + '.tmp')
            tmp.write_text(text)
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.known_hosts)


def summarize(old, new):
    before = {m['id']: m['intent'] for m in old['members']} if old else {}
    return {m['id']: [before.get(m['id']), m['intent']] for m in new['members']
            if before.get(m['id']) != m['intent']}


def seed(roster, assignment):
    """Revision-0 content from today's roster and active assignment: every
    host static, each archive owner pinned to the range it already holds."""
    owners = sorted((w for w in assignment['workers'] if w['role'] == 'archive-owner'), key=lambda w: w['shards'][0])
    ranges, group = [], {}
    for index, worker in enumerate(owners):
        shards = worker['shards']
        if shards != list(range(shards[0], shards[-1] + 1)):
            raise InventoryError(f"{worker['id']} holds a non-contiguous range")
        ranges.append({'id': f'a{index}', 'first': shards[0], 'last': shards[-1]})
        group[worker['id']] = f'a{index}'
    members = []
    for w in roster:
        m = {k: w[k] for k in ('id', 'role', 'ssh_host', 'upstream', 'cache_bytes')}
        m.update(origin='static', intent='enrolled',
                 group=RECENT_GROUP if w['role'] == 'recent-replica' else group[w['id']])
        for optional in ('memory_max', 'build_slots'):
            if optional in w:
                m[optional] = w[optional]
        members.append(m)
    return {'schema': SCHEMA, 'partition': {'ranges': ranges}, 'members': members}


def membership(state_dir, max_age=10):
    value = json.loads((Path(state_dir)/'membership.json').read_text())
    if not 0 <= time.time() - value['updated_unix'] <= max_age:
        raise InventoryError('membership record is stale; is the reconciler running?')
    return value


def serving_recent(record, exclude=()):
    return {mid for mid, m in record['members'].items()
            if m.get('role') == 'recent-replica' and m.get('state') == 'serving'
            and m.get('rendered', True) and m.get('intent', 'enrolled') == 'enrolled' and mid not in exclude}


def scaler_mode(scaler_dir):
    try:
        return json.loads((Path(scaler_dir)/'policy.json').read_text()).get('mode', 'observe')
    except FileNotFoundError:
        return None


def set_intent(inv, member_id, intent):
    for m in inv['members']:
        if m['id'] == member_id:
            m['intent'] = intent
            m['intent_unix'] = time.time()
            return inv
    raise InventoryError(f'no member {member_id}')


RANGE_SPEC = re.compile(r'([A-Za-z0-9_.-]+):(\d+)-(\d+)')
HOST_KEY = re.compile(r'(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(?:256|384|521)) [A-Za-z0-9+/=]+')


def parse_ranges(text):
    ranges = []
    for part in text.split(','):
        match = RANGE_SPEC.fullmatch(part.strip())
        if not match:
            raise InventoryError(f'invalid range {part!r}; expected ID:FIRST-LAST')
        ranges.append({'id': match.group(1), 'first': int(match.group(2)), 'last': int(match.group(3))})
    return ranges


def host_key(value):
    """`value` is a public key or a file holding one (a .pub file or a
    known_hosts line); returns 'type base64'."""
    text = value if HOST_KEY.match(value) else Path(value).read_text()
    match = HOST_KEY.search(text)
    if not match:
        raise InventoryError(f'no SSH host key in {value!r}')
    return match.group(0)


def parse_owner(text):
    """ID=ssh_host,upstream,host_key,cache_bytes,memory_max,build_slots"""
    member_id, _, rest = text.partition('=')
    fields = rest.split(',')
    if not member_id or len(fields) != 6:
        raise InventoryError(f'invalid owner {text!r}; expected '
                             'ID=ssh_host,upstream,host_key,cache_bytes,memory_max,build_slots')
    ssh_host, upstream, key, cache_bytes, memory_max, build_slots = fields
    try:
        cache_bytes, build_slots = int(cache_bytes), int(build_slots)
    except ValueError:
        raise InventoryError(f'owner {member_id}: cache_bytes and build_slots must be integers') from None
    if not 1 <= build_slots <= 99:
        raise InventoryError(f'owner {member_id}: build_slots must be 1..99')
    return {'id': member_id, 'role': 'archive-owner', 'origin': 'static', 'intent': 'enrolled',
            'ssh_host': ssh_host, 'upstream': upstream, 'ssh_host_key': host_key(key),
            'cache_bytes': cache_bytes, 'memory_max': memory_max, 'build_slots': build_slots}


def repartition(inv, ranges, owners, now):
    """The new partition with one new static owner per range, in one revision.

    Every archive owner not yet retired is retired, the replaced ranges move
    to `partition_history`, and `repartition` records the revision to restore.
    The archive's extent never changes; only how it is cut and who holds it.
    """
    old = inv['partition']['ranges']
    used = {r['id'] for r in old + inv.get('partition_history', [])}
    if len(ranges) != len(owners):
        raise InventoryError(f'{len(ranges)} ranges need {len(ranges)} owners, one per range in order')
    if any(r['id'] in used for r in ranges):
        raise InventoryError('new range ids must be fresh; ids in the partition or its history are reserved')
    if old and ranges and ranges[-1]['last'] != old[-1]['last']:
        raise InventoryError(f"the archive ends at shard {old[-1]['last']}; a repartition cannot move that")
    members = {m['id'] for m in inv['members']}
    for owner in owners:
        if owner['id'] in members:
            raise InventoryError(f"{owner['id']} is already a member; a new owner needs a new id")
    for m in inv['members']:
        if m['role'] == 'archive-owner' and m['intent'] != 'retired':
            m.update(intent='retired', intent_unix=now, retired_reason='repartition')
    for r, owner in zip(ranges, owners):
        inv['members'].append(dict(owner, group=r['id'], intent_unix=now))
    inv['partition_history'] = inv.get('partition_history', []) + old
    inv['partition'] = {'ranges': ranges}
    inv['repartition'] = {'from_revision': inv['revision'], 'revision': inv['revision'] + 1, 'unix': now}
    inv.pop('restored_from', None)
    return inv


def archive_state(inv):
    return (inv['partition'], inv.get('partition_history', []),
            sorted((m['id'], m['intent'], m['group']) for m in inv['members'] if m['role'] == 'archive-owner'))


def restore_plan(root, current, revision):
    """(target, repartitioned, old owners) for restoring `revision`, or raise.

    Only the most recent repartition is undone, only to the revision it was
    made from, and only while nothing after it changed the archive.
    """
    record = current.get('repartition')
    if not record or record.get('restored'):
        raise InventoryError('there is no repartition to restore')
    if record['from_revision'] != revision:
        raise InventoryError(f"only revision {record['from_revision']}, the one the last repartition "
                             'replaced, can be restored')
    def load(n):
        try:
            value = json.loads((Path(root)/'inventory.d'/f'{n}.json').read_text())
        except OSError:
            raise InventoryError(f'revision {n} is not kept in inventory.d') from None
        validate(value)
        return value
    target, repartitioned = load(revision), load(record['revision'])
    if repartitioned.get('repartition') != record:
        raise InventoryError(f"revision {record['revision']} is not the recorded repartition")
    for n in range(record['revision'] + 1, current['revision'] + 1):
        if archive_state(load(n)) != archive_state(repartitioned):
            raise InventoryError(f'revision {n} changed the archive after the repartition; restore by hand')
    before = {m['id']: m for m in target['members'] if m['role'] == 'archive-owner' and m['intent'] in ROUTABLE}
    old = [m for m in repartitioned['members'] if m['id'] in before and m.get('retired_reason') == 'repartition']
    return target, repartitioned, old


def restore(inv, target, now):
    """The archive of `target`, the recent tier as it is now. Owners the
    repartition added are retired; their ranges stay reserved in history."""
    prior = {m['id']: m for m in target['members'] if m['role'] == 'archive-owner'}
    history = list(target.get('partition_history', []))
    reserved = {r['id'] for r in history + target['partition']['ranges']}
    for r in inv.get('partition_history', []) + inv['partition']['ranges']:
        if r['id'] not in reserved:
            history.append(r)
            reserved.add(r['id'])
    members = []
    for m in inv['members']:
        if m['role'] != 'archive-owner':
            members.append(m)
        elif m['id'] in prior:
            members.append(dict(copy.deepcopy(prior[m['id']]), intent_unix=now))
        else:
            members.append(dict(m, intent='retired', intent_unix=now, retired_reason='restore'))
    inv['members'] = members
    inv['partition'] = copy.deepcopy(target['partition'])
    inv['partition_history'] = history
    inv['repartition'] = dict(inv['repartition'], restored=True)
    inv['restored_from'] = {'revision': target['revision'], 'repartition_revision': inv['repartition']['revision'],
                            'unix': now}
    return inv


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def fleet_ssh(config, host, command, data=None, known_hosts=None, timeout=30):
    """Run `command` on `host` with the fleet key and a strictly pinned host key."""
    args = ['ssh', '-F', '/dev/null', '-oBatchMode=yes', '-oConnectTimeout=5', '-oStrictHostKeyChecking=yes',
            '-oGlobalKnownHostsFile=/dev/null', '-oUserKnownHostsFile=' + str(known_hosts or config['known_hosts']),
            '-i', config['ssh_key'], 'root@' + host, command]
    result = subprocess.run(args, input=data, capture_output=True, timeout=timeout)
    if result.returncode:
        raise InventoryError(f'{host}: ssh failed: ' + result.stderr.decode(errors='replace')[-300:])
    return result.stdout


def worker_status(config, member, known_hosts=None):
    """The worker's control-socket status, read as the reconciler reads it."""
    command = shlex.join([config.get('control_binary', '/usr/local/bin/shard-control'),
                          config.get('control_socket', '/run/transparent-pir/control.sock')])
    raw = fleet_ssh(config, member['ssh_host'], command, json.dumps({'operation': 'status'}).encode(), known_hosts)
    result = json.loads(raw)
    if not result.get('ok'):
        raise InventoryError(f"{member['id']}: status refused: {result.get('error')}")
    return result['result']


def probe_standby(config, owner):
    """(ready, status) of a host not yet in the inventory, its key pinned from `owner`."""
    ready = http_json(f"http://{owner['upstream']}/v1/ready")
    with tempfile.NamedTemporaryFile('w', suffix='.known_hosts') as known:
        known.write(f"{owner['ssh_host']} {owner['ssh_host_key']}\n")
        known.flush()
        status = worker_status(config, owner, known.name)
        # The coordinator keeps only recent publications, and a standby warms
        # for longer than that; its own copy of the map it serves is the record.
        directory = (status.get('active') or {}).get('directory')
        if directory:
            raw = fleet_ssh(config, owner['ssh_host'], 'cat ' + shlex.quote(directory + '/shards.json'),
                            known_hosts=known.name)
            status['shards'] = json.loads(raw)['shards']
    return ready, status


def archive_manifests(state_dir, digest, last):
    request = Path(state_dir)/f'{digest}.request.json'
    try:
        directory = Path(json.loads(request.read_text())['directory'])
        shards = json.loads((directory/'shards.json').read_text())['shards']
    except (OSError, ValueError, KeyError):
        raise InventoryError(f'publication {digest[:12]} is no longer on the coordinator; '
                             'rerun the standby tool') from None
    return [s['manifest_digest'] for s in shards[:last + 1]]


def check_standby(state_dir, owner, last, ready, status):
    """Refuse unless `owner` serves warm on a publication whose archive shards
    0..last are the active publication's. Map digests move every block; the
    sealed archive shards do not, so the reconciler's next stage hard-links.
    `status['shards']`, when the probe read it, is the served map's shard list."""
    if not ready.get('ready'):
        raise InventoryError(f"{owner['id']} is not ready")
    active = (status.get('active') or {}).get('map_sha256')
    if not active or not status.get('warm') or status.get('invalidated'):
        raise InventoryError(f"{owner['id']} is not warm on a valid publication")
    current = json.loads((Path(state_dir)/'active.json').read_text())['map_sha256']
    if active == current:
        return
    # A map the coordinator has pruned is read from the worker's own copy.
    served = ([s['manifest_digest'] for s in status['shards'][:last + 1]] if status.get('shards')
              else archive_manifests(state_dir, active, last))
    if served != archive_manifests(state_dir, current, last):
        raise InventoryError(f"{owner['id']} is warm on {active[:12]}, whose archive shards differ from the active "
                             'publication; rerun the standby tool')


def probe_alive(config, member):
    """None when `member` still runs and answers ready; otherwise why not."""
    try:
        fleet_ssh(config, member['ssh_host'], 'systemctl is-active --quiet transparent-shard-server')
    except (InventoryError, OSError, subprocess.TimeoutExpired) as error:
        return f'service not active: {error}'
    try:
        if not http_json(f"http://{member['upstream']}/v1/ready").get('ready'):
            return 'not ready'
    except (OSError, ValueError) as error:
        return f'/v1/ready failed: {error}'
    return None


def main(argv=None, probe=probe_standby, alive=probe_alive):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fleet-config', type=Path, default=Path('/opt/transparent-publisher/fleet.json'))
    parser.add_argument('--scaler-dir', type=Path, default=Path('/opt/transparent-publisher/scaler'))
    parser.add_argument('--updated-by', default='operator:' + os.environ.get('SUDO_USER', os.environ.get('USER', 'root')))
    parser.add_argument('--archive', action='store_true', help='allow changing archive owners (never in scaler act mode)')
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init', help='seed the inventory from the roster and active assignment')
    init.add_argument('--assignment', type=Path, help='defaults to the active assignment')
    sub.add_parser('show')
    sub.add_parser('export-roster')
    sub.add_parser('known-hosts')
    enroll = sub.add_parser('enroll')
    for field in ('id', 'ssh-host', 'upstream', 'memory-max'):
        enroll.add_argument('--' + field, required=True)
    enroll.add_argument('--cache-bytes', type=int, required=True)
    enroll.add_argument('--build-slots', type=int, default=1)
    enroll.add_argument('--origin', choices=('static', 'elastic'), default='static')
    for field in ('droplet-id', 'size', 'ssh-host-key', 'installed-release'):
        enroll.add_argument('--' + field)
    for name in ('drain', 'undrain', 'retire', 'quarantine'):
        command = sub.add_parser(name)
        command.add_argument('member')
        command.add_argument('--maintenance', action='store_true',
                             help='allow dropping below two serving replicas (maintenance window only)')
        command.add_argument('--force', action='store_true', help='retire without the drained interval')
    move = sub.add_parser('repartition', help='replace the archive partition and its owners in one revision')
    move.add_argument('--ranges', required=True, help='ID:FIRST-LAST[,ID:FIRST-LAST...], contiguous from shard 0')
    move.add_argument('--owner', action='append', required=True, metavar='ID=SSH_HOST,UPSTREAM,HOST_KEY,CACHE_BYTES,MEMORY_MAX,BUILD_SLOTS',
                      help='one per new range, in range order; HOST_KEY is a public key or a file holding one')
    move.add_argument('--skip-standby-check', action='store_true', help='tests only: do not probe the new owners')
    back = sub.add_parser('restore', help='undo the most recent repartition')
    back.add_argument('--revision', type=int, required=True, help='the revision that repartition replaced')
    back.add_argument('--force', action='store_true', help='restore although an old owner looks stopped')
    args = parser.parse_args(argv)
    config = json.loads(args.fleet_config.read_text())
    inventory = Inventory(config['state_dir'], config['roster'], config.get('known_hosts'))
    state_dir = Path(config['state_dir'])

    if args.command == 'init':
        if inventory.path.exists():
            raise SystemExit('inventory already exists; use its commands')
        path = args.assignment or Path(json.loads((state_dir/'active.json').read_text())['assignment'])
        roster = json.loads(Path(config['roster']).read_text())
        new = inventory.write(0, lambda _: seed(roster, json.loads(path.read_text())), args.updated_by, archive=True)
        print(json.dumps({'revision': new['revision'], 'partition': new['partition']}))
        return
    current = inventory.load()
    if args.command == 'show':
        print(json.dumps(current, indent=1, sort_keys=True))
        return
    if args.command == 'export-roster':
        print(json.dumps(roster_view(current), indent=1))
        return
    if args.command == 'known-hosts':
        print('\n'.join(known_hosts_lines(current)))
        return
    if args.command in ('repartition', 'restore'):
        if not args.archive:
            raise SystemExit(f'{args.command} changes archive owners; it needs --archive')
        if scaler_mode(args.scaler_dir) == 'act':
            raise SystemExit('pause the scaler (mode other than act) before changing archive owners')
        now = time.time()
        if args.command == 'repartition':
            ranges, owners = parse_ranges(args.ranges), [parse_owner(o) for o in args.owner]
            if not args.skip_standby_check:
                for owner in owners:
                    try:
                        ready, status = probe(config, owner)
                    except (OSError, ValueError, InventoryError, subprocess.TimeoutExpired) as error:
                        raise SystemExit(f"{owner['id']} did not answer its standby probe: {error}")
                    check_standby(state_dir, owner, ranges[-1]['last'], ready, status)
            new = inventory.write(current['revision'], lambda inv: repartition(inv, ranges, owners, now),
                                  args.updated_by, archive=True)
            print(json.dumps({'revision': new['revision'], 'repartition': new['repartition'],
                              'partition': new['partition'], 'changed': summarize(current, new)}))
            return
        target, _, old = restore_plan(state_dir, current, args.revision)
        if not args.force:
            try:
                observed = json.loads((state_dir/'membership.json').read_text()).get('members', {})
            except (OSError, ValueError):
                observed = {}
            for m in old:
                if observed.get(m['id'], {}).get('state') == 'unreachable':
                    raise SystemExit(f"{m['id']} was observed unreachable; restore needs every old owner running")
                reason = alive(config, m)
                if reason:
                    raise SystemExit(f"{m['id']} cannot take its range back ({reason}); restore needs every old "
                                     'owner running, or --force')
        new = inventory.write(current['revision'], lambda inv: restore(inv, target, now), args.updated_by,
                              archive=True, restore=True)
        print(json.dumps({'revision': new['revision'], 'restored_from': new['restored_from'],
                          'partition': new['partition'], 'changed': summarize(current, new)}))
        return
    target = args.member if args.command != 'enroll' else None
    member = next((m for m in current['members'] if m['id'] == target), None)
    archive_change = member is not None and member['role'] == 'archive-owner'
    if archive_change:
        if not args.archive:
            raise SystemExit('archive owners change only with --archive')
        if scaler_mode(args.scaler_dir) == 'act':
            raise SystemExit('pause the scaler (mode other than act) before changing archive owners')

    def mutate(inv):
        if args.command == 'enroll':
            m = {'id': args.id, 'role': 'recent-replica', 'group': RECENT_GROUP, 'origin': args.origin,
                 'intent': 'enrolled', 'intent_unix': time.time(), 'ssh_host': args.ssh_host,
                 'upstream': args.upstream, 'cache_bytes': args.cache_bytes, 'memory_max': args.memory_max,
                 'build_slots': args.build_slots}
            for field in ('droplet_id', 'size', 'ssh_host_key', 'installed_release'):
                if getattr(args, field) is not None:
                    m[field] = getattr(args, field)
            inv['members'].append(m)
            return inv
        return set_intent(inv, target, {'drain': 'draining', 'undrain': 'enrolled', 'retire': 'retired',
                                        'quarantine': 'quarantined'}[args.command])

    if args.command == 'drain' and not args.maintenance:
        others = serving_recent(membership(state_dir), exclude={target})
        if len(others) < MIN_OTHER_SERVING:
            raise SystemExit(f'only {len(others)} other recent replicas serve; refusing to drain {target}')
    if args.command == 'retire' and not args.force:
        record = membership(state_dir)['members'].get(target, {})
        since = record.get('drained_since_unix')
        if member['intent'] != 'draining' or since is None or time.time() - since < DRAINED_SECONDS:
            raise SystemExit(f'{target} has not been drained for {DRAINED_SECONDS} s')
    new = inventory.write(current['revision'], mutate, args.updated_by, archive=archive_change)
    print(json.dumps({'revision': new['revision'], 'changed': summarize(current, new)}))


if __name__ == '__main__':
    try:
        main()
    except InventoryError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
