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
import sys
import time

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
    next_shard, range_ids = 0, set()
    for r in ranges:
        if (not isinstance(r, dict) or not isinstance(r.get('id'), str) or r['id'] in range_ids
                or r['id'] == RECENT_GROUP or type(r.get('first')) is not int or type(r.get('last')) is not int):
            fail('invalid archive range')
        if r['first'] != next_shard or r['last'] < r['first']:
            fail(f"archive range {r['id']} must start at shard {next_shard}")
        next_shard = r['last'] + 1
        range_ids.add(r['id'])
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
            if m.get('group') not in range_ids:
                fail(f"archive owner {m['id']} names no archive range")
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


def check_transition(old, new, archive=False):
    """Refuse changes a writer may not make: lost ids, rewritten host facts,
    skipped lifecycle steps, and archive changes without `archive`."""
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
            if prior['intent'] == 'retired':
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
    return [m['ssh_host'] + ' ' + m['ssh_host_key'] for m in inv['members']
            if m['origin'] == 'elastic' and m['intent'] in ROUTABLE and m.get('ssh_host_key')]


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

    def write(self, expected_revision, mutate, updated_by, archive=False):
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
            check_transition(current, new, archive)
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
            elastic = {m['ssh_host'] for m in inv['members'] if m['origin'] == 'elastic'}
            kept = [line for line in self.known_hosts.read_text().splitlines()
                    if line.strip() and line.split()[0] not in elastic]
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


def main(argv=None):
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
