#!/usr/bin/env python3
"""Warm a new archive owner on the active publication before it owns anything.

Operator-only; run on the coordinator as root from the release checkout
(/opt/transparent-publisher/releases/current/repo). The host must exist (added
to `transparent_archive_names`) and must not be in the inventory yet: nothing
routes to it, so every step can be repeated until it is warm. The inventory's
`repartition` then makes it an owner at the next publication, and refuses a
host this tool has not left warm. The actuator and the scaler never run this;
the archive stays static. See transparent/docs/deployment.md.

Steps: the pinned host key and the droplet id from the metadata service; an
x86-64-v3 CPU; the release binaries, checked against the store's SHA256SUMS;
a plan of the active publication with the enrolled recent replicas and this
host pinned to the whole archive; its files, hard-linked from any earlier
standby publication and otherwise copied at a bounded rate, paused while
publication is late; the assignment, written once; the unit, a serving
archive owner's unit with this host's identity and budgets; and the wait until
the worker attests that publication warm. Prints a JSON summary.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time

SCRIPTS = Path(__file__).resolve().parent
LIB = str(SCRIPTS.parents[2]/'ops/lib')
if LIB not in sys.path:
    sys.path.insert(0, LIB)
from wallet_pir_ops import transparent_unit  # noqa: E402


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ACTUATOR = load_module('fleet_actuator', 'transparent-fleet-actuator.py')
INVENTORY = ACTUATOR.INVENTORY
UNIT_PATH = '/etc/systemd/system/transparent-shard-server.service'
RUNTIME_CACHE = '/srv/transparent-pir/runtime-cache'
DIGEST = re.compile(r'[0-9a-f]{64}')
# Helpers a serving unit may run before start, and the repository scripts they
# are installed from (deploy-transparent-publisher.py installs the same pair).
PRESTART_HELPERS = {'/usr/local/lib/transparent-pir/headless-console.py': 'transparent-headless-console.py',
                    '/usr/local/lib/transparent-pir/storage-policy.py': 'transparent-storage-policy.py'}


class StandbyError(RuntimeError):
    pass


def log(event, **fields):
    print(json.dumps({'event': event, 'unix': round(time.time(), 3), **fields}), file=sys.stderr, flush=True)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def release_binaries(store, sha):
    """{name: (path, sha256)} for the worker binaries of release `sha`, each
    checked on the coordinator against the release's SHA256SUMS."""
    directory = Path(store)/sha
    sums = {}
    for line in (directory/'SHA256SUMS').read_text().splitlines():
        digest, _, name = line.partition('  ')
        sums[name.strip().removeprefix('./')] = digest
    binaries = {}
    for name in ('transparent-shard-server', 'shard-control'):
        if name not in sums:
            raise StandbyError(f'release {sha} lists no {name}')
        if sha256_file(directory/name) != sums[name]:
            raise StandbyError(f'release {sha}: {name} does not match its SHA256SUMS')
        binaries[name] = (directory/name, sums[name])
    return binaries


class Standby:
    """One standby host. Every side effect is guarded so a rerun resumes."""

    def __init__(self, args, fleet, remote, fleet_remote, http=ACTUATOR.http_json, execute=subprocess.run,
                 sleep=time.sleep, clock=time.monotonic):
        self.a, self.fleet = args, fleet
        self.remote, self.fleet_remote = remote, fleet_remote
        self.http, self.execute, self.sleep, self.clock = http, execute, sleep, clock
        self.state = Path(fleet['state_dir'])
        self.work = Path(args.work_dir or self.state/'standby'/args.id)
        self.host = args.host
        self.summary = {'id': args.id, 'host': args.host, 'droplet_id': args.droplet_id, 'release': args.release}

    def run(self):
        self.refuse_members()
        self.check_host()
        binaries = release_binaries(self.a.release_store, self.a.release)
        self.summary['binary_sha256'] = binaries['transparent-shard-server'][1]
        self.install(binaries)
        warm = self.already_warm(binaries)
        if warm:
            self.summary.update(map_sha256=warm, warm_seconds=0, started='already')
            return self.summary
        digest, request = self.active()
        self.summary['map_sha256'] = digest
        assignment = self.plan(digest, request)
        publication = f"{self.a.publication_root}/{digest}"
        self.transfer(digest, request, assignment, publication)
        self.write_assignment(assignment, publication)
        started = self.start(self.render(publication), publication, digest)
        self.summary['warm_seconds'] = round(self.wait_warm(digest, binaries, started), 1)
        return self.summary

    # -- checks ----------------------------------------------------------
    def refuse_members(self):
        inv = INVENTORY.Inventory(self.state).load()
        for m in inv['members']:
            if m['id'] == self.a.id:
                raise StandbyError(f'{self.a.id} is already in the inventory; the standby is for new hosts')
            if m['intent'] in INVENTORY.ROUTABLE and m['ssh_host'] == self.host:
                raise StandbyError(f"{self.host} is {m['id']}'s address")
        ranges = inv['partition']['ranges']
        if not ranges:
            raise StandbyError('the inventory has no archive partition')
        self.last = ranges[-1]['last']

    def check_host(self):
        identity = self.remote.run(self.host, 'curl -s --max-time 3 http://169.254.169.254/metadata/v1/id').strip()
        if identity != str(self.a.droplet_id):
            raise StandbyError(f'metadata droplet id {identity!r} is not {self.a.droplet_id}')
        # x86-64-v3: the release is built for it.
        self.remote.run(self.host, "for f in avx2 bmi2 fma; do grep -qw $f /proc/cpuinfo || "
                                   "{ echo missing $f >&2; exit 1; }; done")

    # -- release ---------------------------------------------------------
    def install(self, binaries):
        installed = {}
        output = self.remote.run(self.host, 'sha256sum /usr/local/bin/transparent-shard-server '
                                            '/usr/local/bin/shard-control 2>/dev/null || true')
        for line in output.splitlines():
            digest, _, path = line.partition('  ')
            installed[Path(path.strip()).name] = digest
        if all(installed.get(name) == digest for name, (_, digest) in binaries.items()):
            self.summary['installed'] = 'already'
            return
        staging = '/opt/transparent-pir/staged'
        self.remote.run(self.host, f'mkdir -p {staging}')
        self.remote.copy(self.host, [path for path, _ in binaries.values()], staging + '/')
        check = ''.join(f'{digest}  {staging}/{name}\n' for name, (_, digest) in binaries.items())
        self.remote.run(self.host, f'sha256sum -c --quiet && '
                                   f'install -m755 {staging}/transparent-shard-server /usr/local/bin/transparent-shard-server.next && '
                                   f'mv /usr/local/bin/transparent-shard-server.next /usr/local/bin/transparent-shard-server && '
                                   f'install -m755 {staging}/shard-control /usr/local/bin/shard-control',
                        data=check.encode())
        self.summary['installed'] = 'now'

    def already_warm(self, binaries):
        """The digest this host is already warm on, when that publication's
        archive shards are the active one's and it runs this release; else None.
        This is the condition `repartition` checks."""
        try:
            ready = self.http(f'http://{self.host}:8093/v1/ready')
            if not ready.get('ready') or ready.get('binary_sha256') != binaries['transparent-shard-server'][1]:
                return None
            status = self.status(dict(self.fleet, known_hosts=str(self.a.known_hosts)),
                                 {'id': self.a.id, 'ssh_host': self.host})
            warm = (status.get('active') or {}).get('map_sha256')
            if not warm or not status.get('warm') or status.get('invalidated'):
                return None
            current, _ = self.active()
            if warm == current or (INVENTORY.archive_manifests(self.state, warm, self.last)
                                   == INVENTORY.archive_manifests(self.state, current, self.last)):
                return warm
        except (OSError, ValueError, KeyError, INVENTORY.InventoryError, subprocess.TimeoutExpired):
            pass
        return None

    # -- publication -----------------------------------------------------
    def active(self):
        digest = json.loads((self.state/'active.json').read_text())['map_sha256']
        request = json.loads((self.state/f'{digest}.request.json').read_text())
        return digest, request

    def candidate_roster(self):
        roster = [w for w in json.loads(Path(self.fleet['roster']).read_text())
                  if w['role'] == 'recent-replica' and w.get('intent', 'enrolled') == 'enrolled']
        if not roster:
            raise StandbyError('no enrolled recent replica to plan with')
        roster.append({'id': self.a.id, 'role': 'archive-owner', 'ssh_host': self.host,
                       'upstream': f'{self.host}:8093', 'cache_bytes': self.a.cache_bytes,
                       'memory_max': self.a.memory_max, 'build_slots': self.a.build_slots,
                       'archive_range': [0, self.last]})
        return roster

    def plan(self, digest, request):
        """This host's assignment for `digest`, planned once and kept: a rerun
        writes the same bytes the worker already holds."""
        self.work.mkdir(parents=True, exist_ok=True)
        assignment = self.work/f'{digest}.assignment.json'
        if not assignment.exists():
            roster = self.work/f'{digest}.roster.json'
            INVENTORY.atomic_json(roster, self.candidate_roster())
            partial = assignment.with_name(assignment.name + '.partial')
            self.execute([self.fleet['assign_binary'], 'plan', '--shard-dir', str(request['directory']),
                          '--roster', str(roster), '--recent-from-height', str(request['recent_from']),
                          '--headroom', str(self.a.headroom), '--out-assignment', str(partial),
                          '--source-sha', str(request['source_sha'])], check=True, capture_output=True)
            os.replace(partial, assignment)
        planned = json.loads(assignment.read_text())
        mine = next((w for w in planned.get('workers', []) if w['id'] == self.a.id), None)
        if not mine or mine.get('shards') != list(range(0, self.last + 1)):
            raise StandbyError(f'the plan does not give {self.a.id} archive shards 0..{self.last}')
        return assignment

    def transfer(self, digest, request, assignment, publication):
        listing = self.execute([self.fleet['assign_binary'], 'files', '--shard-dir', str(request['directory']),
                                '--assignment', str(assignment), '--worker-id', self.a.id],
                               check=True, capture_output=True, text=True).stdout.split()
        chunks = {}
        for path in listing:
            top = path.split('/')[0]
            if '/' in path and not DIGEST.fullmatch(top):
                raise StandbyError('unexpected artifact path ' + path)
            chunks.setdefault(top if '/' in path else '', []).append(path)
        # Sealed shards an earlier standby run already holds are hard-linked,
        # as the reconciler stages; only what is missing crosses the network.
        root = shlex.quote(self.a.publication_root)
        target = shlex.quote(publication)
        script = ['set -eu', f'mkdir -p {target}']
        for top in sorted(c for c in chunks if c):
            script.append(f'if [ ! -e {target}/{top} ]; then for p in {root}/*/{top}; do '
                          f'if [ -d "$p" ]; then cp -al "$p" {target}/{top}; break; fi; done; fi')
        self.remote.run(self.host, 'sh -s', data=('\n'.join(script) + '\n').encode())
        for top in sorted(chunks):
            self.wait_fresh()
            with tempfile.NamedTemporaryFile('w') as files:
                files.write('\n'.join(chunks[top]) + '\n')
                files.flush()
                self.remote.copy(self.host, [str(request['directory']) + '/'], publication + '/',
                                 bwlimit=self.a.bwlimit_kbps, files_from=files.name)
        self.summary['files'] = len(listing)

    def wait_fresh(self):
        """Return once publication is serving and fresh. The bulk copy shares
        the coordinator's disk and uplink with publication."""
        deadline = self.clock() + self.a.pause_limit
        while True:
            try:
                status = self.http(self.a.publisher_status)
                if status.get('phase') == 'serving' and status.get('freshness_seconds', 1e9) <= self.a.freshness_pause:
                    return
            except (OSError, ValueError):
                status = {}
            if self.clock() > deadline:
                raise StandbyError(f'publication stayed late for {self.a.pause_limit} s; copy paused')
            log('standby_paused', freshness_seconds=status.get('freshness_seconds'), phase=status.get('phase'))
            self.sleep(5)

    def write_assignment(self, assignment, publication):
        target = shlex.quote(publication + '/assignment.json')
        write_once = (f'set -eu\nif [ -e {target} ]; then\n'
                      f'  cmp -s - {target} || {{ echo "a different assignment exists for this publication" >&2; exit 3; }}\n'
                      f'else\n  cat > {target}.next\n  mv {target}.next {target}\nfi')
        self.remote.run(self.host, write_once, data=assignment.read_bytes())

    # -- unit ------------------------------------------------------------
    def template(self):
        """The unit of a serving archive owner, else of a serving recent
        replica: every flag this host does not override stays the fleet's."""
        roster = json.loads(Path(self.fleet['roster']).read_text())
        for role in ('archive-owner', 'recent-replica'):
            for worker in roster:
                if worker['role'] == role and worker.get('intent', 'enrolled') == 'enrolled':
                    try:
                        return self.fleet_remote.run(worker['ssh_host'], 'cat ' + UNIT_PATH), worker['id']
                    except (ACTUATOR.ActuatorError, subprocess.TimeoutExpired):
                        continue
        raise StandbyError('no serving worker unit to render from')

    def render(self, publication):
        unit, source = self.template()
        cache = self.a.cache_bytes
        unit, found = transparent_unit.rewrite_exec(unit, {
            '--worker-id': self.a.id, '--shard-dir': publication, '--assignment': publication + '/assignment.json',
            '--cache-bytes': cache, '--runtime-cache-dir': RUNTIME_CACHE, '--runtime-cache-max-bytes': 2 * cache,
            '--query-slots': self.a.query_slots, '--build-slots': self.a.build_slots}, append=True)
        if '--worker-id' not in found:
            raise StandbyError(f'the unit of {source} has no --worker-id to replace')
        unit = transparent_unit.set_service(unit, {'MemoryMax': self.a.memory_max,
                                                   'MemoryHigh': self.a.memory_high})
        self.summary['unit_from'] = source
        return unit

    def install_prestart(self, unit):
        for line in unit.splitlines():
            if not line.startswith('ExecStartPre='):
                continue
            for path, script in PRESTART_HELPERS.items():
                if path in line:
                    self.remote.run(self.host, f'mkdir -p {Path(path).parent} && cat > {path}.next && '
                                               f'chmod 755 {path}.next && mv {path}.next {path}',
                                    data=(SCRIPTS/script).read_bytes())
                    self.remote.run(self.host, f'python3 {path} --preflight')

    def start(self, unit, publication, digest):
        """Install `unit` and (re)start the service when anything changed.
        Returns the monotonic time the service was last started from here.

        A worker with a control socket starts from its active record, not
        from --shard-dir, so the record is pointed at this publication too.
        Nothing routes to this host yet; no one else writes that record."""
        current = self.remote.run(self.host, f'cat {UNIT_PATH} 2>/dev/null || true')
        active = self.remote.run(self.host, 'systemctl is-active transparent-shard-server || true').strip()
        if current == unit and active == 'active':
            self.summary['started'] = 'already'
            return self.clock()
        self.install_prestart(unit)
        for directory in transparent_unit.unit_directories(unit):
            self.remote.run(self.host, 'mkdir -p ' + shlex.quote(directory))
        self.remote.run(self.host, f'cat > {UNIT_PATH}.next && mv {UNIT_PATH}.next {UNIT_PATH}', data=unit.encode())
        args = transparent_unit.exec_args(unit)
        if '--active-record' in args:
            record = shlex.quote(args[args.index('--active-record') + 1])
            value = {'directory': publication, 'assignment': publication + '/assignment.json', 'map_sha256': digest}
            self.remote.run(self.host, f'cat > {record}.next && mv {record}.next {record}',
                            data=(json.dumps(value) + '\n').encode())
        self.remote.run(self.host, 'systemctl daemon-reload && systemctl enable transparent-shard-server && '
                                   'systemctl restart transparent-shard-server')
        self.summary['started'] = 'now'
        return self.clock()

    def wait_warm(self, digest, binaries, started):
        member = {'id': self.a.id, 'ssh_host': self.host}
        config = dict(self.fleet, known_hosts=str(self.a.known_hosts))
        deadline = started + self.a.warm_timeout
        while self.clock() < deadline:
            try:
                ready = self.http(f'http://{self.host}:8093/v1/ready')
                if (ready.get('ready') and ready.get('binary_sha256') == binaries['transparent-shard-server'][1]
                        and ready.get('map_sha256') == digest):
                    status = self.status(config, member)
                    active = (status.get('active') or {}).get('map_sha256')
                    if active == digest and status.get('warm') and not status.get('invalidated'):
                        return self.clock() - started
            except (OSError, ValueError, INVENTORY.InventoryError, subprocess.TimeoutExpired):
                pass
            self.sleep(5)
        raise StandbyError(f'{self.a.id} did not attest {digest[:12]} warm within {self.a.warm_timeout} s')

    def status(self, config, member):
        return INVENTORY.worker_status(config, member, config['known_hosts'])


def parse(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--fleet-config', type=Path, default=Path('/opt/transparent-publisher/fleet.json'))
    parser.add_argument('--id', required=True, help='the new owner, e.g. transparent-pir-archive-03')
    parser.add_argument('--host', required=True, help='its VPC address')
    parser.add_argument('--droplet-id', required=True, help='must match the host metadata service')
    parser.add_argument('--known-hosts', required=True, type=Path,
                        help="a known_hosts file pinning the host's key, verified out of band")
    parser.add_argument('--release', required=True, help='directory name under the release store, e.g. a704616c')
    parser.add_argument('--release-store', default='/opt/transparent-publisher/releases')
    parser.add_argument('--cache-bytes', type=int, default=51539607552)
    parser.add_argument('--memory-max', default='56G')
    parser.add_argument('--memory-high', type=int, default=transparent_unit.MEMORY_HIGH['archive-owner'])
    parser.add_argument('--build-slots', type=int, default=1)
    parser.add_argument('--query-slots', type=int, default=2)
    parser.add_argument('--headroom', type=float, default=0.05, help="the live adapter's planning headroom")
    parser.add_argument('--bwlimit-kbps', type=int, default=60000)
    parser.add_argument('--freshness-pause', type=float, default=20, help='pause copying while freshness exceeds this')
    parser.add_argument('--pause-limit', type=float, default=3600, help='fail after pausing this long')
    parser.add_argument('--publisher-status', default='http://127.0.0.1:8094/v1/status')
    parser.add_argument('--publication-root', default='/srv/transparent-pir/publications')
    parser.add_argument('--warm-timeout', type=float, default=40 * 60)
    parser.add_argument('--work-dir', type=Path, help='defaults to <state_dir>/standby/<id>')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'transparent-pir-archive-\d{2}', args.id):
        parser.error('--id must be transparent-pir-archive-NN')
    if not re.fullmatch(r'[0-9a-zA-Z._-]+', args.release):
        parser.error('--release must be a release directory name')
    if args.cache_bytes <= 0 or not 1 <= args.build_slots <= 99 or not 1 <= args.query_slots <= 99:
        parser.error('budgets must be positive')
    return args


def main(argv=None):
    args = parse(argv)
    fleet = json.loads(args.fleet_config.read_text())
    remote = ACTUATOR.Remote({'known_hosts': str(args.known_hosts), 'ssh_key': fleet['ssh_key']})
    standby = Standby(args, fleet, remote, ACTUATOR.Remote(fleet))
    print(json.dumps(standby.run(), indent=1))


if __name__ == '__main__':
    try:
        main()
    except (StandbyError, ACTUATOR.ActuatorError, INVENTORY.InventoryError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
