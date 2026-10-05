"""The archive standby tool against fake SSH, rsync, shard-assign and HTTP."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('standby', Path(__file__).resolve().parents[1]/'scripts/transparent-archive-standby.py')
S = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(S)
I = S.INVENTORY

HOST, NEW = '10.142.1.3', 'transparent-pir-archive-03'
ROUTER = '10.142.0.11'
ACTIVE = 'a' * 64
PEER_UNIT = ('[Unit]\nDescription=w\n\n[Service]\nMemoryHigh=51539607552\nRuntimeDirectory=transparent-pir\n'
             'ExecStart=/usr/local/bin/transparent-shard-server --listen 0.0.0.0:8093 --shard-dir /p/old '
             '--cache-bytes 51539607552 --build-slots 1 --query-slots 2 --assignment /p/old/assignment.json '
             '--worker-id transparent-pir-archive-01 --prune-excess --runtime-cache-dir /srv/transparent-pir/runtime-cache '
             '--runtime-cache-max-bytes 103079215104 --control-socket /run/transparent-pir/control.sock '
             '--active-record /opt/transparent-publisher/active.json\nMemoryMax=56G\nMemorySwapMax=0\n')


class FakeHost:
    """The new host: its metadata, CPU, files and service, driven by commands."""

    def __init__(self):
        self.commands, self.copies, self.files = [], [], {}
        self.droplet_id, self.cpu_ok, self.active = '777', True, False
        self.binaries = {}
        self.restarts = 0

    def run(self, host, command, data=None, timeout=120):
        assert host == HOST
        self.commands.append(command)
        if 'metadata/v1/id' in command:
            return self.droplet_id + '\n'
        if '/proc/cpuinfo' in command:
            if not self.cpu_ok:
                raise S.ACTUATOR.ActuatorError('missing avx2')
            return ''
        if command.startswith('sha256sum /usr/local/bin'):
            return ''.join(f'{d}  /usr/local/bin/{n}\n' for n, d in self.binaries.items())
        if command.startswith('sha256sum -c'):
            for line in data.decode().splitlines():
                digest, _, path = line.partition('  ')
                self.binaries[Path(path).name] = digest
            return ''
        if command.startswith('cat /etc/systemd/system/transparent-shard-server.service'):
            return self.files.get(S.UNIT_PATH, '')
        if command.startswith('systemctl is-active'):
            return 'active\n' if self.active else 'inactive\n'
        if 'systemctl restart' in command:
            self.active = True
            self.restarts += 1
            return ''
        if command.startswith('cat > ') or 'if [ -e ' in command:
            target = shlex.split(command.split('cat > ')[1].split(' ')[0])[0].removesuffix('.next') \
                if command.startswith('cat > ') else command.split('if [ -e ')[1].split(' ')[0]
            existing = self.files.get(target)
            if 'cmp -s' in command and existing is not None and existing != data.decode():
                raise S.ACTUATOR.ActuatorError('a different assignment exists for this publication')
            self.files[target] = data.decode()
            return ''
        return ''

    def copy(self, host, sources, destination, bwlimit=None, files_from=None, timeout=3600):
        assert host == HOST
        listing = Path(files_from).read_text().split() if files_from else [Path(s).name for s in sources]
        self.copies.append((destination, bwlimit, listing))


class FakePeers:
    def __init__(self):
        self.reads, self.dials, self.unreachable = [], [], 0

    def run(self, host, command, data=None, timeout=120):
        if command.startswith('curl '):
            self.dials.append((host, command.split()[-1]))
            if self.unreachable:
                self.unreachable -= 1
                raise S.ACTUATOR.ActuatorError(f'{host}: ssh failed: curl: (28) timed out')
            return ''
        self.reads.append(host)
        assert command == 'cat ' + S.UNIT_PATH
        return PEER_UNIT


class StandbyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = self.root = Path(self.temp.name)
        self.state = root/'state'
        self.state.mkdir()
        roster = [dict(id=f'transparent-pir-recent-0{i}', role='recent-replica', ssh_host=f'10.142.0.{i}',
                       upstream=f'10.142.0.{i}:8093', cache_bytes=5368709120, memory_max='7G', build_slots=1,
                       intent='enrolled', origin='static', replica_group='recent') for i in (1, 2)]
        roster += [dict(id=f'transparent-pir-archive-0{i}', role='archive-owner', ssh_host=f'10.142.1.{i}',
                        upstream=f'10.142.1.{i}:8093', cache_bytes=51539607552, memory_max='56G', build_slots=1)
                   for i in (1, 2)]
        (root/'roster.json').write_text(json.dumps(roster))
        assignment = {'workers': [dict(id='transparent-pir-archive-01', role='archive-owner', shards=list(range(39))),
                                  dict(id='transparent-pir-archive-02', role='archive-owner', shards=list(range(39, 77)))]
                      + [dict(id=w['id'], role='recent-replica', shards=list(range(77, 86))) for w in roster[:2]]}
        I.Inventory(self.state, root/'roster.json').write(0, lambda _: I.seed(roster, assignment), 'test', archive=True)
        self.publish(ACTIVE)
        I.atomic_json(self.state/'active.json', {'map_sha256': ACTIVE, 'workers': []})
        # The release store.
        release = root/'releases'/'a704616c'
        release.mkdir(parents=True)
        sums = []
        for name in ('transparent-shard-server', 'shard-control'):
            (release/name).write_bytes(name.encode())
            sums.append(f'{hashlib.sha256(name.encode()).hexdigest()}  ./{name}')
        (release/'SHA256SUMS').write_text('\n'.join(sums) + '\n')
        self.binary = hashlib.sha256(b'transparent-shard-server').hexdigest()
        self.fleet = dict(state_dir=str(self.state), roster=str(root/'roster.json'), known_hosts=str(root/'fleet_known'),
                          ssh_key=str(root/'id'), assign_binary='shard-assign', router_host=ROUTER)
        (root/'pinned').write_text(f'{HOST} ssh-ed25519 AAAAarchive3\n')
        self.host, self.peers = FakeHost(), FakePeers()
        self.plans, self.freshness, self.sleeps = [], [], 0
        self.clock_value = 0.0
        self.worker = {'ready': False, 'status': None}

    def publish(self, digest, archive='sealed'):
        directory = self.root/'pub'/digest
        directory.mkdir(parents=True)
        shards = [{'manifest_digest': f'{archive}-{i}'} for i in range(77)] + [{'manifest_digest': f'r-{digest}'}]
        (directory/'shards.json').write_text(json.dumps({'shards': shards}))
        I.atomic_json(self.state/f'{digest}.request.json', {'directory': str(directory), 'recent_from': 3100000,
                                                             'source_sha': 'feed', 'map_sha256': digest})

    # -- fakes -----------------------------------------------------------
    def execute(self, args, check=True, capture_output=True, text=False):
        if args[1] == 'plan':
            roster = json.loads(Path(args[args.index('--roster') + 1]).read_text())
            self.plans.append(roster)
            workers = []
            for w in roster:
                if w['role'] == 'archive-owner':
                    first, last = w['archive_range']
                    workers.append({'id': w['id'], 'role': 'archive-owner', 'shards': list(range(first, last + 1))})
                else:
                    workers.append({'id': w['id'], 'role': 'recent-replica', 'shards': list(range(77, 86))})
            Path(args[args.index('--out-assignment') + 1]).write_text(json.dumps({'workers': workers}))
            return subprocess.CompletedProcess(args, 0, b'', b'')
        assert args[1] == 'files'
        tops = ['b' * 64, 'c' * 64]
        listing = 'shards.json\n' + ''.join(f'{t}/manifest.json\n{t}/data.bin\n' for t in tops)
        return subprocess.CompletedProcess(args, 0, listing, '')

    def http(self, url, timeout=5):
        if url.endswith('/v1/status'):
            value = self.freshness.pop(0) if self.freshness else 5
            return {'phase': 'serving', 'freshness_seconds': value}
        assert url == f'http://{HOST}:8093/v1/ready'
        if not self.host.active:
            raise OSError('connection refused')
        self.worker['polls'] = self.worker.get('polls', 0) + 1
        if self.worker['polls'] < 3:
            return {'ready': False}
        record = json.loads(self.host.files['/opt/transparent-publisher/active.json'])
        return {'ready': True, 'binary_sha256': self.host.binaries.get('transparent-shard-server'),
                'map_sha256': record['map_sha256']}

    def status(self, config, member):
        assert config['known_hosts'] == str(self.root/'pinned') and member['ssh_host'] == HOST
        if not self.host.active:
            raise I.InventoryError('no control socket')
        record = json.loads(self.host.files['/opt/transparent-publisher/active.json'])
        return {'active': {'map_sha256': record['map_sha256']}, 'warm': True}

    def sleep(self, seconds):
        self.sleeps += 1
        self.clock_value += seconds

    def standby(self, *extra):
        args = S.parse(['--id', NEW, '--host', HOST, '--droplet-id', '777', '--known-hosts', str(self.root/'pinned'),
                        '--release', 'a704616c', '--release-store', str(self.root/'releases'), *extra])
        tool = S.Standby(args, self.fleet, self.host, self.peers, http=self.http, execute=self.execute,
                         sleep=self.sleep, clock=lambda: self.clock_value)
        tool.status = self.status
        return tool

    # -- tests -----------------------------------------------------------
    def test_a_fresh_host_is_installed_planned_copied_and_warmed(self):
        summary = self.standby().run()
        self.assertEqual((summary['map_sha256'], summary['binary_sha256'], summary['installed'], summary['started']),
                         (ACTIVE, self.binary, 'now', 'now'))
        self.assertEqual(summary['warm_seconds'], 10.0)
        # Planned with the enrolled recent replicas and this host pinned to the whole archive.
        roster = self.plans[0]
        self.assertEqual([w['id'] for w in roster], ['transparent-pir-recent-01', 'transparent-pir-recent-02', NEW])
        self.assertEqual(roster[-1]['archive_range'], [0, 76])
        # Copied per directory at the bounded rate into the publication.
        publication = '/srv/transparent-pir/publications/' + ACTIVE
        self.assertEqual([c[0] for c in self.host.copies[1:]], [publication + '/'] * 3)
        self.assertTrue(all(c[1] == 60000 for c in self.host.copies[1:]))
        self.assertIn(publication + '/assignment.json', self.host.files)
        # The unit is the archive peer's, with this host's identity and budgets.
        unit = self.host.files[S.UNIT_PATH]
        args = S.transparent_unit.exec_args(unit)
        for flag, value in [('--worker-id', NEW), ('--shard-dir', publication), ('--cache-bytes', '51539607552'),
                            ('--runtime-cache-max-bytes', '103079215104'), ('--query-slots', '2'), ('--build-slots', '1'),
                            ('--assignment', publication + '/assignment.json')]:
            self.assertEqual(args[args.index(flag) + 1], value, flag)
        self.assertEqual(unit.count('MemoryMax=56G'), 1)
        self.assertEqual(unit.count('MemoryHigh=51539607552'), 1)
        self.assertEqual(self.peers.reads, ['10.142.1.1'])
        record = json.loads(self.host.files['/opt/transparent-publisher/active.json'])
        self.assertEqual(record, {'directory': publication, 'assignment': publication + '/assignment.json',
                                  'map_sha256': ACTIVE})

    def test_the_router_must_reach_the_warm_host(self):
        self.peers.unreachable = 2
        summary = self.standby().run()
        self.assertEqual(summary['router_reaches'], ROUTER)
        self.assertEqual(self.peers.dials, [(ROUTER, f'http://{HOST}:8093/v1/ready')] * 3)
        self.peers.unreachable = 10**6
        with self.assertRaisesRegex(S.StandbyError, 'cannot reach'):
            self.standby().run()

    def test_a_rerun_on_a_warm_host_changes_nothing(self):
        self.standby().run()
        commands, restarts = len(self.host.commands), self.host.restarts
        summary = self.standby().run()
        self.assertEqual((summary['installed'], summary['started'], summary['map_sha256']), ('already', 'already', ACTIVE))
        self.assertEqual(self.host.restarts, restarts)
        self.assertFalse(any('rsync' in c or 'systemctl restart' in c for c in self.host.commands[commands:]))
        # A newer publication with the same sealed archive is still warm enough.
        self.publish('d' * 64)
        I.atomic_json(self.state/'active.json', {'map_sha256': 'd' * 64, 'workers': []})
        self.assertEqual(self.standby().run()['map_sha256'], ACTIVE)

    def test_a_rerun_after_the_archive_changed_restages_from_hard_links(self):
        self.standby().run()
        self.publish('e' * 64, archive='resealed')
        I.atomic_json(self.state/'active.json', {'map_sha256': 'e' * 64, 'workers': []})
        summary = self.standby().run()
        self.assertEqual((summary['map_sha256'], summary['started']), ('e' * 64, 'now'))
        link = next(c for c in self.host.commands if c == 'sh -s')
        self.assertTrue(link)
        record = json.loads(self.host.files['/opt/transparent-publisher/active.json'])
        self.assertEqual(record['map_sha256'], 'e' * 64)

    def test_copying_pauses_while_publication_is_late(self):
        self.freshness = [45, 30, 5]
        with contextlib.redirect_stderr(io.StringIO()) as logged:
            self.standby().run()
        self.assertIn('standby_paused', logged.getvalue())
        self.assertGreaterEqual(self.sleeps, 2)
        self.freshness = [45] * 1000
        self.host = FakeHost()
        with self.assertRaisesRegex(S.StandbyError, 'stayed late'), contextlib.redirect_stderr(io.StringIO()):
            self.standby('--pause-limit', '30', '--work-dir', str(self.root/'w2')).run()

    def test_identity_cpu_release_and_membership_are_checked_first(self):
        self.host.droplet_id = '778'
        with self.assertRaisesRegex(S.StandbyError, 'metadata droplet id'):
            self.standby().run()
        self.host = FakeHost()
        self.host.cpu_ok = False
        with self.assertRaisesRegex(S.ACTUATOR.ActuatorError, 'avx2'):
            self.standby().run()
        self.host = FakeHost()
        (self.root/'releases'/'a704616c'/'shard-control').write_bytes(b'tampered')
        with self.assertRaisesRegex(S.StandbyError, 'does not match its SHA256SUMS'):
            self.standby().run()
        self.assertFalse(self.host.copies)
        args = S.parse(['--id', 'transparent-pir-archive-01', '--host', '10.142.1.9', '--droplet-id', '1',
                        '--known-hosts', 'k', '--release', 'r'])
        with self.assertRaisesRegex(S.StandbyError, 'already in the inventory'):
            S.Standby(args, self.fleet, self.host, self.peers).run()
        args = S.parse(['--id', NEW, '--host', '10.142.1.1', '--droplet-id', '1', '--known-hosts', 'k', '--release', 'r'])
        with self.assertRaisesRegex(S.StandbyError, "archive-01's address"):
            S.Standby(args, self.fleet, self.host, self.peers).run()

    def test_it_never_waits_forever_for_warmth(self):
        self.worker['polls'] = -10 ** 6
        with self.assertRaisesRegex(S.StandbyError, 'did not attest'):
            self.standby('--warm-timeout', '60').run()

    def test_arguments_are_validated(self):
        for bad in (['--id', 'transparent-pir-recent-05'], ['--release', '../x']):
            with self.subTest(bad=bad):
                base = dict(zip(['--id', '--host', '--droplet-id', '--known-hosts', '--release'],
                                [NEW, HOST, '1', 'k', 'r']))
                base[bad[0]] = bad[1]
                with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                    S.parse([x for kv in base.items() for x in kv])


if __name__ == '__main__':
    unittest.main()
