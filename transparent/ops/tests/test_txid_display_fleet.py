#!/usr/bin/env python3
"""The txid display command transport and observers, against shimmed ssh and rsync.

The ssh shim records its argv and runs the remote command locally; the rsync
shim records its argv and runs the real rsync with the remote prefix removed,
so publication atomicity and hard-link reuse are the real filesystem's. The
worker's `txid-control` is a script that answers like the control socket.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


F = load('txid_display_fleet', ROOT / 'transparent/ops/scripts/txid-display-fleet.py')
O = load('txid_display_observe', ROOT / 'transparent/ops/scripts/txid-display-observe.py')
REAL_RSYNC = shutil.which('rsync')
DIGEST = 'a' * 64
PREVIOUS = 'b' * 64
REVISION = 'c' * 64

SSH_SHIM = r'''#!/usr/bin/env python3
import json, os, subprocess, sys
with open(os.environ['SHIM_LOG'], 'a') as log:
    log.write(json.dumps({'tool': 'ssh', 'argv': sys.argv[1:]}) + '\n')
if os.environ.get('SHIM_SSH_FAIL'):
    sys.stderr.write('ssh: connect to host: Connection timed out\n')
    sys.exit(255)
sys.exit(subprocess.run(['sh', '-c', sys.argv[-1]]).returncode)
'''
RSYNC_SHIM = r'''#!/usr/bin/env python3
import json, os, subprocess, sys
argv = sys.argv[1:]
with open(os.environ['SHIM_LOG'], 'a') as log:
    log.write(json.dumps({'tool': 'rsync', 'argv': argv}) + '\n')
index = argv.index('-e')
local = argv[:index] + argv[index + 2:]
local[-1] = local[-1].split(':', 1)[1]
if os.environ.get('SHIM_RSYNC_FAIL'):
    os.makedirs(local[-1], exist_ok=True)
    open(os.path.join(local[-1], 'partial'), 'w').write('torn')
    sys.exit(23)
sys.exit(subprocess.run([os.environ['REAL_RSYNC'], *local]).returncode)
'''
CONTROL = r'''#!/usr/bin/env python3
import json, os, sys
command = json.loads(sys.stdin.readline())
with open(os.environ['CONTROL_LOG'], 'a') as log:
    log.write(json.dumps({'socket': sys.argv[1], 'command': command}) + '\n')
if command['operation'] == 'invalidate':
    print(json.dumps({'ok': False, 'error': 'reorg reaches sealed display shard 4'}))
    sys.exit(1)
print(json.dumps({'ok': True, 'role': 'recent-replica', 'active': None, 'staged': [], 'warm': False,
                  'binary_sha256': 'f' * 64}))
'''


@unittest.skipUnless(REAL_RSYNC, 'rsync is required')
class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        shims = self.root / 'bin'
        shims.mkdir()
        for name, text in (('ssh', SSH_SHIM), ('rsync', RSYNC_SHIM)):
            (shims / name).write_text(text)
            (shims / name).chmod(0o755)
        control = self.root / 'worker/opt/txid-control'
        control.parent.mkdir(parents=True)
        control.write_text(CONTROL)
        control.chmod(0o755)
        self.log, self.control_log = self.root / 'shim.log', self.root / 'control.log'
        self.publications, self.staged = self.root / 'worker/publications', self.root / 'worker/staged'
        self.config = {
            'schema': F.SCHEMA, 'ssh_key': '/opt/transparent-publisher/credentials/deploy-ssh',
            'known_hosts': '/opt/transparent-publisher/credentials/known_hosts',
            'control_dir': str(self.root / 'state/ssh'),
            'workers': {'recent': {'ssh_host': '10.142.0.6', 'user': 'root', 'publications': str(self.publications),
                                   'staged': str(self.staged), 'control_socket': '/run/transparent-txid-display/control.sock',
                                   'control_binary': str(control)}}}
        (self.root / 'fleet.json').write_text(json.dumps(self.config))
        self.source = self.root / 'coordinator/candidate-100-x-1'
        (self.source / REVISION).mkdir(parents=True)
        (self.source / 'txid-shards.json').write_text('{"shards": []}')
        (self.source / REVISION / 'manifest.json').write_text('{"shard_id": 4}')
        (self.source / REVISION / 'pages.0.bin').write_bytes(b'\0' * 4096)
        self.env = patch.dict(os.environ, PATH=str(shims) + os.pathsep + os.environ['PATH'], SHIM_LOG=str(self.log),
                              CONTROL_LOG=str(self.control_log), REAL_RSYNC=REAL_RSYNC)
        self.env.start()
        self.addCleanup(self.env.stop)

    def call(self, request):
        path = self.root / 'request.json'
        path.write_text(json.dumps(request))
        reply = self.root / 'reply.json'
        with contextlib.redirect_stderr(io.StringIO()):
            code = F.main([str(self.root / 'fleet.json'), str(path), str(reply)])
        return code, json.loads(reply.read_text())

    def calls(self, tool=None):
        if not self.log.exists():
            return []
        entries = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [entry['argv'] for entry in entries if tool is None or entry['tool'] == tool]

    def ship(self, **overrides):
        return self.call({'operation': 'ship', 'worker': 'recent', 'kind': 'candidate', 'source': str(self.source),
                          'name': DIGEST, 'link_dest': None, **overrides})

    def test_ship_publishes_by_rename_with_pinned_multiplexed_ssh(self):
        code, reply = self.ship()
        self.assertEqual((code, reply['ok'], reply['directory']), (0, True, '%s/%s' % (self.publications, DIGEST)))
        final = self.publications / DIGEST
        self.assertEqual((final / REVISION / 'manifest.json').read_text(), '{"shard_id": 4}')
        self.assertFalse((self.publications / ('.tmp-' + DIGEST)).exists())
        probe, publish = self.calls('ssh')
        options = probe[:-2]
        for option in ('-oBatchMode=yes', '-oStrictHostKeyChecking=yes', '-oControlMaster=auto',
                       'UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts',
                       'ControlPath=%s/ship-%%C' % (self.root / 'state/ssh')):
            self.assertIn(option, options)
        self.assertEqual(options[options.index('-i') + 1], '/opt/transparent-publisher/credentials/deploy-ssh')
        self.assertEqual(probe[-2], 'root@10.142.0.6')
        self.assertIn('mv -T', publish[-1])
        rsync, = self.calls('rsync')
        self.assertEqual(rsync[:3], ['-a', '--delete', '--numeric-ids'])
        self.assertIn('--link-dest=%s' % self.staged, rsync)
        self.assertEqual(rsync[-2:], [str(self.source) + '/', 'root@10.142.0.6:%s/.tmp-%s/' % (self.publications,
                                                                                               DIGEST)])
        self.assertIn('ControlPath=%s/ship-%%C' % (self.root / 'state/ssh'), rsync[rsync.index('-e') + 1])
        # A content-addressed directory that exists is complete: reused, not copied again.
        code, reply = self.ship()
        self.assertEqual((code, reply.get('reused')), (0, True))
        self.assertEqual(len(self.calls('rsync')), 1)

    def test_previous_candidate_and_staged_revisions_are_hard_linked(self):
        previous = self.publications / PREVIOUS
        self.assertEqual(self.ship(name=PREVIOUS)[0], 0)
        code, reply = self.ship(link_dest=str(previous))
        self.assertEqual(code, 0)
        self.assertIn('--link-dest=' + str(previous), self.calls('rsync')[-1])
        old, new = previous / REVISION / 'pages.0.bin', self.publications / DIGEST / REVISION / 'pages.0.bin'
        self.assertEqual(old.stat().st_ino, new.stat().st_ino)
        # A sealed revision staged first is reused by the candidate that includes it.
        code, reply = self.call({'operation': 'ship', 'worker': 'recent', 'kind': 'staged',
                                 'source': str(self.source / REVISION), 'name': REVISION, 'link_dest': None})
        self.assertEqual((code, reply['directory']), (0, '%s/%s' % (self.staged, REVISION)))
        self.assertNotIn('--link-dest=' + str(self.staged), self.calls('rsync')[-1])
        third = 'd' * 64
        self.assertEqual(self.ship(name=third)[0], 0)
        self.assertEqual((self.staged / REVISION / 'pages.0.bin').stat().st_ino,
                         (self.publications / third / REVISION / 'pages.0.bin').stat().st_ino)
        # A link source that no longer exists is dropped rather than failing the copy.
        self.assertEqual(self.ship(name='e' * 64, link_dest=str(self.publications / ('f' * 64)))[0], 0)
        self.assertFalse([a for a in self.calls('rsync')[-1] if a.endswith('f' * 64)])

    def test_a_torn_copy_is_never_published_and_resumes(self):
        with patch.dict(os.environ, SHIM_RSYNC_FAIL='1'):
            code, reply = self.ship()
        self.assertEqual((code, reply['ok']), (1, False))
        self.assertIn('rsync exited 23', reply['error'])
        self.assertFalse((self.publications / DIGEST).exists())
        self.assertTrue((self.publications / ('.tmp-' + DIGEST) / 'partial').exists())
        code, reply = self.ship()
        self.assertEqual(code, 0)
        self.assertFalse((self.publications / DIGEST / 'partial').exists())
        self.assertFalse((self.publications / ('.tmp-' + DIGEST)).exists())

    def test_ship_refuses_unsafe_requests_before_any_transfer(self):
        for overrides in ({'name': 'not-a-digest'}, {'kind': 'everything'}, {'source': str(self.root / 'missing')},
                          {'link_dest': '/srv/transparent-pir/publications/' + PREVIOUS},
                          {'link_dest': str(self.publications) + '/../x'}, {'worker': 'archive'},
                          {'extra': True}):
            with self.subTest(overrides=overrides):
                code, reply = self.ship(**overrides)
                self.assertEqual((code, reply['ok']), (1, False))
        self.assertEqual(self.calls(), [])

    def test_control_passes_the_worker_reply_through(self):
        code, reply = self.call({'operation': 'control', 'worker': 'recent', 'command': {'operation': 'status'}})
        self.assertEqual(code, 0)
        self.assertEqual(reply['reply']['role'], 'recent-replica')
        argv, = self.calls('ssh')
        self.assertIn('ControlPath=%s/control-%%C' % (self.root / 'state/ssh'), argv)
        self.assertEqual(argv[-1], '%s /run/transparent-txid-display/control.sock || [ "$?" -eq 1 ]'
                         % self.config['workers']['recent']['control_binary'])
        sent, = [json.loads(line) for line in self.control_log.read_text().splitlines()]
        self.assertEqual(sent, {'socket': '/run/transparent-txid-display/control.sock',
                                'command': {'operation': 'status'}})

    def test_a_worker_refusal_or_lost_transport_fails_closed(self):
        code, reply = self.call({'operation': 'control', 'worker': 'recent',
                                 'command': {'operation': 'invalidate', 'expected': DIGEST, 'from_height': 7}})
        self.assertEqual((code, reply['ok']), (1, False))
        self.assertEqual(reply['error'], 'reorg reaches sealed display shard 4')
        self.assertEqual(reply['reply']['ok'], False)
        with patch.dict(os.environ, SHIM_SSH_FAIL='1'):
            code, reply = self.call({'operation': 'control', 'worker': 'recent', 'command': {'operation': 'status'}})
        self.assertEqual((code, reply['ok']), (1, False))
        self.assertIn('ssh exited 255', reply['error'])

    def test_control_paths_stay_in_the_worker_roots(self):
        for command in ({'operation': 'stage', 'directory': '/srv/transparent-pir/sets/' + REVISION},
                        {'operation': 'prepare', 'expected': '', 'publication': {
                            'directory': str(self.staged / DIGEST), 'map_sha256': DIGEST}},
                        {'operation': 'shutdown'}):
            with self.subTest(command=command):
                code, reply = self.call({'operation': 'control', 'worker': 'recent', 'command': command})
                self.assertEqual((code, reply['ok']), (1, False))
        self.assertEqual(self.calls(), [])

    def test_deadlines_follow_the_controller_contract(self):
        seen = []

        def run(argv, input=None, capture_output=True, timeout=None):
            seen.append(timeout)
            return subprocess.CompletedProcess(argv, 0, b'{"ok": true}\n', b'')
        fleet = F.Fleet(F.load_config(self.root / 'fleet.json'), run=run)
        stage = {'operation': 'stage', 'directory': str(self.staged / REVISION)}
        with contextlib.redirect_stderr(io.StringIO()):
            for command, deadline in (({'operation': 'status'}, None), (stage, None), ({'operation': 'status'}, 1800)):
                fleet.handle({'operation': 'control', 'worker': 'recent', 'command': command,
                              **({'deadline_seconds': deadline} if deadline else {})})
        self.assertEqual(seen, [85, 590, 1800])
        with self.assertRaisesRegex(F.FleetError, 'deadline'):
            fleet.handle({'operation': 'control', 'worker': 'recent', 'command': {'operation': 'status'},
                          'deadline_seconds': 4000})

    def test_config_refuses_history_paths(self):
        for key, value in (('publications', '/srv/transparent-pir/publications'),
                           ('control_socket', '/run/transparent-pir/control.sock'),
                           ('control_binary', '/opt/transparent-publisher/shard-control')):
            with self.subTest(key=key):
                config = json.loads(json.dumps(self.config))
                config['workers']['recent'][key] = value
                (self.root / 'bad.json').write_text(json.dumps(config))
                with self.assertRaisesRegex(F.FleetError, 'history path'):
                    F.load_config(self.root / 'bad.json')

    def test_stdin_and_stdout_by_default(self):
        request = json.dumps({'operation': 'control', 'worker': 'recent', 'command': {'operation': 'status'}})
        result = subprocess.run([sys.executable, str(ROOT / 'transparent/ops/scripts/txid-display-fleet.py'),
                                 str(self.root / 'fleet.json')], input=request, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)['ok'])


class ObserverTests(unittest.TestCase):
    def test_observer_records_each_new_tip_once(self):
        tips = iter(['h1', 'h1', 'h2', RuntimeError('rpc down'), 'h3'])
        events, clock = [], [100.0]

        def call(method, params=()):
            if method == 'getbestblockhash':
                value = next(tips)
                if isinstance(value, Exception):
                    raise value
                return value
            if method == 'getblockheader' and params[0] == 'h3':
                raise RuntimeError('unsupported')
            if method == 'getblockheader':
                return {'height': {'h1': 10, 'h2': 11}[params[0]]}
            return 12

        def sleep(seconds):
            clock[0] += seconds
        O.observe(call, lambda event, **fields: events.append((event, fields)), 0.25, lambda: clock[0], sleep, 5)
        self.assertEqual([(e, f.get('hash'), f.get('height')) for e, f in events],
                         [('tip', 'h1', 10), ('tip', 'h2', 11), ('error', None, None), ('tip', 'h3', 12)])
        self.assertEqual(clock[0], 101.25)

    def test_mapwatch_keeps_maps_checks_manifests_and_sealed_identity(self):
        manifests = {i: json.dumps({'shard_id': i}).encode() for i in range(4)}
        digest = {i: hashlib.sha256(manifests[i]).hexdigest() for i in manifests}

        def entry(i, sealed, end, manifest=None):
            return {'shard_id': i, 'start_height': i * 10, 'end_height': end, 'sealed': sealed, 'records': 10,
                    'min_bucket_records': 10, 'revision': 0, 'manifest_digest': manifest or digest[i]}
        maps = [
            {'first_shard_id': 0, 'start_height': 0, 'shards': [entry(0, True, 9), entry(1, False, 12)]},
            {'first_shard_id': 0, 'start_height': 0, 'shards': [entry(0, True, 9), entry(1, True, 19), entry(2, False, 21)]},
            {'first_shard_id': 1, 'start_height': 10, 'shards': [entry(1, True, 19, digest[3]), entry(2, False, 22)]},
        ]
        bodies = [json.dumps(m).encode() for m in maps]
        sequence = iter([bodies[0], bodies[0], bodies[1], bodies[2]])
        served = dict(manifests)
        served[3] = b'not the manifest'

        def fetch(url):
            if url.endswith('/v1/txid/shards'):
                body = next(sequence)
                return 200, {'x-txid-map-sha256': hashlib.sha256(body).hexdigest()}, body
            shard = int(url.split('/shards/')[1].split('/')[0])
            wanted = url.split('/revisions/')[1].split('/')[0]
            return 200, {}, served[3] if wanted == digest[3] else served[shard]
        with tempfile.TemporaryDirectory() as tmp:
            events = []
            watch = O.MapWatch('https://pir.example/v1/txid/shards', tmp, fetch,
                               lambda event, **fields: events.append((event, fields)), now=lambda: 1.0)
            for _ in range(4):
                watch.poll()
            kinds = [event for event, _ in events]
            self.assertEqual(kinds, ['map', 'map', 'manifest_digest_mismatch', 'sealed_changed', 'window_drop', 'map'])
            first = events[0][1]
            self.assertEqual((first['header_sha256'], first['sealed']), (first['map_sha256'], 1))
            self.assertEqual(events[3][1]['shard_id'], 1)
            self.assertEqual(events[4][1]['shard_ids'], [0])
            saved = sorted(p.name for p in (Path(tmp) / 'manifests').iterdir())
            self.assertEqual(saved, sorted(digest[i] + '.json' for i in (0, 1, 2)))
            self.assertEqual(len(list((Path(tmp) / 'maps').iterdir())), 3)


if __name__ == '__main__':
    unittest.main()
