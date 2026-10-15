"""Staging diagnostics preserve concurrent attribution and cancellation semantics,
and staging carries the runtimes a publisher prepared."""
import asyncio
from contextlib import redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

SPEC = importlib.util.spec_from_file_location('fleet_timing', Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class StageTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_control_timings_exclude_other_response_and_request_fields(self):
        fleet = object.__new__(M.Fleet)
        fleet.c = {}
        fleet.read_only = False
        details = {'loading_seconds': 1.25, 'warming_seconds': 2.5,
                   'unrelated': 'private response payload'}
        fleet.ssh = AsyncMock(return_value=json.dumps({'ok': True, 'result': details}).encode())
        output = io.StringIO()
        with redirect_stderr(output):
            result = await fleet.control({'id': 'a', 'ssh_host': 'worker'},
                                         {'operation': 'prepare', 'unrelated': 'private request payload'})
        self.assertEqual(result, details)
        self.assertEqual(json.loads(output.getvalue()),
                         {'event': 'worker_control_timing', 'worker': 'a', 'operation': 'prepare',
                          'loading_seconds': 1.25, 'warming_seconds': 2.5})
        self.assertNotIn('private', output.getvalue())

    async def test_two_workers_on_one_host_use_their_own_control_sockets(self):
        fleet = object.__new__(M.Fleet)
        fleet.c = {'control_socket': '/run/transparent-pir/control.sock'}
        fleet.read_only = False
        fleet.ssh = AsyncMock(return_value=json.dumps({'ok': True, 'result': {}}).encode())
        archive = {'id': 'v12-archive', 'ssh_host': 'host', 'control_socket': '/run/transparent-pir/v12-archive.sock'}
        recent = {'id': 'v12-recent', 'ssh_host': 'host'}
        with redirect_stderr(io.StringIO()):
            await fleet.control(archive, {'operation': 'status'})
            await fleet.control(recent, {'operation': 'status'})
        commands = [' '.join(str(a) for a in call.args) for call in fleet.ssh.await_args_list]
        self.assertIn('/run/transparent-pir/v12-archive.sock', commands[0])
        self.assertIn('/run/transparent-pir/control.sock', commands[1])
        self.assertNotIn('/run/transparent-pir/control.sock', commands[0])

    async def test_concurrent_stages_keep_worker_and_revision_identity(self):
        fleet = object.__new__(M.Fleet)
        entered = asyncio.Event()
        release = asyncio.Event()
        async def first():
            async with fleet.stage_timing({'id':'a', 'ssh_host':'host-a'}, {'map_sha256':'a'*64}, 'prepare'):
                entered.set()
                await release.wait()
        async def second():
            await entered.wait()
            async with fleet.stage_timing({'id':'b', 'ssh_host':'host-b'}, {'map_sha256':'b'*64}, 'transfer'):
                release.set()
        output = io.StringIO()
        with redirect_stderr(output):
            await asyncio.gather(first(), second())
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        for name, phase in [('a','prepare'), ('b','transfer')]:
            pair = [row for row in rows if row['worker'] == name]
            self.assertEqual([r['state'] for r in pair], ['started','completed'])
            self.assertTrue(all(r['host'] == 'host-'+name and r['phase'] == phase and r['map_sha256'] == name*64 for r in pair))
            self.assertGreaterEqual(pair[1]['seconds'], 0)

    async def test_error_and_cancellation_propagate_without_logging_payload(self):
        fleet = object.__new__(M.Fleet)
        for error in [RuntimeError('private command payload'), asyncio.CancelledError()]:
            output = io.StringIO()
            with redirect_stderr(output):
                with self.assertRaises(type(error)) as caught:
                    async with fleet.stage_timing({'id':'a','ssh_host':'host'}, {'map_sha256':'a'*64}, 'prepare'):
                        raise error
            self.assertIs(caught.exception, error)
            rows = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual(rows[-1]['state'], type(error).__name__)
            self.assertNotIn('private command payload', output.getvalue())

    async def test_staging_never_shares_the_owned_control_connection(self):
        fleet = object.__new__(M.Fleet)
        fleet.read_only = False
        fleet.c = {'control_sessions': True, 'known_hosts': 'known', 'ssh_key': 'key'}
        fleet.roster = [{'id':'a', 'ssh_host':'worker'}]
        fleet.control_dir = Path('/private/control')
        fleet.direct_ssh_args = ['ssh', '-oBatchMode=yes']
        fleet.ssh_args = ['ssh', '-oControlMaster=auto']
        # Status reads travel over the owned control master; a bulk transfer
        # on the same connection delayed them past their budget.
        control = fleet.control_session_args(fleet.roster[0])
        self.assertIn('-oProxyCommand=false', control)
        self.assertEqual(fleet.transfer_ssh_args('worker'), fleet.ssh_args)
        self.assertEqual(fleet.transfer_ssh_args('router'), fleet.ssh_args)
        with patch.object(M, 'run', new=AsyncMock(return_value=b'ok')) as run:
            self.assertEqual(await fleet.ssh('worker', 'true'), b'ok')
            self.assertEqual(run.call_args.args[0], fleet.ssh_args + ['root@worker', 'true'])
            self.assertNotIn(str(fleet.control_path(fleet.roster[0])), ' '.join(run.call_args.args[0]))
            self.assertTrue(run.call_args.kwargs['file_output'])
            await fleet.ssh('worker', 'true', multiplex=False)
            self.assertIn('-oControlPath=none', run.call_args.args[0])
        with patch.object(M, 'run', new=AsyncMock(side_effect=RuntimeError('master unavailable'))) as run:
            with self.assertRaisesRegex(RuntimeError, 'master unavailable'):
                await fleet.ssh('worker', 'true')
            self.assertEqual(run.await_count, 1)
        fleet.c['control_sessions'] = False
        self.assertEqual(fleet.transfer_ssh_args('worker'), fleet.ssh_args)


# Staging a publication with the runtimes its publisher prepared: the worker
# is asked for only its own shards' runtimes, links any the active
# publication already holds instead of receiving them again, and is told
# where they are; without an offer, staging is unchanged.
REVISION = 'ab' * 32
RUNTIME = 'cd' * 32 + '-' + 'ef' * 32 + '.runtime'
STATS = b'''Number of files: 3
Total transferred file size: 40,000 bytes
Total bytes sent: 41,234
Total bytes received: 210
'''


class RuntimeStagingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.worker = dict(id='recent', role='recent-replica', ssh_host='host', upstream='host:8093')
        (self.root / 'roster').write_text(json.dumps([self.worker]))
        self.fleet = M.Fleet(dict(roster=str(self.root / 'roster'), state_dir=str(self.root / 'state'),
                                  known_hosts='unused', ssh_key='unused', assign_binary='shard-assign'))
        self.source = self.root / 'candidate'
        self.source.mkdir()
        (self.source / 'shards.json').write_text('{"shards":[]}')
        self.assignment = self.root / 'assignment.json'
        self.assignment.write_text('{}')
        self.controls = []
        self.scripts = []
        self.commands = []

        async def control(worker, value):
            self.controls.append(value)
            if value['operation'] == 'status':
                return {'active': {'map_sha256': '11' * 32, 'directory': '/srv/old'}}
            if value['operation'] == 'prepare':
                return {'prepared': value['publication']['map_sha256'],
                        'runtimes': {'shipped': 2, 'shipped_fallbacks': 0, 'built': 0}}
            return {}

        async def ssh(host, command, data=None, **kwargs):
            self.scripts.append((data or b'').decode())
            return b''

        async def run(args, data=None, timeout=25, file_output=False):
            args = [str(a) for a in args]
            self.commands.append(args)
            if args[1] == 'files':
                listed = ['shards.json', REVISION + '/']
                if '--runtimes' in args:
                    listed.append(RUNTIME)
                return ('\n'.join(listed) + '\n').encode()
            return STATS

        self.fleet.control = AsyncMock(side_effect=control)
        self.fleet.revoke_orphans = AsyncMock()
        self.fleet.ssh = AsyncMock(side_effect=ssh)
        self.patch = patch.object(M, 'run', side_effect=run)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def request(self, runtimes):
        req = {'map_sha256': '22' * 32, 'directory': str(self.source)}
        if runtimes:
            req['runtimes'] = str(self.source)
        return req

    async def test_offered_runtimes_are_listed_linked_and_named_to_the_worker(self):
        staged = await self.fleet.stage(self.worker, self.request(True), self.assignment)
        inventory = next(c for c in self.commands if c[1] == 'files')
        self.assertIn('--runtimes', inventory)
        # The runtime file the active publication holds is linked, never
        # copied over the network again; revision directories as before.
        script = self.scripts[0]
        self.assertIn(f'ln /srv/old/{RUNTIME} ', script)
        self.assertIn(f'cp -al /srv/old/{REVISION} ', script)
        rsync = next(c for c in self.commands if c[0] == 'rsync')
        self.assertIn('--ignore-existing', rsync)
        self.assertIn('--stats', rsync)
        prepare = next(c for c in self.controls if c['operation'] == 'prepare')
        remote = prepare['publication']['directory']
        self.assertEqual(prepare['publication']['runtimes'], remote)
        self.assertEqual(staged['transferred_bytes'], 41234)
        self.assertEqual(staged['runtimes']['shipped'], 2)
        self.assertEqual(staged['publication']['runtimes'], remote)

    async def test_without_an_offer_staging_is_unchanged(self):
        staged = await self.fleet.stage(self.worker, self.request(False), self.assignment)
        inventory = next(c for c in self.commands if c[1] == 'files')
        self.assertNotIn('--runtimes', inventory)
        self.assertNotIn('.runtime', self.scripts[0])
        prepare = next(c for c in self.controls if c['operation'] == 'prepare')
        self.assertNotIn('runtimes', prepare['publication'])
        self.assertNotIn('runtimes', staged['publication'])

    async def test_runtimes_elsewhere_than_the_publication_are_refused(self):
        req = self.request(True)
        other = self.root / 'other'
        other.mkdir()
        req['runtimes'] = str(other)
        with self.assertRaises(ValueError):
            await self.fleet.stage(self.worker, req, self.assignment)
        self.assertFalse(any(c['operation'] == 'prepare' for c in self.controls))

    async def test_an_unexpected_runtime_name_is_refused_before_anything_is_run(self):
        async def run(args, data=None, timeout=25, file_output=False):
            return b'shards.json\n../escape.runtime\n'
        with patch.object(M, 'run', side_effect=run):
            with self.assertRaises(ValueError):
                await self.fleet.stage(self.worker, self.request(True), self.assignment)
        self.assertEqual(self.scripts, [])

    def test_transfer_bytes_reads_rsync_stats(self):
        self.assertEqual(M.transfer_bytes(STATS), 41234)
        self.assertIsNone(M.transfer_bytes(b''))
        self.assertIsNone(M.transfer_bytes(None))
