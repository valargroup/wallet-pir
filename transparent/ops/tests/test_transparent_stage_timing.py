"""Staging diagnostics preserve concurrent attribution and cancellation semantics."""
import asyncio
from contextlib import redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
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
