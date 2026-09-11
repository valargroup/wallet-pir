"""Staging diagnostics preserve concurrent attribution and cancellation semantics."""
import asyncio
from contextlib import redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('fleet_timing', Path(__file__).resolve().parents[1]/'scripts/transparent-live-fleet.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class StageTimingTests(unittest.IsolatedAsyncioTestCase):
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
