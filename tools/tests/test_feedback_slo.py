"""The timing CLI enforces warm/relink budgets using saved compile receipts."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
import contextlib
import io
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/ci'))
import timings


class FeedbackTests(unittest.TestCase):
    def test_final_timing_check_reports_before_failing_at_the_limit(self):
        for seconds, fails in [(120, False), (179.999, False), (180, True), (240, True)]:
            result = {'jobs': [{'name': 'Fast checks', 'job_seconds': seconds,
                                'reused_from_previous_attempt': False}]}
            with patch.object(sys, 'argv', ['timings.py', '--run', '1', '--check-fast-seconds', '180']), patch.object(timings, 'report', return_value=result), patch.dict(timings.os.environ, {'GITHUB_STEP_SUMMARY': '', 'WALLET_PIR_STAGE_LOG': '', 'WALLET_PIR_ARTIFACT_LOG': ''}), contextlib.redirect_stdout(io.StringIO()) as output:
                if fails:
                    with self.assertRaises(SystemExit):
                        timings.main()
                else:
                    timings.main()
                self.assertIn('job_seconds', output.getvalue())

    def test_workspace_relink_keeps_the_warm_budget_and_rejects_a_long_compile(self):
        warm = {'workspace_compiled': 0, 'third_party_compiled': 0, 'compile_stage_seconds': 0.7}
        relink = {'workspace_compiled': 28, 'third_party_compiled': 0, 'compile_stage_seconds': 151}
        self.assertEqual(timings.fast_budget({**warm, 'job_seconds': 130}, 180), 180)
        self.assertEqual(timings.fast_budget(relink, 180), 331)
        self.assertIsNone(timings.fast_budget({**relink, 'compile_stage_seconds': 301}, 180))
        # Dependency rebuilds and missing receipts stay on the warm target.
        self.assertEqual(timings.fast_budget({**relink, 'third_party_compiled': 2}, 180), 180)
        self.assertEqual(timings.fast_budget({'job_seconds': 200}, 180), 180)

        def check(job):
            result = {'jobs': [{**job, 'name': 'Fast checks', 'reused_from_previous_attempt': False}]}
            with patch.object(sys, 'argv', ['timings.py', '--run', '1', '--check-fast-seconds', '180']), patch.object(timings, 'report', return_value=result), patch.dict(timings.os.environ, {'GITHUB_STEP_SUMMARY': '', 'WALLET_PIR_STAGE_LOG': '', 'WALLET_PIR_ARTIFACT_LOG': ''}), contextlib.redirect_stdout(io.StringIO()):
                timings.main()

        check({**relink, 'job_seconds': 282})
        with self.assertRaises(SystemExit) as over:
            check({**relink, 'job_seconds': 340})
        self.assertIn('331', str(over.exception))
        with self.assertRaises(SystemExit) as ceiling:
            check({**relink, 'compile_stage_seconds': 301, 'job_seconds': 200})
        self.assertIn('ceiling', str(ceiling.exception))

    def test_compile_receipts_name_the_workspace_relink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'stages.jsonl').write_text(json.dumps({
                'stage': 'compile:fast', 'seconds': 151.0, 'exit': 0, 'id': 'compile', 'parent': None}) + '\n')
            (root / 'artifacts.jsonl').write_text(json.dumps({
                'unit': 'query-server-lib', 'fresh': False, 'source': 'path',
                'package': 'transparent-query-server', 'command': 'build'}) + '\n')
            evidence = timings.compile_evidence({
                'WALLET_PIR_STAGE_LOG': str(root / 'stages.jsonl'),
                'WALLET_PIR_ARTIFACT_LOG': str(root / 'artifacts.jsonl')})
        self.assertEqual(evidence['workspace_compiled'], 1)
        self.assertEqual(evidence['third_party_compiled'], 0)
        self.assertEqual(evidence['compile_stage_seconds'], 151.0)
        self.assertEqual(timings.fast_budget({**evidence, 'job_seconds': 280}, 180), 331)
