"""Status uses regular deployment commands after the HTTPS cutover."""
import contextlib
import io
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import cli  # noqa: E402


class StatusCommands(unittest.TestCase):
    def test_retired_https_cutover_commands_are_rejected_by_the_parser(self):
        parser = cli.parser()
        for operation in ('plan', 'preflight', 'deploy', 'resume', 'status', 'rollback'):
            with self.subTest(operation=operation), contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as stopped:
                    parser.parse_args(['status-https-' + operation])
                self.assertEqual(stopped.exception.code, 2)
                self.assertIn('invalid choice', errors.getvalue())
                self.assertNotIn('status-https-', parser.format_help())

    def test_regular_status_deployment_and_wallet_routing_commands_remain(self):
        parser = cli.parser()
        for operation in ('status', 'rollback'):
            with self.subTest(operation=operation):
                args = parser.parse_args([operation, 'status'])
                self.assertEqual(args.command, operation)
                self.assertEqual(args.service, 'status')
        args = parser.parse_args(['status-routing-plan', '--request', '/request.json',
                                  '--request-sha256', 'a' * 64])
        self.assertEqual(args.command, 'status-routing-plan')
        self.assertIn('publisher-cutover-plan', parser.format_help())
