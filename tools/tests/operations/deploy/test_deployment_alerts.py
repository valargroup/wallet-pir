"""Deployment notifications bracket existing validation without changing it."""
import contextlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / 'tools/lib'))
from wallet_pir_ops.deploy import deployment_alerts, remote


class Monitor:
    def __init__(self):
        self.calls = []
        self.locked = False
        self.refuse = False

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        self.locked = True
        try:
            yield self
        finally:
            self.locked = False

    def run(self, host, argv, timeout):
        self.calls.append((self.locked, argv[4:]))
        if self.refuse:
            return 1, 'monitor refused the request'
        return 0, json.dumps({'outcome': None})


class DeploymentNotificationsTests(unittest.TestCase):
    def setUp(self):
        self.inventory = SimpleNamespace(lock={'type': 'remote', 'host': 'coordinator'},
                                         services={'enhance': {}, 'status': {}})
        self.monitor = Monitor()

    def test_window_starts_under_lock_and_finishes_after_one_validation(self):
        validations = []
        with deployment_alerts.Notifications() as notification:
            with remote.production_lock(self.inventory, self.monitor):
                validations.append('single existing pass')
            notification.finish()
        self.assertEqual(validations, ['single existing pass'])
        self.assertEqual(self.monitor.calls, [(True, ['begin', 'enhance', 'status']), (False, ['finish'])])

    def test_failed_validation_restores_alerts_before_rollback(self):
        with deployment_alerts.Notifications('status'):
            with remote.production_lock(self.inventory, self.monitor):
                deployment_alerts.failed()
                self.assertEqual(self.monitor.calls[-1], (True, ['finish', '--failed']))
                # A rollback may take the lock again; it must not reopen suppression.
                with remote.production_lock(self.inventory, self.monitor):
                    pass
        self.assertEqual(len(self.monitor.calls), 2)

    def test_exception_restores_alerts_and_failed_begin_prevents_mutation(self):
        with self.assertRaisesRegex(RuntimeError, 'validation'):
            with deployment_alerts.Notifications('status'):
                with remote.production_lock(self.inventory, self.monitor):
                    raise RuntimeError('validation failed')
        self.assertEqual(self.monitor.calls[-1][1], ['finish', '--failed'])
        self.monitor.refuse = True
        changed = []
        with self.assertRaisesRegex(ValueError, 'control failed'):
            with deployment_alerts.Notifications('status'):
                with remote.production_lock(self.inventory, self.monitor):
                    changed.append('service change')
        self.assertFalse(changed)

    def test_read_only_lock_does_not_contact_monitor(self):
        with remote.production_lock(self.inventory, self.monitor):
            pass
        self.assertFalse(self.monitor.calls)

    def test_publisher_recovery_commands_bracket_the_existing_operation(self):
        from wallet_pir_ops.deploy import cli
        for command in ('publisher-cutover-continue', 'publisher-cutover-reconcile',
                        'publisher-cutover-repair-release', 'publisher-cutover-repair-sandbox'):
            with self.subTest(command=command):
                self.monitor.calls.clear()
                def operation(*_args, **_kwargs):
                    with remote.production_lock(self.inventory, self.monitor):
                        self.assertIsNotNone(deployment_alerts.CURRENT.get())
                    return 0
                with patch.object(cli, 'parser') as parser, patch.object(cli, 'run', side_effect=operation):
                    parser.return_value.parse_args.return_value = SimpleNamespace(command=command, service='publisher')
                    self.assertEqual(cli.main([]), 0)
                self.assertEqual(self.monitor.calls, [(True, ['begin', 'publisher']), (False, ['finish'])])

    def test_publisher_dispatch_restores_alerts_before_automatic_rollback(self):
        from wallet_pir_ops.deploy import publisher
        args = SimpleNamespace(command='publisher-cutover-deploy', request='request', request_sha256='hash',
                               state_dir='state', archive='archive', expect_plan_sha256='plan', serving_binary=None)
        request = {'release': {'git_sha': 'revision', 'binary_sha256': 'digest'}}
        def extract(_archive, bundle, *_args):
            bundle.mkdir()
            (bundle / 'candidate.json').write_text(json.dumps({'source_dirty': False}))
        with patch.object(publisher, 'load_request', return_value=(request, self.inventory)), \
                patch.object(publisher, 'Cutover') as cutover, \
                patch.object(publisher.release, 'extract', side_effect=extract), \
                patch.object(publisher, 'file_sha256', return_value='digest'), \
                patch.object(deployment_alerts, 'failed') as resumed:
            target = cutover.return_value
            target.deploy.side_effect = RuntimeError('validation failed')
            target.latest.return_value = {'request': request, 'phase': 'verifying'}
            target.rollback.side_effect = lambda: resumed.assert_called_once_with()
            with self.assertRaisesRegex(RuntimeError, 'validation failed'):
                publisher.dispatch(args, executor=self.monitor)
            target.rollback.assert_called_once_with()
