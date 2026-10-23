"""Current publisher updates cannot reopen retired migration workflows."""
import contextlib
import copy
import io
import json
import unittest

from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import cli
from wallet_pir_ops.deploy.engine import DeployError


class PublisherRetirementTests(PublisherFixture, unittest.TestCase):
    def test_retired_request_schemas_are_rejected_without_mutations(self):
        for schema in ('transparent-publisher-cutover/1', 'publisher-cutover/2',
                       'publisher-https-cutover/1', 'enhance-https-cutover/1'):
            with self.subTest(schema=schema):
                self.request['schema'] = schema
                with self.assertRaisesRegex(DeployError, 'only publisher-cutover/3'):
                    self.make_target()
                self.assertEqual(self.ex.mutations(), [])

    def test_storage_and_https_handoff_fields_are_rejected_without_mutations(self):
        for field in ('prepared_storage', 'https', 'enhance_https'):
            with self.subTest(field=field):
                self.request[field] = {}
                with self.assertRaisesRegex(DeployError, 'unknown or missing fields'):
                    self.make_target()
                self.request.pop(field)
                self.assertEqual(self.ex.mutations(), [])

    def test_updates_do_not_translate_old_installed_settings(self):
        current = copy.deepcopy(self.request['configuration'])
        for product, field in (('transparent_history', 'http_only'),
                               ('transaction_details', 'trust_sealed')):
            with self.subTest(product=product, field=field):
                installed = copy.deepcopy(current)
                installed[product]['settings'][field] = False
                self.ex.put('coordinator', '/etc/wallet-pir/publisher.json', json.dumps(installed))
                with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
                    self.make_target().plan()
                self.assertEqual(self.ex.mutations(), [])

    def test_retired_commands_and_options_are_not_registered(self):
        parser = cli.parser()
        commands = ('publisher-storage-prepare', 'publisher-storage-cleanup',
                    'publisher-cutover-run', 'publisher-cutover-watchdog',
                    'publisher-cutover-repair-enhance', 'publisher-cutover-prune-qualification')
        for command in commands:
            with self.subTest(command=command), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as result:
                    parser.parse_args([command])
                self.assertEqual(result.exception.code, 2)
        for option in ('--supervision', '--cuda-binary'):
            with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as result:
                    parser.parse_args(['publisher-cutover-plan', '--request', 'request.json',
                                       '--request-sha256', '0' * 64, option, 'retired'])
                self.assertEqual(result.exception.code, 2)
