"""The operations copy of the shared contract policy and journal timing."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/lib'))
from wallet_pir_ops import contract
from wallet_pir_ops.deploy import timing

SHA = 'ab' * 32


def report(**overrides):
    return json.dumps({'schema': contract.CONTRACT_INFO_SCHEMA, 'executable': 'pir-query-server',
                       'contract_version': '1.2.3', 'contract_major': 1,
                       'products': ['status', 'enhance'], 'backends': [], **overrides})


class CompatibilityTests(unittest.TestCase):
    def test_same_major_is_compatible_in_both_directions(self):
        self.assertTrue(contract.compatible('1.0.0', '1.9.4'))
        self.assertTrue(contract.compatible('1.9.4', '1.0.0+build.7'))
        self.assertFalse(contract.compatible('1.9.4', '2.0.0'))
        self.assertFalse(contract.compatible('2.0.0', '1.9.4'))

    def test_unstable_or_malformed_versions_are_refused(self):
        for text in ('0.9.0', '1.0.0-rc.1', '1.0', 'v1.0.0', '01.0.0', '1.0.0+build..7',
                     '18446744073709551616.0.0', None, 1):
            with self.subTest(text=text), self.assertRaises(contract.ContractError):
                contract.contract_major(text)

    def test_source_major_matches_the_discovery_crate(self):
        self.assertGreaterEqual(contract.source_major(), 1)

    def test_discovery_answers(self):
        body = json.dumps({'service': 'pir-publisher', 'version': '1.3.0', 'later': True})
        self.assertEqual(contract.publisher_document_major(200, body), 1)
        refused = [(404, body), (200, 'x'), (200, json.dumps({'service': 'other', 'version': '1.0.0'})),
                   (200, json.dumps({'service': 'pir-publisher', 'version': '0.1.0'})),
                   (200, ' ' * (contract.MAX_DOCUMENT_BYTES + 1)), (200, None)]
        for status, answer in refused:
            with self.subTest(status=status, answer=str(answer)[:40]), self.assertRaises(contract.ContractError):
                contract.publisher_document_major(status, answer)


class ContractInfoTests(unittest.TestCase):
    def test_report_is_bound_to_the_reporting_bytes(self):
        info = contract.parse_contract_info(report(), executable='pir-query-server', sha256=SHA)
        self.assertEqual(info.sha256, SHA)
        self.assertEqual(info.products, ('enhance', 'status'))
        self.assertEqual(contract.missing_products(info, ['status', 'transparent_history']),
                         ['transparent_history'])
        self.assertEqual(info.to_json()['contract_major'], 1)

    def test_invalid_or_disagreeing_reports_are_refused(self):
        invalid = [report(schema='wallet-pir-contract-info/2'), report(executable='pir-publisher'),
                   report(contract_major=2), report(contract_major=True), report(contract_version='1.0.0-rc.1'),
                   report(products=['status', 'status']), report(backends='cuda'), '[]', '']
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(contract.ContractError):
                contract.parse_contract_info(text, executable='pir-query-server', sha256=SHA)


def event(unix, message, **fields):
    return {'unix': unix, 'message': message, **fields}


class TimingTests(unittest.TestCase):
    def test_clocks_and_per_target_gaps_come_from_journal_events(self):
        data = {'created_unix': 100, 'status': 'committed', 'source': {'submitted_unix': 40},
                'events': [event(101, 'staging', host='a'), event(110, 'product check'),
                           event(130, 'product check finished'), event(131, 'status activating'),
                           event(132, 'phase restarting', target='a/worker'),
                           event(135, 'phase verifying', target='a/worker'),
                           event(150, 'phase verified', target='a/worker'),
                           event(151, 'phase restarting', target='b/router'),
                           event(152, 'phase verifying', target='b/router'),
                           event(160, 'phase verified', target='b/router'),
                           event(161, 'status verifying'), event(162, 'exact check'),
                           event(170, 'exact check finished'), event(171, 'status committed')]}
        elapsed = timing.journal_timing(data)
        clocks = elapsed['clocks']
        self.assertEqual(clocks['total'], 71)
        self.assertEqual(clocks['source_to_accepted'], 131)
        self.assertEqual(clocks['preparation'], 31)
        self.assertEqual(clocks['staging'], 9)
        self.assertEqual(clocks['qualification'], 28)
        self.assertEqual(clocks['interruption'], 28)
        self.assertIsNone(clocks['recovery'])
        self.assertEqual(elapsed['targets']['a/worker'], {'interruption': 18, 'restart': 3, 'readiness': 15})
        self.assertEqual(elapsed['largest'], ['preparation', 'activation', 'qualification'])
        self.assertTrue(any('a/worker gap' in line for line in timing.describe(elapsed)))

    def test_rolled_back_transaction_reports_recovery_and_no_acceptance(self):
        data = {'created_unix': 0, 'status': 'rolled-back', 'source': None,
                'events': [event(1, 'status activating'), event(2, 'phase restarting', target='a/worker'),
                           event(3, 'phase verifying', target='a/worker'), event(9, 'failure'),
                           event(10, 'phase restoring', target='a/worker'),
                           event(14, 'phase restored', target='a/worker'), event(15, 'status rolled-back')]}
        clocks = timing.journal_timing(data)['clocks']
        self.assertEqual(clocks['recovery'], 4)
        self.assertEqual(clocks['interruption'], 12)
        self.assertIsNone(clocks['verification'])
        self.assertIsNone(clocks['source_to_accepted'])


if __name__ == '__main__':
    unittest.main()
