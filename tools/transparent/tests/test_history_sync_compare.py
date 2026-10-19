import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/history-sync-compare.py'
SPEC = importlib.util.spec_from_file_location('history_sync_compare', SCRIPT)
COMPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPARE)


class ComparisonTest(unittest.TestCase):
    def test_refuses_escaping_or_duplicate_transfer_paths(self):
        for path in ('/etc/passwd', '../outside', 'a/../outside', 'a//b', './a', 'a\nb'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                COMPARE.artifact_paths([dict(path=path)])
        with self.assertRaises(ValueError):
            COMPARE.artifact_paths([dict(path='a'), dict(path='a')])
        self.assertEqual(COMPARE.artifact_paths([dict(path='revisions/abc/table.bin')]), ['revisions/abc/table.bin'])

    def test_refuses_empty_or_incomplete_history_oracles(self):
        with self.assertRaises(ValueError):
            COMPARE.validate_oracle([])
        samples = [dict(zone=i, script_hex='51', expected=dict(history=[], utxos=[])) for i in range(3)]
        with self.assertRaises(ValueError):
            COMPARE.validate_oracle(samples)

    def test_compares_actual_history_and_balances_not_just_successful_queries(self):
        expected = dict(covered_through=100, history=[{'txid': 'a'}, {'txid': 'b'}], unresolved=0)
        COMPARE.check_answer(dict(covered_through=100, history=[{'txid': 'b'}, {'txid': 'a'}], unresolved=0), expected)
        for wrong in (dict(covered_through=99, history=expected['history'], unresolved=0),
                      dict(covered_through=100, history=[], unresolved=0),
                      dict(covered_through=100, history=expected['history'], unresolved=1)):
            with self.assertRaises(ValueError):
                COMPARE.check_answer(wrong, expected)

    def test_interruption_retains_downloaded_state_before_the_receiver_restarts(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            command = [sys.executable, '-c',
                       'from pathlib import Path; import sys,time; '
                       'Path(sys.argv[1]).write_bytes(b"x"*100); time.sleep(10)', str(state / 'partial')]
            child = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            try:
                ready, fault = COMPARE.wait_for_activation(child, state, 2, 50)
                self.assertIsNone(ready)
                self.assertGreaterEqual(fault['on_disk_bytes'], 50)
            finally:
                COMPARE.stop(child)
            self.assertEqual((state / 'partial').read_bytes(), b'x' * 100)


if __name__ == '__main__':
    unittest.main()
