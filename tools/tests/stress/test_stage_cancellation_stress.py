"""Repeated cancellation of nested stages; full tooling qualification only."""
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_stage_cancellation as contract


@unittest.skipUnless(os.name == 'posix', 'process-group cancellation is POSIX-specific')
class CancellationStressTests(unittest.TestCase):
    def test_fifty_cancellations_leave_no_descendant_listener(self):
        for trial in range(50):
            with self.subTest(trial=trial):
                contract.cancel_nested_listener(self)
