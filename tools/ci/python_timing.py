"""Add case timing to existing unittest entry points during correctness checks."""
import os
import time
import unittest

from test_budget import case_seconds, record


def install():
    seconds = case_seconds(os.environ.get("WALLET_PIR_CASE_SECONDS", 30))
    original_start = unittest.TestResult.startTest
    original_stop = unittest.TestResult.stopTest

    def start(self, test):
        self._pir_case_started = time.monotonic()
        original_start(self, test)

    def stop(self, test):
        elapsed = time.monotonic() - self._pir_case_started
        record(os.environ.get('WALLET_PIR_CASE_LOG'),
               dict(test=test.id(), seconds=round(elapsed, 6), language='python'))
        if elapsed >= seconds and os.environ.get('WALLET_PIR_ENFORCE_BUDGET') == '1':
            error = AssertionError(f'correctness test exceeded {seconds:g} seconds: {elapsed:.3f}s')
            self.addFailure(test, (AssertionError, error, None))
        original_stop(self, test)

    unittest.TestResult.startTest = start
    unittest.TestResult.stopTest = stop
