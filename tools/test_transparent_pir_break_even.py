"""The cost model must keep reproducing the preserved measurements."""

import json
import unittest
from pathlib import Path

import transparent_pir_break_even as be

RESULTS = Path(__file__).resolve().parents[1] / (
    "docs/transparent-pir-evaluation/per-table-reuse/policy.json"
)


class BreakEvenTest(unittest.TestCase):
    def setUp(self):
        if not RESULTS.exists():
            self.skipTest("preserved results are not present")
        self.results = json.loads(RESULTS.read_text())

    def test_the_model_reproduces_the_preserved_runs(self):
        _, failures = be.validate(self.results)
        self.assertEqual(failures, [])

    def test_the_policy_never_chooses_a_plan_worse_than_fresh_keys(self):
        for table in (be.DIRECTORY, be.PAGES):
            for queries in range(1, 200):
                chosen, sets = be.best_plan(table, queries)
                self.assertLessEqual(chosen, be.plan_bytes(table, queries, 1))
                self.assertTrue(1 <= sets <= be.PUBLIC_SETS)

    def test_a_larger_batch_is_charged_for_the_queries_padding_adds(self):
        # Two selections in batches of four are four queries, not two. Leaving
        # the padding out is what makes sharing look free.
        two_alone = be.plan_bytes(be.PAGES, 2, 1)
        two_in_four = be.plan_bytes(be.PAGES, 2, 4)
        four_in_four = be.plan_bytes(be.PAGES, 4, 4)
        self.assertEqual(two_in_four, four_in_four)
        self.assertGreater(two_in_four, two_alone)


if __name__ == "__main__":
    unittest.main()
