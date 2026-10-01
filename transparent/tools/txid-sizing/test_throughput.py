"""Throughput stop gate, immutable input verification, and RPC refusal controls."""
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import census
import throughput


class ThroughputTests(unittest.TestCase):
    def test_72_hour_boundary_is_strict_and_optimistic(self):
        p = throughput.projection(1, 1, 72 * 3600)
        self.assertEqual(p["projected_remaining_hours"], 72)
        self.assertFalse(p["exceeds_limit"])
        self.assertTrue(throughput.projection(1, 1, 72 * 3600 + 1)["exceeds_limit"])
        with self.assertRaises(ValueError):
            throughput.projection(0, 1, 100)

    def test_retained_raw_resume_tamper_and_no_parsed_claim(self):
        calls = []
        def rpc(method, params):
            calls.append((method, params))
            if method == "getblockhash":
                result = census.ANCHOR_HASH if params[0] == census.ANCHOR_HEIGHT else str(params[0])
            elif method == "getblockcount":
                result = census.ANCHOR_HEIGHT + 10
            elif method == "getblock":
                self.assertEqual(params[1], 0)
                result = "010203"
            else:
                self.fail("unexpected method")
            return dict(result=result, error=None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "cache"
            status = Path(temp) / "status.json"
            with patch("throughput.time.monotonic", side_effect=itertools.count()):
                receipt = throughput.measure(root, rpc, status, duration=5)
            self.assertGreater(receipt["fetched_blocks"], 0)
            self.assertEqual(receipt["canonical_scanned_blocks"], 0)
            self.assertEqual(json.loads(status.read_text())["state"], "blocked")
            first_count = receipt["fetched_blocks"]
            calls.clear()
            with patch("throughput.time.monotonic", side_effect=itertools.count()):
                resumed = throughput.measure(root, rpc, status, duration=5)
            self.assertEqual(resumed["first_measured_height"], first_count)
            fetched = [p[0] for m, p in calls if m == "getblockhash" and p[0] != census.ANCHOR_HEIGHT]
            self.assertTrue(all(h >= first_count for h in fetched))
            (root / "raw" / "0.bin").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                throughput.measure(root, rpc, status, duration=5)

    def test_refusal_and_anchor_change_stop_before_raw_acquisition(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "cache"
            status = Path(temp) / "status.json"
            with self.assertRaisesRegex(RuntimeError, "gateway RPC error"):
                throughput.measure(root, lambda *_: dict(error="refused"), status)
            with self.assertRaisesRegex(RuntimeError, "anchor changed"):
                throughput.measure(root, lambda *_: dict(result="wrong", error=None), status)
            self.assertFalse((root / "raw-manifest.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
