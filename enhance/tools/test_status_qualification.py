import unittest

import status_qualification as qualification


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.original = (qualification.SECONDS, qualification.QPS)
        qualification.SECONDS = 2
        qualification.QPS = 2
        self.summary = {
            "phase": "load",
            "protocol": "status-pir-v2-q48", "rows": 8192, "slots": 256,
            "slot_bytes": 40, "columns": 6144, "row_bytes": 12288,
            "database_bytes": 100663296,
            "source": "live",
            "oracle_source": "independent",
            "seconds": 2,
            "offered": 4,
            "source_observations": 2,
            "run_started_ms": 100,
            "run_ended_ms": 2_100,
        }
        self.requests = [
            {"arrival": i, "result": "correct", "duration_ms": 100,
             "observation_age_ms": 200, "scheduled_ms": 100 + i * 500,
             "completed_ms": 200 + i * 500}
            for i in range(4)
        ]
        self.publications = [
            {"observation_id": i, "kind": kind, "source_observed_ms": 100,
             "queryable_ms": 200, "oracle_match": True, "disposition": "published",
             "canonical_continuity": True, "complete_mempool": True}
            for i, kind in enumerate(("block", "mempool"))
        ]
        self.resources = [
            {"sampled_ms": sampled, "oom_events": 0, "swap_bytes": 0}
            for sampled in (100, 2_100)
        ]

    def tearDown(self):
        qualification.SECONDS, qualification.QPS = self.original

    def assess(self):
        return qualification.assess(
            self.summary, self.requests, self.publications, self.resources
        )

    def test_old_or_missing_geometry_blocks(self):
        for key in ("protocol", "rows", "slots", "slot_bytes", "columns", "row_bytes", "database_bytes"):
            original = self.summary.pop(key)
            self.assertFalse(self.assess()["passed"])
            self.summary[key] = "status-pir-v1-q48" if key == "protocol" else original * 2
            self.assertFalse(self.assess()["passed"])
            self.summary[key] = original

    def test_complete_live_capture_passes(self):
        self.assertTrue(self.assess()["passed"])

    def test_synthetic_or_missing_arrival_blocks(self):
        self.summary["source"] = "synthetic_fixture"
        self.requests.pop()
        self.assertFalse(self.assess()["passed"])

    def test_stale_or_lost_publication_blocks(self):
        self.requests[0]["observation_age_ms"] = 20_001
        self.publications[0]["queryable_ms"] = 20_101
        self.assertFalse(self.assess()["passed"])

    def test_explicit_supersession_passes(self):
        self.publications.append({"observation_id": 2, "kind": "mempool",
                                  "source_observed_ms": 100, "disposition": "superseded",
                                  "superseded_by": 3})
        self.publications.append(dict(self.publications[1], observation_id=3))
        self.summary["source_observations"] = 4
        self.assertTrue(self.assess()["passed"])

    def test_missing_successor_and_cycles_block(self):
        self.publications[1].update(disposition="superseded", superseded_by=2)
        self.assertFalse(self.assess()["passed"])
        self.publications.append(dict(self.publications[1], observation_id=2, superseded_by=1))
        self.assertFalse(self.assess()["passed"])

    def test_supersession_does_not_reset_original_deadline(self):
        self.summary["run_ended_ms"] = 30_100
        self.publications[1].update(disposition="superseded", superseded_by=2)
        self.publications.append(dict(self.publications[0], observation_id=2,
                                      source_observed_ms=20_000, queryable_ms=25_000))
        self.assertFalse(self.assess()["passed"])

    def test_incomplete_source_and_evidence_gaps_block(self):
        self.publications[0]["complete_mempool"] = False
        self.assertFalse(self.assess()["passed"])
        self.publications[0]["complete_mempool"] = True
        self.summary["run_ended_ms"] = 30_100
        self.assertFalse(self.assess()["passed"])

    def test_p99_and_resource_pressure_block(self):
        self.requests[0]["duration_ms"] = 1_001
        self.resources[0]["oom_events"] = 1
        result = self.assess()
        self.assertFalse(result["passed"])
        self.assertEqual(result["p99_ms"], 1_001)


if __name__ == "__main__":
    unittest.main()
