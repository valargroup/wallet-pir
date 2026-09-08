"""Offline deployment decisions and orchestration, with no SSH or live services."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ops/scripts/deploy-transparent-shard.sh"


def shell(code, **env):
    return subprocess.run(
        ["bash", "-c", 'source "$SCRIPT" library\n' + code],
        env={**os.environ, "SCRIPT": str(SCRIPT), **env},
        cwd=ROOT, text=True, capture_output=True, check=True,
    ).stdout.strip()


class FleetTests(unittest.TestCase):
    def test_worker_decisions(self):
        wanted = dict(binary="bin", unit="unit", assignment="assignment", map="map")
        installed = dict(binary="bin", unit="unit", recorded_unit="unit")
        ready = dict(ready=True, mode="warm", warm_runtimes=4, target_runtimes=4,
                     binary_sha256="bin", worker_assignment_sha256="assignment", map_sha256="map")
        def classify(r=ready, i=installed, w=wanted, helper="true", force="false"):
            return shell('classify_worker "$READY" "$INSTALLED" "$WANTED" "$HELPER"',
                         READY=json.dumps(r), INSTALLED=json.dumps(i), WANTED=json.dumps(w),
                         HELPER=helper, TRANSPARENT_FORCE_REDEPLOY=force)
        self.assertEqual(classify(), "skip")
        self.assertEqual(classify(helper="false"), "tools")
        self.assertEqual(classify(force="true"), "restart")
        for field in ("binary", "unit", "assignment", "map"):
            self.assertEqual(classify(w={**wanted, field: "changed"}), "restart")
        for field in installed:
            self.assertEqual(classify(i={**installed, field: "drift"}), "restart")
        for r in ({}, {**ready, "ready": False}, {**ready, "mode": "loaded-only"},
                  {**ready, "warm_runtimes": 3}, {**ready, "binary_sha256": "old"}):
            self.assertEqual(classify(r=r), "restart")

    def test_replica_limits(self):
        for total, healthy, expected in [(4, 4, 2), (4, 3, 1), (4, 2, 0), (3, 3, 1), (2, 2, 1), (1, 1, 0)]:
            self.assertEqual(shell(f"replica_batch_limit {total} {healthy} false"), str(expected))
        self.assertEqual(shell("replica_batch_limit 4 0 true"), "2")
        self.assertEqual(shell("replica_batch_limit 4 4 false", TRANSPARENT_REPLICA_ACTIVATION="serial"), "1")

    def test_unit_identity_only_normalizes_assignment_path(self):
        with tempfile.TemporaryDirectory() as temp:
            a, b = Path(temp)/"a", Path(temp)/"b"
            a.write_text("ExecStart=/bin/server --assignment /opt/transparent-pir/assignments/" + "a"*64 + ".json --cache-bytes 10\nMemoryMax=1G\n")
            b.write_text(a.read_text().replace("a"*64, "b"*64))
            self.assertEqual(shell('unit_digest "$UNIT"', UNIT=str(a)), shell('unit_digest "$UNIT"', UNIT=str(b)))
            b.write_text(b.read_text().replace("MemoryMax=1G", "MemoryMax=2G"))
            self.assertNotEqual(shell('unit_digest "$UNIT"', UNIT=str(a)), shell('unit_digest "$UNIT"', UNIT=str(b)))

    def test_transaction_records_only_mutations_and_noop_preserves_latest(self):
        with tempfile.TemporaryDirectory() as temp:
            assignment = Path(temp)/"assignment.json"
            assignment.write_text(json.dumps(dict(workers=[dict(id="one", upstream="one:8093")])))
            root = Path(temp)/"transactions"
            root.mkdir()
            (root/"latest").write_text("previous\n")
            env = dict(TRANSPARENT_ASSIGNMENT=str(assignment), TRANSPARENT_TRANSACTION_DIR=str(root), TRANSPARENT_RELEASE_SHA="a"*40)
            self.assertEqual(shell('transaction_begin; cat "$TRANSPARENT_TRANSACTION_DIR/latest"', **env), "previous")
            output = shell('transaction_begin; transaction_worker one-host one tools; cat "$TRANSACTION_FILE"', **env)
            self.assertEqual(json.loads(output)["workers"], [dict(host="one-host", id="one", action="tools")])

    def test_actual_activation_batches_and_failure_join(self):
        # Exercise fleet_activate_workers itself. Mock SSH consumes stdin and logs
        # activation; wait_ready delays so concurrency and join order are visible.
        with tempfile.TemporaryDirectory() as temp:
            workers = [dict(id=f"r{i}", role="recent-replica", replica_group="recent", ssh_host=f"r{i}", upstream=f"r{i}:8093") for i in range(4)]
            assignment = Path(temp)/"assignment.json"
            assignment.write_text(json.dumps(dict(workers=workers)))
            plan = Path(temp)/"plan.json"
            plan.write_text(json.dumps(dict(workers={w["id"]:dict(action="restart", desired=dict(unit="unit")) for w in workers})))
            code = r'''
DEPLOY_PLAN="$PLAN"
TRANSACTION_ID=test
assignment_digest() { echo assignment; }
transaction_worker() { echo "record $2" >>"$LOG"; }
host_ssh() {
  local host="$1"; shift
  if [[ "$1" == test ]]; then return 0; fi
  if [[ "$1" == bash ]]; then cat >/dev/null; echo "start $host" >>"$LOG"; fi
}
wait_ready() { command sleep 0.05; echo "ready $1" >>"$LOG"; [[ "$1" != "${FAIL_ID:-}" ]]; }
curl() { echo '{"ready":true,"mode":"warm","warm_runtimes":1,"target_runtimes":1}'; }
sleep() { :; }
fleet_activate_workers
'''
            env = dict(TRANSPARENT_FLEET_JSON=json.dumps(workers), TRANSPARENT_ASSIGNMENT=str(assignment),
                       TRANSPARENT_ARTIFACT_DIR=temp, PLAN=str(plan), LOG=str(Path(temp)/"log"), TRANSPARENT_RELEASE_SHA="a"*40)
            shell(code, **env)
            lines = Path(env["LOG"]).read_text().splitlines()
            self.assertLess(lines.index("ready r0"), lines.index("start r2"))
            self.assertLess(lines.index("ready r1"), lines.index("start r2"))
            Path(env["LOG"]).write_text("")
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, FAIL_ID="r0", **env)
            lines = Path(env["LOG"]).read_text().splitlines()
            self.assertIn("ready r1", lines)
            self.assertNotIn("start r2", lines)


if __name__ == "__main__":
    unittest.main()
