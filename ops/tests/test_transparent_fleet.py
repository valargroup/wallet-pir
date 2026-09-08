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
        env={**os.environ, "SCRIPT": str(SCRIPT),
             "TRANSPARENT_PUBLISHER_CONFIG": "/nonexistent/transparent-publisher-test/controller.json", **env},
        cwd=ROOT, text=True, capture_output=True, check=True,
    ).stdout.strip()


class FleetTests(unittest.TestCase):
    def test_publisher_control_paths_and_shadow_guard(self):
        args = "/bin/server --control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json"
        self.assertEqual(shell('publisher_control_enabled "$ARGS"', ARGS=args), "true")
        self.assertEqual(shell('publisher_control_enabled /bin/server'), "false")
        for invalid in [args.replace('/run/transparent-pir/control.sock', '/other.sock'),
                        '/bin/server --active-record /other.json']:
            with self.assertRaises(subprocess.CalledProcessError):
                shell('publisher_control_enabled "$ARGS"', ARGS=invalid)
        with tempfile.TemporaryDirectory() as temp:
            plan, config = Path(temp)/'plan.json', Path(temp)/'controller.json'
            plan.write_text('{"publisher_control":true}')
            env = dict(PLAN=str(plan), TRANSPARENT_PUBLISHER_CONFIG=str(config))
            code = 'DEPLOY_PLAN="$PLAN"; fleet_check_publisher_shadow; echo allowed'
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, **env)
            config.write_text('{"shadow":true}')
            self.assertEqual(shell(code, **env), "allowed")
            config.write_text('{"shadow":false}')
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, **env)
            # Even a legacy/router-only plan must not overwrite an active authority.
            plan.write_text('{}')
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, **env)
            config.unlink()
            self.assertEqual(shell(code, **env), "allowed")

    def test_diff_preserves_publisher_control_and_leaves_retention_intact(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ['transparent-shard-server','shard-prune']:
                (root/name).write_text('binary')
            (root/'transparent-shard-server.service').write_text(
                (ROOT/'ops/infra/digitalocean/production/deploy/transparent-shard-server.service').read_text())
            worker = dict(id='owner',role='archive-owner',replica_group=None,ssh_host='owner',upstream='owner:8093',cache_bytes=1024,memory_max='1G')
            assignment = root/'assignment.json'
            assignment.write_text(json.dumps(dict(workers=[worker])))
            config = root/'controller.json'
            config.write_text('{"shadow":true}')
            installed = dict(binary='old',helper='old',unit='old',recorded_unit='old',drop_ins='',
                exec_start='/bin/server --control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json')
            code = r'''
EXPECTED_MAP_SHA256=map
assignment_digest() { printf '%064d\n' 0; }
worker_digest() { echo assignment; }
shard_set_path() { echo /srv/set; }
host_ssh() { cat >/dev/null; echo read >>"$LOG"; echo "$INSTALLED"; }
curl() { echo "$READY"; }
fleet_diff
fleet_prune
'''
            ready = dict(ready=True,mode='warm',warm_runtimes=0,target_runtimes=0,map_sha256='map',worker_assignment_sha256='assignment')
            env = dict(TRANSPARENT_FLEET_JSON=json.dumps([worker]), TRANSPARENT_ASSIGNMENT=str(assignment),
                TRANSPARENT_ARTIFACT_DIR=temp, TRANSPARENT_PUBLISHER_CONFIG=str(config),
                INSTALLED=json.dumps(installed), LOG=str(root/'calls'))
            shell(code, READY=json.dumps(ready), **env)
            unit = (root/'transparent-shard-server.service.owner.rendered').read_text()
            self.assertEqual(unit.count('RuntimeDirectory=transparent-pir\n'), 1)
            self.assertIn('--control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json', unit)
            self.assertIn('--runtime-cache-dir /srv/transparent-pir/runtime-cache', unit)
            self.assertTrue(json.loads((root/'deploy-plan.json').read_text())['publisher_control'])
            self.assertEqual((root/'calls').read_text().splitlines(), ['read'])
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, READY=json.dumps({**ready,'worker_assignment_sha256':'changed'}), **env)

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

    def test_transaction_restores_files_and_helper_only_never_restarts(self):
        for action, fail in [("restart", False), ("tools", False), ("restart", True)]:
            with self.subTest(action=action, fail=fail), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)/"host"
                previous = {
                    "/usr/local/bin/transparent-shard-server": "old server",
                    "/usr/local/bin/shard-prune": "old helper",
                    "/etc/systemd/system/transparent-shard-server.service": "old unit",
                    "/opt/transparent-pir/current-release": "old release",
                    "/opt/transparent-pir/current-unit-digest": "old unit digest",
                }
                for path, content in previous.items():
                    target = root/path.lstrip("/")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(content)
                sha = "a"*40
                staged = root/("tmp/transparent-pir-"+sha)
                staged.mkdir(parents=True)
                for name in ["transparent-shard-server", "shard-prune", "unit.rendered", "assignment.json"]:
                    (staged/name).write_text("new "+name)
                workers = [dict(id="owner", ssh_host="owner", upstream="owner:8093", role="archive-owner", replica_group=None)]
                assignment = Path(temp)/"assignment.json"
                assignment.write_text(json.dumps(dict(workers=workers)))
                plan = Path(temp)/"plan.json"
                plan.write_text(json.dumps(dict(workers=dict(owner=dict(action=action, desired=dict(unit="new digest"), before={})))))
                code = r'''
DEPLOY_PLAN="$PLAN"
assignment_digest() { echo assignment; }
host_ssh() { shift; python3 ops/tests/remote_host.py "$@"; }
wait_ready() { :; }
transaction_begin
trap 'rc=$?; if ((rc != 0)); then trap - EXIT; rollback_fleet; fi' EXIT
fleet_activate_workers
trap - EXIT
rollback_fleet
'''
                env = dict(HOST_ROOT=str(root), TRANSPARENT_RELEASE_SHA=sha,
                           TRANSPARENT_ARTIFACT_DIR=temp, TRANSPARENT_ASSIGNMENT=str(assignment),
                           TRANSPARENT_FLEET_JSON=json.dumps(workers), PLAN=str(plan),
                           TRANSPARENT_TRANSACTION_DIR=str(Path(temp)/"transactions"),
                           FAIL_ACTIVATION=str(fail).lower())
                if fail:
                    with self.assertRaises(subprocess.CalledProcessError):
                        shell(code, **env)
                else:
                    shell(code, **env)
                for path, content in previous.items():
                    self.assertEqual((root/path.lstrip("/")).read_text(), content)
                if action == "tools":
                    log = (root/"systemctl.log").read_text()
                    self.assertNotIn("stop", log)
                    self.assertNotIn("start", log)
                latest = Path((Path(temp)/"transactions/latest").read_text().strip())
                self.assertEqual(json.loads(latest.read_text())["status"], "rolled-back")


if __name__ == "__main__":
    unittest.main()
