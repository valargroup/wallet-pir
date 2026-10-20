"""Offline deployment decisions and orchestration, with no SSH or live services."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "tools/transparent/scripts/deploy-transparent-shard.sh"


def shell(code, **env):
    return subprocess.run(
        ["bash", "-c", 'source "$SCRIPT" library\n' + code],
        env={**os.environ, "SCRIPT": str(SCRIPT),
             "TRANSPARENT_PUBLISHER_CONFIG": "/nonexistent/transparent-publisher-test/controller.json", **env},
        cwd=ROOT, text=True, capture_output=True, check=True,
    ).stdout.strip()


class FleetTests(unittest.TestCase):
    def test_schema_cache_override_and_later_preservation(self):
        custom = '/srv/transparent-pir/runtime-cache-v10'
        self.assertEqual(shell('runtime_cache_directory /bin/server'),
                         '/srv/transparent-pir/runtime-cache')
        for args in [f'/bin/server --runtime-cache-dir {custom} --query-slots 2',
                     f'/bin/server --runtime-cache-dir={custom}']:
            self.assertEqual(shell('runtime_cache_directory "$ARGS"', ARGS=args), custom)
            self.assertEqual(shell('runtime_cache_directory "$ARGS"', ARGS=args,
                                   TRANSPARENT_RUNTIME_CACHE_DIR='/srv/cache-v11'), '/srv/cache-v11')
        for invalid in ['relative', '/', '/srv/cache with spaces', '/srv/cache;echo']:
            with self.assertRaises(subprocess.CalledProcessError):
                shell('runtime_cache_directory /bin/server', TRANSPARENT_RUNTIME_CACHE_DIR=invalid)

    def test_fixed_cleanup_prunes_the_installed_schema_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = '/srv/transparent-pir/runtime-cache-v10'
            unit = root/'worker.service'
            # A host still running the retired executable with its own flags.
            unit.write_text(f'ExecStart=/bin/server --assignment /assignment.json --runtime-cache-dir {cache} --query-slots 2\n')
            plan = root/'plan.json'
            plan.write_text('{"workers":{"owner":{"publisher_control":false}}}')
            code = r'''
DEPLOY_PLAN="$PLAN"
TRANSACTION_ID=synthetic-cache-test
worker_ids() { echo owner; }
worker_field() { echo host; }
worker_action() { echo restart; }
shard_set_path() { echo /candidate; }
sudo() {
  if [[ "$1" == test ]]; then return 1; fi
  echo "$*" >>"$LOG"
  if [[ "$1" == */shard-prune ]]; then echo '{"deleted":[],"bytes_freed":0}'; fi
}
export -f sudo
host_ssh() {
  shift
  local script
  script="$(cat)"
  script="${script//\/etc\/systemd\/system\/transparent-shard-server.service/$UNIT}"
  "$@" <<<"$script"
}
fleet_prune
'''
            shell(code, PLAN=str(plan), UNIT=str(unit), LOG=str(root/'calls'),
                  TRANSPARENT_SCHEMA_CUTOVER='false')
            calls = (root/'calls').read_text().splitlines()
            arguments = calls[-1].split()
            self.assertEqual(arguments[arguments.index('--runtime-cache-dir')+1], cache)

    def test_configured_cleanup_prunes_through_a_verify_only_configuration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = '/srv/transparent-pir/runtime-cache-v10'
            unit, config = root/'worker.service', root/'shard-server.json'
            unit.write_text('ExecStart=/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json\n')
            config.write_text(json.dumps({'management_listen': '127.0.0.1:18190', 'transparent_history': {
                'enabled': True, 'settings': {'listen': '10.0.0.5:8093', 'shard_dir': '/candidate',
                                              'assignment': '/assignment.json', 'runtime_cache_dir': cache}}}))
            backup = root/'backup'
            backup.mkdir()
            (backup/'transparent-shard-server.service').write_text('ExecStart=/bin/server --shard-dir /previous --query-slots 2\n')
            plan = root/'plan.json'
            plan.write_text('{"workers":{"owner":{"publisher_control":false}}}')
            code = r'''
DEPLOY_PLAN="$PLAN"
TRANSACTION_ID=synthetic-cache-test
worker_ids() { echo owner; }
worker_field() { echo host; }
worker_action() { echo restart; }
shard_set_path() { echo /candidate; }
sudo() {
  case "$1" in
    test|sed|jq) "$@"; return;;
  esac
  if [[ "$1" == */pir-query-server ]]; then cp "$3" "$CONFIG_SEEN"; fi
  echo "$*" >>"$LOG"
  if [[ "$1" == */shard-prune ]]; then echo '{"deleted":[],"bytes_freed":0}'; fi
}
export -f sudo
host_ssh() {
  shift
  local script
  script="$(cat)"
  script="${script//\/etc\/systemd\/system\/transparent-shard-server.service/$UNIT}"
  script="${script//\/etc\/transparent-pir\/shard-server.json/$CONFIG}"
  script="${script//\/opt\/transparent-pir\/transactions\/\$3/$BACKUP}"
  "$@" <<<"$script"
}
fleet_prune
'''
            shell(code, PLAN=str(plan), UNIT=str(unit), CONFIG=str(config), BACKUP=str(backup),
                  LOG=str(root/'calls'), CONFIG_SEEN=str(root/'seen.json'), TRANSPARENT_SCHEMA_CUTOVER='false')
            calls = (root/'calls').read_text().splitlines()
            self.assertIn('--assignment /assignment.json', calls[0])
            self.assertEqual(calls[-1].split()[0], '/usr/local/bin/pir-query-server')
            self.assertEqual(calls[-1].split()[-1], '--verify-only')
            self.assertEqual(json.loads((root/'seen.json').read_text()), {
                'management_listen': '127.0.0.1:18190', 'transparent_history': {'enabled': True, 'settings': {
                    'runtime_cache_dir': cache, 'runtime_cache_prune_set': ['/candidate', '/previous']}}})

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
            for name in ['pir-query-server','shard-prune']:
                (root/name).write_text('binary')
            (root/'transparent-shard-server.service').write_text(
                (ROOT/'tools/transparent/deploy/transparent-shard-server.service').read_text())
            worker = dict(id='owner',role='archive-owner',replica_group=None,ssh_host='owner',upstream='10.0.0.5:8093',cache_bytes=1024,memory_max='1G')
            assignment = root/'assignment.json'
            assignment.write_text(json.dumps(dict(workers=[worker])))
            config = root/'controller.json'
            config.write_text('{"shadow":true}')
            installed = dict(binary='old',helper='old',unit='old',recorded_unit='old',drop_ins='',
                exec_start='/bin/server --control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json --runtime-cache-dir /srv/transparent-pir/runtime-cache-v10 --runtime-cache-max-bytes 2048')
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
            self.assertIn('\nExecStart=/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json\n', unit)
            config = json.loads((root/'shard-server.json.owner.rendered').read_text())
            self.assertEqual(config, {'management_listen': '127.0.0.1:18190', 'transparent_history': {
                'enabled': True, 'settings': {
                    'listen': '10.0.0.5:8093', 'shard_dir': '/srv/set', 'cache_bytes': 1024, 'build_slots': 1,
                    'query_slots': 2, 'assignment': '/opt/transparent-pir/assignments/%064d.json' % 0,
                    'worker_id': 'owner', 'prune_excess': True,
                    'runtime_cache_dir': '/srv/transparent-pir/runtime-cache-v10', 'runtime_cache_max_bytes': 2048,
                    'control_socket': '/run/transparent-pir/control.sock',
                    'active_record': '/opt/transparent-publisher/active.json'}}})
            self.assertTrue(json.loads((root/'deploy-plan.json').read_text())['publisher_control'])
            self.assertEqual((root/'calls').read_text().splitlines(), ['read'])
            # A host already on pir-query-server is read through its configuration.
            configured = dict(installed, exec_start='/usr/local/bin/pir-query-server --config /etc/transparent-pir/shard-server.json',
                              config=json.dumps(config))
            shell(code, READY=json.dumps(ready), **{**env, 'INSTALLED': json.dumps(configured)})
            self.assertEqual(json.loads((root/'shard-server.json.owner.rendered').read_text()), config)
            self.assertTrue(json.loads((root/'deploy-plan.json').read_text())['publisher_control'])
            with self.assertRaises(subprocess.CalledProcessError):
                shell(code, READY=json.dumps({**ready,'worker_assignment_sha256':'changed'}), **env)
            worker['role'] = 'recent-replica'
            assignment.write_text(json.dumps(dict(workers=[worker])))
            env['TRANSPARENT_FLEET_JSON'] = json.dumps([worker])
            shell(code, READY=json.dumps(ready), **env)
            recent_unit = (root/'transparent-shard-server.service.owner.rendered').read_text()
            self.assertIn('MemoryHigh=5905580032\n', recent_unit)
            self.assertIn('MemoryHigh=51539607552\n', unit)

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
                for name in ["pir-query-server", "shard-prune", "unit.rendered", "shard-server.json", "assignment.json"]:
                    (staged/name).write_text("new "+name)
                workers = [dict(id="owner", ssh_host="owner", upstream="owner:8093", role="archive-owner", replica_group=None)]
                assignment = Path(temp)/"assignment.json"
                assignment.write_text(json.dumps(dict(workers=workers)))
                plan = Path(temp)/"plan.json"
                plan.write_text(json.dumps(dict(workers=dict(owner=dict(action=action, desired=dict(unit="new digest"), before={})))))
                code = r'''
DEPLOY_PLAN="$PLAN"
assignment_digest() { echo assignment; }
host_ssh() { shift; python3 tools/tests/operations/remote_host.py "$@"; }
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
                # The host predates pir-query-server: rollback leaves only its
                # legacy executable and unit, with no generated configuration.
                for absent in ("usr/local/bin/transparent-query-server", "usr/local/bin/pir-query-server",
                               "etc/transparent-pir/shard-server.json"):
                    self.assertFalse((root/absent).exists(), absent)
                if action == "restart" and not fail:
                    self.assertFalse((root/"usr/local/bin/pir-query-server.next").exists())
                if action == "tools":
                    log = (root/"systemctl.log").read_text()
                    self.assertNotIn("stop", log)
                    self.assertNotIn("start", log)
                latest = Path((Path(temp)/"transactions/latest").read_text().strip())
                self.assertEqual(json.loads(latest.read_text())["status"], "rolled-back")


class RouterCaddyCompatibility(unittest.TestCase):
    """The production router runs Caddy 2.6.2; rendered routers must load there."""

    NEWER_THAN_2_6 = ('health_fails', 'health_passes', 'handle_errors 4', 'handle_errors 5')

    def test_rendered_router_policies_use_caddy_2_6_directives(self):
        root = Path(__file__).resolve().parents[3]
        texts = {
            'live fleet script': (root / 'tools/transparent/scripts/transparent-live-fleet.py').read_text(),
            'shard-assign renderer': (root / 'src/transparent/transparent-query-server/src/routing/router.rs').read_text().split('#[cfg(test)]')[0],
            'router fixture': (root / 'tools/transparent/fixtures/transparent-shard/Caddyfile.router').read_text(),
        }
        for name, text in texts.items():
            # Comments explain which directives are forbidden; they are not
            # emitted into the Caddyfile. Keep checking executable strings.
            text = '\n'.join(line for line in text.splitlines()
                             if not line.lstrip().startswith(('#', '//')))
            for directive in self.NEWER_THAN_2_6:
                self.assertNotIn(directive, text, f'{name} uses {directive!r}, which Caddy 2.6.2 rejects')


if __name__ == "__main__":
    unittest.main()
