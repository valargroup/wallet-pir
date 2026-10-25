#!/usr/bin/env python3
"""Build and run the explicit unified-publisher/full-consumer HTTPS campaign.

Two independent providers each run one `pir-query-server` process serving
history and transaction detail from generated JSON configuration.

Retains source/binary identities, original released-client provenance, process
logs and raw observations under a new ignored run directory. This is local
functional evidence, not production-state, unchanged-app or performance proof.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
FROZEN = ROOT / "src/pir-query-server/tests/transparent/frozen-client"
TEST = "unified_http_sync_campaign"
CASES = {
    "transparent": (TEST, "unified_publisher_drives_independent_full_history_and_display_consumers", True),
    "receiver": (TEST, "unified_receiver_publisher_drives_two_independent_query_servers", True),
    "unified": ("transparent_provider_installation", "install::one_public_listener_serves_all_five_products", False),
}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build(arguments, log):
    result = subprocess.run(["cargo", *arguments, "--locked", "--offline", "--profile", "release-fast",
                             "--message-format=json"], cwd=ROOT, stdout=subprocess.PIPE,
                            stderr=log, text=True)
    messages = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    for item in messages:
        if item.get("reason") == "compiler-message":
            log.write(item["message"].get("rendered", ""))
    log.flush()
    result.check_returncode()
    return {item["target"]["name"]: Path(item["executable"])
            for item in messages if item.get("reason") == "compiler-artifact" and item.get("executable")}


def wait_for_case_exit(process, timeout):
    """Observe exit without reaping, reserving the leader PID through cleanup."""
    deadline = time.monotonic() + timeout
    while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, timeout)
        time.sleep(min(0.05, remaining))


def case_group_has_live_members(process):
    """Inspect group membership without changing the unreaped leader's ownership."""
    listing = subprocess.check_output(['ps', '-axo', 'pid=,pgid=,stat='], text=True, timeout=5)
    members = [(int(group), state) for _, group, state in
               (line.split() for line in listing.splitlines())]
    return any(group == process.pid and not state.startswith('Z') for group, state in members)


def signal_case_group(process, number):
    """Signal live members while the caller still owns the unreaped leader PID."""
    if not case_group_has_live_members(process):
        return
    try:
        os.killpg(process.pid, number)
    except ProcessLookupError:
        pass
    except PermissionError:
        # macOS can report EPERM when the last live member has just exited.
        # A real permission refusal remains an error; there is no signal retry.
        if case_group_has_live_members(process):
            raise


def run_case(binary, case, ignored, output, env, timeout):
    """Run exactly one declared test, refusing a successful zero-test selection."""
    path = output / (case + ".log")
    with path.open("w") as log:
        process = subprocess.Popen([str(binary), case, "--exact", *(["--ignored"] if ignored else []), "--nocapture"],
                                   cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            wait_for_case_exit(process, timeout)
        finally:
            # Keep the leader unreaped until both signals complete. Its PID
            # still reserves our group even after an abrupt harness exit.
            try:
                signal_case_group(process, signal.SIGTERM)
                try:
                    wait_for_case_exit(process, 10)
                except subprocess.TimeoutExpired:
                    pass
                signal_case_group(process, signal.SIGKILL)
                cleanup_deadline = time.monotonic() + 2
                while case_group_has_live_members(process):
                    if time.monotonic() >= cleanup_deadline:
                        raise RuntimeError("campaign descendants survived group cleanup")
                    time.sleep(0.02)
            finally:
                try:
                    code = process.wait(timeout=10)
                except subprocess.TimeoutExpired as error:
                    raise RuntimeError("campaign case still running after cleanup") from error
    if code == 0 and not re.search(r"^test result: ok\. 1 passed; 0 failed;", path.read_text(), re.MULTILINE):
        with path.open("a") as log:
            log.write("Campaign refused: the selected test did not report exactly one passed case.\n")
        return 1
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / ".local-pir/unified-http-sync")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--products", choices=["all", "transparent", "receiver", "unified"], default="all")
    args = parser.parse_args()
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output.resolve() / f"{stamp}-{source[:12]}"
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    manifest = {"schema": "transparent-unified-http-sync-v1", "source_sha": source,
                "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
                "host": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count()},
                "fixture_recent_privacy_floor": 1, "products": args.products,
                "scope": "synthetic local correctness; production journal snapshots and original wallet application are separate gates"}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    code = 1
    try:
        with (output / "build.log").open("w") as log:
            binaries = build(["build", "-p", "pir-publisher", "--features", "pir-publisher/transparent-history,pir-publisher/transparent-detail,pir-publisher/receiver",
                              "--bin", "pir-publisher"], log)
            # Compile the actual root host with every product needed by the selected case.
            unified = args.products in ("all", "unified")
            query_features = "transparent-history,transparent-detail,receiver" + (",enhance,status,native-reinspiring" if unified else "")
            test_features = "transparent-tests,receiver-tests" + (",enhance-tests,native-reinspiring,qualification-tests" if unified else "")
            binaries.update(build(["build", "-p", "pir-query-server",
                                   "--features", query_features,
                                   "--bin", "pir-query-server"], log))
            tests = build(["test", "-p", "pir-e2e-tests", "--features", test_features,
                            "--test", TEST, *(["--test", "transparent_provider_installation"] if unified else []), "--no-run"], log)
            client = (build(["build", "--manifest-path", str(FROZEN / "Cargo.toml")], log)["frozen-provider-client"]
                      if args.products in ("all", "transparent") else None)
        binaries.update(tests)
        if client is not None:
            binaries["frozen-provider-client"] = client
            with (output / "verify-sources.log").open("w") as verification_log:
                subprocess.run([sys.executable, str(FROZEN / "verify-sources.py"), "--output", str(output / "frozen-client-provenance")],
                               cwd=ROOT, check=True, stdout=verification_log, stderr=subprocess.STDOUT)
        manifest["binaries"] = {name: {"path": str(path), "sha256": digest(path)} for name, path in binaries.items()}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        env = dict(os.environ, WALLET_PIR_PUBLISHER_BIN=str(binaries["pir-publisher"]),
                   WALLET_PIR_QUERY_SERVER_BIN=str(binaries["pir-query-server"]),
                   WALLET_PIR_SOURCE_SHA=source,
                   WALLET_PIR_CAMPAIGN_OUT=str(output), WALLET_PIR_QUALIFICATION_OUTPUT=str(output))
        if client is not None:
            env["WALLET_PIR_FROZEN_CLIENT"] = str(client)
        selected = CASES.values() if args.products == "all" else [CASES[args.products]]
        for target, case, ignored in selected:
            code = run_case(tests[target], case, ignored, output, env, args.timeout)
            if code:
                break
    except subprocess.TimeoutExpired:
        code = 124
    except BaseException:
        # A later failure or interruption cannot inherit an earlier case's success.
        code = 1
        raise
    finally:
        result = {"exit_code": code, "seconds": time.monotonic() - started, "source_sha": source,
                  "production_snapshot_qualification": "not_executed", "original_application_qualification": "not_executed"}
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"output": str(output), **result}), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
