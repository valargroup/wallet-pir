#!/usr/bin/env python3
"""Build and run the explicit unified-publisher/full-consumer HTTPS campaign.

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
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
FROZEN = ROOT / "src/transparent/transparent-query-server/tests/frozen-client"
TEST = "unified_http_sync_campaign"
CASE = "unified_publisher_drives_independent_full_history_and_display_consumers"


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / ".local-pir/unified-http-sync")
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output.resolve() / f"{stamp}-{source[:12]}"
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    manifest = {"schema": "transparent-unified-http-sync-v1", "source_sha": source,
                "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
                "host": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count()},
                "scope": "synthetic local correctness; production journal snapshots and original wallet application are separate gates"}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    code = 1
    try:
        with (output / "build.log").open("w") as log:
            binaries = build(["build", "-p", "pir-publisher", "-p", "transparent-query-server",
                              "--bin", "pir-publisher", "--bin", "transparent-query-server", "--bin", "transparent-txid-server"], log)
            tests = build(["test", "-p", "transparent-publisher", "--test", TEST, "--no-run"], log)
            client = build(["build", "--manifest-path", str(FROZEN / "Cargo.toml")], log)["frozen-provider-client"]
        binaries[TEST] = tests[TEST]
        binaries["frozen-provider-client"] = client
        subprocess.run([sys.executable, str(FROZEN / "verify-sources.py"), "--output", str(output / "frozen-client-provenance")],
                       cwd=ROOT, check=True, stdout=(output / "verify-sources.log").open("w"), stderr=subprocess.STDOUT)
        manifest["binaries"] = {name: {"path": str(path), "sha256": digest(path)} for name, path in binaries.items()}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        env = dict(os.environ, WALLET_PIR_PUBLISHER_BIN=str(binaries["pir-publisher"]),
                   WALLET_PIR_PROVIDER_BIN=str(binaries["transparent-query-server"]),
                   WALLET_PIR_DISPLAY_BIN=str(binaries["transparent-txid-server"]),
                   WALLET_PIR_FROZEN_CLIENT=str(client), WALLET_PIR_SOURCE_SHA=source,
                   WALLET_PIR_CAMPAIGN_OUT=str(output))
        with (output / "campaign.log").open("w") as log:
            process = subprocess.Popen([str(tests[TEST]), CASE, "--exact", "--ignored", "--nocapture"],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                code = process.wait(timeout=args.timeout)
            finally:
                # Every service/TLS child belongs to this newly created group.
                # A killed test cannot run Rust destructors, so reap its children too.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    except subprocess.TimeoutExpired:
        code = 124
    finally:
        result = {"exit_code": code, "seconds": time.monotonic() - started, "source_sha": source,
                  "production_snapshot_qualification": "not_executed", "original_application_qualification": "not_executed"}
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"output": str(output), **result}), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
