#!/usr/bin/env python3
"""Run the explicit real-controller HTTPS update campaign and retain its raw run.

Builds the packaged provider and the ignored campaign test with one Cargo
profile, records exact source/binary/host identities, then runs the campaign
with a new ignored output root under .local-pir/. Failed runs stay on disk and
the exit status is the test's. This is a small-fixture correctness/diagnostic
campaign on the current host, not full-recent 64 GB or p99 qualification.
"""
import argparse
import datetime
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TEST = "controller_update_campaign"


def cargo_artifacts(profile, args):
    out = subprocess.run(["cargo", *args, "--profile", profile, "--message-format=json"],
                         cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True).stdout
    return [json.loads(line) for line in out.splitlines() if line.startswith("{")]


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="release-fast")
    parser.add_argument("--updates", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=900, help="whole-campaign seconds")
    parser.add_argument("--out", type=Path, default=ROOT / ".local-pir/https-update-campaign")
    args = parser.parse_args()
    source = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                            stdout=subprocess.PIPE, text=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, check=True,
                           stdout=subprocess.PIPE, text=True).stdout != ""
    provider = next(a["executable"] for a in cargo_artifacts(args.profile, [
        "build", "-p", "transparent-query-server", "--bin", "transparent-query-server"])
        if a.get("executable"))
    built = [a for a in cargo_artifacts(args.profile, [
        "test", "-p", "transparent-publisher", "--test", TEST, "--no-run"])
        if a.get("executable") or a.get("target", {}).get("name") == "transparent-publish-controller"]
    test = next(a["executable"] for a in built if a.get("target", {}).get("name") == TEST)
    controller = next(a["executable"] for a in built
                      if a.get("target", {}).get("name") == "transparent-publish-controller")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = args.out / f"{stamp}-{source[:12]}"
    run.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema": "transparent-https-update-campaign-v1",
        "source_sha": source, "source_dirty": dirty, "profile": args.profile,
        "updates": args.updates,
        "binaries": {name: {"path": path, "sha256": sha256(path)} for name, path in
                     [("transparent-query-server", provider), ("campaign-test", test),
                      ("transparent-publish-controller", controller)]},
        "host": {"machine": platform.machine(), "cpus": os.cpu_count(),
                 "memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                 "kernel": platform.release()},
        "dataset": "synthetic fixture journal (heights 0-4, then one block per update); "
                   "archive below height 2, decoupled-2k-8k recent tier",
        "scope": "correctness/diagnostic; not full-recent 64GB, p99, original-app, "
                 "isolated-hardware, release or live qualification",
    }
    (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    env = dict(os.environ, WALLET_PIR_PROVIDER_BIN=provider, WALLET_PIR_CAMPAIGN_OUT=str(run),
               WALLET_PIR_CAMPAIGN_UPDATES=str(args.updates), WALLET_PIR_SOURCE_SHA=source)
    with (run / "test.log").open("xb") as log:
        try:
            code = subprocess.run([test, "--ignored", "--exact", "--nocapture",
                                   "controller_updates_reach_independent_https_providers_under_queries"],
                                  cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                  timeout=args.timeout).returncode
        except subprocess.TimeoutExpired:
            code = "timeout"
    (run / "outcome.json").write_text(json.dumps({"exit": code}) + "\n")
    print(run)
    sys.exit(0 if code == 0 else 1)


if __name__ == "__main__":
    main()
