#!/usr/bin/env python3
"""Run the explicit real-controller HTTPS update campaign and retain its raw run.

Builds the packaged provider, the ignored campaign test and the isolated
original released391 client with one Cargo profile, verifies the client's
registry sources, records exact source/binary/host identities, then runs the
campaign and the in-process fencing/invalidation tests it relies on with a new
ignored output root under .local-pir/. Failed runs stay on disk and the exit
status is the campaign's. The manifest gains the dataset identity (journal and
publication digests) the campaign observed; summary.json is computed only from
the raw observations. This is a small-fixture correctness/diagnostic campaign
on the current host, not full-recent 64 GB or p99 qualification.
"""
import argparse
import datetime
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TEST = "controller_update_campaign"
CLIENT = ROOT / "src/transparent/transparent-query-server/tests/frozen-client"
# Fencing, invalidation, replay and interrupted-staging cases stay in-process;
# the campaign does not duplicate them and the run records their outcome.
IN_PROCESS = "history::controller::distribution_tests::"


def cargo_artifacts(profile, args, manifest=None):
    command = ["cargo", *args, "--profile", profile, "--message-format=json"]
    if manifest:
        command += ["--locked", "--manifest-path", str(manifest)]
    out = subprocess.run(command, cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True).stdout
    return [json.loads(line) for line in out.splitlines() if line.startswith("{")]


def executable(artifacts, name):
    return next(a["executable"] for a in artifacts
                if a.get("executable") and a.get("target", {}).get("name") == name)


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))] if ordered else None


def summarize(run):
    """Counts and latencies derived only from raw observation lines."""
    records = [json.loads(line) for line in (run / "observations.jsonl").read_text().splitlines()]
    phases = {}
    for r in (r for r in records if r["kind"] == "recovery"):
        phase = phases.setdefault(r["phase"], {"outcomes": {}, "ok_ms": []})
        key = f"{r['provider']}:{r['outcome']}"
        phase["outcomes"][key] = phase["outcomes"].get(key, 0) + 1
        if r["outcome"] == "ok":
            phase["ok_ms"].append(r["ms"])
    for phase in phases.values():
        latencies = phase.pop("ok_ms")
        phase["ok_latency_ms"] = {"count": len(latencies), "p50": percentile(latencies, .5),
                                  "p99": percentile(latencies, .99),
                                  "max": max(latencies) if latencies else None,
                                  "mean": statistics.fmean(latencies) if latencies else None}
    totals = {}
    for r in (r for r in records if r["kind"] == "recovery"):
        totals[r["outcome"]] = totals.get(r["outcome"], 0) + 1
    stages = [{"phase": r["phase"], "provider": r["provider"], "map_sha256": r["map_sha256"],
               "stages": r["change"]["progress"].get("stages"),
               "downloaded_files": r["change"]["progress"].get("downloaded_files")}
              for r in records if r["kind"] == "provider_stages"]
    refusals = [{"phase": r["phase"], "provider": r["provider"], "error": r["error"]}
                for r in records if r["kind"] == "provider_sync" and r.get("state") == "refused"]
    waves = []
    for r in (r for r in records if r["kind"] == "distribution_wave"):
        statuses, body = {}, 0
        directory = run / r["output"]
        for log in directory.glob(f"consumer-*/{r['wave']}.requests.jsonl"):
            for receipt in map(json.loads, log.read_text().splitlines()):
                statuses[str(receipt["status"])] = statuses.get(str(receipt["status"]), 0) + 1
                body += receipt["bytes"]
        report = r.get("report") or {}
        waves.append({"consumers": r["consumers"], "wave": r["wave"], "exit": r["exit"],
                      "complete": report.get("complete"), "seconds": r["seconds"],
                      "http_statuses": statuses, "body_bytes": body,
                      "body_mb_per_second": body / r["seconds"] / 1e6 if r["seconds"] else None,
                      "cpu_seconds": {k: v / os.sysconf("SC_CLK_TCK") for k, v in r["cpu_ticks_delta"].items()},
                      "peak_rss_bytes": r["peak_rss_bytes"]})
    return {"schema": "transparent-https-update-campaign-summary-v1",
            "derived_from": "observations.jsonl and distribution receipts only",
            "recoveries": totals, "phases": phases, "provider_stages": stages,
            "provider_refusals": refusals, "distribution": waves,
            "faults": [r for r in records if r["kind"] == "fault"],
            "resynchronization": [r for r in records if r["kind"] == "provider_resynchronized"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="release-fast")
    parser.add_argument("--updates", type=int, default=3)
    parser.add_argument("--consumers", default="10",
                        help="comma-separated distribution consumer counts, e.g. 10,100")
    parser.add_argument("--distribution-bytes", type=int, default=8 << 30,
                        help="retained-content budget for each distribution root")
    parser.add_argument("--timeout", type=int, default=2700, help="whole-campaign seconds")
    parser.add_argument("--out", type=Path, default=ROOT / ".local-pir/https-update-campaign")
    args = parser.parse_args()
    source = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                            stdout=subprocess.PIPE, text=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, check=True,
                           stdout=subprocess.PIPE, text=True).stdout != ""
    provider = executable(cargo_artifacts(args.profile, [
        "build", "-p", "transparent-query-server", "--bin", "transparent-query-server"]),
        "transparent-query-server")
    built = cargo_artifacts(args.profile, ["test", "-p", "transparent-publisher", "--test", TEST, "--no-run"])
    test = executable(built, TEST)
    controller = next(a["executable"] for a in built
                      if a.get("target", {}).get("name") == "transparent-publish-controller")
    library = executable(cargo_artifacts(args.profile, [
        "test", "-p", "transparent-publisher", "--lib", "--no-run"]), "transparent_publisher")
    client = executable(cargo_artifacts(args.profile, ["build"], CLIENT / "Cargo.toml"),
                        "frozen-provider-client")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = args.out / f"{stamp}-{source[:12]}"
    run.mkdir(parents=True, exist_ok=False)
    subprocess.run([sys.executable, str(CLIENT / "verify-sources.py"), "--output", str(run / "client-sources")],
                   cwd=ROOT, check=True)
    manifest = {
        "schema": "transparent-https-update-campaign-v2",
        "source_sha": source, "source_dirty": dirty, "profile": args.profile,
        "updates": args.updates, "consumers": args.consumers,
        "binaries": {name: {"path": path, "sha256": sha256(path)} for name, path in
                     [("transparent-query-server", provider), ("campaign-test", test),
                      ("transparent-publish-controller", controller),
                      ("in-process-tests", library), ("frozen-provider-client", client)]},
        "frozen_client": json.loads((run / "client-sources/provenance.json").read_text()),
        "host": {"machine": platform.machine(), "cpus": os.cpu_count(),
                 "affinity_cpus": len(os.sched_getaffinity(0)),
                 "memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                 "kernel": platform.release(), "loadavg": os.getloadavg()},
        "fixture": "synthetic followed journal: one wallet paid at every height from 1; heights 0-4, "
                   "then one block per update; 32k archive tier below height 2 served by a fixed "
                   "owner process, decoupled-2k-8k recent tier",
        "scope": "correctness/diagnostic; not full-recent 64GB, p99, original-app, "
                 "isolated-hardware, release or live qualification",
    }
    (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with (run / "in-process.log").open("xb") as log:
        in_process = subprocess.run([library, IN_PROCESS, "--test-threads", "2"], cwd=ROOT,
                                    stdout=log, stderr=subprocess.STDOUT).returncode
    env = dict(os.environ, WALLET_PIR_PROVIDER_BIN=provider, WALLET_PIR_FROZEN_CLIENT=client,
               WALLET_PIR_CAMPAIGN_OUT=str(run), WALLET_PIR_CAMPAIGN_UPDATES=str(args.updates),
               WALLET_PIR_CAMPAIGN_CONSUMERS=args.consumers,
               WALLET_PIR_CAMPAIGN_DISTRIBUTION_BYTES=str(args.distribution_bytes),
               WALLET_PIR_SOURCE_SHA=source)
    with (run / "test.log").open("xb") as log:
        try:
            code = subprocess.run([test, "--ignored", "--exact", "--nocapture",
                                   "controller_updates_reach_independent_https_providers_under_queries"],
                                  cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                  timeout=args.timeout).returncode
        except subprocess.TimeoutExpired:
            code = "timeout"
    dataset = run / "dataset.json"
    if dataset.exists():
        manifest["dataset"] = json.loads(dataset.read_text())
        manifest["dataset_sha256"] = sha256(dataset)
        (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if (run / "observations.jsonl").exists():
        (run / "summary.json").write_text(json.dumps(summarize(run), indent=2) + "\n")
    (run / "outcome.json").write_text(json.dumps({"exit": code, "in_process_exit": in_process}) + "\n")
    print(run)
    sys.exit(0 if code == 0 and in_process == 0 else 1)


if __name__ == "__main__":
    main()
