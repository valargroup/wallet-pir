#!/usr/bin/env python3
"""Second phase of the trial, after the serial run.sh was stopped early.

Runs the given variants for every table of tables.tsv (filtered by geometry)
that results.jsonl does not have yet, `--jobs` at a time, appending to the same
results.jsonl. Timed variants must run with --jobs 1; certificate builds need
only their output and may run in parallel.

    run_parallel.py <out-dir> <p8_trial> <certify_native.py> --variants v1,v2 \
        [--geometry decoupled-2k-8k] [--jobs 4]
"""
import argparse
import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("out")
parser.add_argument("binary")
parser.add_argument("checker")
parser.add_argument("--variants", required=True)
parser.add_argument("--geometry")
parser.add_argument("--jobs", type=int, default=1)
args = parser.parse_args()

results = os.path.join(args.out, "results.jsonl")
done = set()
for line in open(results):
    j = json.loads(line)
    if not j.get("error"):
        done.add((j["variant"], j["path"]))
tables = [line.rstrip("\n").split("\t") for line in open(os.path.join(args.out, "tables.tsv"))]
work = [(t, v) for t in tables for v in args.variants.split(",")
        if (args.geometry is None or t[0] == args.geometry) and (v, t[2]) not in done]
print(f"{len(work)} runs", flush=True)
lock = threading.Lock()


def run(item):
    (geometry, table, path, query_rows), variant = item
    report = os.path.join(args.out, "reports", "%s-%s-%s-q28.json" % (
        os.path.basename(os.path.dirname(path))[:16], os.path.basename(path)[: -len(".0.bin")],
        variant.removeprefix("certify-")))
    proc = subprocess.run(
        [args.binary, "--variant", variant, "--geometry", geometry, "--table", table,
         "--rows-bin", path, "--queries", "10", "--query-rows", query_rows,
         "--query-bits", "28", "--report", report],
        capture_output=True, text=True, env={**os.environ, "TRANSPARENT_BUILD_THREADS": "8"})
    if proc.returncode != 0:
        line = json.dumps({"variant": variant, "path": path, "error": True, "stderr": proc.stderr[-2000:]})
    else:
        j = json.loads(proc.stdout.strip().splitlines()[-1])
        if variant.startswith("certify-"):
            verdict = os.path.join(args.out, "certify", os.path.basename(report))
            check = subprocess.run(["python3", args.checker, "--require-bits", "128", report],
                                   capture_output=True, text=True)
            open(verdict, "w").write(check.stdout)
            try:
                j["certified_failure_bits"] = json.loads(check.stdout)["actual_profile"]["certified_failure_bits"]
            except (ValueError, KeyError):
                j["certified_failure_bits"] = None
        line = json.dumps(j, sort_keys=True, separators=(",", ":"))
    with lock:
        with open(results, "a") as f:
            f.write(line + "\n")


with ThreadPoolExecutor(args.jobs) as pool:
    list(pool.map(run, work))
print("done", flush=True)
