#!/usr/bin/env python3
"""Measure one batching policy against a persistent isolated full-shard fixture."""
import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
import urllib.request
from pathlib import Path

RECORDS = 32768 * 33 - 1


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read()


def metrics(url):
    return get(url).decode()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--delay-ms", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--external-worker-url")
    parser.add_argument("--phase", choices=["serve", "pilot", "low", "saturation"], required=True)
    args = parser.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    server = args.bin_dir / "enhance-pir-server"
    load = args.bin_dir / "enhance-pir-load-test"
    scenarios = {
        "serve": [],
        "pilot": [(8, 30, 1), (12, 30, 1), (16, 30, 1), (24, 30, 1), (32, 30, 1), (8, 30, 8)],
        "low": [(0.1, 120, 1), (1, 90, 1), (2, 90, 1), (4, 90, 1), (8, 90, 1)],
        "saturation": [(8, 120, 1), (12, 120, 1), (16, 120, 1), (24, 120, 1), (32, 120, 1)],
    }[args.phase]
    report = {
        "phase": args.phase,
        "delay_ms": args.delay_ms,
        "batch_size": args.batch_size,
        "platform": platform.platform(),
        "records": RECORDS,
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for name, path in [("server", server), ("loadtest", load)]},
        "steps": [],
    }
    processes = []
    logs = []
    try:
        replicas = []
        for index in range(2):
            if index == 1 and args.external_worker_url:
                replicas.append({"name": f"replica-{index}",
                                 "url": args.external_worker_url})
                continue
            listen = 19091 + index
            url = f"http://127.0.0.1:{listen}"
            replicas.append({"name": f"replica-{index}", "url": url})
            log = (args.out_dir / f"worker-{index}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(
                [str(server), "worker", "--listen", f"127.0.0.1:{listen}",
                 "--data-dir", str(args.work_dir / f"worker-{index}")],
                stdout=log, stderr=log))
        inventory = args.work_dir / "workers.json"
        inventory.write_text(json.dumps({"groups": [{"name": "group-1", "replicas": replicas}]}))
        listen = 19080
        origin = f"http://127.0.0.1:{listen}"
        log = (args.out_dir / "coordinator.log").open("w")
        logs.append(log)
        env = dict(os.environ, ENHANCE_BATCH_MAX_DELAY_MS=str(args.delay_ms),
                   ENHANCE_BATCH_MAX_SIZE=str(args.batch_size))
        processes.append(subprocess.Popen(
            [str(server), "coordinator", "--listen", f"127.0.0.1:{listen}",
             "--data-dir", str(args.work_dir / "coordinator"),
             "--worker-config", str(inventory), "--isolated-fixture",
             "--fixture-records", str(RECORDS), "--fixture-append-records", "0",
             "--poll-seconds", "30"], stdout=log, stderr=log, env=env))
        deadline = time.monotonic() + 1200
        while True:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("a server exited during startup")
            try:
                manifest = json.loads(get(origin + "/v1/enhance/init"))
                if manifest["coverage"]["records"] == RECORDS:
                    report["manifest"] = manifest
                    break
            except Exception:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("full shard publication timed out")
            time.sleep(2)
        print("full shard ready", flush=True)
        if args.phase == "serve":
            while all(process.poll() is None for process in processes):
                time.sleep(1)
            raise RuntimeError("a server exited while serving")
        for index, (rate, duration, burst) in enumerate(scenarios):
            label = f"{index:02d}-qps-{rate}-burst-{burst}"
            before = {"coordinator": metrics(origin + "/metrics"),
                      "workers": [metrics(r["url"] + "/internal/metrics") for r in replicas]}
            command = [str(load), "--fixture-oracle", "--server", origin,
                       "--parallelism", "8", "--rate", str(rate),
                       "--burst-size", str(burst), "--warmup", "0s",
                       "--duration", f"{duration}s", "--seed", "20260924",
                       "--max-error-rate", "1", "--json-out", str(args.out_dir / f"{label}.json")]
            with (args.out_dir / f"{label}.log").open("w") as output:
                result = subprocess.run(command, stdout=output, stderr=output,
                                        timeout=duration + 180)
            after = {"coordinator": metrics(origin + "/metrics"),
                     "workers": [metrics(r["url"] + "/internal/metrics") for r in replicas]}
            report["steps"].append({"label": label, "rate": rate, "duration": duration,
                                    "burst": burst, "exit_code": result.returncode,
                                    "command": command, "before": before, "after": after})
            (args.out_dir / "run.json").write_text(json.dumps(report, indent=2) + "\n")
            print(label, result.returncode, flush=True)
            if result.returncode:
                raise RuntimeError(f"load driver failed: {label}")
            time.sleep(3)
    finally:
        for process in reversed(processes):
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        (args.out_dir / "run.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    run()
