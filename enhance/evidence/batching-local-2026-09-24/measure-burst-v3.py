#!/usr/bin/env python3
"""Run a disposable full-shard batch-rate sweep on localhost."""
import hashlib
import json
import platform
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent / "burst8-v3"
BIN = ROOT / "target/release-fast"
RECORDS = 32768 * 33 - 1
STEPS = [(8, 90, 8)]


def port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read()


def metrics(url):
    values = {}
    for line in get(url).decode().splitlines():
        if line and not line.startswith("#") and "{" not in line:
            key, value = line.split(" ", 1)
            try:
                values[key] = float(value)
            except ValueError:
                pass
    return values


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    server = BIN / "enhance-pir-server"
    load = BIN / "enhance-pir-load-test"
    report = {
        "kind": "isolated-full-shard-eight-query-bursts",
        "platform": platform.platform(),
        "records": RECORDS,
        "logical_rows": 32768,
        "expected_database_bytes_per_replica": 768 * 1024 * 1024,
        "binaries": {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in [("server", server), ("loadtest", load)]
        },
        "steps": [],
    }
    processes = []
    logs = []
    with tempfile.TemporaryDirectory(prefix="enhance-batch-") as temp:
        work = Path(temp)
        try:
            replicas = []
            for index in range(2):
                listen = port()
                url = f"http://127.0.0.1:{listen}"
                replicas.append({"name": f"replica-{index}", "url": url})
                log = (OUT / f"worker-{index}.log").open("w")
                logs.append(log)
                processes.append(subprocess.Popen(
                    [str(server), "worker", "--listen", f"127.0.0.1:{listen}",
                     "--data-dir", str(work / f"worker-{index}")],
                    stdout=log, stderr=log,
                ))
            inventory = work / "workers.json"
            inventory.write_text(json.dumps({"groups": [{"name": "group-1", "replicas": replicas}]}))
            listen = port()
            origin = f"http://127.0.0.1:{listen}"
            log = (OUT / "coordinator.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(
                [str(server), "coordinator", "--listen", f"127.0.0.1:{listen}",
                 "--data-dir", str(work / "coordinator"), "--worker-config", str(inventory),
                 "--isolated-fixture", "--fixture-records", str(RECORDS),
                 "--fixture-append-records", "0", "--poll-seconds", "30"],
                stdout=log, stderr=log,
            ))
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
                    raise RuntimeError("full-shard publication timed out")
                time.sleep(2)
            print("full shard ready", flush=True)
            for rate, duration, clients in STEPS:
                label = str(rate).replace(".", "p")
                before = {
                    "coordinator": metrics(origin + "/metrics"),
                    "workers": [metrics(replica["url"] + "/internal/metrics") for replica in replicas],
                }
                path = OUT / f"qps-{label}.json"
                command = [
                    str(load), "--fixture-oracle", "--server", origin,
                    "--parallelism", str(clients), "--rate", str(rate),
                    "--burst-size", "8",
                    "--warmup", "0s", "--duration", f"{duration}s",
                    "--seed", "20260924", "--max-error-rate", "1",
                    "--json-out", str(path),
                ]
                (OUT / f"qps-{label}.command.json").write_text(json.dumps(command, indent=2) + "\n")
                with (OUT / f"qps-{label}.log").open("w") as output:
                    result = subprocess.run(command, stdout=output, stderr=output,
                                            timeout=duration + 180)
                after = {
                    "coordinator": metrics(origin + "/metrics"),
                    "workers": [metrics(replica["url"] + "/internal/metrics") for replica in replicas],
                }
                entry = {"rate": rate, "duration": duration, "clients": clients,
                         "exit_code": result.returncode, "before": before, "after": after,
                         "report": path.name}
                report["steps"].append(entry)
                (OUT / "run.json").write_text(json.dumps(report, indent=2) + "\n")
                print(f"rate={rate} exit={result.returncode}", flush=True)
                if result.returncode:
                    raise RuntimeError(f"load driver failed at {rate} QPS")
                time.sleep(5)
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
            (OUT / "run.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
