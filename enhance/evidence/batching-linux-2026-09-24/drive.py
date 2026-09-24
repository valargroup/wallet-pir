#!/usr/bin/env python3
"""Drive the isolated Linux fixture from a separate machine through SSH tunnels."""
import argparse
import hashlib
import json
import platform
import subprocess
import time
import urllib.request
from pathlib import Path


def metrics(port):
    path = "/metrics" if port == 19080 else "/internal/metrics"
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
        return response.read().decode()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loadtest", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--phase", choices=["quick", "low", "saturation", "threshold"], required=True)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    scenarios = {
        "quick": [(4, 30, 1), (8, 30, 1), (12, 30, 1), (16, 30, 1),
                  (24, 30, 1), (32, 30, 1), (8, 30, 8)],
        "low": [(0.1, 120, 1), (1, 90, 1), (2, 90, 1), (4, 90, 1), (8, 90, 1)],
        "saturation": [(6, 120, 1), (8, 120, 1), (10, 120, 1),
                       (12, 120, 1), (16, 120, 1), (20, 120, 1)],
        "threshold": [(12, 60, 1), (14, 60, 1), (16, 60, 1), (8, 30, 8)],
    }[args.phase]
    report = {"phase": args.phase, "platform": platform.platform(),
              "loadtest_sha256": hashlib.sha256(args.loadtest.read_bytes()).hexdigest(),
              "steps": []}
    for index, (rate, duration, burst) in enumerate(scenarios):
        label = f"{index:02d}-qps-{rate}-burst-{burst}"
        before = {str(port): metrics(port) for port in (19080, 19091, 19092)}
        command = [str(args.loadtest), "--fixture-oracle", "--server",
                   "http://127.0.0.1:19080", "--parallelism", "32",
                   "--rate", str(rate), "--burst-size", str(burst),
                   "--warmup", "0s", "--duration", f"{duration}s",
                   "--seed", "20260924", "--max-error-rate", "1",
                   "--json-out", str(args.out_dir / f"{label}.json")]
        with (args.out_dir / f"{label}.log").open("w") as output:
            result = subprocess.run(command, stdout=output, stderr=output,
                                    timeout=duration + 180)
        after = {str(port): metrics(port) for port in (19080, 19091, 19092)}
        report["steps"].append({"label": label, "rate": rate, "duration": duration,
                                "burst": burst, "exit_code": result.returncode,
                                "command": command, "before": before, "after": after})
        (args.out_dir / "run.json").write_text(json.dumps(report, indent=2) + "\n")
        print(label, result.returncode, flush=True)
        if result.returncode:
            raise RuntimeError(f"load driver failed: {label}")
        time.sleep(3)


if __name__ == "__main__":
    run()
