#!/usr/bin/env python3
"""Run isolated capacity cases on an idle Linux benchmark host, never a live worker.

Configuration: {"binary": "/absolute/binary", "cases": [{"name": "...", "args": [...]}]}.
Each case has a fresh cgroup; only units created by this invocation are stopped.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


def command(args):
    return subprocess.run(args, text=True, capture_output=True, check=True).stdout


def ci_active():
    return "Runner.Worker" in command(["ps", "-eo", "comm="]).splitlines()


def properties(unit):
    output = command(["systemctl", "show", unit, "-p", "ActiveState", "-p",
                      "SubState", "-p", "Result", "-p", "ExecMainStatus", "-p",
                      "ControlGroup", "-p", "MemoryHigh", "-p", "MemoryMax",
                      "-p", "MemorySwapMax", "-p", "CPUQuotaPerSecUSec"])
    return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)


def sample(unit, started):
    result = {"elapsed_seconds": time.monotonic() - started, **properties(unit)}
    group = result.get("ControlGroup")
    if group:
        directory = Path("/sys/fs/cgroup") / group.lstrip("/")
        for name in ["memory.current", "memory.peak", "memory.swap.current"]:
            try:
                result[name] = int((directory / name).read_text())
            except FileNotFoundError:
                pass
        for name in ["memory.events", "memory.stat"]:
            try:
                result[name] = {key: int(value) for key, value in
                                (line.split() for line in (directory / name).read_text().splitlines())}
            except FileNotFoundError:
                pass
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    if ci_active():
        raise RuntimeError("CI job active; leave shared benchmark host untouched")
    if subprocess.run(["systemctl", "is-active", "--quiet", "enhance-pir-worker"]).returncode == 0:
        raise RuntimeError("Enhance worker active; this runner requires an isolated host")
    config = json.loads(args.config.read_text())
    memory_high = config.get("memory_high", "6G")
    memory_max = config.get("memory_max", "7G")
    binary = Path(config["binary"]).resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "uname": command(["uname", "-a"]), "cpu": command(["lscpu"]),
        "meminfo": Path("/proc/meminfo").read_text(), "config": config,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "limits": {"MemoryHigh": memory_high, "MemoryMax": memory_max, "MemorySwapMax": "0",
                   "CPUQuota": "400%", "AllowedCPUs": "0-3", "RAYON_NUM_THREADS": "4"},
        "scope": "Isolated worker-runtime fixture; not architecture-2 full-service qualification",
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    summaries = []
    for index, case in enumerate(config["cases"]):
        name = case["name"]
        if not re.fullmatch(r"[a-z0-9-]+", name):
            raise ValueError("invalid case name")
        if ci_active():
            raise RuntimeError("CI became active; remaining cases not started")
        unit = f"enhance-capacity-{os.getpid()}-{index}"
        invocation = ["systemd-run", "--quiet", "--unit", unit,
                      "-p", "Type=exec", "-p", "RemainAfterExit=yes",
                      "-p", f"MemoryHigh={memory_high}", "-p", f"MemoryMax={memory_max}", "-p", "MemorySwapMax=0",
                      "-p", "CPUQuota=400%", "-p", "AllowedCPUs=0-3",
                      "-p", f"StandardOutput=append:{output / (name + '.jsonl')}",
                      "-p", f"StandardError=append:{output / (name + '.stderr')}",
                      "--setenv", "RAYON_NUM_THREADS=4", "/usr/bin/time", "-v", "-o",
                      str(output / (name + '.time')), str(binary), *map(str, case["args"])]
        command(invocation)
        started = time.monotonic()
        samples = []
        stopped = None
        try:
            with (output / (name + '.cgroup.jsonl')).open("w") as log:
                while True:
                    point = sample(unit, started)
                    samples.append(point)
                    log.write(json.dumps(point) + "\n")
                    log.flush()
                    if point.get("SubState") in ("exited", "failed", "dead"):
                        break
                    if ci_active():
                        stopped = "CI job became active"
                        break
                    if time.monotonic() - started > args.timeout:
                        stopped = "case timeout"
                        break
                    time.sleep(0.5)
        finally:
            command(["systemctl", "stop", unit])
        summary = {"name": name, "command": invocation, "stopped": stopped,
                   "wall_seconds": time.monotonic() - started,
                   "peak_cgroup_bytes": max((s.get("memory.peak", 0) for s in samples), default=0),
                   "max_anon_bytes": max((s.get("memory.stat", {}).get("anon", 0) for s in samples), default=0),
                   "max_file_bytes": max((s.get("memory.stat", {}).get("file", 0) for s in samples), default=0),
                   "events": {k: max((s.get("memory.events", {}).get(k, 0) for s in samples), default=0)
                              for k in ["high", "max", "oom", "oom_kill"]},
                   "final": samples[-1] if samples else {}}
        summaries.append(summary)
        (output / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
        print(json.dumps({k: v for k, v in summary.items() if k not in ["command", "final"]}), flush=True)
        if stopped:
            break


if __name__ == "__main__":
    main()
