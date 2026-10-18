#!/usr/bin/env python3
"""Own the bounded full-ingest health guard and retain its terminal result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def unit_state(unit):
    raw = subprocess.check_output(["systemctl", "show", unit, "--property=ActiveState,Result,MainPID,NRestarts,ExecMainStatus"], text=True)
    return dict(line.split("=", 1) for line in raw.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--through", type=int, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--disk", type=Path, action="append", required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    initial = unit_state(args.unit)
    assert initial["ActiveState"] == "active" and int(initial["MainPID"]) > 0
    pid = initial["MainPID"]
    binary = Path(f"/proc/{pid}/exe")
    owner = dict(pid=os.getpid(), guarded_unit=args.unit, guarded_pid=int(pid),
                 source_sha=args.source_sha, through=args.through, started_unix=time.time(),
                 binary_sha256=hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
                 driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (args.evidence / "owner.json").write_text(json.dumps(owner, indent=2) + "\n")
    failure = None
    state = initial
    with (args.evidence / "health.jsonl").open("x") as log:
        while True:
            state = unit_state(args.unit)
            memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
            available = int(memory["MemAvailable"].split()[0]) / int(memory["MemTotal"].split()[0])
            disks = {str(path): os.statvfs(path).f_bavail / os.statvfs(path).f_blocks for path in args.disk}
            log.write(json.dumps(dict(unix=time.time(), state=state, memory_available=available, disk_available=disks)) + "\n")
            log.flush()
            if available < .2 or any(value < .2 for value in disks.values()):
                failure = "memory or disk available fraction below 20 percent"
            elif state["ActiveState"] == "active" and (state["MainPID"] != pid or state["NRestarts"] != initial["NRestarts"]):
                failure = "unexpected ingest restart or process replacement"
            elif state["ActiveState"] != "active":
                if state.get("Result") != "success" or state.get("ExecMainStatus") != "0":
                    failure = "ingest terminated unsuccessfully"
                break
            if failure:
                # Candidate ingestion only; preserve all canonical service owners.
                subprocess.run(["systemctl", "stop", args.unit], check=True)
                break
            time.sleep(5)
    if not failure:
        checkpoint = (args.journal / "checkpoint.bin").read_bytes()
        metadata = json.loads((args.journal / "meta.json").read_text())
        if (len(checkpoint) != 16 or metadata["version"] != 3
                or int.from_bytes(checkpoint[8:], "little") != (args.through - metadata["start_height"] + 1) * 48):
            failure = "completed journal checkpoint does not reach the fixed anchor"
    (args.evidence / "result.json").write_text(json.dumps(dict(**owner, ended_unix=time.time(),
        status="failed" if failure else "passed", failure=failure, terminal_state=state), indent=2) + "\n")
    return 1 if failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
