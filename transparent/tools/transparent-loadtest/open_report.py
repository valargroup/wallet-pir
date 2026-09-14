#!/usr/bin/env python3
"""Remember completed Make runs and open the newest available HTML report."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def latest_file():
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["git", "rev-parse", "--git-path", "transparent-sim-latest-report"],
        cwd=repo, check=True, capture_output=True, text=True,
    )
    return repo / result.stdout.strip()


def remembered_report(marker):
    try:
        return Path(json.loads(marker.read_text()))
    except (OSError, ValueError, TypeError):
        return None


def remember(directory):
    report = (Path(directory) / "report.html").resolve(strict=True)
    marker = latest_file()
    previous = remembered_report(marker)
    if previous and previous.is_file() and previous.stat().st_mtime_ns > report.stat().st_mtime_ns:
        return
    # Keep the previous pointer readable if this process is interrupted.
    with tempfile.NamedTemporaryFile(mode="w", dir=marker.parent, delete=False) as output:
        temporary = Path(output.name)
        json.dump(str(report), output)
    try:
        os.replace(temporary, marker)
    finally:
        temporary.unlink(missing_ok=True)


def select_report(directory):
    if directory:
        report = (Path(directory) / "report.html").resolve()
        if not report.is_file():
            raise ValueError(f"No report at {report}")
        return report
    candidates = set()
    previous = remembered_report(latest_file())
    if previous:
        candidates.add(previous)
    # Also discover default runs made before the latest-report pointer existed.
    for root in {Path(os.environ.get("TMPDIR") or "/tmp"), Path("/tmp")}:
        candidates.update(root.glob("transparent-sim-*/report.html"))
    available = [path for path in candidates if path.is_file()]
    if not available:
        raise ValueError("No saved simulation report found. Run make transparent-sim, or set SIM_OUT=/path/to/run.")
    return max(available, key=lambda path: (path.stat().st_mtime_ns, str(path))).resolve()


def display_report(report):
    """Older runs stored preparation failures separately from an empty main report."""
    try:
        data = json.loads(report.with_suffix(".json").read_text())
    except (OSError, ValueError):
        return report
    if not isinstance(data, dict) or data.get("users") or data.get("preparation"):
        return report
    if data.get("success") or not data.get("errors"):
        return report
    batches = list(report.parent.glob("preparation/batch-*/report.html"))
    if not batches:
        return report
    print("Measured wave did not start; opening its preparation results.", flush=True)
    return max(batches, key=lambda path: path.stat().st_mtime_ns).resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remember", metavar="DIRECTORY")
    args = parser.parse_args()
    if args.remember:
        remember(args.remember)
        return
    report = display_report(select_report(os.environ.get("SIM_OUT")))
    opener = "open" if sys.platform == "darwin" else "xdg-open"
    if not shutil.which(opener):
        raise ValueError(f"{opener} is unavailable. Open this file in your browser: {report}")
    print(f"Opening report: {report}", flush=True)
    subprocess.run([opener, str(report)], check=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Cannot open/remember simulation report: {error}", file=sys.stderr)
        sys.exit(1)
