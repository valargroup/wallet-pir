#!/usr/bin/env python3
"""Collect bounded, aggregate-only evidence for the shadow/active rollout gates."""
import argparse
import datetime
import json
import time
import urllib.request
from pathlib import Path


def sample(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = json.load(response)
        return {
            "progress_ok": body.get("progress_ok"),
            "shadow": body.get("shadow"),
            "evaluated_at": body.get("evaluated_at"),
            "canary_at": body.get("canary_at"),
            "canary_ok": body.get("canary_ok"),
            "canary_duration_seconds": body.get("canary_duration_seconds"),
            "failure_category": body.get("failure_category"),
            "delivery": body.get("delivery"),
            "publication": body.get("publication"),
            "quality": body.get("quality"),
            "services": body.get("services"),
            "chain": body.get("chain"),
            "incident_details": [i for i in body.get("incidents", [])
                                 if i.get("active")],
            "active": [i["condition"]["key"] for i in body.get("incidents", []) if i.get("active")],
            "unknown": [i["condition"]["key"] for i in body.get("incidents", [])
                        if i.get("condition") and not i["condition"].get("retired")
                        and i["condition"].get("firing") is None],
        }
    except Exception as exc:
        return {"unavailable": type(exc).__name__}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--hours", type=float, default=49)
    args = parser.parse_args()
    if not 0 < args.hours <= 72:
        parser.error("hours must be between zero and 72")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    until = time.monotonic() + args.hours * 3600
    while time.monotonic() < until:
        now = datetime.datetime.now(datetime.timezone.utc)
        row = {"at": now.isoformat(),
               "apm": sample("https://enhance-pir.valargroup.dev/apm/monitor-status"),
               "monitor": sample("https://monitor-pir.valargroup.dev/monitor-status")}
        path = args.output_dir / (now.strftime("%Y-%m-%d") + ".jsonl")
        with path.open("a") as output:
            output.write(json.dumps(row, sort_keys=True) + "\n")
        time.sleep(min(60, max(0, until - time.monotonic())))


if __name__ == "__main__":
    main()
