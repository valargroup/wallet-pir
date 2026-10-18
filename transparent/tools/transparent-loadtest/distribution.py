#!/usr/bin/env python3
"""Explicit prepared-history distribution load; never a serving/authority verifier.

Run against the real controller's configured HTTPS terminator. Each logical
consumer owns a cache and transfers one object at a time. Reuse the output root
with a new phase name after the producer updates. Raw attempts, including failed
and partial bodies, remain in that root. This is separate from CPU correctness.
"""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import random
import ssl
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

PREFIX = "/provider/v1/"
METADATA_LIMIT = 16 << 20


def digest_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save(path, value):
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class Consumer:
    def __init__(self, args, number, barrier):
        self.args, self.number, self.barrier = args, number, barrier
        self.root = args.output / (f"consumer-{number:03}" if number >= 0 else "control")
        self.root.mkdir(exist_ok=True)
        self.objects = self.root / "objects"
        self.objects.mkdir(exist_ok=True)
        self.responses = self.root / "responses"
        self.responses.mkdir(exist_ok=True)
        self.log = self.root / f"{args.phase}.requests.jsonl"
        if self.log.exists():
            raise ValueError(f"attempt log already exists: {self.log}")
        self.context = ssl.create_default_context(cafile=args.ca)
        self.receipts = []
        self.reused = []
        self.charged = 0

    def request(self, suffix, limit, etag=None, target=None):
        """Every response byte read, even on errors, is charged and retained."""
        for attempt in range(1, self.args.attempts + 1):
            start = time.monotonic()
            record = {"path": PREFIX + suffix, "attempt": attempt,
                      "started_unix": time.time(), "status": None,
                      "bytes": 0, "if_none_match": etag, "error": None}
            connection = http.client.HTTPSConnection(
                self.args.origin.hostname, self.args.origin.port,
                context=self.context, timeout=self.args.timeout)
            hasher = hashlib.sha256()
            payload = bytearray()
            stream = None
            retry = False
            try:
                connection.request("GET", record["path"],
                                   headers={"If-None-Match": etag} if etag else {})
                transport = connection.sock
                response = connection.getresponse()
                record["status"] = response.status
                record["etag"] = response.getheader("ETag")
                if target is not None and response.status == 200:
                    stream = target.open("wb")
                while True:
                    if response.isclosed():
                        break
                    remaining = self.args.timeout - (time.monotonic() - start)
                    if remaining <= 0:
                        raise TimeoutError("whole request deadline")
                    # HTTPResponse can own the socket after connection.sock is cleared.
                    transport.settimeout(remaining)
                    chunk = response.read1(min(1 << 20, limit - record["bytes"] + 1))
                    if not chunk:
                        break
                    record["bytes"] += len(chunk)
                    self.charged += len(chunk)
                    hasher.update(chunk)
                    if record["bytes"] > limit or self.charged > 64 << 30:
                        raise ValueError("response body exceeds bound")
                    if stream:
                        stream.write(chunk)
                    else:
                        payload.extend(chunk)
                if response.status not in (200, 304):
                    retry = response.status in (408, 429, 500, 502, 503, 504)
                    raise ValueError(f"HTTP {response.status}")
                if response.status == 304 and (not etag or record["bytes"]):
                    raise ValueError("invalid conditional response")
                return bytes(payload), record
            except (OSError, http.client.HTTPException, ValueError) as error:
                record["error"] = f"{type(error).__name__}: {error}"
                retry = retry or isinstance(error, (OSError, http.client.HTTPException))
                if not retry or attempt == self.args.attempts:
                    raise
            finally:
                if stream:
                    stream.close()
                connection.close()
                record["sha256"] = hasher.hexdigest()
                record["seconds"] = time.monotonic() - start
                if payload:
                    retained = self.responses / record["sha256"]
                    if not retained.exists():
                        retained.write_bytes(payload)
                self.receipts.append(record)
                with self.log.open("a") as log:
                    log.write(json.dumps(record) + "\n")
            time.sleep(min(5, .2 * 2 ** (attempt - 1)) + random.uniform(0, .5))
        raise AssertionError("unreachable")

    def run(self):
        self.barrier.wait()
        start = time.monotonic()
        result = {"consumer": self.number, "complete": False}
        try:
            state_path = self.root / "current.json"
            old = json.loads(state_path.read_text()) if state_path.exists() else None
            if (self.args.phase == "bootstrap") == (old is not None):
                raise ValueError("bootstrap needs fresh caches; update needs bootstrap state")
            if old and old["publisher"] != self.args.publisher:
                raise ValueError("cache belongs to a different publisher")
            raw, response = self.request("current", METADATA_LIMIT,
                                         old["etag"] if old else None)
            if response["status"] == 304:
                raise ValueError("update phase did not observe a producer update")
            else:
                current = json.loads(raw)
                if current["eligibility"] != "canonical" or (
                    current["invalidated_from"] is not None and
                    current["invalidated_from"] <= current["anchor_height"]
                ):
                    raise ValueError("current is ineligible")
                if old and current["revision"] < old["current"]["revision"]:
                    raise ValueError("publisher revision regressed")
                if old and current["revision"] == old["current"]["revision"] and current != old["current"]:
                    raise ValueError("publisher contradicted an equal revision")
                inventory_sha = current["inventory_sha256"]
                raw, _ = self.request("inventories/" + inventory_sha, METADATA_LIMIT)
                if hashlib.sha256(raw).hexdigest() != inventory_sha:
                    raise ValueError("inventory digest mismatch")
                inventory = json.loads(raw)
                if inventory["schema"] != "transparent-prepared-distribution-v1":
                    raise ValueError("unexpected inventory schema")
                # This load client never activates data. The actual providers
                # independently validate classifications through the native loader.
                if inventory["profile"] != "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1":
                    raise ValueError("unexpected profile")
                save(self.root / f"{self.args.phase}.inventory.json", inventory)
                artifacts = inventory["artifacts"]
                maps = [a for a in artifacts if a["path"] == "shards.json" and a["kind"] == "metadata"]
                if len(maps) != 1:
                    raise ValueError("missing/duplicate map")
                ordered = maps + [a for a in artifacts if a is not maps[0]]
                shard_roles = None
                for artifact in ordered:
                    if artifact["kind"] == "archive_table":
                        if shard_roles.get(artifact["path"].split("/")[0]) != "archive_table":
                            raise ValueError("archive classification disagrees with map")
                        continue
                    if artifact["kind"] not in ("metadata", "recent_table", "recent_runtime"):
                        raise ValueError("unknown artifact classification")
                    if artifact["kind"] == "recent_table" and shard_roles.get(artifact["path"].split("/")[0]) != "recent_table":
                        raise ValueError("recent classification disagrees with map")
                    sha = artifact["sha256"]
                    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                        raise ValueError("invalid digest")
                    target = self.objects / sha
                    if target.exists() and target.stat().st_size == artifact["bytes"] and digest_file(target) == sha:
                        self.reused.append(sha)
                    else:
                        partial = target.with_suffix(".partial")
                        _, receipt = self.request("artifacts/" + sha, artifact["bytes"], target=partial)
                        if receipt["bytes"] != artifact["bytes"] or receipt["sha256"] != sha:
                            raise ValueError("artifact length/digest mismatch")
                        partial.replace(target)
                    if artifact is maps[0]:
                        mapping = json.loads(target.read_text())
                        if mapping["network"] != current["network"] or mapping["genesis_hash"] != current["genesis_hash"]:
                            raise ValueError("map chain differs from current")
                        shard_roles = {s["manifest_digest"]: "recent_table" if s["shard_id"] >= inventory["recent_from_shard"] else "archive_table" for s in mapping["shards"]}
                # Freeze the observed snapshot only after all immutable transfers.
                raw, final = self.request("current", METADATA_LIMIT)
                if json.loads(raw) != current:
                    raise ValueError("publisher changed during transfer")
                save(state_path, {"publisher": self.args.publisher, "current": current, "etag": final["etag"]})
                result["current"] = current
                result["unchanged"] = False
            for _ in range(self.args.polls):
                time.sleep(self.args.poll_interval)
                state = json.loads(state_path.read_text())
                _, poll = self.request("current", METADATA_LIMIT, state["etag"])
                if poll["status"] != 304:
                    raise ValueError("publication changed during unchanged polling")
            result["complete"] = True
        except Exception as error:
            result["error"] = f"{type(error).__name__}: {error}"
        result.update(seconds=time.monotonic() - start, reused=self.reused,
                      requests=len(self.receipts),
                      bytes=sum(r["bytes"] for r in self.receipts),
                      failed_attempts=sum(r["error"] is not None for r in self.receipts))
        save(self.root / f"{self.args.phase}.result.json", result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publisher", required=True)
    parser.add_argument("--ca", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=("bootstrap", "update"), required=True)
    parser.add_argument("--consumers", type=int, choices=(10, 100), required=True)
    parser.add_argument("--attempts", type=int, choices=range(1, 9), default=8)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--polls", type=int, default=2)
    parser.add_argument("--poll-interval", type=float, default=5)
    parser.add_argument("--cache-budget-bytes", type=int, default=8 << 30,
                        help="conservative total cache-plus-candidate bound (default 8 GiB)")
    args = parser.parse_args()
    args.origin = urlsplit(args.publisher)
    if (args.origin.scheme != "https" or not args.origin.hostname or
        args.origin.username or args.origin.password or args.origin.path not in ("", "/") or
        args.origin.query or args.origin.fragment):
        parser.error("publisher must be an authenticated HTTPS origin")
    if args.timeout <= 0 or args.polls < 0 or args.poll_interval < 0 or args.cache_budget_bytes <= 0:
        parser.error("invalid timeout/poll bounds")
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / f"{args.phase}.json").exists():
        parser.error("phase output already exists")
    # Reserve an entire candidate beside retained objects before starting a wave.
    # Reuse is still measured by each consumer; this conservative disk guard is
    # deliberately not a supported holdings or publisher capacity claim.
    control = Consumer(args, -1, None)
    raw, _ = control.request("current", METADATA_LIMIT)
    current = json.loads(raw)
    raw, _ = control.request("inventories/" + current["inventory_sha256"], METADATA_LIMIT)
    if hashlib.sha256(raw).hexdigest() != current["inventory_sha256"]:
        parser.error("budget inventory digest mismatch")
    inventory = json.loads(raw)
    candidate_bytes = sum(a["bytes"] for a in inventory["artifacts"] if a["kind"] != "archive_table") * args.consumers
    retained_bytes = sum(p.stat().st_size for p in args.output.glob("consumer-*/objects/*") if p.is_file())
    bound = {"candidate_bytes": candidate_bytes, "retained_bytes": retained_bytes,
             "cache_budget_bytes": args.cache_budget_bytes,
             "permitted": candidate_bytes + retained_bytes <= args.cache_budget_bytes}
    save(args.output / f"{args.phase}.disk-bound.json", bound)
    if not bound["permitted"]:
        print(json.dumps({"complete": 0, "phase": args.phase, "disk_bound": bound}))
        return 2
    barrier = threading.Barrier(args.consumers)
    consumers = [Consumer(args, number, barrier) for number in range(args.consumers)]
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.consumers) as pool:
        results = list(pool.map(lambda consumer: consumer.run(), consumers))
    report = {"publisher": args.publisher, "consumers": args.consumers,
              "phase": args.phase, "seconds": time.monotonic() - started,
              "complete": sum(r["complete"] for r in results), "results": results,
              "scope": "artifact transfer diagnostics; no native activation or capacity qualification"}
    save(args.output / f"{args.phase}.json", report)
    print(json.dumps({k: v for k, v in report.items() if k != "results"}))
    return 0 if report["complete"] == args.consumers else 1


if __name__ == "__main__":
    raise SystemExit(main())
