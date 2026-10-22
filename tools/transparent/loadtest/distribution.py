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
import os
import random
import re
import socket
import subprocess
import sys
import ssl
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from wallet_pir_ops.artifact_checksum import StreamHasher, hash_bytes, hash_file, valid as valid_checksum

PREFIX = "/provider/v1/"
METADATA_LIMIT = 16 << 20
ETAG_LIMIT = 256
ERROR_LIMIT = 512
CURRENT_LIMIT = 2048
# ensure_ascii can encode a character as a surrogate pair (12 bytes). Include
# both tags, the bounded error and fixed receipt fields before any network IO.
RECEIPT_BYTES = 2048 + 12 * (2 * ETAG_LIMIT + ERROR_LIMIT)


def json_bytes(value):
    return (json.dumps(value) + "\n").encode()


def consumer_credit_bytes(publisher, artifacts=0):
    # State and result are both retained. Current is bounded by its JSON encoded
    # size, publisher by its actual encoding, and reused entries are SHA256s.
    return (len(json_bytes(publisher)) + 2 * CURRENT_LIMIT + 12 * ETAG_LIMIT +
            12 * ERROR_LIMIT + 4096 + 80 * artifacts)


def wave_evidence_bytes(args, inventory, current):
    artifacts = [a for a in inventory["artifacts"] if a["kind"] != "archive_table"]
    return args.consumers * (
        len(json_bytes(inventory)) + 2 * len(json_bytes(current)) +
        RECEIPT_BYTES * args.attempts * (len(artifacts) + 3 + args.polls) +
        2 * consumer_credit_bytes(args.publisher, len(inventory["artifacts"])))


def digest_file(path):
    return hash_file(path)


class BudgetExceeded(ValueError):
    pass


class Credit:
    def __init__(self, budget, amount):
        self.budget, self.left = budget, amount
        budget.reserve(amount)

    def add(self, amount):
        self.budget.reserve(amount)
        self.left += amount

    def write(self, path, data, append=False):
        if len(data) > self.left:
            raise BudgetExceeded("reserved receipt/report space exhausted")
        self.left -= len(data)
        with path.open("ab" if append else "xb") as stream:
            stream.write(data)

    def save(self, path, value):
        raw = json_bytes(value)
        temporary = path.with_name(path.name + ".new")
        self.write(temporary, raw)
        temporary.replace(path)


class DiskBudget:
    """One hard retained-file-content ceiling across the whole wave.

    Hardlinks to immutable attempt bodies consume no additional body storage.
    Reservations conservatively include receipts and reports before reading.
    Filesystem metadata/block rounding is outside this file-content counter.
    """
    def __init__(self, root, limit):
        self.limit, self.used, self.lock = limit, 0, threading.Lock()
        seen = set()
        for path in root.rglob("*"):
            if path.is_symlink():
                raise ValueError("cache root contains a symlink")
            if path.is_file():
                st = path.stat()
                key = (st.st_dev, st.st_ino)
                if key not in seen:
                    seen.add(key)
                    self.used += st.st_size
        if self.used > limit:
            raise BudgetExceeded("retained files already exceed disk budget")

    def reserve(self, amount):
        with self.lock:
            if amount < 0 or self.used + amount > self.limit:
                raise BudgetExceeded("retained body/evidence disk budget exhausted")
            self.used += amount

    def refund(self, amount):
        with self.lock:
            self.used -= amount

    def capacity(self, maximum):
        with self.lock:
            amount = min(maximum, self.limit - self.used)
            if amount <= 0:
                raise BudgetExceeded("retained body/evidence disk budget exhausted")
            self.used += amount
            return amount


def is_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def check_current(current):
    if (not isinstance(current, dict) or
        current.get("schema") != "transparent-prepared-distribution-v2" or
        not valid_checksum(current.get("inventory_checksum")) or
        not is_digest(current.get("genesis_hash")) or not is_digest(current.get("anchor_hash")) or
        type(current.get("revision")) is not int or current["revision"] < 0 or
        type(current.get("anchor_height")) is not int or current["anchor_height"] < 0 or
        type(current.get("recovery_epoch")) is not int or current["recovery_epoch"] < 0 or
        not isinstance(current.get("network"), str) or not 0 < len(current["network"]) <= 32 or
        len(json_bytes(current)) - 1 > CURRENT_LIMIT or
        (current.get("invalidated_from") is not None and type(current["invalidated_from"]) is not int)):
        raise ValueError("invalid current identity/digest")
    if current["eligibility"] != "canonical" or (current["invalidated_from"] is not None and current["invalidated_from"] <= current["anchor_height"]):
        raise ValueError("current is ineligible")


PROFILE = "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1"
RUNTIME = "transparent-runtime-v4/native-two-mask-p8-g27x1/interleaved-u8/ipir-35069f1/reinspiring-0.1.3-rc.1"


def validate_inventory(inventory, current):
    if (inventory.get("schema") != "transparent-prepared-distribution-v2" or
        inventory.get("profile") != PROFILE or inventory.get("runtime") != RUNTIME or
        inventory.get("shard_schema") != "transparent-shard-v12" or
        inventory.get("network") != current["network"] or
        inventory.get("genesis_hash") != current["genesis_hash"] or
        not is_digest(inventory.get("map_sha256"))):
        raise ValueError("incompatible inventory identity/profile/runtime")
    artifacts = inventory.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) > 100000:
        raise ValueError("invalid artifact inventory size")
    seen = set()
    for artifact in artifacts:
        path, kind, size = artifact["path"], artifact["kind"], artifact["bytes"]
        if (not isinstance(path, str) or len(path) > 256 or path in seen or
            any(c in ("", ".", "..") or not re.fullmatch(r"[a-zA-Z0-9_.-]+", c) for c in path.split("/")) or
            not valid_checksum(artifact["checksum"]) or type(size) is not int or size <= 0):
            raise ValueError("unsafe/duplicate artifact path, digest or length")
        seen.add(path)
        if kind == "metadata":
            if size > METADATA_LIMIT or (path != "shards.json" and not re.fullmatch(r"[0-9a-f]{64}/(?:manifest.json|filter.bin)", path)):
                raise ValueError("invalid metadata classification/path/size")
        elif kind == "recent_runtime":
            if not re.fullmatch(r"[0-9a-f]{64}-[0-9a-f]{64}\.runtime", path):
                raise ValueError("invalid runtime identity/path")
        elif kind in ("recent_table", "archive_table"):
            if not re.fullmatch(r"[0-9a-f]{64}/(?:directory|pages-[0-9]+)\.[0-9]+\.bin", path):
                raise ValueError("invalid table path")
        else:
            raise ValueError("unknown artifact classification")
    if not any(a["path"] == "shards.json" and a["kind"] == "metadata" for a in artifacts):
        raise ValueError("inventory omits shard map")


def validate_map(inventory, current, mapping):
    canonical = json.dumps(mapping, separators=(",", ":"), ensure_ascii=False).encode()
    shards = mapping.get("shards")
    boundary = inventory.get("recent_from_shard")
    if (hashlib.sha256(canonical).hexdigest() != inventory["map_sha256"] or
        mapping.get("network") != current["network"] or mapping.get("genesis_hash") != current["genesis_hash"] or
        not isinstance(shards, list) or not shards or len(shards) > 100000 or
        type(boundary) is not int or not 0 <= boundary < len(shards)):
        raise ValueError("inventory/map digest, chain or holdings mismatch")
    expected = {"shards.json"}
    roles = {}
    for index, shard in enumerate(shards):
        digest = shard["manifest_digest"]
        if shard["shard_id"] != index or not is_digest(digest) or digest in roles:
            raise ValueError("invalid map shard identity")
        roles[digest] = "recent_table" if index >= boundary else "archive_table"
        expected.update((digest + "/manifest.json", digest + "/filter.bin"))
    if shards[-1].get("end_height") != current["anchor_height"] or shards[-1].get("terminal_block_hash") != current["anchor_hash"]:
        raise ValueError("map/current anchor mismatch")
    for artifact in inventory["artifacts"]:
        if artifact["kind"] == "metadata":
            if artifact["path"] not in expected:
                raise ValueError("metadata path disagrees with map")
            expected.remove(artifact["path"])
        elif artifact["kind"] in ("recent_table", "archive_table"):
            if roles.get(artifact["path"].split("/")[0]) != artifact["kind"]:
                raise ValueError("table classification disagrees with map")
    if expected:
        raise ValueError("inventory omits required metadata")


def resolve(host, port, timeout):
    # A killable, joined resolver keeps blocking system DNS inside this attempt's
    # deadline too; no unowned resolver thread survives a timed-out request.
    code = "import socket,json,sys;print(json.dumps(socket.getaddrinfo(sys.argv[1],int(sys.argv[2]),type=socket.SOCK_STREAM)))"
    try:
        result = subprocess.run([sys.executable, "-c", code, host, str(port)],
                                capture_output=True, text=True, timeout=timeout, check=True)
    except subprocess.TimeoutExpired as error:
        raise TimeoutError("whole request deadline during DNS") from error
    return json.loads(result.stdout)


class CompleteResponse(http.client.HTTPResponse):
    def begin(self):
        original = self.fp
        class Headers:
            def readline(self, *args):
                line = original.readline(*args)
                if not line:
                    raise http.client.RemoteDisconnected("incomplete HTTP headers")
                return line
            def __getattr__(self, name):
                return getattr(original, name)
        self.fp = Headers()
        try:
            super().begin()
        finally:
            if self.fp is not None:
                self.fp = original

    def _get_chunk_left(self):
        if self.chunk_left == 0 and self._safe_read(2) != b"\r\n":
            raise http.client.HTTPException("invalid chunk delimiter")
        if self.chunk_left in (None, 0):
            line = self.fp.readline(65537)
            size = line.split(b";", 1)[0].strip()
            if len(line) > 65536 or not line.endswith(b"\r\n") or not re.fullmatch(b"[0-9a-fA-F]+", size):
                raise http.client.IncompleteRead(b"")
            self.chunk_left = int(size, 16)
            if self.chunk_left == 0:
                self._read_and_discard_trailer()
                self._close_conn()
                self.chunk_left = None
        return self.chunk_left

    def _read_and_discard_trailer(self):
        for _ in range(101):
            line = self.fp.readline(65537)
            if not line:
                raise http.client.IncompleteRead(b"")
            if len(line) > 65536:
                raise http.client.LineTooLong("trailer")
            if line in (b"\r\n", b"\n"):
                return
        raise http.client.HTTPException("too many trailer fields")


class DeadlineHTTPS(http.client.HTTPSConnection):
    response_class = CompleteResponse
    def __init__(self, origin, context, deadline, sockets):
        super().__init__(origin.hostname, origin.port, context=context,
                         timeout=max(.001, deadline - time.monotonic()))
        self.deadline, self.sockets = deadline, sockets

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("whole request deadline")
        return remaining

    def connect(self):
        addresses = resolve(self.host, self.port, self.remaining())
        last = None
        for family, kind, protocol, _, address in addresses:
            sock = socket.socket(family, kind, protocol)
            self.sockets.append(sock)
            try:
                sock.settimeout(self.remaining())
                sock.connect(tuple(address))
                self.sock = self._context.wrap_socket(sock, server_hostname=self.host,
                                                      do_handshake_on_connect=False)
                self.sockets.append(self.sock)
                self.sock.settimeout(self.remaining())
                self.sock.do_handshake()
                self.remaining()
                return
            except OSError as error:
                last = error
                sock.close()
                if self.sock:
                    self.sock.close()
                self.remaining()
        raise last or OSError("DNS returned no stream addresses")


class Consumer:
    def __init__(self, args, number, barrier):
        self.args, self.number, self.barrier = args, number, barrier
        self.root = args.output / (f"consumer-{number:03}" if number >= 0 else "control")
        self.root.mkdir(exist_ok=True)
        self.objects = self.root / "objects"
        self.objects.mkdir(exist_ok=True)
        self.responses = self.root / "responses"
        self.responses.mkdir(exist_ok=True)
        self.bodies = self.root / "attempts"
        self.bodies.mkdir(exist_ok=True)
        self.log = self.root / f"{args.phase}.requests.jsonl"
        if self.log.exists():
            raise ValueError(f"attempt log already exists: {self.log}")
        self.context = ssl.create_default_context(cafile=args.ca)
        self.budget = getattr(args, "budget", None) or DiskBudget(args.output, getattr(args, "cache_budget_bytes", 8 << 30))
        self.result_credit = Credit(self.budget, consumer_credit_bytes(args.publisher) if number >= 0 else 0)
        self.receipts, self.reused, self.charged = [], [], 0

    def request(self, suffix, limit, etag=None, target=None):
        """Retain each unique attempt body, including framing/timeout failures."""
        if suffix != "current" and not any(suffix.startswith(prefix) and is_digest(suffix[len(prefix):]) for prefix in ("inventories/", "artifacts/")):
            raise ValueError("invalid request digest/path")
        if etag is not None and (not isinstance(etag, str) or len(etag) > ETAG_LIMIT):
            raise ValueError("invalid cached ETag")
        for attempt in range(1, self.args.attempts + 1):
            receipt_credit = Credit(self.budget, RECEIPT_BYTES)
            start = time.monotonic()
            record = {"path": PREFIX + suffix, "attempt": attempt,
                      "started_unix": time.time(), "status": None,
                      "bytes": 0, "if_none_match": etag, "error": None}
            body_path = self.bodies / f"{self.args.phase}-{len(self.receipts):06}.body"
            record["body_path"] = str(body_path.relative_to(self.root))
            sockets, expired = [], threading.Event()
            def expire():
                expired.set()
                for sock in list(sockets):
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()
            timer = threading.Timer(self.args.timeout, expire)
            connection = DeadlineHTTPS(self.args.origin, self.context, start + self.args.timeout, sockets)
            hasher, payload, retry = StreamHasher(), bytearray(), False
            response = None
            timer.start()
            try:
                with body_path.open("xb") as stream:
                    connection.request("GET", record["path"], headers={"If-None-Match": etag} if etag else {})
                    response = connection.getresponse()
                    connection.remaining()
                    record["status"] = response.status
                    received_etag = response.getheader("ETag")
                    if received_etag and len(received_etag) > ETAG_LIMIT:
                        # Rejected headers can approach http.client's aggregate
                        # header bound. Never put them unbounded in the receipt.
                        # Identify the rejected value without pretending it is an
                        # accepted cache validator; no body has been read yet.
                        record["rejected_etag"] = {
                            "characters": len(received_etag),
                            "sha256": hashlib.sha256(received_etag.encode("latin-1")).hexdigest()}
                        raise ValueError("oversized ETag")
                    record["etag"] = received_etag
                    lengths = response.headers.get_all("Content-Length", [])
                    # HTTP field values are Latin-1 strings here. Unicode digit
                    # predicates accept values (e.g. superscript two) that the
                    # HTTP parser cannot interpret as a length and treats as EOF.
                    if lengths and (len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]) or response.chunked):
                        raise ValueError("ambiguous HTTP framing")
                    if lengths:
                        # http.client falls back to close-delimited reading when
                        # int() rejects a decimal field (including its digit
                        # limit). Validate independently before reading any body.
                        try:
                            declared_length = int(lengths[0])
                        except ValueError as error:
                            raise ValueError("invalid numeric Content-Length") from error
                        # A 304 has no body; its length can describe the selected
                        # representation rather than this response's payload.
                        if response.status != 304:
                            if declared_length > limit:
                                raise ValueError("Content-Length exceeds body bound")
                            if response.length != declared_length:
                                raise ValueError("inconsistent parsed Content-Length")
                    encoding = response.getheader("Transfer-Encoding")
                    if encoding and encoding.lower() != "chunked":
                        raise ValueError("unsupported transfer encoding")
                    while not response.isclosed() and response.length != 0:
                        connection.remaining()
                        capacity = self.budget.capacity(min(1 << 20, limit - record["bytes"] + 1, (64 << 30) - self.charged + 1,
                                                            response.length if response.length else 1 << 20))
                        try:
                            chunk = response.read1(capacity)
                        except BaseException:
                            self.budget.refund(capacity)
                            raise
                        self.budget.refund(capacity - len(chunk))
                        if not chunk:
                            break
                        stream.write(chunk)
                        record["bytes"] += len(chunk)
                        self.charged += len(chunk)
                        hasher.update(chunk, deadline=connection.deadline)
                        if target is None:
                            payload.extend(chunk)
                        if record["bytes"] > limit or self.charged > 64 << 30:
                            raise ValueError("response body exceeds bound")
                    connection.remaining()
                    if response.length not in (None, 0) or (response.chunked and response.chunk_left is not None):
                        raise http.client.IncompleteRead(b"", response.length)
                    if response.status not in (200, 304):
                        retry = response.status in (408, 429, 500, 502, 503, 504)
                        raise ValueError(f"HTTP {response.status}")
                    if response.status == 304 and (not etag or record["bytes"]):
                        raise ValueError("invalid conditional response")
                    if suffix.startswith("artifacts/") and (record["bytes"] != limit or hasher.hexdigest() != suffix.split("/")[1]):
                        raise ValueError("artifact length/digest mismatch")
                if target is not None:
                    os.link(body_path, target)
                elif payload:
                    retained = self.responses / hasher.hexdigest()
                    if not retained.exists():
                        os.link(body_path, retained)
                return bytes(payload), record
            except (OSError, http.client.HTTPException, ValueError, subprocess.SubprocessError) as error:
                record["error"] = ("TimeoutError: whole request deadline" if expired.is_set() else f"{type(error).__name__}: {error}")[:ERROR_LIMIT]
                retry = retry or (not isinstance(error, BudgetExceeded) and isinstance(error, (OSError, http.client.HTTPException)))
                if not retry or attempt == self.args.attempts:
                    if expired.is_set():
                        raise TimeoutError("whole request deadline") from error
                    raise
            finally:
                timer.cancel()
                timer.join()
                if response is not None:
                    response.close()
                connection.close()
                for sock in sockets:
                    sock.close()
                checksum_error = None
                try:
                    record["checksum"] = hasher.finish(timeout=self.args.timeout)
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    record["checksum"] = None
                    if record["error"] is None:
                        record["error"] = ("artifact checksum helper failed: " + str(error))[:ERROR_LIMIT]
                        checksum_error = error
                finally:
                    hasher.cancel()
                record["seconds"] = time.monotonic() - start
                self.receipts.append(record)
                receipt_credit.write(self.log, json_bytes(record), append=True)
                if checksum_error is not None:
                    raise checksum_error
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
                check_current(current)
                if getattr(self.args, "snapshot", None) is not None and current != self.args.snapshot:
                    raise ValueError("publisher changed since wave disk reservation")
                if old and current == old["current"]:
                    raise ValueError("update phase did not observe a producer update")
                if current["eligibility"] != "canonical" or (
                    current["invalidated_from"] is not None and
                    current["invalidated_from"] <= current["anchor_height"]
                ):
                    raise ValueError("current is ineligible")
                if old and current["revision"] < old["current"]["revision"]:
                    raise ValueError("publisher revision regressed")
                if old and current["revision"] == old["current"]["revision"] and current != old["current"]:
                    raise ValueError("publisher contradicted an equal revision")
                inventory_checksum = current["inventory_checksum"]
                inventory_sha = inventory_checksum["digest"]
                if not is_digest(inventory_sha):
                    raise ValueError("invalid inventory digest")
                raw, _ = self.request("inventories/" + inventory_sha, METADATA_LIMIT)
                if hash_bytes(raw) != inventory_checksum:
                    raise ValueError("inventory digest mismatch")
                inventory = json.loads(raw)
                if inventory["schema"] != "transparent-prepared-distribution-v2":
                    raise ValueError("unexpected inventory schema")
                # This load client never activates data. Native providers still
                # independently validate prepared material through their loader.
                if inventory["profile"] != "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1":
                    raise ValueError("unexpected profile")
                validate_inventory(inventory, current)
                self.result_credit.add(80 * len(inventory["artifacts"]))
                os.link(self.root / self.receipts[-1]["body_path"], self.root / f"{self.args.phase}.inventory.json")
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
                    checksum = artifact["checksum"]
                    sha = checksum["digest"]
                    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                        raise ValueError("invalid digest")
                    target = self.objects / (checksum["algorithm"] + "-" + sha)
                    if target.exists() and target.stat().st_size == artifact["bytes"] and digest_file(target) == checksum:
                        self.reused.append(sha)
                    else:
                        partial = target.with_name(sha + f".{self.args.phase}.{len(self.receipts)}.partial")
                        _, receipt = self.request("artifacts/" + sha, artifact["bytes"], target=partial)
                        if receipt["bytes"] != artifact["bytes"] or receipt["checksum"] != checksum:
                            raise ValueError("artifact length/digest mismatch")
                        if target.exists():
                            os.link(target, self.bodies / f"{self.args.phase}-replaced-{len(self.receipts)}.body")
                        partial.replace(target)
                    if artifact is maps[0]:
                        mapping = json.loads(target.read_text())
                        validate_map(inventory, current, mapping)
                        if mapping["network"] != current["network"] or mapping["genesis_hash"] != current["genesis_hash"]:
                            raise ValueError("map chain differs from current")
                        shard_roles = {s["manifest_digest"]: "recent_table" if s["shard_id"] >= inventory["recent_from_shard"] else "archive_table" for s in mapping["shards"]}
                # Freeze the observed snapshot only after all immutable transfers.
                raw, final = self.request("current", METADATA_LIMIT)
                if json.loads(raw) != current:
                    raise ValueError("publisher changed during transfer")
                self.result_credit.save(state_path, {"publisher": self.args.publisher, "current": current, "etag": final["etag"]})
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
            result["error"] = f"{type(error).__name__}: {error}"[:ERROR_LIMIT]
        result.update(seconds=time.monotonic() - start, reused=self.reused,
                      requests=len(self.receipts),
                      bytes=sum(r["bytes"] for r in self.receipts),
                      failed_attempts=sum(r["error"] is not None for r in self.receipts))
        self.result_credit.save(self.root / f"{self.args.phase}.result.json", result)
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
                        help="hard retained-file-content budget, including evidence (default 8 GiB)")
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
    # Every retained body, retry, metadata file and receipt shares this ceiling.
    # Too-small budgets refuse before even creating network/evidence work.
    if args.cache_budget_bytes < 8192 + len(json_bytes(args.publisher)):
        parser.error("disk budget too small for mandatory evidence")
    args.budget = DiskBudget(args.output, args.cache_budget_bytes)
    report_credit = Credit(args.budget, 8192 + len(json_bytes(args.publisher)))
    started = time.monotonic()
    try:
        control = Consumer(args, -1, None)
        raw, _ = control.request("current", METADATA_LIMIT)
        current = json.loads(raw)
        check_current(current)
        raw, _ = control.request("inventories/" + current["inventory_checksum"]["digest"], METADATA_LIMIT)
        if hash_bytes(raw) != current["inventory_checksum"]:
            raise ValueError("budget inventory digest mismatch")
        inventory = json.loads(raw)
        validate_inventory(inventory, current)
        map_artifact = next(a for a in inventory["artifacts"] if a["path"] == "shards.json")
        raw, _ = control.request("artifacts/" + map_artifact["checksum"]["digest"], map_artifact["bytes"])
        validate_map(inventory, current, json.loads(raw))
        args.snapshot = current
        artifacts = [a for a in inventory["artifacts"] if a["kind"] != "archive_table"]
        candidate_bytes = sum(a["bytes"] for a in artifacts) * args.consumers
        # Nominal full candidate plus all metadata/control receipts and final
        # reports. Error/partial bodies additionally acquire capacity BEFORE read;
        # they can refuse a wave but can never overrun the shared hard ceiling.
        evidence_bytes = wave_evidence_bytes(args, inventory, current)
        bound = {"candidate_bytes": candidate_bytes, "retained_bytes": args.budget.used,
                 "evidence_reservation_bytes": evidence_bytes,
                 "cache_budget_bytes": args.cache_budget_bytes,
                 "inventory_checksum": current["inventory_checksum"],
                 "permitted": args.budget.used + candidate_bytes + evidence_bytes <= args.cache_budget_bytes}
        report_credit.save(args.output / f"{args.phase}.disk-bound.json", bound)
        if not bound["permitted"]:
            report = {"complete": 0, "phase": args.phase, "disk_bound": bound}
            report_credit.save(args.output / f"{args.phase}.json", report)
            print(json.dumps(report))
            return 2
        report_credit.add(args.consumers * consumer_credit_bytes(args.publisher, len(inventory["artifacts"])))
        barrier = threading.Barrier(args.consumers)
        consumers = [Consumer(args, number, barrier) for number in range(args.consumers)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.consumers) as pool:
            results = list(pool.map(lambda consumer: consumer.run(), consumers))
        report = {"publisher": args.publisher, "consumers": args.consumers,
                  "phase": args.phase, "seconds": time.monotonic() - started,
                  "complete": sum(r["complete"] for r in results), "results": results,
                  "reserved_retained_bytes": args.budget.used,
                  "scope": "artifact transfer diagnostics; no native activation or capacity qualification"}
        report_credit.save(args.output / f"{args.phase}.json", report)
        print(json.dumps({k: v for k, v in report.items() if k != "results"}))
        return 0 if report["complete"] == args.consumers else 1
    except Exception as error:
        report = {"complete": 0, "phase": args.phase, "error": str(error)[:512],
                  "reserved_retained_bytes": args.budget.used}
        report_credit.save(args.output / f"{args.phase}.json", report)
        print(json.dumps(report))
        return 2 if isinstance(error, BudgetExceeded) else 1


if __name__ == "__main__":
    raise SystemExit(main())
