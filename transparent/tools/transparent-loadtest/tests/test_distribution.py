"""Small real-TLS controls for the artifact driver, not publisher qualification."""
import argparse
import hashlib
import http.server
import importlib.util
import json
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest

TOOL = Path(__file__).resolve().parents[1] / "distribution.py"
SPEC = importlib.util.spec_from_file_location("distribution", TOOL)
distribution = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(distribution)


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.paths = []
        self.refuse = False
        self.truncate = False
        self.publish(b"recent-one", 1)
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-days", "1", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost",
            "-addext", "basicConstraints=critical,CA:FALSE",
            "-keyout", str(self.root / "key.pem"), "-out", str(self.root / "ca.pem"),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                fixture.paths.append(self.path)
                status, body = 200, fixture.replies.get(self.path, b"")
                if fixture.refuse:
                    status, body = 503, b"bounded refusal"
                elif self.path.endswith("/current"):
                    if self.headers.get("If-None-Match") == fixture.etag:
                        status, body = 304, b""
                    else:
                        body = json.dumps(fixture.current).encode()
                self.send_response(status)
                self.send_header("ETag", fixture.etag)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if fixture.truncate and "/artifacts/" in self.path:
                    body = body[:1]
                self.wfile.write(body)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.root / "ca.pem", self.root / "key.pem")
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01))
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def publish(self, recent, revision):
        archive_revision, recent_revision = "a" * 64, "b" * 64
        mapping = {"network": "main", "genesis_hash": "00" * 32,
                   "shards": [{"shard_id": 0, "manifest_digest": archive_revision, "end_height": 0, "terminal_block_hash": "00" * 32},
                              {"shard_id": 1, "manifest_digest": recent_revision, "end_height": revision, "terminal_block_hash": f"{revision:064x}"}]}
        artifacts = []
        self.replies = getattr(self, "replies", {})
        for path, body, kind in (
            ("shards.json", json.dumps(mapping).encode(), "metadata"),
            (archive_revision + "/directory.0.bin", b"never download", "archive_table"),
            (recent_revision + "/directory.0.bin", recent, "recent_table"),
            (archive_revision + "/manifest.json", b"{}", "metadata"),
            (archive_revision + "/filter.bin", b"a", "metadata"),
            (recent_revision + "/manifest.json", b"{}", "metadata"),
            (recent_revision + "/filter.bin", b"b", "metadata"),
        ):
            digest = hashlib.sha256(body).hexdigest()
            artifacts.append({"path": path, "sha256": digest,
                              "bytes": len(body), "kind": kind})
            self.replies[distribution.PREFIX + "artifacts/" + digest] = body
        inventory = {"schema": "transparent-prepared-distribution-v1",
                     "profile": distribution.PROFILE, "runtime": distribution.RUNTIME,
                     "shard_schema": "transparent-shard-v12", "network": "main",
                     "genesis_hash": "00" * 32,
                     "map_sha256": hashlib.sha256(json.dumps(mapping, separators=(",", ":")).encode()).hexdigest(),
                     "recent_from_shard": 1, "artifacts": artifacts}
        raw = json.dumps(inventory).encode()
        digest = hashlib.sha256(raw).hexdigest()
        self.replies[distribution.PREFIX + "inventories/" + digest] = raw
        self.current = {"schema": "transparent-prepared-distribution-v1", "recovery_epoch": 0,
                        "revision": revision, "eligibility": "canonical",
                        "invalidated_from": None, "anchor_height": revision,
                        "anchor_hash": f"{revision:064x}",
                        "inventory_sha256": digest, "network": "main",
                        "genesis_hash": "00" * 32}
        self.etag = f'"revision-{revision}"'
        self.archive_digest = artifacts[1]["sha256"]

    def consumer(self, phase):
        output = self.root / "output"
        output.mkdir(exist_ok=True)
        args = argparse.Namespace(
            output=output, phase=phase, ca=self.root / "ca.pem",
            publisher=f"https://localhost:{self.server.server_port}",
            origin=distribution.urlsplit(f"https://localhost:{self.server.server_port}"),
            attempts=2, timeout=2, polls=1, poll_interval=0)
        return distribution.Consumer(args, 0, threading.Barrier(1))

    def test_update_reuses_verified_objects_and_never_fetches_archive(self):
        first = self.consumer("bootstrap").run()
        self.assertTrue(first["complete"], first)
        self.publish(b"recent-two", 2)
        second = self.consumer("update").run()
        self.assertTrue(second["complete"], second)
        self.assertEqual(len(second["reused"]), 4)
        self.assertNotIn(distribution.PREFIX + "artifacts/" + self.archive_digest, self.paths)
        receipts = [json.loads(line) for line in
                    (self.root / "output/consumer-000/update.requests.jsonl").read_text().splitlines()]
        self.assertEqual(sum(r["status"] == 304 for r in receipts), 1)

    def assert_retained_receipts(self, consumer):
        receipts = [json.loads(line) for line in consumer.log.read_text().splitlines()]
        self.assertEqual(receipts, consumer.receipts)
        for receipt in receipts:
            body = consumer.root / receipt["body_path"]
            self.assertEqual(body.stat().st_size, receipt["bytes"])
            self.assertEqual(distribution.digest_file(body), receipt["sha256"])
        return receipts

    def test_escaping_heavy_etags_retain_bootstrap_update_and_conditional_receipts(self):
        self.etag = '"' + chr(255) * 254 + '"'
        consumer = self.consumer("bootstrap")
        result = consumer.run()
        self.assertTrue(result["complete"], result)
        receipts = self.assert_retained_receipts(consumer)
        self.assertEqual(len(receipts), 9)
        self.assertEqual(receipts[-1]["status"], 304)
        self.assertGreater(len(distribution.json_bytes(receipts[-1])), 2048)
        self.assertEqual(receipts[-1]["if_none_match"], self.etag)
        self.assertEqual(receipts[-1]["etag"], self.etag)
        self.publish(b"recent-two", 2)
        self.etag = '"' + chr(254) * 254 + '"'
        update = self.consumer("update")
        result = update.run()
        self.assertTrue(result["complete"], result)
        self.assertEqual(len(result["reused"]), 4)
        self.assert_retained_receipts(update)
        self.assertEqual(json.loads((update.root / "current.json").read_text())["etag"], self.etag)
        self.assertEqual(json.loads((update.root / "update.result.json").read_text()), result)

    def test_rejected_oversized_escaped_etag_still_retains_failure_receipt(self):
        self.etag = '"' + chr(255) * 6000 + '"'
        consumer = self.consumer("bootstrap")
        result = consumer.run()
        self.assertFalse(result["complete"], result)
        self.assertEqual(result["failed_attempts"], 1)
        self.assertIn("oversized ETag", result["error"])
        receipt, = self.assert_retained_receipts(consumer)
        self.assertEqual(receipt["status"], 200)
        self.assertEqual(receipt["bytes"], 0)
        self.assertEqual(receipt["rejected_etag"], {
            "characters": len(self.etag),
            "sha256": hashlib.sha256(self.etag.encode("latin-1")).hexdigest()})
        self.assertNotIn("etag", receipt)
        self.assertFalse((consumer.root / "current.json").exists())

    def test_escaped_conditional_budget_failure_preserves_attempt_and_result(self):
        self.etag = '"' + chr(255) * 254 + '"'
        consumer = self.consumer("bootstrap")
        consumer.args.attempts = 1
        # Permit a real bootstrap, then leave only enough for the poll receipt.
        # A body on the poll must fail before overrunning the hard ceiling.
        original = consumer.request
        def constrained(suffix, limit, etag=None, target=None):
            if etag is not None:
                consumer.budget.limit = consumer.budget.used + distribution.RECEIPT_BYTES + 3
                self.refuse = True
            return original(suffix, limit, etag, target)
        consumer.request = constrained
        result = consumer.run()
        self.assertFalse(result["complete"], result)
        self.assertIn("BudgetExceeded", result["error"])
        self.assertEqual(result["failed_attempts"], 1)
        receipts = self.assert_retained_receipts(consumer)
        self.assertEqual(len(receipts), 9)
        self.assertEqual(receipts[-1]["status"], 503)
        self.assertEqual(receipts[-1]["bytes"], 3)
        self.assertEqual(receipts[-1]["if_none_match"], self.etag)
        self.assertIn("BudgetExceeded", receipts[-1]["error"])
        self.assertLessEqual(self.retained_size(consumer.args.output), consumer.budget.limit)
        self.assertEqual(json.loads((consumer.root / "bootstrap.result.json").read_text()), result)

    def test_wave_escaping_etags_preserve_aggregate_report_with_constrained_budget(self):
        self.etag = '"' + chr(255) * 254 + '"'
        output, budget = self.root / "escaped-wave", 4 << 20
        result = subprocess.run([
            sys.executable, str(TOOL), "--publisher", f"https://localhost:{self.server.server_port}",
            "--ca", str(self.root / "ca.pem"), "--output", str(output), "--consumers", "10",
            "--phase", "bootstrap", "--cache-budget-bytes", str(budget), "--polls", "1",
            "--poll-interval", "0", "--attempts", "1"],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads((output / "bootstrap.json").read_text())
        self.assertEqual(report["complete"], 10)
        for number in range(10):
            root = output / f"consumer-{number:03}"
            receipts = [json.loads(line) for line in (root / "bootstrap.requests.jsonl").read_text().splitlines()]
            self.assertEqual(len(receipts), 9)
            self.assertEqual(receipts[-1]["status"], 304)
            self.assertEqual(json.loads((root / "bootstrap.result.json").read_text()), report["results"][number])
        bound = json.loads((output / "bootstrap.disk-bound.json").read_text())
        self.assertTrue(bound["permitted"])
        self.assertLessEqual(self.retained_size(output), budget)

    def test_truncated_artifact_cannot_complete_or_publish_cache_pointer(self):
        self.truncate = True
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"])
        self.assertIn("IncompleteRead", result["error"])
        self.assertFalse((self.root / "output/consumer-000/current.json").exists())
        self.assertTrue(list((self.root / "output/consumer-000/attempts").glob("*.body")))

    def test_exhausted_refusals_retain_every_error_body_and_attempt(self):
        self.refuse = True
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_attempts"], 2)
        self.assertEqual(result["requests"], 2)
        self.assertEqual(result["bytes"], 2 * len(b"bounded refusal"))

    def test_premature_http_eof_with_correct_digest_is_rejected(self):
        consumer = self.consumer("bootstrap")
        consumer.args.attempts = 1
        body = b"complete artifact, incomplete HTTP"
        def premature(handler):
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(body) + 1))
            handler.end_headers()
            handler.wfile.write(body)
        self.server.RequestHandlerClass.do_GET = premature
        target = consumer.objects / "candidate"
        with self.assertRaises(distribution.http.client.IncompleteRead):
            consumer.request("artifacts/" + hashlib.sha256(body).hexdigest(), len(body), target=target)
        self.assertFalse(target.exists())
        receipt = consumer.receipts[0]
        self.assertEqual(receipt["bytes"], len(body))
        self.assertEqual((consumer.root / receipt["body_path"]).read_bytes(), body)

    def test_non_ascii_or_empty_content_length_rejects_exact_artifact_at_eof(self):
        consumer = self.consumer("bootstrap")
        consumer.args.attempts = 1
        body = b"exact artifact"
        length = [""]
        def malformed(handler):
            handler.send_response(200)
            handler.send_header("Content-Length", length[0])
            handler.end_headers()
            handler.wfile.write(body)
        self.server.RequestHandlerClass.do_GET = malformed
        for index, value in enumerate((chr(178), "", "1" + chr(178))):
            with self.subTest(length=value):
                length[0] = value
                target = consumer.objects / f"malformed-{index}"
                with self.assertRaisesRegex(ValueError, "ambiguous HTTP framing"):
                    consumer.request("artifacts/" + hashlib.sha256(body).hexdigest(), len(body), target=target)
                self.assertFalse(target.exists())
        receipts = self.assert_retained_receipts(consumer)
        self.assertEqual(len(receipts), 3)
        self.assertTrue(all(r["status"] == 200 and r["bytes"] == 0 and
                            "ambiguous HTTP framing" in r["error"] for r in receipts))

    def test_non_ascii_content_length_cannot_complete_full_bootstrap(self):
        original = self.server.RequestHandlerClass.send_header
        def malformed(handler, name, value):
            if name == "Content-Length" and "/artifacts/" in handler.path:
                value = chr(178)
            original(handler, name, value)
        self.server.RequestHandlerClass.send_header = malformed
        consumer = self.consumer("bootstrap")
        result = consumer.run()
        self.assertFalse(result["complete"], result)
        self.assertIn("ambiguous HTTP framing", result["error"])
        self.assertEqual(result["requests"], 3)
        self.assertEqual(result["failed_attempts"], 1)
        self.assertFalse((consumer.root / "current.json").exists())
        self.assertFalse(list(consumer.objects.iterdir()))
        receipts = self.assert_retained_receipts(consumer)
        self.assertEqual(receipts[-1]["status"], 200)
        self.assertEqual(receipts[-1]["bytes"], 0)
        self.assertEqual(json.loads((consumer.root / "bootstrap.result.json").read_text()), result)

    def test_close_delimited_without_content_length_completes_full_bootstrap(self):
        original = self.server.RequestHandlerClass.send_header
        def close_delimited(handler, name, value):
            if name != "Content-Length":
                original(handler, name, value)
        self.server.RequestHandlerClass.send_header = close_delimited
        consumer = self.consumer("bootstrap")
        result = consumer.run()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["requests"], 9)
        self.assertEqual(result["failed_attempts"], 0)
        self.assertTrue((consumer.root / "current.json").exists())
        self.assertEqual(len(list(consumer.objects.iterdir())), 5)
        self.assert_retained_receipts(consumer)
        self.assertNotIn(distribution.PREFIX + "artifacts/" + self.archive_digest, self.paths)

    def test_chunked_completion_requires_terminal_chunk_and_trailer(self):
        consumer = self.consumer("bootstrap")
        consumer.args.attempts = 1
        body = b"chunked-artifact"
        wire = [b""]
        def chunked(handler):
            handler.wfile.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + wire[0])
        self.server.RequestHandlerClass.do_GET = chunked
        prefix = f"{len(body):x}\r\n".encode() + body
        for index, ending in enumerate((b"\r\n0\r\n", b"xx0\r\n\r\n", b"\r\n0\r\n\r\n")):
            wire[0] = prefix + ending
            target = consumer.objects / f"chunk-{index}"
            if index < 2:
                with self.assertRaises(distribution.http.client.HTTPException):
                    consumer.request("artifacts/" + hashlib.sha256(body).hexdigest(), len(body), target=target)
                self.assertFalse(target.exists())
            else:
                consumer.request("artifacts/" + hashlib.sha256(body).hexdigest(), len(body), target=target)
                self.assertEqual(target.read_bytes(), body)
        self.assertEqual([r["bytes"] for r in consumer.receipts], [len(body)] * 3)

    def test_absolute_deadline_includes_dribbling_headers(self):
        consumer = self.consumer("bootstrap")
        consumer.args.attempts, consumer.args.timeout = 1, .15
        def slow(handler):
            try:
                for byte in b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\n\r\nx":
                    handler.wfile.write(bytes([byte]))
                    time.sleep(.025)
            except OSError:
                pass
        self.server.RequestHandlerClass.do_GET = slow
        start = time.monotonic()
        with self.assertRaises((OSError, distribution.http.client.HTTPException)):
            consumer.request("current", 100)
        self.assertLess(time.monotonic() - start, .6)
        self.assertIn("deadline", consumer.receipts[0]["error"])

    def test_retry_retains_partial_body_separately_from_success(self):
        consumer = self.consumer("bootstrap")
        consumer.args.timeout = .2
        body, attempts = b"partial-then-complete", []
        def retry(handler):
            attempts.append(1)
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body[:3] if len(attempts) == 1 else body)
            if len(attempts) == 1:
                time.sleep(.3)
        self.server.RequestHandlerClass.do_GET = retry
        target = consumer.objects / "candidate"
        consumer.request("artifacts/" + hashlib.sha256(body).hexdigest(), len(body), target=target)
        self.assertEqual([r["bytes"] for r in consumer.receipts], [3, len(body)])
        self.assertEqual([(consumer.root / r["body_path"]).read_bytes() for r in consumer.receipts], [body[:3], body])
        self.assertEqual(target.read_bytes(), body)
        self.assertTrue(consumer.receipts[0]["error"])

    def test_mislabeled_archive_is_refused_before_transfer(self):
        inventory = json.loads(self.replies[distribution.PREFIX + "inventories/" + self.current["inventory_sha256"]])
        inventory["artifacts"][1]["kind"] = "metadata"
        self.replace_inventory(inventory)
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"], result)
        self.assertIn("metadata classification", result["error"])
        self.assertFalse(any("/artifacts/" in path for path in self.paths))

    def replace_inventory(self, inventory):
        raw = json.dumps(inventory).encode()
        digest = hashlib.sha256(raw).hexdigest()
        self.current["inventory_sha256"] = digest
        self.replies[distribution.PREFIX + "inventories/" + digest] = raw

    def test_invalid_inventory_digest_never_becomes_request_path(self):
        self.current["inventory_sha256"] = "../unsafe"
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"])
        self.assertEqual(self.paths, [distribution.PREFIX + "current"])

    def test_inventory_profile_runtime_and_map_identity_are_mandatory(self):
        inventory = json.loads(self.replies[distribution.PREFIX + "inventories/" + self.current["inventory_sha256"]])
        for field in ("profile", "runtime", "shard_schema"):
            with self.subTest(field=field):
                bad = dict(inventory, **{field: "wrong"})
                with self.assertRaises(ValueError):
                    distribution.validate_inventory(bad, self.current)
        inventory["map_sha256"] = "f" * 64
        self.replace_inventory(inventory)
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"], result)
        self.assertIn("map digest", result["error"])
        self.assertNotIn(distribution.PREFIX + "artifacts/" + self.archive_digest, self.paths)

    def test_unchanged_current_200_is_not_an_update(self):
        self.assertTrue(self.consumer("bootstrap").run()["complete"])
        original = self.server.RequestHandlerClass.do_GET
        def ignore_conditional(handler):
            if "If-None-Match" in handler.headers:
                del handler.headers["If-None-Match"]
            original(handler)
        self.server.RequestHandlerClass.do_GET = ignore_conditional
        result = self.consumer("update").run()
        self.assertFalse(result["complete"], result)
        self.assertIn("did not observe a producer update", result["error"])
        self.assertEqual(result["requests"], 1)

    def test_wave_rejects_current_changed_after_disk_preflight(self):
        original = self.server.RequestHandlerClass.do_GET
        changed = []
        def advance(handler):
            original(handler)
            if "/inventories/" in handler.path and not changed:
                changed.append(1)
                self.publish(b"x" * (8 << 20), 2)
        self.server.RequestHandlerClass.do_GET = advance
        output, budget = self.root / "race", 4 << 20
        result = subprocess.run([
            sys.executable, str(TOOL), "--publisher", f"https://localhost:{self.server.server_port}",
            "--ca", str(self.root / "ca.pem"), "--output", str(output), "--consumers", "10",
            "--phase", "bootstrap", "--cache-budget-bytes", str(budget), "--polls", "0",
            "--attempts", "1"], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads((output / "bootstrap.json").read_text())
        self.assertEqual(report["complete"], 0)
        self.assertTrue(all("changed since wave" in r["error"] for r in report["results"]))
        self.assertLessEqual(self.retained_size(output), budget)
        self.assertFalse(any(p.stat().st_size >= 8 << 20 for p in output.rglob("*.body")))

    @staticmethod
    def retained_size(root):
        sizes = {}
        for path in root.rglob("*"):
            if path.is_file():
                st = path.stat()
                sizes[(st.st_dev, st.st_ino)] = st.st_size
        return sum(sizes.values())

    def test_disk_ceiling_counts_error_partial_and_existing_evidence(self):
        consumer = self.consumer("bootstrap")
        budget = 24000
        consumer.budget = distribution.DiskBudget(consumer.args.output, budget)
        consumer.result_credit = distribution.Credit(consumer.budget, 4096)
        body = b"e" * 18000
        def refuse(handler):
            handler.send_response(503)
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            try:
                handler.wfile.write(body)
            except OSError:
                pass
        self.server.RequestHandlerClass.do_GET = refuse
        result = consumer.run()
        self.assertFalse(result["complete"])
        self.assertIn("BudgetExceeded", result["error"])
        self.assertLessEqual(self.retained_size(consumer.args.output), budget)
        self.assertTrue(consumer.receipts)
        for receipt in consumer.receipts:
            retained = consumer.root / receipt["body_path"]
            self.assertEqual(retained.stat().st_size, receipt["bytes"])
            self.assertEqual(distribution.digest_file(retained), receipt["sha256"])
        with self.assertRaises(distribution.BudgetExceeded):
            distribution.DiskBudget(consumer.args.output, self.retained_size(consumer.args.output) - 1)

    def test_disk_guard_stops_wave_before_any_artifact_transfer(self):
        output = self.root / "bounded"
        result = subprocess.run([
            sys.executable, str(TOOL), "--publisher",
            f"https://localhost:{self.server.server_port}",
            "--ca", str(self.root / "ca.pem"), "--output", str(output),
            "--consumers", "10", "--phase", "bootstrap", "--cache-budget-bytes", "1",
        ], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("too small", result.stderr)
        self.assertFalse(any("/artifacts/" in path for path in self.paths))
        self.assertFalse(list(output.rglob("*.body")))


if __name__ == "__main__":
    unittest.main()
