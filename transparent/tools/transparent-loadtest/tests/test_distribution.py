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
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def publish(self, recent, revision):
        mapping = {"network": "main", "genesis_hash": "00" * 32,
                   "shards": [{"shard_id": 0, "manifest_digest": "archive"},
                              {"shard_id": 1, "manifest_digest": "recent"}]}
        artifacts = []
        self.replies = {}
        for path, body, kind in (
            ("shards.json", json.dumps(mapping).encode(), "metadata"),
            ("archive/directory.0.bin", b"never download", "archive_table"),
            ("recent/directory.0.bin", recent, "recent_table"),
        ):
            digest = hashlib.sha256(body).hexdigest()
            artifacts.append({"path": path, "sha256": digest,
                              "bytes": len(body), "kind": kind})
            self.replies[distribution.PREFIX + "artifacts/" + digest] = body
        inventory = {"schema": "transparent-prepared-distribution-v1",
                     "profile": "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1",
                     "recent_from_shard": 1, "artifacts": artifacts}
        raw = json.dumps(inventory).encode()
        digest = hashlib.sha256(raw).hexdigest()
        self.replies[distribution.PREFIX + "inventories/" + digest] = raw
        self.current = {"revision": revision, "eligibility": "canonical",
                        "invalidated_from": None, "anchor_height": revision,
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
        self.assertEqual(len(second["reused"]), 1)
        self.assertNotIn(distribution.PREFIX + "artifacts/" + self.archive_digest, self.paths)
        receipts = [json.loads(line) for line in
                    (self.root / "output/consumer-000/update.requests.jsonl").read_text().splitlines()]
        self.assertEqual(sum(r["status"] == 304 for r in receipts), 1)

    def test_truncated_artifact_cannot_complete_or_publish_cache_pointer(self):
        self.truncate = True
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"])
        self.assertIn("length/digest mismatch", result["error"])
        self.assertFalse((self.root / "output/consumer-000/current.json").exists())
        self.assertTrue(list((self.root / "output/consumer-000/objects").glob("*.partial")))

    def test_exhausted_refusals_retain_every_error_body_and_attempt(self):
        self.refuse = True
        result = self.consumer("bootstrap").run()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_attempts"], 2)
        self.assertEqual(result["requests"], 2)
        self.assertEqual(result["bytes"], 2 * len(b"bounded refusal"))

    def test_disk_guard_stops_wave_before_any_artifact_transfer(self):
        output = self.root / "bounded"
        result = subprocess.run([
            sys.executable, str(TOOL), "--publisher",
            f"https://localhost:{self.server.server_port}",
            "--ca", str(self.root / "ca.pem"), "--output", str(output),
            "--consumers", "10", "--phase", "bootstrap", "--cache-budget-bytes", "1",
        ], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(json.loads((output / "bootstrap.disk-bound.json").read_text())["permitted"])
        self.assertFalse(any("/artifacts/" in path for path in self.paths))
        self.assertEqual(len(list((output / "control/responses").iterdir())), 2)


if __name__ == "__main__":
    unittest.main()
