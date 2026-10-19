#!/usr/bin/env python3
"""TLS termination fixture forwarding every byte from the real publisher.

No publication construction or consumer logic lives here. Optional transport
faults exercise authenticated-origin failure behavior through real sockets.
"""
import hashlib
import http.client
import http.server
import json
import ssl
import sys
import time
from pathlib import Path

upstream, cert, key, control = sys.argv[1:5]
# An optional fixed port lets a restarted terminator keep the configured origin.
port = int(sys.argv[5]) if len(sys.argv) > 5 else 0

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        with Path(control + ".requests").open("a") as requests:
            requests.write(self.path + "\n")
        fault = Path(control).read_text().strip()
        if fault == "malformed-current" and self.path.endswith("/current"):
            self.send_response(200)
            self.send_header("Content-Length", "1")
            self.end_headers()
            self.wfile.write(b"{")
            return
        if fault == "refused-body":
            body = b"refused" * 1024
            self.send_response(503)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
                pass
            return
        if fault == "redirect":
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:1/forbidden")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if (fault == "pause-activation" or fault.startswith("pause-current-")) and self.path.endswith("/current"):
            counter = Path(control + ".current-count")
            count = int(counter.read_text()) + 1 if counter.exists() else 1
            counter.write_text(str(count))
            pause_at = int(fault.rsplit("-", 1)[1]) if fault.startswith("pause-current-") else 2
            if count >= pause_at:
                Path(control + ".entered").touch()
                while Path(control).read_text().strip() == fault:
                    time.sleep(0.01)
        if fault == "pause-anchor" and "/anchors/" in self.path:
            Path(control + ".entered").touch()
            while Path(control).read_text().strip() == fault:
                time.sleep(0.01)
        if fault == "pause-artifact" and "/artifacts/" in self.path:
            Path(control + ".entered").touch()
            while Path(control).read_text().strip() == "pause-artifact":
                time.sleep(0.01)
        if fault in {"runtime", "unsafe-path", "missing-runtime"} and (
            self.path.endswith("/current") or "/inventories/" in self.path
        ):
            connection = http.client.HTTPConnection(upstream, timeout=10)
            connection.request("GET", "/provider/v1/current")
            current = json.loads(connection.getresponse().read())
            connection.request("GET", "/provider/v1/inventories/" + current["inventory_sha256"])
            inventory = json.loads(connection.getresponse().read())
            connection.close()
            if fault == "runtime":
                inventory["runtime"] += "-incompatible"
            elif fault == "unsafe-path":
                inventory["artifacts"][0]["path"] = "../escape"
            else:
                inventory["artifacts"] = [f for f in inventory["artifacts"] if f["kind"] != "recent_runtime"]
            body = json.dumps(inventory, separators=(",", ":")).encode()
            current["inventory_sha256"] = hashlib.sha256(body).hexdigest()
            if self.path.endswith("/current"):
                body = json.dumps(current, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        connection = http.client.HTTPConnection(upstream, timeout=10)
        try:
            connection.request("GET", self.path, headers={k: v for k, v in self.headers.items() if k.lower() in {"if-none-match", "cache-control"}})
            response = connection.getresponse()
            # Faults affect actual transport bytes, never replace the producer
            # or the provider loader with a mock implementation.
            status = response.status
            if fault == "missing" and "/artifacts/" in self.path:
                response.read()
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(status)
            if response.getheader("ETag"):
                self.send_header("ETag", response.getheader("ETag"))
            if status == 304:
                self.end_headers()
                return
            self.send_header("Connection", "close")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.close_connection = True
            first = True
            while chunk := response.read(1 << 20):
                if first and "/artifacts/" in self.path:
                    if fault == "corrupt":
                        chunk = bytes([chunk[0] ^ 1]) + chunk[1:]
                    if fault == "truncated":
                        self.wfile.write(b"1\r\n" + chunk[:1] + b"\r\n")
                        return
                self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                first = False
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
            pass
        finally:
            connection.close()

server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
server.daemon_threads = True
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
server.socket = context.wrap_socket(server.socket, server_side=True)
print(server.server_port, flush=True)
server.serve_forever()
