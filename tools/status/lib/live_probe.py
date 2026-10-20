"""Run the encrypted live probe through a bounded loopback-only client simulator.

The child retains its independent chain oracle and arrival accounting. This
transport supplies distinct forwarded client addresses, as the wallet edge does,
without retries or changes to query bodies, responses, or admission limits.
"""
import argparse
from contextlib import contextmanager
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import itertools
import json
from pathlib import Path
import subprocess
import threading
from urllib.parse import urlsplit


def loopback_origin(origin):
    parsed = urlsplit(origin)
    if (parsed.scheme != 'http' or parsed.username or parsed.password or parsed.query
            or parsed.fragment or parsed.path not in ('', '/') or not parsed.hostname
            or not ipaddress.ip_address(parsed.hostname).is_loopback or not parsed.port):
        raise ValueError('probe origin must be an explicit loopback http:// address and port')
    return parsed


@contextmanager
def simulated_clients(origin, clients):
    upstream = loopback_origin(origin)
    if type(clients) is not int or not 1 <= clients <= 64:
        raise ValueError('simulated client count must be 1..=64')
    arrivals = itertools.count()
    arrival_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # Never log encrypted requests or session identifiers.

        def forward(self):
            valid = self.path == '/v1/status/init' or self.path.startswith('/v1/status/session/')
            if self.command == 'POST':
                valid = self.path == '/v1/status/query'
            if not valid:
                self.send_error(404)
                return
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 <= length <= 512 * 1024 or self.headers.get('Transfer-Encoding'):
                self.send_error(413)
                return
            with arrival_lock:
                client = next(arrivals) % clients
            headers = {'X-Forwarded-For': '192.0.2.' + str(client + 1)}
            if self.headers.get('Content-Type'):
                headers['Content-Type'] = self.headers['Content-Type']
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=5)
            try:
                connection.request(self.command, self.path, body=self.rfile.read(length), headers=headers)
                response = connection.getresponse()
                body = response.read()
                self.send_response(response.status)
                for name in ('Content-Type', 'Retry-After'):
                    if response.getheader(name):
                        self.send_header(name, response.getheader(name))
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (OSError, http.client.HTTPException):
                self.send_error(502)
            finally:
                connection.close()

        do_GET = forward
        do_POST = forward

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield 'http://127.0.0.1:' + str(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--origin', required=True)
    parser.add_argument('--cookie', type=Path, required=True)
    parser.add_argument('--rpc-url', default='http://127.0.0.1:8232')
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--clients', type=int, default=8)
    parser.add_argument('--report-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    with simulated_clients(args.origin, args.clients) as origin:
        args.report_dir.mkdir(parents=True, exist_ok=True)
        evidence = {'origin': args.origin, 'simulated_clients': args.clients,
                    'transport': 'loopback_forwarded_clients', 'retries': 0}
        (args.report_dir / 'probe-transport.json').write_text(json.dumps(evidence) + '\n')
        return subprocess.run([str(args.client), 'probe-live-load', '--origin', origin,
                               '--cookie', str(args.cookie), '--rpc-url', args.rpc_url,
                               '--seconds', str(args.seconds), '--report-dir', str(args.report_dir)]).returncode


if __name__ == '__main__':
    raise SystemExit(main())
