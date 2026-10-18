#!/usr/bin/env python3
"""Isolated HTTPS termination for real prepared-publication and archive fixtures."""
import http.client
import http.server
import ssl
import sys

upstream, cert, key = sys.argv[1:]


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        connection = http.client.HTTPConnection(upstream, timeout=10)
        try:
            connection.request(self.command, self.path, body=body,
                               headers={k: v for k, v in self.headers.items()
                                        if k.lower() in {'content-type', 'if-none-match', 'x-transparent-archive-hop'}})
            response = connection.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for key in ('Content-Type', 'Retry-After', 'ETag'):
                if response.getheader(key):
                    self.send_header(key, response.getheader(key))
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            connection.close()


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
server.daemon_threads = True
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
server.socket = context.wrap_socket(server.socket, server_side=True)
print(server.server_port, flush=True)
server.serve_forever()
