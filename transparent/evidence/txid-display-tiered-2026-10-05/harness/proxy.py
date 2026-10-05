#!/usr/bin/env python3
"""Local stand-in for the router's txid display snippet.

Routes `/v1/txid/archive/*` to the archive owner and every other request to the
recent replica, as `transparent/ops/deploy/txid-display-routes.caddy.in` does.
Local end-to-end runs only; it adds no health checks or retries.

    proxy.py --listen 127.0.0.1:18090 --archive 127.0.0.1:18095 --recent 127.0.0.1:18096
"""
import argparse
import http.client
import http.server
import threading

HOP = {'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'te', 'trailer', 'upgrade'}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    local = threading.local()

    def upstream(self):
        target = ARGS.archive if self.path.startswith('/v1/txid/archive/') else ARGS.recent
        pool = getattr(self.local, 'pool', None)
        if pool is None:
            pool = self.local.pool = {}
        if target not in pool:
            host, port = target.rsplit(':', 1)
            pool[target] = http.client.HTTPConnection(host, int(port), timeout=120)
        return target, pool[target]

    def forward(self):
        length = int(self.headers.get('content-length') or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        for attempt in range(2):
            target, connection = self.upstream()
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                break
            except (http.client.HTTPException, OSError):
                connection.close()
                del self.local.pool[target]
                if attempt:
                    self.send_error(502)
                    return
        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() not in HOP and key.lower() != 'content-length':
                self.send_header(key, value)
        self.send_header('content-length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = forward
    do_POST = forward

    def log_message(self, *args):
        pass


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('--listen', required=True)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--recent', required=True)
    ARGS = parser.parse_args()
    host, port = ARGS.listen.rsplit(':', 1)
    server = http.server.ThreadingHTTPServer((host, int(port)), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == '__main__':
    main()
