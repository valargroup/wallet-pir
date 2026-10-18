"""Bounded loopback raw RPC capture for the locked candidate oracle owner.

This component neither launches a reader nor qualifies a snapshot. The caller
owns ProductionLock, writer restoration proof, child adoption/recovery and the
aggregate deadline. Every potentially long body operation invokes its budget
and resource check. Authentication headers are forwarded only in memory;
access logs, headers and exception messages never enter retained evidence.
There is deliberately no standalone production entry point.
"""
import gzip
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import math
import os
import socket
from pathlib import Path
import threading
import time

from activity_oracle_rpc import MAX_ATTEMPTS, MAX_BODY, METHODS, integer, text
from activity_candidate_reports import immutable_json, json_bytes, require, unique

UPSTREAM = ('127.0.0.1', 8232)
CHUNK = 1 << 20
IO_SECONDS = 8


def calls(raw):
    """Only the exact read methods/parameters used by event-spotcheck."""
    decoded = json.loads(raw, object_pairs_hook=unique,
                         parse_constant=lambda v: (_ for _ in ()).throw(ValueError('nonfinite RPC JSON')))
    entries = decoded if isinstance(decoded, list) else [decoded]
    require(entries and len(entries) <= 4096, 'empty or oversized RPC batch')
    ids = set()
    for call in entries:
        require(isinstance(call, dict) and set(call) <= {'jsonrpc', 'id', 'method', 'params'} and
                call.get('jsonrpc') in ('1.0', '2.0') and type(call.get('id')) in (str, int) and
                call['id'] not in ids and call.get('method') in METHODS and isinstance(call.get('params'), list),
                'unsupported or malformed RPC call')
        ids.add(call['id'])
        method, params = call['method'], call['params']
        if method == 'getblockhash':
            require(len(params) == 1, 'invalid canonical hash parameters')
            integer(params[0])
        elif method == 'getblockheader':
            require(len(params) in (1, 2) and (len(params) == 1 or params[1] is True), 'invalid header parameters')
            text(params[0])
        elif method == 'getrawtransaction':
            require(len(params) == 2 and type(params[1]) is int and params[1] == 1, 'invalid verbose transaction parameters')
            text(params[0])
        else:
            require(len(params) == 2 and type(params[1]) is int and params[1] == 1, 'invalid verbose block parameters')
            height = params[0]
            require(type(height) is int or isinstance(height, str), 'invalid block identity')
            if type(height) is int:
                integer(height)
            elif height.isascii() and height.isdecimal():
                integer(int(height))
            else:
                text(height)
    return entries


class Capture:
    """Single owned ephemeral listener; the caller must close before sealing.

    `check` must enforce the caller's monotonic aggregate deadline, lock and
    resource floors. `authorization` is the native reader's runtime cookie
    header, never an evidence field. The fixed upstream has no URL injection.
    """
    def __init__(self, directory, authorization, deadline, check):
        require(isinstance(authorization, str) and authorization.startswith('Basic ') and
                '\r' not in authorization and '\n' not in authorization, 'invalid runtime authentication')
        require(callable(check) and type(deadline) in (float, int) and math.isfinite(deadline) and
                deadline > time.monotonic(),
                'invalid capture budget')
        # Refuse before creating a namespace or listener if the owning lock,
        # resource floors or aggregate budget have already been lost.
        check()
        require(deadline > time.monotonic(), 'oracle capture deadline exceeded')
        self.directory = Path(directory)
        require(self.directory.is_absolute(), 'capture directory must be absolute')
        for path in [self.directory, *self.directory.parents]:
            require(not path.is_symlink(), 'capture directory contains a link')
        self.directory.mkdir(mode=0o700)  # Exclusive namespace, never replay.
        self.authorization, self.deadline, self.check = authorization, deadline, check
        self.started_unix, self.started_monotonic = time.time(), time.monotonic()
        self.attempts, self.failures, self.timings = [], [], []
        self.sockets, self.sockets_lock = set(), threading.Lock()
        capture = self

        class Server(HTTPServer):
            def get_request(self):
                connection, address = super().get_request()
                try:
                    connection.settimeout(capture.budget())
                    capture.register(connection)
                    return connection, address
                except Exception as error:
                    connection.close()
                    capture.failures.append({'attempt':len(capture.attempts), 'error_type':type(error).__name__})
                    raise OSError('capture admission refused') from None

            def handle_error(self, request, client_address):
                # No traceback or request/header text may expose runtime auth.
                capture.failures.append({'attempt':len(capture.attempts), 'error_type':'HandlerFailure'})

            def close_request(self, request):
                capture.unregister(request)
                super().close_request(request)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send_error(self, code, message=None, explain=None):
                capture.failures.append({'attempt':len(capture.attempts), 'error_type':'HTTPBoundaryRefusal'})
                super().send_error(code, 'oracle capture refused')

            def do_POST(self):
                capture.handle(self)

            def do_GET(self):
                self.send_error(405)

        self.server = Server(('127.0.0.1', 0), Handler)
        self.server.timeout = .1
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval':.05}, name='oracle-rpc-capture', daemon=True)
        self.thread.start()
        # A slow trickle through HTTP headers must not reset socket timeouts
        # forever. Shutdown wakes an outstanding socket read at the deadline.
        self.timer = threading.Timer(max(0, deadline-time.monotonic()), self.abort_sockets)
        self.timer.daemon = True
        self.timer.start()

    def register(self, connection):
        with self.sockets_lock:
            expired = time.monotonic() >= self.deadline
            if not expired:
                self.sockets.add(connection)
        if expired:
            # The one-shot watchdog may already have taken its socket list.
            # A socket registered afterward must never escape that deadline.
            try: connection.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            raise ValueError('oracle capture deadline exceeded')

    def unregister(self, connection):
        with self.sockets_lock:
            self.sockets.discard(connection)

    def abort_sockets(self):
        with self.sockets_lock:
            connections = list(self.sockets)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    @property
    def url(self):
        return 'http://127.0.0.1:%d/' % self.server.server_port

    def budget(self):
        require(time.monotonic() < self.deadline, 'oracle capture deadline exceeded')
        self.check()
        remaining = self.deadline-time.monotonic()
        require(remaining > 0, 'oracle capture deadline exceeded')
        return min(IO_SECONDS, remaining)

    def canonical(self, expected, phase):
        """Capture one complete canonical boundary through the owned listener.

        The locked caller derives `expected` from the fully verified immutable
        snapshot, calls before launching the reader and again after its owned
        exit. This checks the replies immediately; the report producer still
        verifies original raw bytes and both clocks independently. No passed
        flag or caller-provided timing substitutes for those retained attempts.
        """
        require(isinstance(expected, dict) and len(expected) == 17 and
                0 in expected and 3500738 in expected and phase in ('before', 'after'),
                'canonical boundary recipe differs')
        for height, identity in expected.items():
            integer(height); text(identity)
        self.budget()
        requests = [{'jsonrpc':'2.0', 'id':'canonical-%s-%d' % (phase, height),
                     'method':'getblockhash', 'params':[height]} for height in sorted(expected)]
        raw = json.dumps(requests, separators=(',', ':')).encode()
        peer = http.client.HTTPConnection(*self.server.server_address, timeout=self.budget())
        connection = None
        try:
            peer.connect(); connection = peer.sock; self.register(connection)
            peer.request('POST', '/', raw,
                         {'Content-Type':'application/json', 'Authorization':self.authorization})
            with peer.getresponse() as response:
                require(response.status == 200, 'canonical boundary HTTP response failed')
                size = response.getheader('Content-Length')
                require(isinstance(size, str) and size.isascii() and size.isdecimal() and
                        0 < int(size) <= 32768, 'canonical boundary response exceeds bound')
                replies = json_bytes(self.read(response, int(size)))
        finally:
            if connection is not None:
                self.unregister(connection)
            peer.close()
        require(isinstance(replies, list) and len(replies) == len(requests) and
                all(isinstance(reply, dict) and type(reply.get('id')) is str and
                    reply.get('error') is None for reply in replies),
                'canonical boundary logical response failed')
        by_id = {reply['id']:reply.get('result') for reply in replies}
        require(len(by_id) == len(replies) and set(by_id) == {call['id'] for call in requests},
                'canonical boundary response identities differ')
        for call in requests:
            identity = by_id[call['id']]; text(identity)
            require(identity == expected[call['params'][0]], 'canonical boundary changed from snapshot')
        self.budget()
        return dict(expected)

    def read(self, stream, length=None):
        chunks, size = [], 0
        while length is None or size < length:
            self.budget()
            read = getattr(stream, 'read1', stream.read)
            chunk = read(min(CHUNK, MAX_BODY+1-size, length-size if length is not None else CHUNK))
            if not chunk:
                break
            chunks.append(chunk); size += len(chunk)
            require(size <= MAX_BODY, 'oracle RPC body exceeds bound')
        require(length is None or size == length, 'truncated oracle RPC request')
        self.budget()
        return b''.join(chunks)

    def retain(self, name, raw):
        path = self.directory/(name+'.json.gz')
        with path.open('xb') as output:
            os.fchmod(output.fileno(), 0o600)
            with gzip.GzipFile(fileobj=output, mode='wb', filename='', mtime=0) as packed:
                for offset in range(0, len(raw), CHUNK):
                    self.budget(); packed.write(raw[offset:offset+CHUNK])
            output.flush(); os.fsync(output.fileno()); os.fchmod(output.fileno(), 0o400)
        self.sync_directory()
        self.budget()
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            while chunk := stream.read(CHUNK):
                self.budget(); digest.update(chunk)
        return {'path':str(path), 'sha256':digest.hexdigest(), 'decoded_sha256':hashlib.sha256(raw).hexdigest()}

    def sync_directory(self):
        fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def handle(self, handler):
        index = len(self.attempts)
        started_unix, started_monotonic = time.time(), time.monotonic()
        try:
            handler.connection.settimeout(self.budget())
            require(index < MAX_ATTEMPTS and not self.failures, 'capture attempt limit or earlier failure')
            require(handler.path == '/' and handler.headers.get('Authorization') == self.authorization and
                    len(handler.headers.get_all('Authorization', [])) == 1 and
                    not handler.headers.get_all('Transfer-Encoding') and
                    len(handler.headers.get_all('Content-Length', [])) == 1, 'invalid capture request boundary')
            length = int(handler.headers['Content-Length'])
            require(0 < length <= MAX_BODY, 'invalid capture request size')
            raw = self.read(handler.rfile, length)
            calls(raw)
            request_ref = self.retain('%04d-request' % index, raw)
            upstream = http.client.HTTPConnection(*UPSTREAM, timeout=self.budget())
            connection = None
            try:
                upstream.connect()
                connection = upstream.sock
                self.register(connection)
                self.budget()
                upstream.request('POST', '/', raw,
                                 {'Content-Type':'application/json', 'Authorization':self.authorization})
                with upstream.getresponse() as response:
                    # HTTPConnection never follows redirects; retain the status/body.
                    status = response.status
                    reply = self.read(response)
            finally:
                if connection is not None:
                    self.unregister(connection)
                upstream.close()
            response_ref = self.retain('%04d-response' % index, reply)
            attempt = {'status':status, 'request':request_ref, 'response':response_ref}
            timing = {'attempt_index':index, 'started_unix':started_unix, 'ended_unix':time.time(),
                      'started_monotonic':started_monotonic, 'ended_monotonic':time.monotonic()}
            # Timing stays separate so the strict raw-RPC validator consumes
            # original request/response references without extra asserted fields.
            with (self.directory/'timing.jsonl').open('a') as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(json.dumps(timing, sort_keys=True)+'\n'); stream.flush(); os.fsync(stream.fileno())
            self.sync_directory()
            # Bodies are durable before their index entry. Failures keep partial bytes.
            with (self.directory/'attempts.jsonl').open('a') as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(json.dumps(attempt, sort_keys=True)+'\n'); stream.flush(); os.fsync(stream.fileno())
            self.sync_directory()
            self.attempts.append(attempt)
            self.timings.append(timing)
            handler.connection.settimeout(self.budget())
            handler.send_response(status)
            handler.send_header('Content-Type', 'application/json')
            handler.send_header('Content-Length', str(len(reply)))
            handler.end_headers()
            for offset in range(0, len(reply), CHUNK):
                self.budget(); handler.wfile.write(reply[offset:offset+CHUNK])
        except Exception as error:
            self.failures.append({'attempt':index, 'error_type':type(error).__name__})
            try:
                handler.send_error(502, 'oracle capture refused')
            except OSError:
                pass

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=IO_SECONDS+1)
        self.timer.cancel()
        require(not self.thread.is_alive(), 'oracle capture did not quiesce')
        self.authorization = ''
        for name in ('attempts.jsonl', 'timing.jsonl'):
            if (self.directory/name).exists():
                os.chmod(self.directory/name, 0o400)
                self.sync_directory()
        self.budget()
        require(not self.failures, 'oracle RPC capture failed; preserve partial evidence')
        require(self.attempts, 'oracle RPC capture is empty')
        require(len(self.timings) == len(self.attempts), 'oracle capture timing coverage differs')
        references = {}
        for key, name in (('attempts', 'attempts.jsonl'), ('timings', 'timing.jsonl')):
            digest = hashlib.sha256()
            path = self.directory/name
            with path.open('rb') as stream:
                while chunk := stream.read(CHUNK):
                    self.budget(); digest.update(chunk)
            references[key] = {'path':str(path), 'sha256':digest.hexdigest()}
        self.budget()
        self.result = {'schema':'transparent-oracle-capture-v1', 'status':'passed', 'failures':[],
                       'attempt_count':len(self.attempts), 'timing_count':len(self.timings),
                       **references,
                       'interval':{'started_unix':self.started_unix, 'ended_unix':time.time(),
                                   'started_monotonic':self.started_monotonic, 'ended_monotonic':time.monotonic()}}
        # Only a quiesced, nonfailed listener can seal successful closure.
        # Failed/partial captures preserve their bytes and have no passed result.
        immutable_json(self.directory/'result.json', self.result, check=self.budget)
        self.budget()
        return list(self.attempts)
