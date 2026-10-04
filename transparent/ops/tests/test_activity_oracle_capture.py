"""Fictional loopback peers only; no production or candidate qualification."""
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
spec = importlib.util.spec_from_file_location('oracle_capture_test', HERE.parents[1]/'lib/activity_oracle_capture.py')
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)
import activity_oracle_rpc as RPC


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name).resolve()
        self.auth = 'Basic ZmljdGlvbmFsOnRlc3Q='
        self.forwarded, self.status, self.reply, self.delay = [], 200, b'{"id":1,"error":null,"result":"' + b'0'*64 + b'"}', 0
        self.trickle = False
        test = self

        class Peer(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                raw = self.rfile.read(int(self.headers['Content-Length']))
                test.forwarded.append((raw, self.headers.get('Authorization')))
                if test.trickle:
                    for byte in b'HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n{}':
                        try: self.connection.sendall(bytes([byte]))
                        except OSError: break
                        time.sleep(.04)
                    return
                if test.delay: time.sleep(test.delay)
                reply = test.reply(raw) if callable(test.reply) else test.reply
                self.send_response(test.status)
                self.send_header('Location', 'http://127.0.0.1:1/forbidden')
                self.send_header('Content-Length', str(len(reply))); self.end_headers()
                try: self.wfile.write(reply)
                except OSError: pass

        self.peer = HTTPServer(('127.0.0.1', 0), Peer)
        self.thread = threading.Thread(target=self.peer.serve_forever, kwargs={'poll_interval':.01}, daemon=True)
        self.thread.start()
        self.patch = patch.object(M, 'UPSTREAM', self.peer.server_address); self.patch.start()
        self.capture = None
        self.checks = 0

    def tearDown(self):
        if self.capture:
            try: self.capture.close()
            except ValueError: pass
        self.peer.shutdown(); self.peer.server_close(); self.thread.join(2)
        self.patch.stop(); self.temp.cleanup()

    def check(self): self.checks += 1

    def start(self, seconds=3):
        self.capture = M.Capture(self.root/'capture', self.auth, time.monotonic()+seconds, self.check)

    def post(self, raw=None, authorization=None):
        raw = raw or b'{"jsonrpc":"1.0","id":1,"method":"getblockhash","params":[0]}'
        peer = http.client.HTTPConnection(*self.capture.server.server_address, timeout=2)
        try:
            peer.request('POST', '/', raw, {'Authorization':authorization or self.auth})
            with peer.getresponse() as reply: return reply.status, reply.read()
        finally: peer.close()

    def finish(self):
        value = self.capture.close(); self.capture = None; return value

    def test_exact_bytes_hashes_and_no_authentication_in_retained_evidence(self):
        self.start(); status, reply = self.post()
        self.assertEqual((status, reply), (200, self.reply))
        attempts = self.finish(); self.assertEqual(len(attempts), 1)
        self.assertEqual(self.forwarded[0][1], self.auth)
        self.assertEqual(RPC.decode(attempts[0]['response'])['result'], '0'*64)
        for part in ('request', 'response'):
            ref = attempts[0][part]; path = Path(ref['path'])
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), ref['sha256'])
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            self.assertNotIn(self.auth, json.dumps(RPC.decode(ref)))
        self.assertNotIn(self.auth, (self.root/'capture/attempts.jsonl').read_text())
        path = self.root/'capture/result.json'
        closure = json.loads(path.read_text())
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        self.assertEqual((closure['attempt_count'], closure['timing_count'], closure['failures']), (1,1,[]))
        for key, name in (('attempts','attempts.jsonl'),('timings','timing.jsonl')):
            raw = (self.root/'capture'/name).read_bytes()
            self.assertEqual(closure[key], {'path':str(self.root/'capture'/name),
                                            'sha256':hashlib.sha256(raw).hexdigest()})
        self.assertNotIn(self.auth, path.read_text())
        self.assertGreater(self.checks, 5)

    def test_oversize_application_refusal_and_retry_are_both_retained(self):
        self.start(); self.reply = b'{"error":{"code":-32011},"id":null}'
        raw = json.dumps([{'jsonrpc':'2.0','id':0,'method':'getrawtransaction','params':['1'*64,1]}]).encode()
        self.assertEqual(self.post(raw)[0], 200)
        self.reply = b'[{"id":0,"error":null,"result":{"txid":"' + b'1'*64 + b'","vin":[],"vout":[]}}]'
        self.assertEqual(self.post(raw)[0], 200)
        attempts = self.finish()
        parsed = RPC.transactions(attempts)
        self.assertEqual(len(parsed[-1]), 1)
        self.assertEqual(len(attempts), 2)

    def test_durable_times_cover_raw_attempts_without_headers_or_clock_assumptions(self):
        self.start()
        before = time.monotonic()
        self.assertEqual(self.post()[0], 200)
        between = time.monotonic()
        self.assertEqual(self.post()[0], 200)
        capture = self.capture
        self.finish()
        timings = [json.loads(line) for line in (self.root/'capture/timing.jsonl').read_text().splitlines()]
        self.assertEqual(timings, capture.timings)
        self.assertEqual([t['attempt_index'] for t in timings], [0,1])
        self.assertLessEqual(before, timings[0]['started_monotonic'])
        self.assertLessEqual(timings[0]['ended_monotonic'], between)
        self.assertLessEqual(between, timings[1]['started_monotonic'])
        self.assertTrue(all(t['started_monotonic'] <= t['ended_monotonic'] for t in timings))
        self.assertEqual((self.root/'capture/timing.jsonl').stat().st_mode & 0o777, 0o400)
        self.assertNotIn(self.auth, json.dumps(timings))

    def test_redirect_is_retained_and_never_followed(self):
        self.start(); self.status = 302; self.reply = b'{}'
        self.assertEqual(self.post()[0], 302)
        attempts = self.finish()
        self.assertEqual(len(self.forwarded), 1)
        self.assertEqual(attempts[0]['status'], 302)
        with self.assertRaises(ValueError): RPC.transactions(attempts)

    def test_unsupported_method_is_refused_before_forwarding(self):
        self.start()
        self.assertEqual(self.post(b'{"jsonrpc":"2.0","id":1,"method":"stop","params":[]}')[0], 502)
        self.assertEqual(self.forwarded, [])
        with self.assertRaises(ValueError): self.finish()
        self.assertFalse((self.root/'capture/result.json').exists())

    def test_wrong_authentication_never_reaches_node_or_evidence(self):
        self.start()
        self.assertEqual(self.post(authorization='Basic fictional-other')[0], 502)
        self.assertFalse(self.forwarded)
        self.assertFalse(list((self.root/'capture').glob('*.gz')))

    def test_nonpost_attempt_fences_capture_even_if_a_valid_call_follows(self):
        self.start()
        peer = http.client.HTTPConnection(*self.capture.server.server_address, timeout=2)
        try:
            peer.request('GET', '/')
            with peer.getresponse() as reply: self.assertEqual(reply.status, 405); reply.read()
        finally: peer.close()
        self.assertEqual(self.post()[0], 502)
        self.assertFalse(self.forwarded)
        with self.assertRaises(ValueError): self.finish()

    def test_response_byte_limit_keeps_partial_request_and_fails(self):
        self.start(); self.reply = b'x'*200
        with patch.object(M, 'MAX_BODY', 100):
            self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(self.forwarded), 1)
        self.assertTrue((self.root/'capture/0000-request.json.gz').exists())
        with self.assertRaises(ValueError): self.finish()

    def test_aggregate_deadline_interrupts_pending_upstream_headers(self):
        self.delay = .6; self.start(seconds=.12)
        started = time.monotonic()
        try: self.assertEqual(self.post()[0], 502)
        except (OSError, http.client.HTTPException): pass
        self.assertLess(time.monotonic()-started, .5)
        with self.assertRaises(ValueError): self.finish()

    def test_idle_inbound_connection_cannot_hold_owner_past_deadline(self):
        self.start(seconds=.12)
        peer = socket.create_connection(self.capture.server.server_address, timeout=1)
        try:
            started = time.monotonic()
            peer.settimeout(1)
            self.assertEqual(peer.recv(1), b'')
            self.assertLess(time.monotonic()-started, .5)
        finally: peer.close()
        with self.assertRaises(ValueError): self.finish()

    def test_slow_header_trickle_cannot_extend_aggregate_deadline(self):
        self.trickle = True; self.start(seconds=.16)
        started = time.monotonic()
        try: self.assertEqual(self.post()[0], 502)
        except (OSError, http.client.HTTPException): pass
        self.assertLess(time.monotonic()-started, .5)
        with self.assertRaises(ValueError): self.finish()

    def test_attempt_limit_preserves_prior_body_without_replay(self):
        self.start(); self.assertEqual(self.post()[0], 200)
        original = (self.root/'capture/0000-request.json.gz').read_bytes()
        with patch.object(M, 'MAX_ATTEMPTS', 1): self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(self.forwarded), 1)
        self.assertEqual((self.root/'capture/0000-request.json.gz').read_bytes(), original)
        with self.assertRaises(ValueError): self.finish()

    def test_resource_check_failure_refuses_node_dispatch(self):
        self.start(); self.capture.check = lambda: (_ for _ in ()).throw(ValueError('fictional resource floor'))
        with self.assertRaises((OSError, http.client.HTTPException)): self.post()
        self.assertFalse(self.forwarded)

    def test_registration_after_watchdog_deadline_cannot_escape_shutdown(self):
        capture = object.__new__(M.Capture)
        capture.sockets, capture.sockets_lock = set(), threading.Lock()
        capture.deadline = time.monotonic()-1
        connection, peer = socket.socketpair()
        try:
            with self.assertRaises(ValueError): capture.register(connection)
            peer.settimeout(.5)
            self.assertEqual(peer.recv(1), b'')
            self.assertFalse(capture.sockets)
        finally: connection.close(); peer.close()

    def test_invalid_deadline_is_refused_without_namespace_or_listener(self):
        for deadline in (float('inf'), float('-inf'), float('nan'), True,
                         time.monotonic()-1):
            with self.subTest(deadline=deadline), self.assertRaises(ValueError):
                M.Capture(self.root/'capture', self.auth, deadline, self.check)
            self.assertFalse((self.root/'capture').exists())

    def test_lost_owner_budget_is_refused_before_namespace_creation(self):
        check = lambda: (_ for _ in ()).throw(ValueError('fictional lost production lock'))
        with self.assertRaises(ValueError):
            M.Capture(self.root/'capture', self.auth, time.monotonic()+3, check)
        self.assertFalse((self.root/'capture').exists())

    def test_owner_check_cannot_consume_initial_deadline_then_start_listener(self):
        with patch.object(M.time, 'monotonic', side_effect=[10, 12]):
            with self.assertRaises(ValueError):
                M.Capture(self.root/'capture', self.auth, 11, self.check)
        self.assertFalse((self.root/'capture').exists())

    def test_closed_rpc_parameter_identity_and_duplicate_ids(self):
        for value in ({'jsonrpc':'2.0','id':1,'method':'getrawtransaction','params':['1'*64,0]},
                      {'jsonrpc':'2.0','id':1,'method':'getblockhash','params':[True]},
                      {'jsonrpc':'2.0','id':1,'method':'getblock','params':['-1',1]}):
            with self.assertRaises(ValueError): M.calls(json.dumps(value).encode())
        call={'jsonrpc':'2.0','id':1,'method':'getblockhash','params':[0]}
        with self.assertRaises(ValueError): M.calls(json.dumps([call,call]).encode())

    def canonical_peer(self, expected, mutate=lambda replies: None):
        def reply(raw):
            calls = json.loads(raw)
            replies = [{'id':call['id'], 'error':None, 'result':expected[call['params'][0]]}
                       for call in calls]
            mutate(replies)
            return json.dumps(replies).encode()
        self.reply = reply

    def test_canonical_boundaries_retain_original_requests_replies_and_both_clocks(self):
        expected = {h:format(h+1, '064x') for h in [*range(16), 3500738]}
        self.canonical_peer(expected); self.start()
        for phase in ('before', 'after'):
            self.assertEqual(self.capture.canonical(expected, phase), expected)
        attempts = self.finish()
        self.assertEqual(len(attempts), 2)
        for attempt, phase in zip(attempts, ('before', 'after')):
            requests = RPC.decode(attempt['request'])
            self.assertEqual([call['params'][0] for call in requests], sorted(expected))
            self.assertTrue(all(call['method'] == 'getblockhash' and
                                call['id'].startswith('canonical-'+phase+'-') for call in requests))
            self.assertEqual(len(RPC.decode(attempt['response'])), 17)
        self.assertEqual(len(self.forwarded), 2)
        self.assertNotIn(self.auth, (self.root/'capture/attempts.jsonl').read_text())

    def test_canonical_recipe_refuses_before_dispatch(self):
        self.start()
        for expected, phase in (({0:'0'*64}, 'before'),
                                ({h:'0'*64 for h in [*range(16),3500738]}, 'unchecked')):
            with self.assertRaises(ValueError): self.capture.canonical(expected, phase)
        self.assertEqual(self.forwarded, [])

    def test_changed_canonical_reply_refuses_but_raw_failure_evidence_survives(self):
        expected = {h:format(h+1, '064x') for h in [*range(16),3500738]}
        self.canonical_peer(expected, lambda replies: replies[0].update(result='f'*64)); self.start()
        with self.assertRaisesRegex(ValueError, 'changed from snapshot'):
            self.capture.canonical(expected, 'before')
        attempts = self.finish()
        self.assertEqual(RPC.decode(attempts[0]['response'])[0]['result'], 'f'*64)

    def test_duplicate_or_refused_canonical_reply_never_passes(self):
        expected = {h:format(h+1, '064x') for h in [*range(16),3500738]}
        self.canonical_peer(expected, lambda replies: replies[-1].update(id=replies[0]['id'])); self.start()
        with self.assertRaisesRegex(ValueError, 'identities differ'):
            self.capture.canonical(expected, 'before')
        self.canonical_peer(expected, lambda replies: replies[0].update(error={'code':-1}))
        with self.assertRaisesRegex(ValueError, 'logical response failed'):
            self.capture.canonical(expected, 'after')
        self.assertEqual(len(self.finish()), 2)


if __name__ == '__main__': unittest.main()
