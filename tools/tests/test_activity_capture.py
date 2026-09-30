"""Exercise capture at the HTTP boundary, including credentials and failures."""
import contextlib
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

SCRIPT = Path(__file__).resolve().parents[2] / 'transparent/ops/scripts/activity-capture.py'

@contextlib.contextmanager
def capture(mode):
    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_GET(self): self.reply()
        def do_POST(self): self.reply()
        def reply(self):
            self.rfile.read(int(self.headers.get('Content-Length','0')))
            # Retain a failed transport attempt without losing its public body.
            body=b'{"error":"public fixture unavailable"}'
            self.send_response(503)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers();self.wfile.write(body)
    upstream=ThreadingHTTPServer(('127.0.0.1',0),Upstream)
    thread=threading.Thread(target=upstream.serve_forever,daemon=True);thread.start()
    with tempfile.TemporaryDirectory() as directory:
        evidence=Path(directory)/'evidence'
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1',0));port=reservation.getsockname()[1]
        process=subprocess.Popen([sys.executable,str(SCRIPT),'serve','--mode',mode,'--port',str(port),'--upstream',f'http://127.0.0.1:{upstream.server_port}', '--evidence',str(evidence),'--source-sha','a'*40],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            for _ in range(100):
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.1): break
                except OSError: time.sleep(.01)
            else: raise AssertionError('capture did not start')
            yield f'http://127.0.0.1:{port}',evidence
        finally:
            process.terminate();process.wait(timeout=3)
    upstream.shutdown();upstream.server_close();thread.join(timeout=3)

def attempt(request):
    try: return urllib.request.urlopen(request,timeout=3).read()
    except urllib.error.HTTPError as error:
        with error: return error.read()

def records(evidence, count):
    for _ in range(100):
        path=evidence/'attempts.jsonl'
        data=[json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        if len(data)>=count: return data
        time.sleep(.01)
    raise AssertionError('attempt index missing')

class CaptureTests(unittest.TestCase):
    def test_raw_rpc_retains_failure_body_without_authorization_headers(self):
        with capture('rpc') as (origin,evidence):
            body=json.dumps({'jsonrpc':'2.0','id':1,'method':'getblockhash','params':[10]}).encode()
            attempt(urllib.request.Request(origin+'/',data=body,headers={'Authorization':'Basic test-only-credential-marker'}))
            record=records(evidence,1)[0]
            self.assertEqual(record['status'],503)
            self.assertEqual(record['rpc_methods'],['getblockhash'])
            raw=[gzip.decompress(path.read_bytes()) for path in (evidence/'bodies').glob('*.gz')]
            self.assertIn(body,raw)
            self.assertIn(b'{"error":"public fixture unavailable"}',raw)
            self.assertFalse(any(b'test-only-credential-marker' in value for value in raw))
            self.assertNotIn('Authorization',json.dumps(record))
            attempt(urllib.request.Request(origin+'/',data=b'{"method":"sendrawtransaction","params":[]}'))
            denied=records(evidence,2)[1]
            self.assertEqual(denied['failure'],'ValueError')
            self.assertNotIn('request_sha256',denied)

    def test_pir_retains_attempts_but_never_query_bodies_or_lookup_identifiers(self):
        with capture('pir') as (origin,evidence):
            route='/v1/shards/2/revisions/'+'a'*64+'/query/pages'
            body=b'test-only-encrypted-query'
            attempt(urllib.request.Request(origin+route,data=body))
            record=records(evidence,1)[0]
            self.assertEqual(record['status'],503)
            self.assertEqual(record['request_bytes'],len(body))
            self.assertFalse((evidence/'bodies').exists())
            attempt(origin+'/txid/test-only-private-identifier')
            denied=records(evidence,2)[1]
            self.assertEqual(denied['failure'],'ValueError')
            self.assertNotIn('path',denied)
            self.assertNotIn('test-only-private-identifier',(evidence/'attempts.jsonl').read_text())
            self.assertNotIn('test-only-encrypted-query',(evidence/'attempts.jsonl').read_text())

if __name__=='__main__': unittest.main()
