#!/usr/bin/env python3
"""Loopback-only evidence capture: raw public chain RPC, or PIR HTTP route digests.

No request/response headers are persisted. RPC bodies are restricted to public
chain reads; PIR bodies are never retained, only sizes and hashes. Every failed
attempt remains in the index. Run with explicit systemd resource limits.
"""
import argparse
import base64
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

RPC_METHODS = {'getblockhash', 'getblock', 'getblockheader', 'getrawtransaction'}
PIR_GET = re.compile(r'/v1/(filters/shards(?:/[0-9]+/filter)?|shards/init|shards/[0-9]+/revisions/[0-9a-f]{64}/(manifest|setup/(directory|pages)/[0-9]+))\Z')
PIR_POST = re.compile(r'/v1/shards/[0-9]+/revisions/[0-9a-f]{64}/query/(directory|pages)\Z')
MAX_BODY = 128 * 1024 * 1024


def check_rpc(body):
    values = json.loads(body)
    values = values if isinstance(values, list) else [values]
    if not values or len(values) > 1024:
        raise ValueError('invalid RPC batch bound')
    for value in values:
        if not isinstance(value, dict) or value.get('method') not in RPC_METHODS:
            raise ValueError('only public block/transaction reads allowed')
        # Permit only normal JSON-RPC fields, never arbitrary header-like payloads.
        if set(value) - {'jsonrpc', 'id', 'method', 'params'}:
            raise ValueError('unsupported RPC fields')
    return values


def retain(root, body):
    digest = hashlib.sha256(body).hexdigest()
    destination = root / 'bodies' / (digest + '.json.gz')
    destination.parent.mkdir(exist_ok=True)
    if not destination.exists():
        # Serialize shared content writes in the server. Snapshot body hashes do
        # not contain credentials; headers never enter this function.
        with destination.open('xb') as file:
            file.write(gzip.compress(body, mtime=0))
    elif hashlib.sha256(gzip.decompress(destination.read_bytes())).hexdigest() != digest:
        raise ValueError('retained input hash mismatch')
    return digest


def serve(args):
    upstream = urllib.parse.urlsplit(args.upstream)
    if upstream.scheme != 'http' or upstream.hostname != '127.0.0.1' or upstream.path not in ('', '/') or upstream.query or upstream.fragment or upstream.username:
        raise ValueError('upstream must be an explicit loopback HTTP origin')
    args.evidence.mkdir(parents=True, exist_ok=False)
    (args.evidence / 'owner.json').write_text(json.dumps({'source_sha': args.source_sha,
        'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'mode': args.mode, 'port': args.port, 'upstream': args.upstream,
        'started': time.time(), 'pid': __import__('os').getpid()}, indent=2)+'\n')
    lock = threading.Lock()
    slots = threading.BoundedSemaphore(8)
    class Capture(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.0'
        def log_message(self, *_):
            pass
        def do_GET(self): self.forward()
        def do_POST(self): self.forward()
        def forward(self):
            started = time.time()
            record = {'unix': started, 'method': self.command, 'status': 502}
            body = b''
            response = b''
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if size < 0 or size > MAX_BODY or self.headers.get('Transfer-Encoding'):
                    raise ValueError('unsupported request size/framing')
                body = self.rfile.read(size)
                if len(body) != size: raise ValueError('truncated request')
                if args.mode == 'rpc':
                    if self.command != 'POST' or self.path != '/': raise ValueError('RPC POST required')
                    calls = check_rpc(body)
                    record['rpc_methods'] = [value['method'] for value in calls]
                    with lock:
                        record['request_sha256'] = retain(args.evidence, body)
                else:
                    # Avoid writing a rejected URL that could contain a wallet identifier.
                    pattern = PIR_GET if self.command == 'GET' else PIR_POST if self.command == 'POST' else None
                    if pattern is None or not pattern.fullmatch(self.path):
                        raise ValueError('public lookup or unsupported PIR route rejected')
                    if self.command == 'GET' and body: raise ValueError('GET body rejected')
                    record['path'] = self.path
                    record['request_sha256'] = hashlib.sha256(body).hexdigest()
                record['request_bytes'] = len(body)
                headers = {'Content-Type': self.headers.get('Content-Type','application/octet-stream')}
                if args.mode == 'rpc' and self.headers.get('Authorization'):
                    headers['Authorization'] = self.headers['Authorization']
                request = urllib.request.Request(args.upstream.rstrip('/') + self.path, data=body if self.command=='POST' else None, headers=headers, method=self.command)
                with slots:
                    try:
                        source = urllib.request.urlopen(request, timeout=60)
                    except urllib.error.HTTPError as error:
                        source = error
                    with source:
                        response = source.read(MAX_BODY+1)
                        if len(response)>MAX_BODY: raise ValueError('response bound exceeded')
                        record['status'] = source.status
                        content_type = source.headers.get('Content-Type','application/octet-stream')
                record['response_bytes'] = len(response)
                if args.mode == 'rpc':
                    with lock:
                        record['response_sha256'] = retain(args.evidence, response)
                else:
                    record['response_sha256'] = hashlib.sha256(response).hexdigest()
                self.send_response(record['status'])
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length',str(len(response)))
                self.end_headers()
                self.wfile.write(response)
            except Exception as error:
                # Exception strings may contain sensitive URLs or headers. Retain only class.
                record['failure'] = type(error).__name__
                try:
                    self.send_response(502); self.send_header('Content-Length','0'); self.end_headers()
                except Exception:
                    pass
            finally:
                record['seconds'] = time.time()-started
                with lock, (args.evidence/'attempts.jsonl').open('a') as output:
                    output.write(json.dumps(record, sort_keys=True)+'\n')
                    output.flush()
    ThreadingHTTPServer(('127.0.0.1',args.port), Capture).serve_forever()


def snapshot(args):
    if args.through < args.birthday or args.birthday<1 or args.through-args.birthday>=10000:
        raise ValueError('bounded header range required')
    cookie = args.cookie.read_text().strip()
    authorization = 'Basic '+base64.b64encode(cookie.encode()).decode()
    def call(calls):
        request=urllib.request.Request(args.rpc, data=json.dumps(calls).encode(),headers={'Content-Type':'application/json','Authorization':authorization})
        with urllib.request.urlopen(request,timeout=60) as response:
            raw=response.read(MAX_BODY+1)
        if len(raw)>MAX_BODY: raise ValueError('RPC response bound exceeded')
        values=json.loads(raw)
        if len(values)!=len(calls): raise ValueError('RPC batch cardinality mismatch')
        by_id={value['id']:value for value in values}
        if len(by_id)!=len(calls): raise ValueError('duplicate RPC response id')
        result=[]
        for value in calls:
            item=by_id[value['id']]
            if item.get('error') is not None: raise ValueError('RPC read failed')
            result.append(item['result'])
        return result
    headers=[]
    for start in range(args.birthday-1,args.through+1,64):
        heights=list(range(start,min(start+64,args.through+1)))
        hashes=call([{'jsonrpc':'2.0','id':height,'method':'getblockhash','params':[height]} for height in heights])
        batch=call([{'jsonrpc':'2.0','id':height,'method':'getblockheader','params':[block_hash,True]} for height,block_hash in zip(heights,hashes)])
        for height,block_hash,header in zip(heights,hashes,batch):
            if header['hash']!=block_hash or header['height']!=height: raise ValueError('RPC header identity mismatch')
            if headers and header['previousblockhash']!=headers[-1]['hash']: raise ValueError('RPC chain discontinuity')
            headers.append({key:header[key] for key in ('height','hash','time','previousblockhash')})
    scripts=json.loads(args.scripts.read_text())
    if not isinstance(scripts,list) or not 1<=len(scripts)<=64 or any(not isinstance(s,str) or not re.fullmatch('[0-9a-f]{46,50}',s) for s in scripts):
        raise ValueError('bounded public script fixture required')
    with args.out.open('x') as output:
        json.dump({'source':'independent local Zakura RPC reads; retained capture bodies','birthday':args.birthday,'through':args.through,'headers':headers,'scripts':scripts},output,indent=2)
        output.write('\n')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    server=sub.add_parser('serve')
    server.add_argument('--mode',choices=['rpc','pir'],required=True)
    server.add_argument('--port',type=int,required=True)
    server.add_argument('--upstream',required=True)
    server.add_argument('--evidence',type=Path,required=True)
    server.add_argument('--source-sha',required=True)
    collect=sub.add_parser('snapshot')
    collect.add_argument('--rpc',default='http://127.0.0.1:18232')
    collect.add_argument('--cookie',type=Path,required=True)
    collect.add_argument('--birthday',type=int,required=True)
    collect.add_argument('--through',type=int,required=True)
    collect.add_argument('--scripts',type=Path,required=True)
    collect.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='serve':
        if not re.fullmatch('[0-9a-f]{40}',args.source_sha) or not 1024<=args.port<=65535:
            parser.error('full source SHA and unprivileged port required')
        serve(args)
    else:
        parsed=urllib.parse.urlsplit(args.rpc)
        if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.username or parsed.path not in ('','/') or parsed.query or parsed.fragment:
            parser.error('RPC capture must be on loopback')
        snapshot(args)

if __name__=='__main__': main()
