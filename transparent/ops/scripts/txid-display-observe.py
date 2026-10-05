#!/usr/bin/env python3
"""Independent observers for the txid display proof of concept's measurements.

    txid-display-observe.py observer --rpc-url URL --cookie FILE --out FILE.jsonl
    txid-display-observe.py mapwatch --url https://HOST/v1/txid/shards --out-dir DIR

`observer` polls the node's best block every 250 ms and records when each new
tip was first seen; it shares nothing with the controller, so the controller's
own timeline cannot vouch for itself. `mapwatch` polls the public display map
every second and keeps every distinct map and manifest it sees, with each
manifest checked against its digest. It records window drops and, as a stop
trigger, any sealed shard whose range or digest changes. Output is JSONL with
unix timestamps; nothing secret is written.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

# Loopback RPC and the public router only: never through an environment proxy.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def rpc_client(url, cookie, timeout=5):
    """JSON-RPC over the node's cookie; the cookie is reread so a node restart is followed."""
    def call(method, params=()):
        token = base64.b64encode(Path(cookie).read_bytes().strip()).decode()
        body = json.dumps({'jsonrpc': '1.0', 'id': 'txid-display-observe', 'method': method,
                           'params': list(params)}).encode()
        request = urllib.request.Request(url, data=body, headers={
            'Content-Type': 'application/json', 'Authorization': 'Basic ' + token})
        with OPENER.open(request, timeout=timeout) as response:
            reply = json.load(response)
        if reply.get('error'):
            raise RuntimeError('%s: %s' % (method, reply['error']))
        return reply['result']
    return call


def http_fetch(url, timeout=10):
    """`(status, lower-cased headers, body)`; status 0 when nothing answered."""
    try:
        with OPENER.open(urllib.request.Request(url, headers={'Cache-Control': 'no-cache'}), timeout=timeout) as response:
            return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read()
    except urllib.error.HTTPError as error:
        return error.code, {k.lower(): v for k, v in error.headers.items()}, error.read()
    except (urllib.error.URLError, OSError) as error:
        return 0, {}, str(error).encode()


class Writer:
    def __init__(self, stream):
        self.stream = stream

    def __call__(self, event, **fields):
        self.stream.write(json.dumps({'event': event, **fields}, sort_keys=True) + '\n')
        self.stream.flush()


def paced(interval, now, sleep, iterations):
    """Yield once per interval; `iterations` bounds the loop for tests."""
    count = 0
    while iterations is None or count < iterations:
        started = now()
        yield started
        count += 1
        sleep(max(0.0, interval - (now() - started)))


def observe(call, emit, interval=0.25, now=time.time, sleep=time.sleep, iterations=None):
    """Record each new best block hash with the time it was first seen."""
    last = None
    for started in paced(interval, now, sleep, iterations):
        try:
            tip = call('getbestblockhash')
            seen = now()
            if tip == last:
                continue
            try:
                height = call('getblockheader', [tip]).get('height')
            except Exception:
                height = call('getblockcount')
            emit('tip', hash=tip, height=height, observed_unix=round(seen, 6), poll_unix=round(started, 6))
            last = tip
        except Exception as error:
            emit('error', error='%s: %s' % (type(error).__name__, error), unix=round(started, 6))


class MapWatch:
    """Keeps every distinct map and manifest, and checks sealed shards never change."""

    def __init__(self, url, out_dir, fetch, emit, now=time.time):
        self.url = url
        self.base = url.rsplit('/v1/txid/shards', 1)[0]
        self.out = Path(out_dir)
        self.fetch, self.emit, self.now = fetch, emit, now
        self.last = None
        self.sealed = {}
        for name in ('maps', 'manifests'):
            (self.out / name).mkdir(parents=True, exist_ok=True)

    def save(self, kind, digest, body):
        path = self.out / kind / (digest + '.json')
        if not path.exists():
            temporary = path.with_suffix('.tmp')
            temporary.write_bytes(body)
            temporary.replace(path)

    def poll(self):
        observed = self.now()
        status, headers, body = self.fetch(self.url)
        if status != 200:
            self.emit('map_error', status=status, unix=round(observed, 6))
            return
        digest = hashlib.sha256(body).hexdigest()
        if digest == self.last:
            return
        mapping = json.loads(body)
        self.save('maps', digest, body)
        shards = mapping.get('shards', [])
        for entry in shards:
            self.manifest(entry)
        self.check_sealed(digest, shards)
        self.emit('map', map_sha256=digest, header_sha256=headers.get('x-txid-map-sha256'),
                  observed_unix=round(observed, 6), shards=len(shards),
                  first_shard_id=mapping.get('first_shard_id'), start_height=mapping.get('start_height'),
                  end_height=shards[-1]['end_height'] if shards else None,
                  sealed=sum(1 for e in shards if e.get('sealed')),
                  recent=[{k: e.get(k) for k in ('shard_id', 'start_height', 'end_height', 'records',
                                                 'min_bucket_records', 'manifest_digest', 'revision')}
                          for e in shards if not e.get('sealed')])
        self.last = digest

    def manifest(self, entry):
        digest = entry['manifest_digest']
        if (self.out / 'manifests' / (digest + '.json')).exists():
            return
        url = '%s/v1/txid/shards/%d/revisions/%s/manifest' % (self.base, entry['shard_id'], digest)
        status, _, body = self.fetch(url)
        if status != 200:
            self.emit('manifest_error', shard_id=entry['shard_id'], digest=digest, status=status)
            return
        if hashlib.sha256(body).hexdigest() != digest:
            self.emit('manifest_digest_mismatch', shard_id=entry['shard_id'], digest=digest)
            return
        self.save('manifests', digest, body)

    def check_sealed(self, map_sha256, shards):
        current = {e['shard_id']: (e['start_height'], e['end_height'], e['manifest_digest'])
                   for e in shards if e.get('sealed')}
        for shard_id, identity in current.items():
            previous = self.sealed.get(shard_id)
            if previous is not None and previous != identity:
                self.emit('sealed_changed', shard_id=shard_id, previous=list(previous), current=list(identity),
                          map_sha256=map_sha256)
        dropped = sorted(set(self.sealed) - set(current))
        if dropped:
            self.emit('window_drop', shard_ids=dropped, map_sha256=map_sha256)
        self.sealed = {**{k: v for k, v in self.sealed.items() if k not in dropped}, **current}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    observer = commands.add_parser('observer')
    observer.add_argument('--rpc-url', required=True)
    observer.add_argument('--cookie', required=True)
    observer.add_argument('--interval-ms', type=int, default=250)
    observer.add_argument('--out', required=True)
    watch = commands.add_parser('mapwatch')
    watch.add_argument('--url', required=True)
    watch.add_argument('--interval-ms', type=int, default=1000)
    watch.add_argument('--out-dir', required=True)
    args = parser.parse_args(argv)
    if args.command == 'observer':
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, 'a') as stream:
            observe(rpc_client(args.rpc_url, args.cookie), Writer(stream), args.interval_ms / 1000)
    else:
        Path(args.out_dir).mkdir(parents=True, exist_ok=True)
        with open(Path(args.out_dir) / 'mapwatch.jsonl', 'a') as stream:
            watcher = MapWatch(args.url, args.out_dir, http_fetch, Writer(stream))
            for _ in paced(args.interval_ms / 1000, time.time, time.sleep, None):
                try:
                    watcher.poll()
                except Exception as error:
                    watcher.emit('error', error='%s: %s' % (type(error).__name__, error), unix=round(time.time(), 6))
    return 0


if __name__ == '__main__':
    sys.exit(main())
