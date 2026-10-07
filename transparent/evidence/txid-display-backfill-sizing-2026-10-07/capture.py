#!/usr/bin/env python3
"""Fetch the public history and txid display maps and every display manifest.

Plain GETs of the public routes any wallet uses; no credentials, no state
changed. Writes the raw bytes under inputs/ and their provenance to
inputs/capture.json.
"""
import datetime
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

BASE = 'https://transparent-pir.valargroup.dev'
OUT = Path(__file__).resolve().parent / 'inputs'


def get(path):
    started = datetime.datetime.now(datetime.timezone.utc)
    with urllib.request.urlopen(BASE + path, timeout=60) as response:
        body = response.read()
        status = response.status
        encoding = response.headers.get('Content-Encoding')
    return body, {'url': BASE + path, 'status': status, 'content_encoding': encoding,
                  'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
                  'fetched_utc': started.isoformat(timespec='seconds')}


def main():
    OUT.mkdir(exist_ok=True)
    records = []
    for name, path in (('history-shards.json', '/v1/shards'), ('txid-shards.json', '/v1/txid/shards')):
        body, meta = get(path)
        (OUT / name).write_bytes(body)
        records.append({'file': name, **meta})
    display = json.loads((OUT / 'txid-shards.json').read_bytes())
    for shard in display['shards']:
        name = 'txid-manifest-%02d.json' % shard['shard_id']
        body, meta = get('/v1/txid/shards/%d/revisions/%s/manifest' % (shard['shard_id'], shard['manifest_digest']))
        (OUT / name).write_bytes(body)
        records.append({'file': name, **meta})
    (OUT / 'capture.json').write_text(json.dumps({'base': BASE, 'files': records}, indent=1) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
