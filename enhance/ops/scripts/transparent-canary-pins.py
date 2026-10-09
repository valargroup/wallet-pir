#!/usr/bin/env python3
"""Derive the Transparent quality canary's pins from a sealed-row fixture.

The fixture is an export of row hashes read from published plaintext
(`transparent/ops/scripts/activity-query-fixture.py`), such as the continuous
load's. This checks that every fixture table names a sealed revision the given
live shard map still serves, that all four traffic groups are covered and that
each table has an occupied sample. It prints the fixture's SHA-256 and its
anchor: the terminal block of the highest fixture shard. With --rpc-url and
--cookie it also requires the node to return that block hash at that height.
It never reads a PIR answer, so it cannot adopt one as expected bytes.
"""
import argparse
import base64
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

SCHEMA = 'transparent-shard-v11'
GROUPS = {(g, t) for g in ('recent-4k-8k', 'archive-wide') for t in ('directory', 'pages')}


def read(path, limit):
    with Path(path).open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError(f'{path} exceeds {limit} bytes')
    return raw


def pins(fixture_raw, map_raw):
    fixture = json.loads(fixture_raw)
    if fixture.get('schema') != SCHEMA:
        raise ValueError('fixture schema is not ' + SCHEMA)
    shards = {s['shard_id']: s for s in json.loads(map_raw)['shards']}
    groups, anchor = set(), None
    for table in fixture.get('tables') or []:
        entry = shards.get(table['shard_id'])
        if (entry is None or entry['sealed'] is not True or entry['manifest_digest'] != table['revision']
                or entry['geometry'] != table['geometry']):
            raise ValueError(f"shard {table['shard_id']} {table['table']} is not a sealed revision in the map")
        samples = table['samples']
        if not any(s['nonempty'] for s in samples) or any(len(s['sha256']) != table['segments'] for s in samples):
            raise ValueError(f"shard {table['shard_id']} {table['table']} has no usable occupied sample")
        groups.add((table['geometry'], table['table']))
        if anchor is None or entry['end_height'] > anchor['end_height']:
            anchor = entry
    if groups != GROUPS:
        raise ValueError('fixture does not cover every recent/archive directory/pages group')
    return {'schema': SCHEMA, 'fixture_sha256': hashlib.sha256(fixture_raw).hexdigest(),
            'map_sha256': hashlib.sha256(map_raw).hexdigest(), 'tables': len(fixture['tables']),
            'anchor_height': anchor['end_height'], 'anchor_hash': anchor['terminal_block_hash']}


def node_hash(url, cookie, height):
    user, password = Path(cookie).read_text().strip().split(':', 1)
    token = base64.b64encode(f'{user}:{password}'.encode()).decode()
    body = json.dumps({'jsonrpc': '1.0', 'id': 'canary-pins', 'method': 'getblockhash',
                       'params': [height]}).encode()
    request = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json',
                                                              'Authorization': 'Basic ' + token})
    with urllib.request.urlopen(request, timeout=10) as response:
        reply = json.load(response)
    if reply.get('error') is not None:
        raise ValueError('node refused getblockhash')
    return reply['result']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--shard-map', type=Path, required=True, help="the live publication's shards.json")
    parser.add_argument('--rpc-url')
    parser.add_argument('--cookie', type=Path)
    args = parser.parse_args(argv)
    if (args.rpc_url is None) != (args.cookie is None):
        parser.error('--rpc-url and --cookie go together')
    result = pins(read(args.fixture, 2 * 1024 * 1024), read(args.shard_map, 4 * 1024 * 1024))
    if args.rpc_url:
        if node_hash(args.rpc_url, args.cookie, result['anchor_height']) != result['anchor_hash']:
            print(json.dumps({'error': 'anchor is not canonical at the node', **result}))
            return 1
        result['anchor_checked'] = 'node getblockhash'
    print(json.dumps(result))
    return 0


if __name__ == '__main__':
    sys.exit(main())
