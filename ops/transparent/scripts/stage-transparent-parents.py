#!/usr/bin/env python3
"""Stage a previously evaluated archive parent bundle against the live map.

This does not rebuild filters or alter public routing. Only the verified
manifest and its content-addressed bodies are copied into the serving tree.
"""
import argparse
import hashlib
import json
import re
import shutil
import urllib.request
from pathlib import Path


def stage(candidate, map_url, output):
    raw = Path(candidate).read_bytes()
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError('manifest too large')
    manifest = json.loads(raw)
    with urllib.request.urlopen(map_url, timeout=30) as response:
        live_raw = response.read(2 * 1024 * 1024 + 1)
    live = json.loads(live_raw)
    if manifest['schema'] != 'transparent-parent-evaluation-v1':
        raise ValueError('wrong manifest schema')
    children = {c['shard_id']: c for c in live['shards']}
    seen, bodies = set(), {}
    for parent in manifest['parents']:
        if (parent['m'], parent['p']) != (100, 6):
            raise ValueError('unexpected archive precision')
        if parent['genesis_hash'] != live['genesis_hash'] or parent['profile'] != live['profile']:
            raise ValueError('wrong chain/profile')
        group = parent['children']
        if not 1 <= len(group) <= 8:
            raise ValueError('unexpected group size')
        for i, child in enumerate(group):
            if child != children.get(child['shard_id']) or not child['sealed'] or child['geometry'] != 'archive-wide':
                raise ValueError('parent child does not match sealed production archive')
            if child['shard_id'] in seen or (i and group[i-1]['end_height'] + 1 != child['start_height']):
                raise ValueError('overlapping or noncontiguous children')
            seen.add(child['shard_id'])
        digest = parent['filter_hash']
        if not re.fullmatch('[0-9a-f]{64}', digest) or not 0 < parent['bytes'] <= 64 * 1024 * 1024:
            raise ValueError('invalid body identity/size')
        body = Path(candidate).parent / 'artifacts' / (digest + '.bin')
        if body.stat().st_size != parent['bytes'] or hashlib.sha256(body.read_bytes()).hexdigest() != digest:
            raise ValueError('parent artifact digest/size mismatch')
        bodies[digest] = body
    if seen != {c['shard_id'] for c in live['shards'] if c['sealed'] and c['geometry'] == 'archive-wide'}:
        raise ValueError('bundle must cover the complete sealed archive')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'artifacts').mkdir()
    for digest, body in bodies.items():
        shutil.copyfile(body, output / 'artifacts' / (digest + '.bin'))
    (output / 'archive-wide.json').write_bytes(raw)
    return {'manifest_sha256': hashlib.sha256(raw).hexdigest(), 'live_map_sha256': hashlib.sha256(live_raw).hexdigest(),
            'covered_children': len(seen), 'parents': len(bodies), 'parent_bytes': sum(p.stat().st_size for p in bodies.values()),
            'public_tip': live['shards'][-1]['end_height'], 'map_url': map_url, 'output': str(output)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--map-url', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.candidate, args.map_url, args.out), indent=2))
