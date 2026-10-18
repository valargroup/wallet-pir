#!/usr/bin/env python3
"""Export exact row hashes from a verified, immutable candidate publication."""
import argparse
import datetime
import hashlib
import json
import sys
from pathlib import Path

SCHEMA = 'transparent-shard-v12'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard-dir', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True, help='Immutable output file, or - for stdout')
    parser.add_argument('--include-provisional', action='store_true',
                        help='Only for a frozen disposable candidate, never moving-tail load')
    args = parser.parse_args()
    raw = (args.shard_dir / 'shards.json').read_bytes()
    tables = []
    for entry in json.loads(raw)['shards']:
        if not entry['sealed'] and not args.include_provisional:
            continue
        root = args.shard_dir / entry['manifest_digest']
        manifest = json.loads((root / 'manifest.json').read_bytes())
        if manifest['schema'] != SCHEMA:
            raise ValueError('candidate fixture requires ' + SCHEMA)
        # A directory shard's own table, then each page shard's `pages-<j>`;
        # an empty page shard has no table, and an open one is still growing.
        named = [('directory', manifest['directory_segments'])] + [
            (f"pages-{child['index']}", child['page_segments']) for child in manifest['page_children']
            if child['page_segments'] and (child['sealed'] or args.include_provisional)]
        for name, segments in named:
            rows, width = segments[0]['rows'], segments[0]['row_bytes']
            if width <= 0 or not all(s['rows'] == rows and s['row_bytes'] == width for s in segments):
                raise ValueError('inconsistent table geometry')
            indices = {0, rows - 1, *[i * (rows - 1) // 31 for i in range(32)]}
            files = [(root / f'{name}.{i}.bin').open('rb') for i in range(len(segments))]
            try:
                for index in range(rows):
                    files[0].seek(index * width)
                    if any(files[0].read(width)):
                        indices.add(index)
                        break
                samples = []
                for index in sorted(indices):
                    values = []
                    for file in files:
                        file.seek(index * width)
                        value = file.read(width)
                        if len(value) != width:
                            raise ValueError('truncated table')
                        values.append(value)
                    samples.append(dict(row=index, sha256=[hashlib.sha256(v).hexdigest() for v in values],
                                        nonempty=any(any(v) for v in values)))
                if not any(s['nonempty'] for s in samples):
                    raise ValueError('fixture has no occupied sample')
            finally:
                for file in files:
                    file.close()
            tables.append(dict(shard_id=entry['shard_id'], revision=entry['manifest_digest'],
                               geometry=entry['geometry'], table=name, rows=rows, row_bytes=width,
                               segments=len(segments), samples=samples, sealed=entry['sealed'],
                               source_table_sha256=[s['sha256'] for s in segments]))
    result = dict(schema=SCHEMA, source=str(args.shard_dir),
                  created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  source_map_sha256=hashlib.sha256(raw).hexdigest(), tables=tables,
                  oracle='Exact row hashes from independently verified published plaintext; extraction correctness is a separate chain oracle gate',
                  provisional_frozen=args.include_provisional)
    encoded = (json.dumps(result, separators=(',', ':'))+'\n').encode()
    if args.out == Path('-'):
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()
    else:
        with args.out.open('xb') as file:
            file.write(encoded)
    print(json.dumps(dict(tables=len(tables), sha256=hashlib.sha256(encoded).hexdigest())),
          file=sys.stderr if args.out == Path('-') else sys.stdout)


if __name__ == '__main__':
    main()
