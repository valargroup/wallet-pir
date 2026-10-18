#!/usr/bin/env python3
"""Verify locally captured schema-10/11 records against retained manifests.

This script has no networking. Capture the records and manifests read-only first.
Every shard's padded unit bytes must match its public content hash before running
analysis; the Rust extractor then requires the published c1 digest to match too.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from verify import analyze


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--records', type=Path, required=True)
    p.add_argument('--manifests', nargs='+', type=Path, required=True)
    p.add_argument('--record-width', type=int, choices=[653, 737], required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    data = args.records.read_bytes()
    if len(data) % args.record_width:
        raise ValueError('truncated record file')
    report, results = [], {}
    for path in args.manifests:
        capture = json.loads(path.read_text())
        manifest = capture.get('manifest', capture)
        if manifest['schema_version'] != {653: 11, 737: 10}[args.record_width]:
            raise ValueError('manifest schema and record width disagree')
        for shard in manifest['coverage']['shards']:
            sid = shard['id']
            start = shard['global_row_start'] * 33 * args.record_width
            end = start + shard['records'] * args.record_width
            if end > len(data):
                raise ValueError('captured records do not cover manifest')
            records = data[start:end]
            units = manifest['unit_identities'][str(sid)]
            if len(units) != len(shard['units']):
                raise ValueError('unit identity coverage mismatch')
            checks = []
            for spec, identity in zip(shard['units'], units):
                if identity['shard_id'] != sid or identity['local_row_start'] != spec['local_row_start'] or identity['allocated_rows'] != spec['allocated_rows']:
                    raise ValueError('unit identity geometry mismatch')
                offset = spec['local_row_start'] * 33 * args.record_width
                length = spec['allocated_rows'] * 33 * args.record_width
                content = records[offset:offset+length].ljust(length, b'\0')
                if sha(content) != identity['content_sha256']:
                    raise ValueError('snapshot unit content mismatch')
                checks.append(identity['content_sha256'])
            session = next(s for s in manifest['sessions'] if s['shard_id'] == sid)
            public = session['public_params_sha256']
            identity = f'{sid}-{sha(records)}-{public}'
            row = dict(generation=manifest['generation'], shard=sid, schema=manifest['schema_version'],
                       manifest_sha256=sha(path.read_bytes()), records=len(records)//args.record_width,
                       record_sha256=sha(records), public_sha256=public, unit_sha256=checks)
            if identity not in results:
                record_file = args.output / f'{identity}.records'
                result_file = args.output / f'{identity}.json'
                record_file.write_bytes(records)
                command = [str(args.binary.resolve()), '--pattern', 'records', '--records', str(record_file),
                           '--record-width', str(args.record_width), '--shard', str(sid),
                           '--expected-public-sha256', public, '--output', str(result_file)]
                with (args.output / f'{identity}.log').open('w') as log:
                    subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
                evidence = json.loads(result_file.read_text())
                results[identity] = dict(file=result_file.name, sha256=sha(result_file.read_bytes()), **analyze(evidence))
                record_file.unlink()  # Only this script's disposable local copy.
            row['analysis'] = results[identity]
            report.append(row)
            (args.output / 'summary.json').write_text(json.dumps(report, indent=2)+'\n')
            print(f'g{manifest["generation"]}/s{sid}: {row["analysis"]["reason"]}', flush=True)


if __name__ == '__main__':
    main()
