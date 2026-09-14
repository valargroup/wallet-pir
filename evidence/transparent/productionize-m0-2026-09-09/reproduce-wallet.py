#!/usr/bin/env python3
"""Reconstruct the captured M0 source without modifying the original checkout."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--repo', type=Path, required=True, help='Local wallet-libraries Git repository containing the recorded commit')
parser.add_argument('--out', type=Path, required=True, help='New, nonexistent source directory')
args = parser.parse_args()
evidence = Path(__file__).resolve().parent
record = json.loads((evidence / 'wallet-source-baseline.json').read_text())
archive = subprocess.check_output(['git', '-C', str(args.repo), 'archive', '--format=tar', record['head']])
assert hashlib.sha256(archive).hexdigest() == record['head_archive_sha256']
patch = gzip.decompress((evidence / 'wallet-working.patch.gz').read_bytes())
assert hashlib.sha256(patch).hexdigest() == record['combined_patch_sha256']
args.out.mkdir(parents=True, exist_ok=False)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    source.extractall(args.out, filter='data')
subprocess.run(['git', 'apply', '-'], input=patch, cwd=args.out, check=True)
for name, digest in record['included_untracked_source_sha256'].items():
    data = (evidence / 'untracked-source' / name).read_bytes()
    assert hashlib.sha256(data).hexdigest() == digest
    destination = args.out / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
for name, digest in {**record['tracked_file_sha256'], **record['included_untracked_source_sha256']}.items():
    assert hashlib.sha256((args.out / name).read_bytes()).hexdigest() == digest, name
print(f'Verified {len(record["tracked_file_sha256"])} tracked files and {len(record["included_untracked_source_sha256"])} untracked source file(s): {args.out.resolve()}')
