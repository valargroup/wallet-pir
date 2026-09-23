#!/usr/bin/env python3
"""Copy a stopped Enhance record journal into v4's 33-record row layout.

Only packing metadata changes. Record bytes and block positions are preserved;
compiled artifacts and publication state are never migrated. Destination must
not exist. The source writer lock must be available for the entire copy.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def migrate(source, destination):
    if source.is_symlink() or any((source / n).is_symlink() for n in ('journal.lock', 'manifest.json', 'records.bin')):
        raise ValueError('source journal must not use symlinks')
    source = source.resolve(strict=True)
    destination = destination.absolute()
    if destination.exists() or destination.is_symlink() or source == destination:
        raise ValueError('destination must be a new journal directory')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (source / 'journal.lock').open('r+b') as lock, (destination.parent / '.v4-migration.lock').open('a+b') as output_lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(output_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if destination.exists() or destination.is_symlink():
            raise ValueError('destination already exists')
        original = (source / 'manifest.json').read_bytes()
        manifest = json.loads(original)
        if (set(manifest) != {'version', 'record_bytes', 'records_per_row', 'table', 'tree_size', 'blocks'}
                or any(type(manifest[k]) is not int for k in ('version', 'record_bytes', 'records_per_row'))
                or manifest['version'] != 7 or manifest['table'] != 'enhance'
                or manifest['record_bytes'] != 737 or manifest['records_per_row'] not in (29, 33)):
            raise ValueError('unsupported source journal format')
        position, height = 0, None
        for block in manifest['blocks']:
            if (set(block) != {'height', 'hash', 'first_position', 'action_count'}
                    or any(type(block[k]) is not int or not 0 <= block[k] < 2**64 for k in ('height', 'first_position', 'action_count'))
                    or block['first_position'] != position or (height is not None and block['height'] != height + 1)
                    or not isinstance(block['hash'], str) or not re.fullmatch('[0-9a-f]{64}', block['hash'])):
                raise ValueError('invalid block continuity')
            position += block['action_count']
            height = block['height']
        if type(manifest['tree_size']) is not int or manifest['tree_size'] != position:
            raise ValueError('invalid committed tree size')
        expected = position * 737
        if (source / 'records.bin').stat().st_size != expected:
            raise ValueError('record file does not match committed length')
        stage = Path(tempfile.mkdtemp(prefix='.v4-journal-', dir=destination.parent))
        try:
            digest = hashlib.sha256()
            with (source / 'records.bin').open('rb') as src, (stage / 'records.bin').open('xb') as dst:
                while chunk := src.read(1024 * 1024):
                    digest.update(chunk)
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            if (stage / 'records.bin').stat().st_size != expected or (source / 'manifest.json').read_bytes() != original:
                raise ValueError('source changed during migration')
            manifest['records_per_row'] = 33
            with (stage / 'manifest.json').open('x') as handle:
                json.dump(manifest, handle)
                handle.write('\n')
                handle.flush()
                os.fsync(handle.fileno())
            with (stage / 'journal.lock').open('x') as handle:
                os.fsync(handle.fileno())
            receipt = {'kind': 'enhance-v4-journal-migration', 'source_manifest_sha256': hashlib.sha256(original).hexdigest(),
                       'records_sha256': digest.hexdigest(), 'records': position, 'bytes': expected,
                       'last_height': height, 'records_per_row': 33}
            with (stage / 'migration.json').open('x') as handle:
                json.dump(receipt, handle, indent=2)
                handle.write('\n')
                handle.flush()
                os.fsync(handle.fileno())
            sync_dir(stage)
            os.rename(stage, destination)
            sync_dir(destination.parent)
            return receipt
        finally:
            if stage.exists():
                shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(migrate(args.source, args.destination), sort_keys=True))


if __name__ == '__main__':
    main()
