#!/usr/bin/env python3
"""Stream a committed journal range as tar, without writing on the source host.

Run over authenticated SSH using Python on stdin. Save stdout privately and
accept the archive only when SSH exits successfully and capture.json is present.
The native journal exporter must subsequently verify the received journal; this
transport preserves bytes and offsets but does not replace the native decoder.
"""
import argparse
import gzip
import io
import json
from pathlib import Path
import struct
import sys
import tarfile


class SliceReader:
    def __init__(self, source, length):
        self.source = source
        self.remaining = length

    def read(self, size=-1):
        size = self.remaining if size < 0 else min(size, self.remaining)
        body = self.source.read(size)
        if len(body) != size:
            raise ValueError('journal shortened during transfer')
        self.remaining -= len(body)
        return body


def regular_file(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'expected a regular journal file: {path.name}')
    return path.open('rb')


def capture(source, first, last, output):
    """Copy only committed event extents and check the endpoint after streaming."""
    with regular_file(source / 'meta.json') as file:
        meta = json.load(file)
    with regular_file(source / 'checkpoint.bin') as file:
        event_length, block_length = struct.unpack('<QQ', file.read())
    start = meta['start_height']
    if meta['version'] not in (2, 3) or block_length % 48:
        raise ValueError('unsupported journal version or block checkpoint')
    if first < start or first > last or last >= start + block_length // 48:
        raise ValueError('requested range is not committed')
    index = (first - start) * 48
    count = last - first + 1
    with regular_file(source / 'blocks.bin') as file:
        parent = None
        if first > start:
            file.seek(index - 48)
            parent = file.read(32)[::-1].hex()
        file.seek(index)
        raw = file.read(count * 48)
        if len(raw) != count * 48:
            raise ValueError('journal blocks shortened during capture')
        following = file.read(48) if index + count * 48 < block_length else b''
    endpoint = raw[-48:]
    offset = struct.unpack_from('<Q', raw, 32)[0]
    end_offset = struct.unpack_from('<Q', following, 32)[0] if following else event_length
    if not 0 <= offset <= end_offset <= event_length:
        raise ValueError('journal event offsets exceed checkpoint')
    blocks = bytearray(raw)
    for at in range(0, len(blocks), 48):
        position = struct.unpack_from('<Q', blocks, at + 32)[0]
        if not offset <= position <= end_offset:
            raise ValueError('block offset outside event extent')
        struct.pack_into('<Q', blocks, at + 32, position - offset)

    def add_bytes(archive, name, body):
        info = tarfile.TarInfo(name)
        info.size = len(body)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(body))

    absent = 0
    sidecar_bytes = 0
    with tarfile.open(fileobj=output, mode='w|') as archive:
        with regular_file(source / 'events.bin') as file:
            file.seek(offset)
            info = tarfile.TarInfo('events.bin')
            info.size = end_offset - offset
            info.mode = 0o600
            archive.addfile(info, SliceReader(file, info.size))
        add_bytes(archive, 'blocks.bin', blocks)
        for at in range(0, len(raw), 48):
            name = raw[at:at + 32][::-1].hex() + '.bin'
            present = False
            for family in ('display-v1', 'display-v2x'):
                directory = source / family
                if directory.is_symlink():
                    raise ValueError('sidecar directory must not be a symlink')
                path = directory / name
                if not path.exists():
                    continue
                with regular_file(path) as file:
                    info = tarfile.TarInfo(f'{family}/{name}')
                    info.size = path.stat().st_size
                    info.mode = 0o600
                    archive.addfile(info, file)
                    sidecar_bytes += info.size
                present = True
            absent += not present
        # A live append is harmless. A rollback/rewrite at the captured
        # endpoint invalidates the entire transfer, even if a tar was emitted.
        with regular_file(source / 'checkpoint.bin') as file:
            _, current_blocks = struct.unpack('<QQ', file.read())
        with regular_file(source / 'blocks.bin') as file:
            file.seek(index + (count - 1) * 48)
            unchanged = file.read(48) == endpoint
        with regular_file(source / 'meta.json') as file:
            same_identity = json.load(file) == meta
        if current_blocks < index + count * 48 or not unchanged or not same_identity:
            raise ValueError('journal changed during capture; discard archive')
        meta['start_height'] = first
        add_bytes(archive, 'meta.json', json.dumps(meta).encode())
        add_bytes(archive, 'checkpoint.bin', struct.pack('<QQ', end_offset - offset, len(blocks)))
        report = dict(start_height=first, end_height=last, blocks=count,
                      end_hash=endpoint[:32][::-1].hex(), parent_hash=parent,
                      event_bytes=end_offset - offset, sidecar_bytes=sidecar_bytes,
                      blocks_without_sidecars=absent, genesis_hash=meta['genesis_hash'])
        add_bytes(archive, 'capture.json', json.dumps(report, indent=2).encode())
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--start-height', type=int, required=True)
    parser.add_argument('--end-height', type=int, required=True)
    parser.add_argument('--gzip', action='store_true', help='compress setup transfer; never part of benchmark timings')
    args = parser.parse_args(argv)
    if args.gzip:
        with gzip.GzipFile(fileobj=sys.stdout.buffer, mode='wb', compresslevel=1, mtime=0) as output:
            capture(args.source, args.start_height, args.end_height, output)
    else:
        capture(args.source, args.start_height, args.end_height, sys.stdout.buffer)


if __name__ == '__main__':
    main()
