#!/usr/bin/env python3
"""Launch continuous receiver PIR serving from public chain data. No wallet access."""
import argparse
import os
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary-dir', type=Path, required=True)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--rpc-url', required=True)
    p.add_argument('--refresh-seconds', type=int, default=10)
    a = p.parse_args()
    if a.refresh_seconds < 1:
        p.error('refresh interval must be positive')
    indexer = a.binary_dir.resolve() / 'receiver-directory'
    if not indexer.is_file():
        p.error('build receiver-directory first')
    os.execv(str(indexer), [
        str(indexer), '--serve', '--data-dir', str(a.data_dir.resolve()),
        '--rpc-url', a.rpc_url, '--no-auth', '--witnesses',
        '--min-rows', '8192', '--concurrency', '12',
        '--poll-seconds', str(a.refresh_seconds),
    ])


if __name__ == '__main__':
    main()
