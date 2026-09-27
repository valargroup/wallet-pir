#!/usr/bin/env python3
"""Refresh a loopback receiver POC from public chain data. No wallet access."""
import argparse
import json
import signal
import subprocess
import time
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary-dir', type=Path, required=True)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--rpc-url', required=True)
    p.add_argument('--refresh-seconds', type=int, default=300)
    a = p.parse_args()
    if a.refresh_seconds < 60:
        p.error('refresh interval must be at least 60 seconds')
    indexer = a.binary_dir.resolve() / 'receiver-directory'
    server = a.binary_dir.resolve() / 'receiver-pir-server'
    if not indexer.is_file() or not server.is_file():
        p.error('build both binaries first')
    a.data_dir.mkdir(parents=True, exist_ok=True)
    current = a.data_dir / 'publications/current.json'
    serving = None
    indexing = None
    revision = None

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        while True:
            indexing = subprocess.Popen([
                str(indexer), '--data-dir', str(a.data_dir.resolve()),
                '--rpc-url', a.rpc_url, '--no-auth', '--witnesses',
                '--min-rows', '8192', '--concurrency', '12',
            ])
            status = indexing.wait()
            indexing = None
            if status == 0:
                publication = current.read_bytes()
                # An oversized publication is not supported by the fixed PIR geometry.
                if json.loads(publication)['rows'] != 8192:
                    raise RuntimeError('publication outgrew the POC geometry')
                if publication != revision or serving is None or serving.poll() is not None:
                    if serving is not None and serving.poll() is None:
                        serving.terminate()
                        serving.wait(timeout=15)
                    serving = subprocess.Popen([str(server), '--manifest', str(current.resolve())])
                    revision = publication
            else:
                print(f'Indexing failed ({status}); retaining previous service for inspection', flush=True)
            time.sleep(a.refresh_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        for child in (indexing, serving):
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


if __name__ == '__main__':
    main()
