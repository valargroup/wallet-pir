#!/usr/bin/env python3
"""Run the schema-11 server against the unmodified, pinned PR #28 wallet checkout."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

REVISION = None
ROOT = Path(__file__).resolve().parents[3]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('wallet_checkout', type=Path)
    parser.add_argument('--wallet-revision', help='Require this exact wallet commit')
    args = parser.parse_args()
    wallet = args.wallet_checkout.resolve()
    actual = subprocess.check_output(['git', '-C', str(wallet), 'rev-parse', 'HEAD'], text=True).strip()
    if args.wallet_revision and actual != args.wallet_revision:
        raise SystemExit(f'wallet checkout must be at {args.wallet_revision}')
    import hashlib
    files = sorted((wallet / 'zakura/pir-enhance').rglob('*.rs'))
    fingerprint = hashlib.sha256()
    for path in files:
        fingerprint.update(str(path.relative_to(wallet)).encode())
        fingerprint.update(path.read_bytes())
    print(f'wallet revision={actual} source_sha256={fingerprint.hexdigest()}', flush=True)
    with tempfile.TemporaryDirectory(prefix='schema11-interop-') as directory:
        project = Path(directory)
        dependencies = {
            'enhance-pir-server': ROOT / 'enhance/services/enhance-pir-server',
            'enhance-pir': ROOT / 'enhance/crates/enhance-pir',
        }
        manifest = '[package]\nname="schema11-interop"\nversion="0.0.0"\nedition="2021"\n'
        manifest += '[[test]]\nname="interop"\npath=' + json.dumps(str(Path(__file__).with_name('interop.rs'))) + '\n[dependencies]\n'
        for name, path in dependencies.items():
            manifest += f'{name} = {{ path = {json.dumps(str(path))} }}\n'
        for name, subdir, features in [
            ('zakura-pir-enhance', 'zakura/pir-enhance', ['wallet']),
            ('zakura-client-backend', 'librustzcash/zcash_client_backend', ['zakura-pir-enhance', 'test-dependencies']),
            ('zakura-client-sqlite', 'librustzcash/zcash_client_sqlite', ['zakura-pir-enhance', 'test-dependencies']),
        ]:
            manifest += f'{name} = {{ path = {json.dumps(str(wallet / subdir))}, features = {json.dumps(features)} }}\n'
        manifest += '''zakura-primitives = "=1.2.0"
zcash_protocol = { version = "0.10.4", features = ["local-consensus"] }
axum = "0.7"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net"] }
tempfile = "3"
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
futures-util = "0.3"
orchard = { package = "zakura-orchard", version = "=1.2.0", default-features = false }
zcash_note_encryption = "=0.4.2"
serde_json = "1"
hex = "0.4"
[profile.test]
opt-level = 2
debug = 0
[profile.test.package.valar-spiral-rs]
overflow-checks = false
'''
        (project / 'Cargo.toml').write_text(manifest)
        env = {**os.environ, 'CARGO_TARGET_DIR': os.environ.get('CARGO_TARGET_DIR', str(ROOT / 'target/schema11-interop'))}
        subprocess.run(['cargo', 'test', '--manifest-path', str(project / 'Cargo.toml'),
                        '--test', 'interop', '--', '--nocapture', '--test-threads=1'], env=env, check=True)


if __name__ == '__main__':
    main()
