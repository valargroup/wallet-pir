#!/usr/bin/env python3
"""Build or run the pinned wallet client against a chain-derived public oracle."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from run import REVISION, ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('wallet_checkout', type=Path)
    parser.add_argument('--build-only', action='store_true')
    parser.add_argument('--server', default='https://enhance-pir.valargroup.dev')
    parser.add_argument('--oracle', type=Path)
    args = parser.parse_args()
    if not args.build_only and args.oracle is None:
        parser.error('--oracle is required unless --build-only is set')

    wallet = args.wallet_checkout.resolve()
    actual = subprocess.check_output(['git', '-C', str(wallet), 'rev-parse', 'HEAD'], text=True).strip()
    if actual != REVISION:
        raise SystemExit(f'wallet checkout must be at {REVISION}')
    subprocess.run(['git', '-C', str(wallet), 'diff', '--exit-code', 'HEAD', '--',
                    'Cargo.toml', 'Cargo.lock', 'zakura', 'librustzcash'], check=True,
                   stdout=subprocess.DEVNULL)

    with tempfile.TemporaryDirectory(prefix='schema11-public-') as directory:
        project = Path(directory)
        source = Path(__file__).with_name('public-client.rs')
        manifest = '[package]\nname="public-client"\nversion="0.0.0"\nedition="2021"\n'
        manifest += '[[bin]]\nname="public-client"\npath=' + json.dumps(str(source)) + '\n'
        manifest += '[dependencies]\n'
        manifest += 'zakura-pir-enhance = { path = ' + json.dumps(str(wallet / 'zakura/pir-enhance')) + ' }\n'
        manifest += '''anyhow = "1"
futures-util = "0.3"
hex = "0.4"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
[profile.release]
debug = 0
'''
        (project / 'Cargo.toml').write_text(manifest)
        env = {**os.environ, 'CARGO_TARGET_DIR': str(ROOT / 'target/schema11-interop')}
        subprocess.run(['cargo', 'build', '--release', '--manifest-path', str(project / 'Cargo.toml')],
                       env=env, check=True)
        if not args.build_only:
            binary = ROOT / 'target/schema11-interop/release/public-client'
            subprocess.run([str(binary), args.server, str(args.oracle.resolve())], check=True)


if __name__ == '__main__':
    main()
