#!/usr/bin/env python3
"""Read-only prerequisite checks; subprocess output and credentials stay private."""
import argparse
import ctypes.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]


def probe(command, timeout=15):
    try:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=timeout, env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def libclang_available():
    """Check that bindgen can load a native clang library, not just its CLI."""
    candidates = [str(p) for p in Path(os.environ.get('LIBCLANG_PATH', '/nonexistent')).glob('*clang*')]
    candidates += [ctypes.util.find_library(name) for name in ['clang', 'clang-18', 'clang-19', 'clang-17', 'clang-14']]
    if platform.system() == 'Darwin':
        candidates.append('/Library/Developer/CommandLineTools/usr/lib/libclang.dylib')
    return any(path and probe(['python3', '-c', 'import ctypes,sys; ctypes.CDLL(sys.argv[1])', path]) for path in candidates)


def inspect(network=False):
    channel = tomllib.loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
    rows = []
    def add(name, ok, remedy):
        rows.append({'check': name, 'ok': bool(ok), 'remedy': None if ok else remedy})
    # rustup run never installs a missing toolchain in a read-only doctor.
    for tool, args in [('rustc', ['--version']), ('rustfmt', ['--version']), ('cargo', ['clippy', '--version'])]:
        add(tool, probe(['rustup', 'run', channel, tool, *args]), f'rustup toolchain install {channel} --component rustfmt --component clippy')
    for tool in ['git', 'python3', 'clang', 'protoc', 'jq', 'shellcheck', 'node', 'make']:
        add(tool, shutil.which(tool), 'Install repository prerequisites listed in docs/development.md')
    add('libclang', libclang_available(),
        'Install libclang (macOS: Xcode Command Line Tools; Linux: libclang-dev) or set LIBCLANG_PATH')
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    add('dependency cache', (cargo_home / 'registry').is_dir() and (cargo_home / 'git').is_dir()
        and probe(['rustup', 'run', channel, 'cargo', 'metadata', '--locked', '--offline', '--no-deps', '--format-version', '1']), 'make prepare-dev')
    target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'target'))
    parent = target
    while not parent.exists():
        parent = parent.parent
    add('target writable', parent.is_dir() and os.access(parent, os.W_OK), 'Choose a writable CARGO_TARGET_DIR owned by this development user')
    add('red-main pre-push guard', probe(['python3', 'tools/ci/main_health.py', '--installed']), 'make prepare-dev')
    if network:
        add('GitHub authentication', probe(['gh', 'auth', 'status']), 'gh auth login')
        for repo in ['valargroup/ipir-sp', 'valargroup/spiral-rs', 'zakura-core/zakura', 'zakura-core/wallet-libraries']:
            add(repo + ' access', probe(['git', 'ls-remote', '--exit-code', 'https://github.com/' + repo + '.git', 'HEAD']), 'Configure git credential access for the pinned dependency; then make prepare-dev')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--network', action='store_true')
    args = parser.parse_args()
    rows = inspect(args.network)
    print(json.dumps(rows, indent=2))
    raise SystemExit(0 if all(row['ok'] for row in rows) else 1)


if __name__ == '__main__':
    main()
