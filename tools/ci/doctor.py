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
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PROTOC_TIMEOUT = 5
# Modules outside the standard library that the tools/ unittest suites import.
# The repository has no Python requirements file; nothing here installs them.
PYTHON_MODULES = ('cryptography',)


def first_line(data):
    return next((line.strip() for line in data.decode(errors='replace').splitlines() if line.strip()), '')


def probe_detail(command, timeout=15):
    """Return (ok, detail): the first output line on success, else why it failed."""
    try:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=timeout, env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
    except subprocess.TimeoutExpired:
        return False, f'timed out after {timeout}s'
    except OSError as error:
        return False, error.strerror or str(error)
    if result.returncode == 0:
        return True, first_line(result.stdout)
    status = f'killed by signal {-result.returncode}' if result.returncode < 0 else f'exit {result.returncode}'
    stderr = first_line(result.stderr)
    return False, f'{status}: {stderr}' if stderr else status


def probe(command, timeout=15):
    return probe_detail(command, timeout)[0]


def selected_protoc(env):
    """The protoc prost-build runs: PROTOC when set, else the first one on PATH."""
    return env.get('PROTOC') or shutil.which('protoc', path=env.get('PATH'))


def protoc_status(env):
    """Return (path, ok, detail) after running the selected protoc --version."""
    path = selected_protoc(env)
    if not path:
        return None, False, 'not found on PATH and PROTOC is unset'
    return (path, *probe_detail([path, '--version'], PROTOC_TIMEOUT))


IMPORT_SCRIPT = """import importlib, json, sys
missing = {}
for name in sys.argv[1:]:
    try:
        importlib.import_module(name)
    except Exception as error:
        missing[name] = f'{type(error).__name__}: {error}'
print(json.dumps(missing))
"""


def missing_python_modules(python=sys.executable):
    """Map each PYTHON_MODULES entry the interpreter cannot import to its error."""
    try:
        result = subprocess.run([python, '-c', IMPORT_SCRIPT, *PYTHON_MODULES], capture_output=True, timeout=30)
        return json.loads(result.stdout)
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        return {name: f'import check failed: {error}' for name in PYTHON_MODULES}


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
    for tool in ['git', 'python3', 'clang', 'jq', 'shellcheck', 'node', 'make']:
        add(tool, shutil.which(tool), 'Install repository prerequisites listed in docs/development.md')
    path, ok, detail = protoc_status(os.environ)
    add('protoc', ok, f'set PROTOC=<working protoc> or repair {path} ({detail})' if path
        else 'Install repository prerequisites listed in docs/development.md or set PROTOC')
    missing = missing_python_modules()
    add('python modules', not missing, f'{sys.executable} cannot import '
        + '; '.join(f'{name} ({error})' for name, error in missing.items()) + '; install them for this interpreter')
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
