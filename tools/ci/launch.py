#!/usr/bin/env python3
"""Run validation with explicit tools, permissions and an atomic result receipt.

Launch this through a tracked hub job for long remote work. This program waits
for its child; SSH inspection and job lifetime remain separate.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import tomllib


def configuration(cwd, env):
    from cargo_cache import build_flags, cargo_configs
    value = {'flags': build_flags(env), 'cargo': cargo_configs(cwd, env)[0],
             'toolchain': (cwd / 'rust-toolchain.toml').read_text()}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def execute(command, receipt, *, cwd=Path.cwd(), rust=True):
    cwd = cwd.resolve()
    if receipt.exists():
        raise ValueError('receipt already exists; choose a new attempt path')
    os.umask(0o022)
    env = dict(os.environ, PATH=str(Path.home() / '.cargo/bin') + os.pathsep + os.environ['PATH'])
    toolchain = tomllib.loads((cwd / 'rust-toolchain.toml').read_text())['toolchain']['channel']
    env['RUSTUP_TOOLCHAIN'] = toolchain
    if rust:
        for program in ('cargo', 'rustc', 'clang', 'protoc'):
            if not shutil.which(program, path=env['PATH']):
                raise ValueError(f'missing validation prerequisite: {program}')
        subprocess.run(['rustc', '--version'], cwd=cwd, env=env, check=True)
        subprocess.run(['cargo', '--version'], cwd=cwd, env=env, check=True)
    git = lambda *args: subprocess.check_output(['git', *args], cwd=cwd, text=True).strip()
    sha, state = git('rev-parse', 'HEAD'), git('status', '--porcelain')
    if state:
        raise ValueError('durable validation requires a clean committed checkout')
    if receipt.resolve().is_relative_to(cwd):
        raise ValueError('keep validation receipts outside the checkout')
    config = configuration(cwd, env)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()
    begin = time.monotonic()
    code = subprocess.run(command, cwd=cwd, env=env).returncode
    unchanged = (sha == git('rev-parse', 'HEAD') and state == git('status', '--porcelain')
                 and config == configuration(cwd, env))
    result = {'sha': sha, 'toolchain': toolchain, 'started_at': started,
              'seconds': round(time.monotonic() - begin, 3), 'exit': code,
              'configuration_sha256': config,
              'source_unchanged': unchanged, 'passed': code == 0 and unchanged}
    temporary = receipt.with_suffix(receipt.suffix + '.partial')
    temporary.write_text(json.dumps(result, sort_keys=True) + '\n')
    temporary.replace(receipt)
    return 0 if result['passed'] else code or 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--no-rust', action='store_true')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('supply a validation command after --')
    raise SystemExit(execute(command, args.receipt, rust=not args.no_rust))


if __name__ == '__main__':
    main()
