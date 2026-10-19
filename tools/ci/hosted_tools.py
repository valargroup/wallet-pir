#!/usr/bin/env python3
"""Reuse usable hosted prerequisites; install only missing native packages."""
import argparse
import json
import os
import subprocess
import tomllib

import doctor


def missing_native_packages():
    missing = []
    if not doctor.libclang_available():
        missing.append('libclang-dev')
    if not doctor.probe(['protoc', '--version']):
        missing.append('protobuf-compiler')
    return missing


def inspect():
    channel = tomllib.loads((doctor.ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
    tools = [('rustc', ['--version']), ('rustfmt', ['--version']), ('cargo', ['clippy', '--version'])]
    # rustup run does not install a missing toolchain as a side effect.
    ready = all(doctor.probe(['rustup', 'run', channel, tool, *args]) for tool, args in tools)
    return {'toolchain': channel, 'rust_ready': ready, 'missing_native': missing_native_packages()}


def install_native():
    packages = missing_native_packages()
    if not packages:
        return
    subprocess.run(['sudo', 'apt-get', 'update', '-qq'], check=True)
    subprocess.run(['sudo', 'apt-get', 'install', '-y', '-qq', *packages], check=True)
    remaining = missing_native_packages()
    if remaining:
        raise RuntimeError('Native prerequisites remain unusable: ' + ', '.join(remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['probe', 'install-native'])
    args = parser.parse_args(argv)
    if args.action == 'install-native':
        install_native()
        return
    status = inspect()
    print('HOSTED_PREREQUISITES ' + json.dumps(status))
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a') as stream:
            stream.write('toolchain=' + status['toolchain'] + '\n')
            stream.write('rust-ready=' + str(status['rust_ready']).lower() + '\n')
            stream.write('native-ready=' + str(not status['missing_native']).lower() + '\n')


if __name__ == '__main__':
    main()
