#!/usr/bin/env python3
"""Register the two services on the shared CI host using tokens on stdin.

Run as root on the bootstrapped host. Input is a JSON object with fast/build
registration tokens obtained just in time from GitHub. Neither tokens nor config
output are printed or persisted by this script.
"""
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    if os.geteuid() != 0:
        raise SystemExit('run as root on the CI host')
    tokens = json.load(sys.stdin)
    for lane in ('fast', 'build'):
        user = f'wallet-pir-{lane}'
        directory = Path('/home') / user / 'actions-runner'
        if not (directory / '.runner').exists():
            env = dict(os.environ, ACTIONS_RUNNER_INPUT_TOKEN=tokens[lane],
                       PATH=f'/home/{user}/.cargo/bin:' + os.environ['PATH'])
            result = subprocess.run([
                'runuser', '-u', user, '--', './config.sh', '--unattended',
                '--url', 'https://github.com/valargroup/wallet-pir',
                '--name', f'wallet-pir-ci-{lane}', '--labels', f'wallet-pir-{lane}',
                '--work', '_work'], cwd=directory, env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode:
                raise SystemExit(f'{lane} runner registration failed (exit {result.returncode})')
        if not (directory / '.service').exists():
            subprocess.run(['./svc.sh', 'install', user], cwd=directory, check=True)
        service = (directory / '.service').read_text().strip()
        dropin = Path('/etc/systemd/system') / (service + '.d')
        dropin.mkdir(exist_ok=True)
        fast = lane == 'fast'
        (dropin / 'resources.conf').write_text(
            '[Service]\n'
            f'Environment=PATH=/home/{user}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n'
            'Environment=RUSTUP_TOOLCHAIN=1.97.1\n'
            'Environment=CARGO_BUILD_JOBS=4\n'
            f'CPUWeight={1000 if fast else 100}\n'
            f'IOWeight={1000 if fast else 100}\n'
            f'Nice={0 if fast else 10}\n'
            # A soft limit can trap the listener and tests in direct reclaim
            # indefinitely before MemoryMax triggers an OOM kill.
            'MemoryHigh=infinity\n'
            f'MemoryMax={6 if fast else 9}G\n'
            'MemorySwapMax=0\n'
            # Recover the whole runner after OOM and clean up test descendants.
            'OOMPolicy=kill\n'
            'KillMode=control-group\n'
            'TimeoutStopSec=30s\n'
            'Restart=always\n'
            'RestartSec=5s\n'
            'UMask=0077\n'
            'NoNewPrivileges=true\n'
            'PrivateTmp=true\n'
            'ProtectSystem=full\n'
            'ProtectKernelTunables=true\n'
            'ProtectKernelModules=true\n'
            'ProtectControlGroups=true\n')
        subprocess.run(['systemctl', 'daemon-reload'], check=True)
        # Replace persistent set-property overrides, which take precedence over
        # resources.conf on hosts tuned before this configuration was installed.
        subprocess.run(['systemctl', 'set-property', service,
                        'MemoryHigh=infinity'], check=True)
        subprocess.run(['systemctl', 'enable', '--now', service], check=True)
        print(f'{lane}: {service} enabled')


if __name__ == '__main__':
    main()
