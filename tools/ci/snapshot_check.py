#!/usr/bin/env python3
"""Run one check on a clean commit; refuse stale completion even without a push."""
import subprocess
import sys


def snapshot():
    return (subprocess.check_output(['git', 'rev-parse', 'HEAD']),
            subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all']))


def main():
    command = sys.argv[1:]
    if command[:1] == ['--']:
        command = command[1:]
    before = snapshot()
    if not command or before[1]:
        raise SystemExit('durable validation needs a command and a clean committed checkout')
    result = subprocess.run(command)
    if snapshot() != before:
        raise SystemExit('snapshot changed; validation result is stale')
    raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
