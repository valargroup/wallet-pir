#!/usr/bin/env python3
"""Advise Linux to reclaim clean Cargo output pages before service integration tests."""
import os
from pathlib import Path
import stat


def reclaim(root):
    if not hasattr(os, 'posix_fadvise'):
        return 0
    count = 0
    for directory, _, files in os.walk(root, followlinks=False):
        for name in files:
            path = Path(directory) / name
            if not stat.S_ISREG(path.lstat().st_mode):
                continue
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                count += 1
            finally:
                os.close(fd)
    return count


if __name__ == '__main__':
    # Include this runner's lint/release lanes, which share its cgroup.
    root = Path(os.environ.get('WALLET_PIR_BUILD_CACHE_ROOT')
                or os.environ.get('CARGO_TARGET_DIR', 'target'))
    print(f'Advised reclaim of clean build cache pages in {reclaim(root)} files')
