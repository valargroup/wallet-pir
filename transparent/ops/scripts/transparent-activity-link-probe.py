#!/usr/bin/env python3
"""Probe the installed publisher mount namespace using an immutable map link."""
import os
from pathlib import Path

ROOT = Path('/srv/transparent-activity/full-v11/publications')
SOURCE = ROOT/'initial/shards.json'


def main():
    if not SOURCE.is_file() or SOURCE.is_symlink():
        raise ValueError('installed sandbox lacks the verified publication map')
    link = ROOT/('.installed-sandbox-link-'+str(os.getpid()))
    try:
        os.link(SOURCE, link, follow_symlinks=False)
        source, target = SOURCE.stat(), link.stat()
        if (source.st_dev, source.st_ino) != (target.st_dev, target.st_ino) or source.st_nlink < 2:
            raise ValueError('installed sandbox hard-link identity differs')
    finally:
        link.unlink(missing_ok=True)
    print('installed publisher sandbox hard-link probe passed')


if __name__ == '__main__':
    main()
