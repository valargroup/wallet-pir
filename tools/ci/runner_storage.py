#!/usr/bin/env python3
"""Bound runner caches without deleting current, recent or open generations."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import time

LANES = {'fast', 'full-test', 'full-lint', 'release', 'release-native'}


def open_paths(proc=Path('/proc')):
    """Conservatively retain generations referenced by any readable process."""
    paths = []
    for process in proc.glob('[0-9]*'):
        for name in ('cwd', 'exe'):
            try:
                paths.append(os.readlink(process / name))
            except OSError:
                pass
        try:
            paths.extend((process / 'cmdline').read_bytes().decode(errors='replace').split('\0'))
            for fd in (process / 'fd').iterdir():
                try:
                    paths.append(os.readlink(fd))
                except OSError:
                    pass
        except OSError:
            pass
    return paths


def candidates(root, *, keep=2, age=86400, now=None, references=()):
    now = time.time() if now is None else now
    result = []
    if root.is_symlink():
        raise ValueError('cache root must not be a symlink')
    for lane in sorted(LANES):
        directory = root / lane
        if not directory.is_dir() or directory.is_symlink():
            continue
        generations = [p for p in directory.iterdir() if p.is_dir() and not p.is_symlink()
                       and re.fullmatch('[0-9a-f]{16}', p.name)]
        used = lambda p: (p / '.last-used').stat().st_mtime if (p / '.last-used').is_file() else p.stat().st_mtime
        generations.sort(key=used, reverse=True)
        for path in generations[keep:]:
            if now - used(path) < age or any(str(path) in reference for reference in references):
                continue
            result.append(path)
    return result


def incremental_candidates(root, *, age=86400, now=None, references=()):
    """Reclaim stale compiler sessions while retaining reusable final artifacts."""
    now = time.time() if now is None else now
    result = []
    for lane in sorted(LANES):
        directory = root / lane
        if not directory.is_dir() or directory.is_symlink():
            continue
        for generation in directory.iterdir():
            if (not generation.is_dir() or generation.is_symlink()
                    or not re.fullmatch('[0-9a-f]{16}', generation.name)):
                continue
            for profile in ('debug', 'release', 'release-fast'):
                incremental = generation / profile / 'incremental'
                if (not incremental.is_dir() or incremental.is_symlink()
                        or (generation / profile).is_symlink()):
                    continue
                for crate in incremental.iterdir():
                    if not crate.is_dir() or crate.is_symlink():
                        continue
                    modified = max([crate.stat().st_mtime,
                                    *(child.stat().st_mtime for child in crate.iterdir())])
                    if (now - modified >= age
                            and not any(str(crate) in ref for ref in references)):
                        result.append(crate)
    return sorted(result)


def drop_target_incremental(target):
    """Remove incremental sessions from one Cargo target.

    A rustc killed by the fast runner's memory cap can leave a session that the
    next compile of that crate grows past the cap. Fingerprints and finished
    artifacts stay, so an unchanged crate is still fresh.
    """
    target = Path(target)
    if target.is_symlink() or not target.is_dir():
        raise ValueError('Cargo target must be a real directory')
    removed = []
    for profile in target.iterdir():
        if not profile.is_dir() or profile.is_symlink():
            continue
        incremental = profile / 'incremental'
        if not incremental.is_dir() or incremental.is_symlink():
            continue
        for crate in list(incremental.iterdir()):
            removed.append(f'{profile.name}/incremental/{crate.name}')
            if crate.is_symlink() or not crate.is_dir():
                crate.unlink()
            else:
                shutil.rmtree(crate)
        try:
            incremental.rmdir()
        except OSError:
            pass
    return sorted(removed)


def maintain(root, *, apply=False, keep=2, minimum_free=10 * 1024**3, diagnostics=None):
    root = root.resolve(strict=True)
    with (root / '.maintenance.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        removed = []
        for path in candidates(root, keep=keep, references=open_paths()):
            # Names only; logs never enumerate credentials or cache contents.
            removed.append(str(path.relative_to(root)))
            if apply:
                shutil.rmtree(path)
        incremental = []
        for path in incremental_candidates(root, references=open_paths()):
            incremental.append(str(path.relative_to(root)))
            if apply:
                shutil.rmtree(path)
        logs = []
        if diagnostics and diagnostics.is_dir() and not diagnostics.is_symlink():
            references = open_paths()
            for path in diagnostics.iterdir():
                if (path.is_file() and not path.is_symlink()
                        and re.fullmatch(r'(Runner|Worker)_[0-9-]+-utc\.log', path.name)
                        and time.time() - path.stat().st_mtime > 14 * 86400
                        and not any(str(path) in reference for reference in references)):
                    logs.append(path.name)
                    if apply:
                        path.unlink()
        free = shutil.disk_usage(root).free
    return {'apply': apply, 'generations': removed, 'incremental': incremental,
            'diagnostic_logs': logs, 'free_bytes': free,
            'minimum_free_bytes': minimum_free, 'ready': free >= minimum_free}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path)
    parser.add_argument('--drop-incremental-target', type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--keep', type=int, default=2)
    parser.add_argument('--minimum-free-gib', type=int, default=10)
    parser.add_argument('--diagnostics', type=Path)
    args = parser.parse_args()
    if args.drop_incremental_target:
        if args.root is not None or args.apply or args.diagnostics is not None:
            parser.error('--drop-incremental-target does not combine with cache maintenance')
        print(json.dumps({'incremental': drop_target_incremental(args.drop_incremental_target)}, sort_keys=True))
        return
    if args.root is None:
        parser.error('--root is required')
    if args.keep < 1 or args.minimum_free_gib < 0:
        parser.error('keep must be positive and free space must be nonnegative')
    if args.root.is_symlink():
        parser.error('cache root must not be a symlink')
    args.root.mkdir(parents=True, exist_ok=True)
    result = maintain(args.root, apply=args.apply, keep=args.keep,
                      minimum_free=args.minimum_free_gib * 1024**3, diagnostics=args.diagnostics)
    print(json.dumps(result, sort_keys=True))
    if args.apply and not result['ready']:
        raise SystemExit('insufficient runner disk space; retain active caches and reclaim other storage')


if __name__ == '__main__':
    main()
