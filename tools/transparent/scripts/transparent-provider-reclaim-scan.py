#!/usr/bin/env python3
"""Estimate disk freed by HTTPS History collection on a quiescent provider root.

    transparent-provider-reclaim-scan.py ROOT [RETAINED]

Read-only: reads active.json and lists/stats files without changing them. Retention
is an estimate: keep the active tree and RETAINED newest other staging trees by
mtime (default 7). The real owner keeps three predecessors, every older view held
by a request, any candidate and three unused trees. Its in-memory held views are
unavailable here, so seven is a default estimate, not an upper bound.

Count allocated bytes once per inode. Model removal of unused trees and digest
catalog directories, then completed digest-named cache files with one remaining
link. Copy-fallback roots skip object collection. Partials, foreign cache names
and external hard links cannot contribute reclaimed bytes. Directory blocks and
filesystem snapshots are not included. Run between stages; lock contention or
concurrent changes can defer collection or invalidate the estimate.
"""
from collections import Counter
import json
import os
import re
import stat
import sys

STAGE = re.compile(r'staging-(\d+)-(\d+)')
DIGEST = re.compile('[0-9a-f]{64}')


def files(top):
    """Regular files and metadata below a real directory, without following links."""
    if not os.path.isdir(top) or os.path.islink(top):
        return
    for directory, _, names in os.walk(top, followlinks=False):
        for name in names:
            path = os.path.join(directory, name)
            status = os.lstat(path)
            if stat.S_ISREG(status.st_mode):
                yield path, status


def inode(status):
    return status.st_dev, status.st_ino


def scan(root, retained):
    root = os.path.realpath(root)
    if retained < 0:
        raise ValueError('retained tree count must be nonnegative')
    with open(os.path.join(root, 'active.json')) as record:
        active = os.path.realpath(json.load(record)['directory'])
    stages = sorted((entry.path for entry in os.scandir(root)
                     if STAGE.fullmatch(entry.name) and entry.is_dir(follow_symlinks=False)),
                    key=lambda path: os.stat(path).st_mtime_ns, reverse=True)
    others = [tree for tree in stages if os.path.realpath(tree) != active]
    keep = {active} | set(others[:retained])
    revisions, reachable = set(), set()
    for tree in keep:
        # Match the collector's conservative reference rule, even for malformed links.
        revisions.update(name for name in os.listdir(tree) if DIGEST.fullmatch(name))
        reachable.update(inode(status) for _, status in files(tree))
    catalog = os.path.join(root, '.sources')
    for revision in revisions:
        reachable.update(inode(status) for _, status in files(os.path.join(catalog, revision)))
    removed = Counter()
    for tree in stages:
        if os.path.realpath(tree) not in keep:
            removed.update(inode(status) for _, status in files(tree))
    if os.path.isdir(catalog) and not os.path.islink(catalog):
        for entry in os.scandir(catalog):
            if (DIGEST.fullmatch(entry.name) and entry.name not in revisions
                    and entry.is_dir(follow_symlinks=False)):
                removed.update(inode(status) for _, status in files(entry.path))
    objects = os.path.join(root, 'objects')
    object_collection_skipped = os.lstat(os.path.join(active, 'shards.json')).st_nlink < 2
    if not object_collection_skipped and os.path.isdir(objects) and not os.path.islink(objects):
        for entry in os.scandir(objects):
            status = entry.stat(follow_symlinks=False)
            if (DIGEST.fullmatch(entry.name) and stat.S_ISREG(status.st_mode)
                    and status.st_nlink - removed[inode(status)] == 1):
                removed[inode(status)] += 1
    allocated = {}
    for top in stages + [catalog, objects]:
        allocated.update((inode(status), status) for _, status in files(top))
    reclaimable = sum(status.st_blocks * 512 for key, status in allocated.items()
                      if removed[key] == status.st_nlink)
    reachable_bytes = sum(status.st_blocks * 512 for key, status in allocated.items()
                          if key in reachable)
    allocated_bytes = sum(status.st_blocks * 512 for status in allocated.values())
    return {
        'root': root,
        'active': active,
        'stages': len(stages),
        'retained': sorted(os.path.basename(tree) for tree in keep),
        'retained_catalog_revisions': len(revisions),
        'retention_is_estimate': True,
        'object_collection_skipped': object_collection_skipped,
        'allocated_bytes': allocated_bytes,
        'reachable_bytes': reachable_bytes,
        'preserved_other_bytes': allocated_bytes - reachable_bytes - reclaimable,
        'reclaimable_bytes': reclaimable,
    }


if __name__ == '__main__':
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    print(json.dumps(scan(os.path.realpath(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) == 3 else 7)))
