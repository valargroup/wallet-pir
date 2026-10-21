#!/usr/bin/env python3
"""Report the disk an HTTPS History provider root would free under its collection rule.

    transparent-provider-reclaim-scan.py ROOT [RETAINED]

Read-only: it lists and stats files and never opens, writes or deletes one. ROOT is
the provider `directory` (its `active.json`, `staging-<revision>-<ns>/`, `.sources/`
and `objects/`). Retained trees are `active.json`'s tree plus the RETAINED newest other
staging trees by revision and creation time (default 7: three retired predecessors,
one held view and three unused trees, the most the manager's collection keeps). The
owner's actual retired set is in memory, so this is an approximation by revision order.

Every inode is counted once by allocated size. Reachable inodes are files under the
retained trees and under the `.sources/<revision>` directories those trees link;
everything else under the staging trees, `.sources/` and `objects/` is reported as
reclaimable, which is what tree, catalog and link-count-one object collection frees.
"""
import json
import os
import re
import sys

STAGE = re.compile(r'staging-(\d+)-(\d+)')
DIGEST = re.compile('[0-9a-f]{64}')


def files(top):
    """Stat results of regular files below `top`, never following symbolic links."""
    for directory, _, names in os.walk(top, followlinks=False):
        for name in names:
            status = os.lstat(os.path.join(directory, name))
            if not os.path.islink(os.path.join(directory, name)):
                yield status


def scan(root, retained):
    with open(os.path.join(root, 'active.json')) as record:
        active = os.path.realpath(json.load(record)['directory'])
    stages = sorted((name for name in os.listdir(root) if STAGE.fullmatch(name)),
                    key=lambda name: tuple(map(int, STAGE.fullmatch(name).groups())), reverse=True)
    others = [name for name in stages if os.path.realpath(os.path.join(root, name)) != active]
    keep = {active} | {os.path.realpath(os.path.join(root, name)) for name in others[:retained]}
    revisions, reachable = set(), set()
    for tree in keep:
        for name in os.listdir(tree):
            if DIGEST.fullmatch(name) and os.path.islink(os.path.join(tree, name)):
                revisions.add(name)
        reachable.update((s.st_dev, s.st_ino) for s in files(tree))
    for revision in revisions:
        reachable.update((s.st_dev, s.st_ino) for s in files(os.path.join(root, '.sources', revision)))
    allocated = {}
    for top in [os.path.join(root, name) for name in stages] + [os.path.join(root, '.sources'),
                                                                 os.path.join(root, 'objects')]:
        if os.path.isdir(top) and not os.path.islink(top):
            allocated.update(((s.st_dev, s.st_ino), s.st_blocks * 512) for s in files(top))
    return {
        'root': root,
        'active': active,
        'stages': len(stages),
        'retained': sorted(os.path.basename(tree) for tree in keep),
        'retained_catalog_revisions': len(revisions),
        'allocated_bytes': sum(allocated.values()),
        'reachable_bytes': sum(size for inode, size in allocated.items() if inode in reachable),
        'reclaimable_bytes': sum(size for inode, size in allocated.items() if inode not in reachable),
    }


if __name__ == '__main__':
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    print(json.dumps(scan(os.path.realpath(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) == 3 else 7)))
