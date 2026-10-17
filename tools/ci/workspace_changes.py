"""Recognize workspace-only manifest/lock edits without invoking Cargo.

Library splits change membership and local lock entries without changing external
dependencies. Replace those global paths with the affected package manifests.
Anything else keeps the planner's conservative whole-workspace selection.
"""
from pathlib import Path
import subprocess
import tomllib


def package_paths(paths, packages, *, base, head, local, root):
    if not base or not {'Cargo.toml', 'Cargo.lock'} & set(paths):
        return paths

    def read(revision, path):
        if revision is None:
            return tomllib.loads((root / path).read_text())
        return tomllib.loads(subprocess.check_output(
            ['git', 'show', f'{revision}:{path}'], cwd=root, stderr=subprocess.DEVNULL).decode())

    try:
        before = read(base, 'Cargo.toml')
        after = read(None if local else head, 'Cargo.toml')
        # Only workspace membership may change globally. Profiles, resolver,
        # inherited dependencies, patches and other build settings stay broad.
        old_members = set(before['workspace'].pop('members'))
        new_members = set(after['workspace'].pop('members'))
        if before != after or old_members - new_members:
            return paths
        manifests = {p['name']: Path(p['manifest_path']).relative_to(root).as_posix()
                     for p in packages}
        old_lock = read(base, 'Cargo.lock')
        new_lock = read(None if local else head, 'Cargo.lock')
        if old_lock.get('version') != new_lock.get('version'):
            return paths

        def entries(lock):
            return {(p['name'], p['version'], p.get('source')): p for p in lock['package']}

        old_entries, new_entries = entries(old_lock), entries(new_lock)
        changed = {key for key in old_entries.keys() | new_entries.keys()
                   if old_entries.get(key) != new_entries.get(key)}
        # A version, source, checksum or transitive dependency change outside
        # this workspace can affect feature unification: retain complete checks.
        if any(source is not None or name not in manifests for name, _, source in changed):
            return paths
        selected = set(paths) - {'Cargo.toml', 'Cargo.lock'}
        selected.update(manifests[name] for name, _, _ in changed)
        for member in new_members - old_members:
            manifest = member.rstrip('/') + '/Cargo.toml'
            if manifest not in manifests.values():
                return paths  # Unknown/globbed membership must not drop coverage.
            selected.add(manifest)
        return sorted(selected)
    except (KeyError, TypeError, ValueError, OSError, subprocess.CalledProcessError):
        return paths
