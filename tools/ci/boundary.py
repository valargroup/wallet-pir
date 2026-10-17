#!/usr/bin/env python3
"""Dependency boundary between wallet-pir and the Zakura chain and wallet libraries.

wallet-libraries owns the canonical PIR primitive and protocol packages
(`CANONICAL`); wallet-pir owns serving, stores and operations. Dependencies run
one way: wallet-pir may consume wallet-libraries; wallet-libraries never consumes
wallet-pir packages. The receiver framing packages use only the library-owned
wire codec, with cryptography disabled.

No manifest may declare a direct dependency of any kind on a Zakura Common or
node package except the ratcheted allowlist. Pure PIR packages may use only the
canonical PIR packages and the explicit receiver codec adapter from
wallet-libraries. Their normal/build graphs
must contain no Common or node packages. Cargo unifies features in whole-workspace
metadata, where the indexer enables receiver cryptography: the resolved check
therefore also resolves a fresh standalone receiver consumer with crypto disabled.

Serving libraries (`SERVING`) serve prepared publications. No dependency of any
kind, including target-specific ones, may reach a chain reader, an ingestion or
application package (`INGESTION`, or any package under a product's `services/`
or `tools/`), or another product's package. Their workspace graphs (including
their own dev-dependencies) and standalone graphs are resolved and checked too.

Common and node packages are named exactly: wallet-libraries adapters are
intentionally `zakura-*` packages too, and low-level PIR cryptography (ipir-sp,
reinspiring, spiral, hashes, randomness) is wallet-pir's own.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = ROOT / 'tools/ci/boundary-allowlist.json'

# Zakura Common: the forks zakura-core/libraries publishes, and the crates.io
# originals they replace, which would be the same code under another identity.
COMMON = {
    'zakura-address', 'zakura-bellman', 'zakura-bls12-381', 'zakura-equihash',
    'zakura-halo2-gadgets', 'zakura-halo2-legacy-pdqsort', 'zakura-halo2-poseidon',
    'zakura-halo2-proofs', 'zakura-jubjub', 'zakura-keys', 'zakura-note-encryption',
    'zakura-orchard', 'zakura-pairing', 'zakura-pasta-curves', 'zakura-primitives',
    'zakura-proofs', 'zakura-protocol', 'zakura-reddsa', 'zakura-redjubjub',
    'zakura-sapling-crypto', 'zakura-sinsemilla', 'zakura-transparent', 'zakura-zip321',
    'bellman', 'bls12_381', 'equihash', 'halo2_gadgets', 'halo2_legacy_pdqsort',
    'halo2_poseidon', 'halo2_proofs', 'jubjub', 'orchard', 'pairing', 'pasta_curves',
    'reddsa', 'redjubjub', 'sapling-crypto', 'sinsemilla', 'zcash_address', 'zcash_keys',
    'zcash_note_encryption', 'zcash_primitives', 'zcash_proofs', 'zcash_protocol',
    'zcash_transparent', 'zip321',
}
# The Zakura node's crates, from zakura-core/zakura.
NODE = {
    'zakura-chain', 'zakura-consensus', 'zakura-header-chain', 'zakura-mmr-tree',
    'zakura-network', 'zakura-node-services', 'zakura-rpc', 'zakura-script',
    'zakura-state', 'zakura-test',
}
WALLET_LIBRARIES = 'github.com/zakura-core/wallet-libraries'
# Protocol framing packages that must not acquire chain or wallet behaviour.
PURE = {
    'receiver-pir-server',
    'pir-observability', 'pir-control', 'pir-runtime', 'enhance-pir', 'transparent-blocks',
    'transparent-shard-server', 'transparent-server-backend',
    'enhance-serving', 'status-serving',
}
# Serving libraries and the chain reading, ingestion and application packages
# they must never reach. Each product's applications compose its serving library;
# the shared raw PIR runtime they call is held to the same rule.
SERVING = {'enhance-serving', 'status-serving', 'transparent-server-backend', 'pir-runtime'}
INGESTION = {
    'pir-pipeline', 'zakura-server-chain', 'enhance-pir-server', 'receiver-indexer',
    'transparent-filter-server', 'transparent-block-server', 'transparent-measure',
}
PRODUCTS = ('enhance/', 'transparent/', 'receiver/')
SHARED = 'shared/'

# Lifecycle code has no product, chain, wallet or cryptographic dependency.
# The direct surface is deliberately smaller than tokio's feature-unified utility
# graph. Every kind of edge counts, including optional and target-specific edges.
PIPELINE = 'pir-pipeline'
PIPELINE_DIRECT = {'tokio'}
PIPELINE_UTILITIES = {
    'tokio', 'tokio-macros', 'pin-project-lite', 'proc-macro2', 'quote', 'syn',
    'unicode-ident', 'bytes', 'libc', 'mio', 'parking_lot', 'parking_lot_core',
    'lock_api', 'scopeguard', 'smallvec', 'signal-hook-registry', 'socket2',
    'cfg-if', 'errno', 'wasi', 'windows-sys', 'windows-link', 'windows-targets',
    'windows_aarch64_gnullvm', 'windows_aarch64_msvc', 'windows_i686_gnu',
    'windows_i686_gnullvm', 'windows_i686_msvc', 'windows_x86_64_gnu',
    'windows_x86_64_gnullvm', 'windows_x86_64_msvc', 'redox_syscall', 'bitflags',
    'hermit-abi', 'log', 'once_cell', 'tracing', 'tracing-attributes',
    'tracing-core', 'valuable',
}

RECEIVER = {'receiver-pir-server'}
CODEC = 'zakura-receiver-crypto'
# The canonical PIR primitive and protocol packages wallet-libraries owns. A pure
# package may consume them; their own graphs are checked like the package's.
CANONICAL = {
    'zakura-pir-native', 'zakura-transparent-native', 'zakura-transparent-events',
    'zakura-transparent-filter', 'zakura-transparent-shard',
    'zakura-transparent-txid-client', 'zakura-receiver-directory', 'zakura-receiver-pir',
    'zakura-pir-transparent',
}
# Preserved sources outside the active product: legacy demos and retained evidence.
# They are still checked for direct Common and node edges, which includes the
# manually buildable legacy demo workspace; only the pure-package rules skip them.
EXCLUDED = ('demos/', 'enhance/crates/transparent-spend-pir/', 'evidence/',
            'enhance/evidence/', 'transparent/evidence/', 'receiver/evidence/', 'target/')


def serving_target_problem(owner, target):
    """Why a serving library at repository path `owner` may not reach the package at
    repository path `target`, or None. Paths are directories relative to the root."""
    product = next((p for p in PRODUCTS if owner.startswith(p)), None)
    if any(target.startswith(p + kind) for p in PRODUCTS for kind in ('services/', 'tools/')):
        return 'application package'
    if not target.startswith(SHARED) and (product is None or not target.startswith(product)):
        return "another product's package"
    return None


def relative_directory(path, root):
    """`path` relative to `root` as a POSIX directory with a trailing slash, or None
    outside the repository."""
    try:
        return Path(os.path.normpath(path)).relative_to(root).as_posix().rstrip('/') + '/'
    except ValueError:
        return None


def shared_dependencies(path, root):
    """The [workspace.dependencies] of the nearest workspace enclosing `path`."""
    for directory in path.parents:
        manifest = directory / 'Cargo.toml'
        if manifest.exists():
            workspace = tomllib.loads(manifest.read_text()).get('workspace')
            if workspace is not None:
                return workspace.get('dependencies', {})
        if directory == root:
            break
    return {}


def manifests(root=ROOT):
    """Every package manifest, with its direct dependencies of every kind and whether
    it lies in a preserved (EXCLUDED) tree."""
    packages = []
    paths = []
    for directory, children, files in os.walk(root):
        directory = Path(directory)
        # A nested checkout is another source snapshot. Preserved evidence and
        # demos in this checkout still receive the direct dependency checks.
        if directory != root and '.git' in (*children, *files):
            children[:] = []
            continue
        children[:] = sorted(child for child in children if child not in {'.git', 'target'}
                             and not (directory == root and child == '.local-pir'))
        if 'Cargo.toml' in files:
            paths.append(directory / 'Cargo.toml')
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix()
        if relative.startswith('target/') or '/target/' in relative:
            continue
        data = tomllib.loads(path.read_text())
        if 'package' not in data:
            continue
        shared = shared_dependencies(path, root)
        dependencies = []
        tables = [('', data)] + [(f'target.{t}.', v) for t, v in data.get('target', {}).items()]
        for prefix, table in tables:
            for kind in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                for key, value in table.get(kind, {}).items():
                    if isinstance(value, dict) and value.get('workspace'):
                        value = shared.get(key, {})
                    value = value if isinstance(value, dict) else {'version': value}
                    local = value.get('path')
                    if local is not None:
                        local = relative_directory(path.parent / local, root)
                    dependencies.append({'name': value.get('package', key),
                                         'kind': prefix + kind, 'git': value.get('git', ''),
                                         'path': local,
                                         'default-features': value.get('default-features', True),
                                         'features': value.get('features', [])})
        packages.append({'name': data['package']['name'], 'manifest': relative,
                         'dependencies': dependencies,
                         'preserved': relative.startswith(EXCLUDED)})
    return packages


def violations(packages, allowed):
    """Direct edges that cross the boundary, and allowlist entries that are stale."""
    found, problems = set(), []
    for package in packages:
        for dependency in package['dependencies']:
            name = dependency['name']
            if package['name'] == PIPELINE and (
                    name not in PIPELINE_DIRECT or dependency.get('path')
                    or dependency['git']):
                problems.append(f"{package['manifest']}: pipeline has a "
                                f"{dependency['kind']} outside its utility boundary: {name}")
            if name in COMMON or name in NODE:
                edge = f"{package['name']} -> {name}"
                found.add(edge)
                if edge not in allowed:
                    problems.append(f"{package['manifest']}: {dependency['kind']} on "
                                    f"{'Common' if name in COMMON else 'node'} package {name}")
            if (package['name'] in PURE and not package.get('preserved')
                    and WALLET_LIBRARIES in dependency['git']
                    and name not in CANONICAL):
                problems.append(f"{package['manifest']}: pure PIR package has a "
                                f"{dependency['kind']} on wallet-libraries package {name}")
            if package['name'] in SERVING and not package.get('preserved'):
                owner = package['manifest'].rsplit('/', 1)[0] + '/'
                reason = ('chain reading or ingestion package' if name in INGESTION else
                          dependency.get('path') and serving_target_problem(owner, dependency['path']))
                if reason:
                    problems.append(f"{package['manifest']}: serving library has a "
                                    f"{dependency['kind']} on {reason} {name}")
    for edge in sorted(set(allowed) - found):
        problems.append(f'{edge}: allowlisted edge no longer exists; remove it from '
                        f'{ALLOWLIST.relative_to(ROOT)}')
    return problems


def resolved_violations(metadata, *, workspace_unified=False):
    """Check pure normal/build graphs, allowing only the canonical codec adapter.

    Workspace metadata unifies indexer crypto into that adapter. Ignore Common
    reached through that adapter only in this workspace view; the standalone
    consumer below must independently pass without that exemption. Node packages
    and other wallet-library packages are always refused.
    """
    packages = {p['id']: p for p in metadata['packages']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    problems = set()
    for root_id, package in packages.items():
        if package['name'] == PIPELINE and package['source'] is None:
            seen, queue = set(), [root_id]
            while queue:
                current = queue.pop()
                if current in seen:
                    continue
                seen.add(current)
                target = packages[current]
                if current != root_id and (
                        target['name'] not in PIPELINE_UTILITIES
                        or not (target['source'] or '').startswith('registry')):
                    problems.add(f"{PIPELINE}: resolves outside its utility boundary: {target['name']}")
                queue.extend(dep['pkg'] for dep in nodes[current]['deps'])
        if package['name'] in SERVING and package['source'] is None:
            problems.update(serving_resolved_violations(root_id, packages, nodes))
        if package['name'] not in PURE or package['source'] is not None:
            continue
        seen, queue = set(), [(root_id, False)]
        while queue:
            current, through_codec = queue.pop()
            if (current, through_codec) in seen:
                continue
            seen.add((current, through_codec))
            target = packages[current]
            library = WALLET_LIBRARIES in (target['source'] or '')
            codec = package['name'] in RECEIVER and library and target['name'] == CODEC
            through_codec = through_codec or codec
            if current != root_id:
                if (target['name'] in NODE
                        or (library and not codec and target['name'] not in CANONICAL)
                        or (target['name'] in COMMON
                            and not (workspace_unified and through_codec))):
                    problems.add(f"{package['name']}: resolves {target['name']}")
            for dep in nodes[current]['deps']:
                if any(k['kind'] in (None, 'build') for k in dep['dep_kinds']):
                    queue.append((dep['pkg'], through_codec))
    return sorted(problems)


def serving_resolved_violations(root_id, packages, nodes):
    """Packages a serving library's resolved graph reaches but must not: chain
    readers, nodes, ingestion and application packages, or another product's
    packages. Every kind of edge counts; dev-dependencies are resolved only for the
    library itself, so only its own appear."""
    package = packages[root_id]
    owner = relative_directory(Path(package['manifest_path']).parent, ROOT) or ''
    problems, seen, queue = set(), set(), [root_id]
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        seen.add(current)
        target = packages[current]
        if current != root_id:
            local = target['source'] is None and relative_directory(
                Path(target['manifest_path']).parent, ROOT)
            if (target['name'] in INGESTION or target['name'] in NODE
                    or (local and serving_target_problem(owner, local))):
                problems.add(f"{package['name']}: serving library resolves {target['name']}")
        queue.extend(dep['pkg'] for dep in nodes[current]['deps'])
    return problems


def standalone_metadata(names):
    """Resolve a fresh consumer of the named repository packages alone, without
    features other workspace members unify into them (such as the indexer's
    receiver crypto).

    Seed from the committed lockfile so compatible versions stay pinned. The
    temporary consumer adds only its own root; it cannot modify repository files.
    Dependencies have already been fetched by the locked workspace metadata check.
    """
    folders = {p['name']: p['manifest'].rsplit('/', 1)[0]
               for p in manifests() if not p['preserved']}
    with tempfile.TemporaryDirectory(prefix='boundary-') as directory:
        root = Path(directory)
        lines = ['[package]', 'name = "boundary-consumer"',
                 'version = "0.0.0"', 'edition = "2021"', '[workspace]', '[dependencies]']
        for name in sorted(names):
            lines.append(f'{name} = {{ path = {json.dumps(str(ROOT / folders[name]))} }}')
        (root / 'Cargo.toml').write_text('\n'.join(lines) + '\n')
        (root / 'src').mkdir()
        (root / 'src/lib.rs').write_text('')
        (root / 'Cargo.lock').write_bytes((ROOT / 'Cargo.lock').read_bytes())
        return json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=root))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--resolved', action='store_true',
                        help='also check pure packages against cargo metadata')
    parser.add_argument('--offline', action='store_true',
                        help='resolve only already-fetched dependencies without network access')
    args = parser.parse_args(argv)
    allowed = json.loads(ALLOWLIST.read_text())['edges']
    problems = violations(manifests(), allowed)
    if args.resolved:
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version', '1',
             *(['--offline'] if args.offline else [])], cwd=ROOT))
        problems += resolved_violations(metadata, workspace_unified=True)
        problems += resolved_violations(standalone_metadata(RECEIVER))
        for name in sorted(SERVING | {PIPELINE}):
            problems += resolved_violations(standalone_metadata({name}))
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
