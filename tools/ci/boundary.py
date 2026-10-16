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

Common and node packages are named exactly: wallet-libraries adapters are
intentionally `zakura-*` packages too, and low-level PIR cryptography (ipir-sp,
reinspiring, spiral, hashes, randomness) is wallet-pir's own.
"""
from __future__ import annotations

import argparse
import json
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
    'receiver-directory', 'receiver-pir', 'receiver-pir-server',
    'pir-observability', 'pir-control', 'enhance-pir', 'transparent-blocks',
    'transparent-wallet', 'transparent-wallet-store', 'transparent-shard-server',
}
RECEIVER = {'receiver-directory', 'receiver-pir', 'receiver-pir-server'}
CODEC = 'zakura-receiver-crypto'
# The canonical PIR primitive and protocol packages wallet-libraries owns. A pure
# package may consume them; their own graphs are checked like the package's.
CANONICAL = {
    'zakura-pir-native', 'zakura-transparent-native', 'zakura-transparent-events',
    'zakura-transparent-filter', 'zakura-transparent-shard',
    'zakura-transparent-txid-client',
}
# Preserved sources outside the active product: legacy demos and retained evidence.
EXCLUDED = ('demos/', 'enhance/crates/transparent-spend-pir/', 'evidence/',
            'enhance/evidence/', 'transparent/evidence/', 'receiver/evidence/', 'target/')


def manifests(root=ROOT):
    """Every package manifest in scope, with its direct dependencies of every kind."""
    workspace = tomllib.loads((root / 'Cargo.toml').read_text()).get('workspace', {})
    shared = workspace.get('dependencies', {})
    packages = []
    for path in sorted(root.rglob('Cargo.toml')):
        relative = path.relative_to(root).as_posix()
        if relative.startswith(EXCLUDED) or '/target/' in relative:
            continue
        data = tomllib.loads(path.read_text())
        if 'package' not in data:
            continue
        dependencies = []
        tables = [('', data)] + [(f'target.{t}.', v) for t, v in data.get('target', {}).items()]
        for prefix, table in tables:
            for kind in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                for key, value in table.get(kind, {}).items():
                    if isinstance(value, dict) and value.get('workspace'):
                        value = shared.get(key, {})
                    value = value if isinstance(value, dict) else {'version': value}
                    dependencies.append({'name': value.get('package', key),
                                         'kind': prefix + kind, 'git': value.get('git', ''),
                                         'default-features': value.get('default-features', True),
                                         'features': value.get('features', [])})
        packages.append({'name': data['package']['name'], 'manifest': relative,
                         'dependencies': dependencies})
    return packages


def violations(packages, allowed):
    """Direct edges that cross the boundary, and allowlist entries that are stale."""
    found, problems = set(), []
    for package in packages:
        for dependency in package['dependencies']:
            name = dependency['name']
            if name in COMMON or name in NODE:
                edge = f"{package['name']} -> {name}"
                found.add(edge)
                if edge not in allowed:
                    problems.append(f"{package['manifest']}: {dependency['kind']} on "
                                    f"{'Common' if name in COMMON else 'node'} package {name}")
            if (package['name'] in PURE and WALLET_LIBRARIES in dependency['git']
                    and name not in CANONICAL
                    and not (package['name'] == 'receiver-directory' and name == CODEC
                             and dependency['kind'] == 'dependencies'
                             and dependency.get('default-features') is False
                             and not dependency.get('features'))):
                problems.append(f"{package['manifest']}: pure PIR package has a "
                                f"{dependency['kind']} on wallet-libraries package {name}")
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


def standalone_receiver_metadata():
    """Resolve receiver consumers without the indexer's unified crypto feature.

    Seed from the committed lockfile so compatible versions stay pinned. The
    temporary consumer adds only its own root; it cannot modify repository files.
    Dependencies have already been fetched by the locked workspace metadata check.
    """
    with tempfile.TemporaryDirectory(prefix='receiver-boundary-') as directory:
        root = Path(directory)
        lines = ['[package]', 'name = "receiver-boundary-consumer"',
                 'version = "0.0.0"', 'edition = "2021"', '[workspace]', '[dependencies]']
        for name in sorted(RECEIVER):
            folder = ('receiver/services/' if name.endswith('-server') else 'receiver/crates/') + name
            lines.append(f'{name} = {{ path = {json.dumps(str(ROOT / folder))} }}')
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
        problems += resolved_violations(standalone_receiver_metadata())
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
