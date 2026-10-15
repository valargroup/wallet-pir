#!/usr/bin/env python3
"""Dependency boundary between wallet-pir and the Zakura chain and wallet libraries.

wallet-pir owns PIR algorithms, protocol framing and serving. Zakura Common
cryptography, node types and wallet behaviour live in the Zakura repositories:
services reach them only through wallet-libraries adapters, and the PIR packages
that wallets consume never reach wallet-libraries at all, so the package graph
stays acyclic although the two repositories depend on each other.

Two checks, both over every manifest in the repository except preserved legacy
and evidence sources:

- No manifest may declare a direct dependency, of any kind (normal, optional,
  target-specific, build or development), on a Zakura Common or node package,
  except the edges in `boundary-allowlist.json`. The allowlist is a ratchet: it
  lists what is still to move, and an entry that no longer exists fails too, so
  removing an edge must also remove its entry.
- A pure PIR package may not declare a dependency of any kind on a
  wallet-libraries package, and with `--resolved` its resolved normal and build
  dependency graph may not contain a Common, node or wallet-libraries package.

Common and node packages are named exactly: wallet-libraries adapters are
intentionally `zakura-*` packages too, and low-level PIR cryptography (ipir-sp,
reinspiring, spiral, hashes, randomness) is wallet-pir's own.
"""
from __future__ import annotations

import argparse
import json
import subprocess
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
# PIR packages that wallets and wallet-libraries consume.
PURE = {
    'receiver-wire', 'receiver-directory', 'receiver-pir', 'receiver-pir-server',
    'pir-observability', 'pir-native', 'pir-control', 'enhance-pir',
    'transparent-events', 'transparent-blocks', 'transparent-filter',
    'transparent-native', 'transparent-shard', 'transparent-txid-client',
    'transparent-wallet', 'transparent-wallet-store', 'transparent-shard-server',
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
                                         'kind': prefix + kind, 'git': value.get('git', '')})
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
            if package['name'] in PURE and WALLET_LIBRARIES in dependency['git']:
                problems.append(f"{package['manifest']}: pure PIR package has a "
                                f"{dependency['kind']} on wallet-libraries package {name}")
    for edge in sorted(set(allowed) - found):
        problems.append(f'{edge}: allowlisted edge no longer exists; remove it from '
                        f'{ALLOWLIST.relative_to(ROOT)}')
    return problems


def resolved_violations(metadata):
    """Common, node or wallet-libraries packages reachable from a pure PIR package
    through normal or build dependencies, the graph its consumers resolve."""
    packages = {p['id']: p for p in metadata['packages']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    problems = []
    for root_id, package in packages.items():
        if package['name'] not in PURE or package['source'] is not None:
            continue
        seen, queue = set(), [root_id]
        while queue:
            current = queue.pop()
            if current in seen:
                continue
            seen.add(current)
            for dep in nodes[current]['deps']:
                if any(k['kind'] in (None, 'build') for k in dep['dep_kinds']):
                    queue.append(dep['pkg'])
        for reached in sorted(seen - {root_id}):
            target = packages[reached]
            if (target['name'] in COMMON or target['name'] in NODE
                    or WALLET_LIBRARIES in (target['source'] or '')):
                problems.append(f"{package['name']}: resolves {target['name']}")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--resolved', action='store_true',
                        help='also check pure packages against cargo metadata')
    args = parser.parse_args(argv)
    allowed = json.loads(ALLOWLIST.read_text())['edges']
    problems = violations(manifests(), allowed)
    if args.resolved:
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version', '1'], cwd=ROOT))
        problems += resolved_violations(metadata)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
