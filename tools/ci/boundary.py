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
metadata, where the indexer enables receiver cryptography and client fixtures
enable test cryptography: fresh standalone consumers independently enforce
production graphs without those unified features.

Serving libraries (`SERVING`) serve prepared publications. No dependency of any
kind, including target-specific ones, may reach a chain reader, an ingestion or
application package (`INGESTION` or `APPLICATIONS`), or another product's
package, or a tool under `tools/`. Product packages live under `src/<product>/`,
shared libraries under `src/shared/`, and the publisher and query-server hosts at
`src/pir-publisher` and `src/pir-query-server`. Their workspace graphs (including
their own dev-dependencies) and standalone graphs are resolved and checked too.

Product publisher and query cores (`RUNTIME_FREE`) leave the Tokio runtime and
HTTP transport (`TRANSPORT`) to the publisher and query-server hosts. They declare
no transport edge of any kind, and their standalone production graphs with every
feature they define enabled resolve none. Shared foundations the cores use
(`HOST_FEATURES`) may offer host adapters only as optional features; their
default standalone graphs resolve none. `RUNTIME_PENDING` lists the cores that
still own transport while that refactor moves it into the hosts; an entry that is
no longer needed fails the resolved check.

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
    'zakura-orchard', 'zakura-orchard-legacy', 'zakura-pairing', 'zakura-pasta-curves',
    'zakura-primitives', 'zakura-proofs', 'zakura-protocol', 'zakura-reddsa',
    'zakura-reddsa-frost', 'zakura-redjubjub',
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
    'pir-observability', 'pir-control', 'pir-runtime',
    'transparent-shared', 'transparent-query-server',
    'enhance-query-server', 'status-query-server', 'pir-serving',
}
# Serving libraries and the chain reading, ingestion and application packages
# they must never reach. Each product's applications compose its serving library;
# the shared raw PIR runtime they call is held to the same rule.
SERVING = {'enhance-query-server', 'status-query-server', 'transparent-shared', 'pir-runtime',
           'pir-serving', 'pir-cuda'}
INGESTION = {
    'pir-publisher', 'pir-pipeline', 'zakura-server-chain', 'enhance-publisher', 'receiver-indexer',
    'transparent-publisher', 'status-publisher',
    # Optional publisher diagnostics make the tools package node-capable.
    'transparent-tools',
}
PRODUCTS = ('src/enhance/', 'src/status/', 'src/transparent/', 'src/receiver/')
SHARED = 'src/shared/'
# Development and operations tools consume product libraries, never the reverse.
TOOLS = 'tools/'
# Application packages: services, operational dashboards and development tools.
# Serving libraries compose into them, never the reverse.
APPLICATIONS = {
    'pir-publisher', 'enhance-publisher', 'status-publisher', 'pir-query-server', 'pir-apm', 'enhance-pir-load-test', 'pir-monitor',
    'enhance-pir-client', 'enhance-pir-exercise', 'native-certificate', 'transparent-publisher',
    'transparent-query-server', 'transparent-tools', 'receiver-indexer', 'receiver-pir-server',
}
# Shared packages consumed by a subset of products. A serving library of any other
# product reaching one still crosses a product boundary.
SHARED_SCOPES = {
    'src/shared/pir-serving/': ('src/enhance/', 'src/status/'),
}

# Lifecycle code has no product, chain, wallet or cryptographic dependency.
# The direct surface is deliberately smaller than tokio's feature-unified utility
# graph. Every kind of edge counts, including optional and target-specific edges.
# Synchronization is transport and scheduling, never a product or PIR engine.
SYNC_MANAGER = 'pir-sync-manager'
SYNC_DIRECT = {'bytes', 'pir-pipeline', 'reqwest', 'tokio',
               'tempfile', 'rustls', 'tokio-rustls'}
PIPELINE = 'pir-pipeline'
PIPELINE_DIRECT = {'tokio'}
# These chain-free serving foundations also must not import the lifecycle.
# transparent-query-server is an application; its HTTPS manager uses the lifecycle.
PIPELINE_FOUNDATIONS = {'receiver-pir-server', 'pir-control', 'pir-observability'}
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

# Runtime and HTTP transport packages the hosts own.
TRANSPORT = {'tokio', 'tokio-stream', 'tokio-util', 'axum', 'axum-core', 'hyper', 'hyper-util',
             'reqwest', 'tower', 'tower-http', 'h2'}
# Shared foundations and the host-only features that alone may reach TRANSPORT.
HOST_FEATURES = {'pir-observability': {'http'}, 'pir-control': {'admission'}}
RUNTIME_FREE = {
    'pir-runtime', 'pir-serving', 'pir-cuda', 'receiver-store',
    'enhance-publisher', 'enhance-query-server', 'status-publisher', 'status-query-server',
    'transparent-publisher', 'transparent-query-server', 'transparent-shared',
    'receiver-indexer', 'receiver-pir-server',
}
# Temporary exceptions; remove each as its product's transport moves to the hosts.
RUNTIME_PENDING = {
    'enhance-publisher', 'enhance-query-server', 'status-publisher', 'status-query-server',
    'transparent-publisher', 'transparent-query-server', 'transparent-shared',
    'receiver-indexer', 'receiver-pir-server',
}

RECEIVER = {'receiver-pir-server'}
CODEC = 'zakura-receiver-crypto'
# The canonical PIR primitive and protocol packages wallet-libraries owns. A pure
# package may consume them; their own graphs are checked like the package's.
CANONICAL = {
    'zakura-filters', 'zakura-pir-protocol', 'zakura-pir-client',
    'zakura-pir-publisher', 'zakura-pir-query-server', 'zakura-pir-server-core',
    'zakura-receiver-directory', 'zakura-receiver-pir',
}
# Retained evidence outside the active product. It is still checked for direct
# Common and node edges; only the pure-package rules skip it.
EXCLUDED = ('evidence/', 'target/')


def serving_target_problem(owner, target, name=None):
    """Why a serving library at repository path `owner` may not reach the package
    `name` at repository path `target`, or None. Paths are directories relative to
    the root."""
    product = next((p for p in PRODUCTS if owner.startswith(p)), None)
    if name in APPLICATIONS:
        return 'application package'
    if target.startswith(TOOLS):
        return 'tool package'
    if not target.startswith(SHARED) and (product is None or not target.startswith(product)):
        return "another product's package"
    scope = next((s for folder, s in SHARED_SCOPES.items() if target.startswith(folder)), None)
    if scope is not None and not owner.startswith(scope):
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
        # A nested checkout is another source snapshot. Preserved evidence in
        # this checkout still receives the direct dependency checks.
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
                                         'optional': value.get('optional', False),
                                         'default-features': value.get('default-features', True),
                                         'features': value.get('features', [])})
        packages.append({'name': data['package']['name'], 'manifest': relative,
                         'dependencies': dependencies,
                         'features': sorted(data.get('features', {})),
                         'preserved': relative.startswith(EXCLUDED)})
    return packages


def violations(packages, allowed):
    """Direct edges that cross the boundary, and allowlist entries that are stale."""
    found, problems = set(), []
    for package in packages:
        for dependency in package['dependencies']:
            name = dependency['name']
            reason = runtime_edge_problem(package, dependency)
            if reason:
                problems.append(f"{package['manifest']}: {reason}")
            if package['name'] == SYNC_MANAGER:
                local_pipeline = (name == PIPELINE and not dependency['git'] and
                                  dependency.get('path') == 'src/shared/pir-pipeline/')
                registry_utility = (name in SYNC_DIRECT - {PIPELINE} and
                                    not dependency.get('path') and not dependency['git'])
                if not (local_pipeline or registry_utility):
                    problems.append(f"{package['manifest']}: sync manager has a "
                                    f"{dependency['kind']} outside its utility boundary: {name}")
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
            if package['name'] in PIPELINE_FOUNDATIONS and name == PIPELINE:
                problems.append(f"{package['manifest']}: serving foundation has a "
                                f"{dependency['kind']} on ingestion engine {name}")
            if package['name'] in SERVING and not package.get('preserved'):
                owner = package['manifest'].rsplit('/', 1)[0] + '/'
                reason = ('chain reading or ingestion package' if name in INGESTION else
                          dependency.get('path') and serving_target_problem(owner, dependency['path'], name))
                if reason:
                    problems.append(f"{package['manifest']}: serving library has a "
                                    f"{dependency['kind']} on {reason} {name}")
    for edge in sorted(set(allowed) - found):
        problems.append(f'{edge}: allowlisted edge no longer exists; remove it from '
                        f'{ALLOWLIST.relative_to(ROOT)}')
    return problems


def runtime_edge_problem(package, dependency):
    """Why a direct edge gives a runtime-free core or a shared foundation the host
    runtime or transport, or None. Cores may declare no transport edge of any kind;
    foundations only optional production edges, which their host features enable."""
    owner, name = package['name'], dependency['name']
    if name not in TRANSPORT or package.get('preserved'):
        return None
    if owner in RUNTIME_FREE and owner not in RUNTIME_PENDING:
        return f"runtime-free core has a {dependency['kind']} on host transport {name}"
    if (owner in HOST_FEATURES and dependency['kind'].endswith('dependencies')
            and not dependency['kind'].endswith('dev-dependencies')
            and not dependency.get('optional')):
        return f"shared foundation has a non-optional {dependency['kind']} on host transport {name}"
    return None


def runtime_resolved_violations(metadata, name):
    """Host transport packages the standalone consumer `metadata` of the repository
    package `name` reaches through normal or build edges."""
    packages = {p['id']: p for p in metadata['packages']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    roots = [p['id'] for p in metadata['packages'] if p['name'] == name and p['source'] is None]
    reached, seen, pending = set(), set(), list(roots)
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        if packages[current]['name'] in TRANSPORT:
            reached.add(packages[current]['name'])
        pending.extend(dep['pkg'] for dep in nodes[current]['deps']
                       if any(k['kind'] in (None, 'build') for k in dep['dep_kinds']))
    return sorted(reached)


def runtime_graph_violations():
    """Resolve each runtime-free core with every feature it defines, and each shared
    foundation with its defaults, alone; report transport they reach and pending
    exceptions that are no longer needed."""
    features = {p['name']: p['features'] for p in manifests() if not p['preserved']}
    problems = []
    for name in sorted(RUNTIME_FREE | set(HOST_FEATURES)):
        selected = [] if name in HOST_FEATURES else features[name]
        reached = runtime_resolved_violations(standalone_metadata({name}, {name: selected}), name)
        if name in RUNTIME_PENDING:
            if not reached:
                problems.append(f'{name}: no longer reaches host transport; '
                                f'remove it from RUNTIME_PENDING')
        elif reached:
            problems.append(f"{name}: production graph resolves host transport {', '.join(reached)}")
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
        if package['name'] == SYNC_MANAGER and package['source'] is None:
            seen, queue = set(), [root_id]
            while queue:
                current = queue.pop()
                if current in seen:
                    continue
                seen.add(current)
                target = packages[current]
                if current != root_id and (
                        (target['source'] is None and target['name'] != PIPELINE)
                        or target['name'] in COMMON | NODE | CANONICAL
                        or target['name'] in {'ipir-sp', 'inspiring', 'reinspiring',
                                              'simplepir-kernel', 'valar-spiral-rs'}
                        or (target['source'] is not None and
                            not target['source'].startswith('registry'))):
                    problems.add(f"{SYNC_MANAGER}: resolves outside its utility boundary: {target['name']}")
                queue.extend(dep['pkg'] for dep in nodes[current]['deps'])
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
        if package['name'] in PIPELINE_FOUNDATIONS and package['source'] is None:
            seen, queue = set(), [root_id]
            while queue:
                current = queue.pop()
                if current in seen:
                    continue
                seen.add(current)
                if current != root_id and packages[current]['name'] == PIPELINE:
                    problems.add(f"{package['name']}: serving foundation resolves {PIPELINE}")
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
            # Workspace dev-dependencies unify client fixture features into
            # every use of the client. Standalone serving consumers below
            # resolve without those features and receive no exemption.
            client_fixture = (library and target['name'] == 'zakura-pir-client'
                              and 'testing' in nodes[current].get('features', []))
            through_codec = through_codec or codec or client_fixture
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
                    or (local and serving_target_problem(owner, local, target['name']))):
                problems.add(f"{package['name']}: serving library resolves {target['name']}")
        queue.extend(dep['pkg'] for dep in nodes[current]['deps'])
    return problems


DATABASE = {'zakura-state', 'rocksdb', 'librocksdb-sys'}
DATABASE_FREE = {
    'transparent-publisher': DATABASE,
    'transparent-tools': DATABASE | {'zakura-server-chain', 'transparent-publisher'},
}


def database_reader_violations(metadata):
    """The default publisher may read node RPCs, but must not build a database reader;
    default tools builds resolve neither."""
    packages = {p['id']: p for p in metadata['packages']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    problems = set()
    for root in metadata['packages']:
        if root['name'] not in DATABASE_FREE or root['source'] is not None:
            continue
        seen, pending = set(), [root['id']]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            name = packages[current]['name']
            if name in DATABASE_FREE[root['name']]:
                problems.add(f"{root['name']}: default build resolves {name}")
            pending.extend(dep['pkg'] for dep in nodes[current]['deps']
                           if any(k['kind'] in (None, 'build') for k in dep['dep_kinds']))
    return sorted(problems)


def standalone_metadata(names, features=None):
    """Resolve a fresh consumer of the named repository packages alone, without
    features other workspace members unify into them (such as the indexer's
    receiver crypto). `features` optionally maps a package to the features the
    consumer enables on it.

    Seed from the committed lockfile and source overrides so dependencies retain
    the workspace's pinned identities. The
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
            selected = (features or {}).get(name, [])
            lines.append(f'{name} = {{ path = {json.dumps(str(ROOT / folders[name]))}, '
                         f'features = {json.dumps(selected)} }}')
        patches = tomllib.loads((ROOT / 'Cargo.toml').read_text()).get('patch', {})
        for source, dependencies in patches.items():
            lines.append(f'[patch.{json.dumps(source)}]')
            for name, specification in dependencies.items():
                if isinstance(specification, dict):
                    specification = dict(specification)
                    if 'path' in specification:
                        specification['path'] = str((ROOT / specification['path']).resolve())
                    value = '{ ' + ', '.join(
                        f'{json.dumps(key)} = {json.dumps(value)}'
                        for key, value in specification.items()) + ' }'
                else:
                    value = json.dumps(specification)
                lines.append(f'{json.dumps(name)} = {value}')
        (root / 'Cargo.toml').write_text('\n'.join(lines) + '\n')
        (root / 'src').mkdir()
        (root / 'src/lib.rs').write_text('')
        (root / 'Cargo.lock').write_bytes((ROOT / 'Cargo.lock').read_bytes())
        return json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=root))


def common_family_violations(packages, patches):
    """Reject stale Common releases and mixed sources, including unused lock entries.

    Common's protocol patch selects the family revision. Every Common patch and
    resolved package must use it, so updating a direct dependency cannot leave an
    older copy hidden behind a transitive or optional dependency.
    """
    names = {name for name in COMMON if name.startswith('zakura-')}
    patches = {name: patch for name, patch in patches.items() if name in names}
    selected = patches['zakura-protocol']
    expected = f"git+{selected['git']}?rev={selected['rev']}#{selected['rev']}"
    problems = []
    for name, patch in patches.items():
        if patch.get('git') != selected['git'] or patch.get('rev') != selected['rev']:
            problems.append(f'{name}: Common patch must use the same revision as zakura-protocol')
    for package in packages:
        if package['name'] not in names:
            continue
        if package['version'].split('.')[0] != '4' or package.get('source') != expected:
            problems.append(f"{package['name']} {package['version']}: Common must use the selected v4 main revision")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--resolved', action='store_true',
                        help='also check pure packages against cargo metadata')
    parser.add_argument('--offline', action='store_true',
                        help='resolve only already-fetched dependencies without network access')
    args = parser.parse_args(argv)
    allowed = json.loads(ALLOWLIST.read_text())['edges']
    from cuda_boundary import manifest_violations, cpu_graph_violations
    problems = violations(manifests(), allowed) + manifest_violations()
    patches = tomllib.loads((ROOT / 'Cargo.toml').read_text())['patch']['crates-io']
    # The historical frozen-client fixture is an independent published client;
    # these are the two workspaces used by the current services and sizing tool.
    for name in ('Cargo.lock', 'tools/txid/txid-sizing/export/Cargo.lock'):
        packages = tomllib.loads((ROOT / name).read_text())['package']
        problems += [f'{name}: {problem}'
                     for problem in common_family_violations(packages, patches)]
    if args.resolved:
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version', '1',
             *(['--offline'] if args.offline else [])], cwd=ROOT))
        problems += cpu_graph_violations(metadata)
        from backend_features import cpu_features
        features = cpu_features([p['name'] for p in metadata['packages'] if p['source'] is None])
        cpu_metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version', '1', '--features', ','.join(features),
             *(['--offline'] if args.offline else [])], cwd=ROOT))
        problems += cpu_graph_violations(cpu_metadata)
        problems += common_family_violations(metadata['packages'], patches)
        problems += resolved_violations(metadata, workspace_unified=True)
        problems += resolved_violations(standalone_metadata(RECEIVER))
        for name in ('enhance-query-server', 'status-query-server', 'pir-query-server'):
            problems += cpu_graph_violations(standalone_metadata({name}))
        problems += database_reader_violations(standalone_metadata(set(DATABASE_FREE)))
        for name in sorted(SERVING | PIPELINE_FOUNDATIONS | {PIPELINE, SYNC_MANAGER}):
            problems += resolved_violations(standalone_metadata({name}))
        problems += runtime_graph_violations()
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
