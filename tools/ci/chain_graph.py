#!/usr/bin/env python3
"""Verify the chain reader's fresh consumer graphs.

`zakura-server-chain` (src/shared/pir-chain) reads Zakura node data for the
publishers and tools. Fresh consumers, each resolved alone so no other workspace
member unifies features into the reader, prove that:

- decoding and fixture consumers resolve no runtime or HTTP stack;
- the production RPC consumer compiles no node package or RocksDB, while the
  optional RocksDB and fixture configurations may still resolve them;
- each product feature activates only its own adapter, and only `receiver`
  resolves receiver recovery;
- the RocksDB reader's public surface needs no node type.

Only active normal and build edges count for the runtime, node and database
rules: an optional dependency that Cargo resolves but does not enable, and a
dev-dependency, never enter the consumer's build. Each consumer is then
type-checked. Consumers seed the committed lockfile and source patches, so they
resolve the same Common and node identities as the workspace. The query-serving
boundary is tools/ci/boundary.py.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
READER = 'zakura-server-chain'
READER_DIR = ROOT / 'src/shared/pir-chain'
PRODUCTS = ('transparent', 'enhance', 'status', 'receiver')
# The Zakura node's crates and its database.
NODE = {
    'zakura-chain', 'zakura-consensus', 'zakura-header-chain', 'zakura-mmr-tree',
    'zakura-network', 'zakura-node-services', 'zakura-rpc', 'zakura-script',
    'zakura-state', 'zakura-test',
}
DATABASE = {'rocksdb', 'librocksdb-sys'}
# Runtime and HTTP stacks that only the `rpc` client and the HTTP node double add.
TRANSPORT_PACKAGES = {'tokio', 'reqwest', 'axum', 'hyper', 'hyper-util', 'h2', 'tower'}


def active_packages(graph):
    """Names the consumer root reaches through normal or build edges."""
    packages = {package['id']: package for package in graph['packages']}
    nodes = {node['id']: node for node in graph['resolve']['nodes']}
    pending, visited = [graph['resolve']['root']], set()
    while pending:
        package_id = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        for dependency in nodes[package_id]['deps']:
            if any(kind['kind'] != 'dev' for kind in dependency['dep_kinds']):
                pending.append(dependency['pkg'])
    return [packages[package_id]['name'] for package_id in visited]


def rpc_dependency_problems(graph, node_packages=NODE):
    """The active RPC consumer must compile no node packages or RocksDB."""
    return [f'RPC consumer compiles node/database package {name}'
            for name in sorted(set(active_packages(graph)))
            if name in node_packages or name in DATABASE]


def transport_dependency_problems(graph, kind):
    """A consumer without `rpc` must resolve no runtime or HTTP package, transitively."""
    return [f'{kind}: decoding consumer resolves runtime/HTTP package {name}'
            for name in sorted(set(active_packages(graph))) if name in TRANSPORT_PACKAGES]


def product_dependency_problems(graph, product):
    """A fresh product consumer must not activate another adapter or receiver recovery."""
    adapters = [p for p in graph['packages'] if p['name'] == READER]
    if len(adapters) != 1:
        return [f'{product}: expected one chain adapter']
    nodes = {n['id']: n for n in graph['resolve']['nodes']}
    adapter = nodes[adapters[0]['id']]
    active = set(adapter['features']) & set(PRODUCTS)
    errors = []
    if active != ({product} if product else set()):
        errors.append(f'{product}: unexpected active adapters {sorted(active)}')
    if product != 'receiver' and any(p['name'] == 'zakura-receiver-crypto' for p in graph['packages']):
        errors.append(f'{product}: resolves receiver recovery')
    return errors


def local_dependency_problems(graph):
    """The reader is a leaf of this repository: it reaches no other local package."""
    return [f'{READER}: depends on repository package {p["name"]}'
            for p in graph['packages']
            if p['source'] is None and p['name'] not in {READER} and not p['name'].endswith('-consumer')]


ROCKSDB_SOURCE = '''use std::path::PathBuf;
use zakura_server_chain::{Network, transparent::{TransparentBlock, state::{StateError, StateReader}}};
fn main() {
    let opened: Result<StateReader, StateError> = StateReader::open("/nonexistent", Network::Mainnet);
    let _: Option<Result<TransparentBlock, _>> = opened.as_ref().ok().map(|r| r.transparent_block(1));
    match opened {
        Err(StateError::Open { path, reason }) => { let _: (PathBuf, String) = (path, reason); }
        Err(StateError::NotArchive { path, reason }) => { let _: (PathBuf, &'static str) = (path, reason); }
        _ => {}
    }
}
'''

SERVER_SOURCE = '''use zakura_server_chain::{Network, NodeClient, NodeError, receiver::ReceiverBlock, enhance::EnhanceBlock, status::StatusBlock};
use zakura_server_chain::transparent::{CompactBlock, Fee, TransparentBlock};
fn public_results(receiver: ReceiverBlock, enhance: EnhanceBlock, status: StatusBlock, transparent: TransparentBlock, compact: CompactBlock) {
    let _: Vec<[u8; 32]> = receiver.commitments;
    let _: Vec<[u8; 32]> = status.txids;
    let _: Option<u64> = enhance.actions.into_iter().next().and_then(|a| a.fee_zatoshis);
    let _: Option<Fee> = transparent.transactions.first().map(|t| t.fee);
    let _: Option<Vec<u8>> = transparent.transactions.into_iter().next().and_then(|t| t.outputs.into_iter().next()).map(|o| o.script);
    let _: u32 = compact.ironwood_tree_size;
}
fn main() {
    let _: Result<NodeClient, NodeError> = NodeClient::new("http://127.0.0.1:1".into(), None);
    let _ = Network::Mainnet;
    let _ = public_results;
    let _ = zakura_server_chain::testing::Tx::from_bytes;
}
'''

PUBLIC_TYPES = {'transparent': 'TransparentBlock', 'enhance': 'EnhanceBlock',
                'status': 'StatusBlock', 'receiver': 'ReceiverBlock'}


def consumers():
    """(name, reader dependency options, main.rs) for every fresh consumer."""
    result = [
        ('server-production', 'features = ["rpc"]',
         'fn main() { let _ = zakura_server_chain::NodeClient::new("http://127.0.0.1:1".into(), None); }\n'),
        ('server-fixtures', 'default-features = false, features = ["testing"]',
         'fn main() { let _ = zakura_server_chain::testing::Tx::from_bytes; }\n'),
        # The RocksDB reader's public surface: its errors carry only the reader's
        # own types, so a consumer needs no node package to match them.
        ('server-rocksdb', 'features = ["rocksdb"]', ROCKSDB_SOURCE),
        ('server', 'features = ["testing-node", "transparent", "enhance", "status", "receiver"]', SERVER_SOURCE),
    ]
    for product, public_type in PUBLIC_TYPES.items():
        for testing in (False, True):
            features = [product, *(['testing'] if testing else [])]
            source = f'fn main() {{ let _: Option<zakura_server_chain::{product}::{public_type}> = None;'
            if testing:
                source += ' let _ = zakura_server_chain::testing::Tx::from_bytes;'
            result.append(('product-' + product + ('-testing' if testing else ''),
                           f'default-features = false, features = {json.dumps(features)}', source + ' }\n'))
    return result


def checks(kind):
    """Graph rules for a consumer, by name."""
    if kind == 'server-production':
        return [rpc_dependency_problems, lambda g: product_dependency_problems(g, None)]
    if kind == 'server-fixtures':
        return [lambda g: product_dependency_problems(g, None),
                lambda g: transport_dependency_problems(g, kind)]
    if kind.startswith('product-'):
        product = kind.removeprefix('product-').removesuffix('-testing')
        rules = [lambda g: product_dependency_problems(g, product),
                 # Product cores consume decoded values: neither the product
                 # interpretation nor its fixtures may pull a runtime or HTTP stack.
                 lambda g: transport_dependency_problems(g, kind)]
        if not kind.endswith('-testing'):
            rules.append(rpc_dependency_problems)
        return rules
    return []


def patch_lines():
    """The workspace's source patches, with paths made absolute."""
    lines = []
    for source, dependencies in tomllib.loads((ROOT / 'Cargo.toml').read_text()).get('patch', {}).items():
        lines.append(f'[patch.{json.dumps(source)}]')
        for name, specification in dependencies.items():
            specification = dict(specification)
            if 'path' in specification:
                specification['path'] = str((ROOT / specification['path']).resolve())
            lines.append(f'{json.dumps(name)} = {{ ' + ', '.join(
                f'{json.dumps(key)} = {json.dumps(value)}' for key, value in specification.items()) + ' }')
    return lines


def write_consumer(directory, kind, options, source):
    (directory / 'src').mkdir(parents=True)
    (directory / 'Cargo.toml').write_text('\n'.join([
        '[package]', f'name = "{kind}-consumer"', 'version = "0.0.0"', 'edition = "2024"',
        '[workspace]', '[dependencies]',
        f'{READER} = {{ path = {json.dumps(str(READER_DIR))}, {options} }}', *patch_lines()]) + '\n')
    (directory / 'src/main.rs').write_text(source)
    (directory / 'Cargo.lock').write_bytes((ROOT / 'Cargo.lock').read_bytes())


def metadata(manifest, offline):
    return json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--format-version', '1', '--manifest-path', str(manifest),
         *(['--offline'] if offline else [])], cwd=ROOT, text=True))


def consumer_environment(target):
    """The type check proves each consumer's public surface, not lint cleanliness:
    single-product feature subsets of the reader warn (unused imports), and the
    reader is linted with every feature by full_packages.py --lint. Cap lints so a
    CI-wide `-D warnings` does not turn those warnings into graph failures."""
    flags = ' '.join(filter(None, [os.environ.get('RUSTFLAGS', ''), '--cap-lints=warn']))
    return dict(os.environ, CARGO_TARGET_DIR=str(target), RUSTFLAGS=flags)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--offline', action='store_true',
                        help='resolve only already-fetched dependencies without network access')
    parser.add_argument('--no-check', action='store_true',
                        help='check graphs only; skip type-checking each consumer')
    args = parser.parse_args(argv)
    errors = []
    # One shared target directory: consumers differ only in reader features.
    target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'target')) / 'chain-graph'
    with tempfile.TemporaryDirectory(prefix='chain-consumers-') as temporary:
        for kind, options, source in consumers():
            consumer = Path(temporary) / kind
            write_consumer(consumer, kind, options, source)
            fresh = metadata(consumer / 'Cargo.toml', args.offline)
            found = local_dependency_problems(fresh)
            for rule in checks(kind):
                found.extend(rule(fresh))
            errors.extend(found)
            if not found and not args.no_check:
                subprocess.run(['cargo', 'check', '--locked', '--quiet', *(['--offline'] if args.offline else []),
                                '--manifest-path', str(consumer / 'Cargo.toml')],
                               cwd=ROOT, check=True, env=consumer_environment(target))
    if errors:
        raise SystemExit('chain reader graph failed:\n  ' + '\n  '.join(sorted(set(errors))))
    print(f'verified: {len(consumers())} fresh chain reader consumers; decoding is runtime-free, '
          'RPC is node-free and products are isolated')


if __name__ == '__main__':
    main()
