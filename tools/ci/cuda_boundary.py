"""CUDA dependency ownership and CPU graph checks, shared by CI boundaries."""
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[2]
FORWARDERS = {'enhance-query-server', 'status-query-server', 'status-publisher',
              'pir-publisher', 'pir-query-server'}


def manifest_violations(root=ROOT):
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']
    problems = []
    for member in workspace['members']:
        for manifest in root.glob(member + '/Cargo.toml'):
            data = tomllib.loads(manifest.read_text())
            name = data['package']['name']
            features = data.get('features', {})
            if 'cuda' in features and name not in FORWARDERS | {'pir-cuda'}:
                problems.append(f'{name}: CUDA features require an explicit backend boundary')
            tables = [data, *data.get('target', {}).values()]
            for table in tables:
                for kind in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                    for alias, value in table.get(kind, {}).items():
                        if not isinstance(value, dict):
                            value = {'version': value}
                        if value.get('workspace'):
                            value = {**workspace['dependencies'][alias], **value}
                        dependency = value.get('package', alias)
                        if name != 'pir-cuda' and (dependency == 'cudarc' or 'cuda' in value.get('features', [])):
                            problems.append(f'{name}: CUDA dependencies belong in pir-cuda')
                        if dependency == 'pir-cuda' and (name not in FORWARDERS or not value.get('optional')):
                            problems.append(f'{name}: pir-cuda must be an optional serving backend')
            for feature, children in features.items():
                for child in children:
                    if child == 'cuda' or child.endswith('/cuda'):
                        if feature != 'cuda' or name not in FORWARDERS | {'pir-cuda'}:
                            problems.append(f'{name}/{feature}: only cuda may activate a CUDA feature')
                        elif name != 'pir-cuda' and child.split('/')[0].rstrip('?') not in FORWARDERS | {'pir-cuda'}:
                            problems.append(f'{name}/{feature}: CUDA implementation dependencies belong in pir-cuda')
            if name != 'pir-cuda':
                for folder in ('src', 'tests', 'examples'):
                    for source in (manifest.parent / folder).rglob('*.rs'):
                        if any(reference in source.read_text() for reference in
                               ('simplepir_kernel::cuda::', 'cudarc::')):
                            problems.append(f'{source.relative_to(root)}: concrete CUDA construction belongs in pir-cuda')
    return sorted(set(problems))


def cpu_graph_violations(metadata):
    packages = {package['id']: package['name'] for package in metadata['packages']}
    return sorted(f'CPU build resolves CUDA: {packages[node["id"]]}'
                  for node in metadata['resolve']['nodes']
                  if packages[node['id']] == 'cudarc' or 'cuda' in node.get('features', []))
