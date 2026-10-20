"""Backend selections shared by full-package checks and qualification shards."""
from pathlib import Path
import tomllib


def cpu_features(names, root=Path(__file__).resolve().parents[2]):
    """Keep all ordinary lint coverage, excluding the explicit CUDA feature.

    cuda_boundary checks that other features cannot activate CUDA indirectly.
    """
    selected = set(names)
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']
    result = []
    for member in workspace['members']:
        for manifest in root.glob(member + '/Cargo.toml'):
            data = tomllib.loads(manifest.read_text())
            name = data['package']['name']
            if name in selected:
                features = data.get('features', {})
                suppressed = {child.removeprefix('dep:') for children in features.values()
                              for child in children if child.startswith('dep:')}
                implicit = {alias for table in [data, *data.get('target', {}).values()]
                            for kind in ('dependencies', 'build-dependencies')
                            for alias, dependency in table.get(kind, {}).items()
                            if isinstance(dependency, dict) and dependency.get('optional')
                            and alias not in suppressed}
                result.extend(f'{name}/{feature}' for feature in set(features) | implicit
                              if feature != 'cuda')
    return sorted(result)

NATIVE = ['enhance-query-server/native-reinspiring', 'status-query-server/native-reinspiring',
          'enhance-publisher/native-reinspiring', 'status-publisher/native-reinspiring', 'status-publisher/cli',
          'enhance-pir-client/native-reinspiring', 'enhance-pir-load-test/native-reinspiring',
          'native-certificate/native-reinspiring', 'pir-query-server/status',
          'pir-query-server/native-reinspiring']
CUDA = ['enhance-query-server/cuda', 'enhance-query-server/native-reinspiring', 'status-query-server/cuda',
        'status-publisher/cuda', 'pir-query-server/enhance', 'pir-query-server/native-reinspiring',
        'pir-query-server/cuda', 'enhance-pir-load-test/native-reinspiring']
TRANSPARENT = ['transparent-publisher/rocksdb-reader', 'transparent-tools/test-support',
               'transparent-tools/compact-export', 'pir-publisher/transparent',
               'pir-e2e-tests/transparent-tests',
               'transparent-query-server/transparent-history',
               'transparent-query-server/transparent-detail']
