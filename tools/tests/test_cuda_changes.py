"""CUDA selection follows backend contracts, not broad service membership."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import cuda_changes as changes
from cuda_boundary import manifest_violations, cpu_graph_violations
from backend_features import cpu_features


class CudaSelectionTests(unittest.TestCase):
    def select(self, paths, before=None, after=None):
        before = before or {}
        after = after or before
        return changes.select(paths, before.__getitem__, after.__getitem__)

    def test_implementation_and_contract_changes_select_cuda(self):
        for path in ('src/shared/pir-cuda/src/lib.rs',
                     'src/enhance/enhance-query-server/src/matvec.rs',
                     'src/status/status-query-server/src/backend.rs',
                     'src/shared/pir-runtime/src/evaluation.rs',
                     'src/pir-query-server/tests/operator.rs',
                     '.github/workflows/ci-cuda.yml', 'rust-toolchain.toml'):
            with self.subTest(path=path):
                self.assertTrue(self.select([path])['cuda'])

    def test_unrelated_products_docs_helpers_and_ci_do_not_select_cuda(self):
        paths = ['docs/enhance/cuda_workers.md', 'src/transparent/transparent-query-server/src/lib.rs',
                 'src/enhance/enhance-query-server/src/router.rs',
                 'tools/ci/fast.py', 'tools/tests/test_iteration.py',
                 '.github/workflows/deploy-transparent.yml']
        self.assertEqual(self.select(paths), {'cuda': False, 'reasons': []})

    def test_lock_selection_uses_transitive_records_not_all_dependencies(self):
        before = {
            'Cargo.toml': '[workspace]\nmembers = ["old"]\nresolver = "2"\n',
            'Cargo.lock': '''version = 4
[[package]]
name = "simplepir-kernel"
version = "1"
dependencies = ["cudarc"]
[[package]]
name = "cudarc"
version = "1"
checksum = "first"
[[package]]
name = "unrelated"
version = "1"
checksum = "first"
'''}
        after = copy.deepcopy(before)
        after['Cargo.lock'] = before['Cargo.lock'].replace('name = "unrelated"\nversion = "1"', 'name = "unrelated"\nversion = "2"')
        after['Cargo.toml'] = before['Cargo.toml'].replace('["old"]', '["old", "new"]')
        self.assertFalse(self.select(['Cargo.toml', 'Cargo.lock'], before, after)['cuda'])
        after['Cargo.lock'] = before['Cargo.lock'].replace('checksum = "first"', 'checksum = "second"', 1)
        self.assertTrue(self.select(['Cargo.lock'], before, after)['cuda'])
        after['Cargo.lock'] = before['Cargo.lock']
        after['Cargo.toml'] += '[profile.release]\nlto = true\n'
        self.assertTrue(self.select(['Cargo.toml'], before, after)['cuda'])

    def test_ambiguous_missing_or_deleted_inputs_require_cuda(self):
        for path in ['Cargo.lock', 'src/status/status-query-server/Cargo.toml']:
            self.assertTrue(self.select([path])['cuda'])
        lock = {'package': [{'name': 'simplepir-kernel', 'version': '1', 'dependencies': ['helper']},
                            {'name': 'helper', 'version': '1'}, {'name': 'helper', 'version': '2'}]}
        with self.assertRaises(ValueError):
            changes.locked_backend(lock)

    def test_feature_forwarding_matters_but_manifest_description_does_not(self):
        path = 'src/pir-query-server/Cargo.toml'
        before = {path: '[package]\ndescription = "old"\n[features]\ncuda = ["server/cuda"]\n'}
        after = {path: before[path].replace('"old"', '"new"')}
        self.assertFalse(self.select([path], before, after)['cuda'])
        after[path] = before[path].replace('["server/cuda"]', '[]')
        self.assertTrue(self.select([path], before, after)['cuda'])

    def test_cpu_workflow_edits_skip_cuda_but_cuda_setup_changes_do_not(self):
        path = '.github/workflows/ci-full.yml'
        before = {path: 'jobs:\n  enhance-lint:\n    timeout-minutes: 30\n  build-cuda:\n    run: build\n'}
        after = {path: before[path].replace('30', '40')}
        self.assertFalse(self.select([path], before, after)['cuda'])
        after[path] = before[path].replace('run: build', 'run: another-build')
        self.assertTrue(self.select([path], before, after)['cuda'])

    def test_deleted_or_renamed_cuda_paths_are_seen_at_both_revisions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
            git('init')
            git('config', 'user.email', 'test@example.invalid')
            git('config', 'user.name', 'Test')
            original = root / changes.BACKEND / 'src/lib.rs'
            original.parent.mkdir(parents=True)
            original.write_text('original')
            git('add', str(original))
            git('commit', '-m', 'Add backend')
            base = git('rev-parse', 'HEAD')
            original.rename(root / 'moved.rs')
            git('add', '-u')
            git('add', 'moved.rs')
            git('commit', '-m', 'Move backend')
            self.assertTrue(changes.comparison(base, root=root)['cuda'])
            self.assertTrue(changes.comparison('f' * 40, root=root)['cuda'])

    def test_repository_cpu_features_and_dependency_ownership(self):
        self.assertEqual(manifest_violations(), [])
        from fast import workspace_packages
        features = cpu_features([p['name'] for p in workspace_packages()])
        self.assertFalse(any(feature.endswith('/cuda') for feature in features))
        self.assertIn('pir-publisher/rocksdb-reader', features)
        self.assertIn('pir-query-server/native-reinspiring', features)

    def test_cpu_graph_rejects_transitive_cuda_activation(self):
        metadata = {'packages': [{'id': 'kernel', 'name': 'simplepir-kernel'}],
                    'resolve': {'nodes': [{'id': 'kernel', 'features': []}]}}
        self.assertEqual(cpu_graph_violations(metadata), [])
        metadata['resolve']['nodes'][0]['features'] = ['cuda']
        self.assertTrue(cpu_graph_violations(metadata))

    def test_manifest_boundary_rejects_aliases_and_target_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'Cargo.toml').write_text('[workspace]\nmembers = ["server"]\n')
            (root / 'server').mkdir()
            manifest = root / 'server/Cargo.toml'
            manifest.write_text('''[package]
name = "enhance-query-server"
version = "0.1.0"
[features]
default = []
cuda = ["pir-cuda/cuda"]
[dependencies]
pir-cuda = { path = "../backend", optional = true }
''')
            self.assertEqual(manifest_violations(root), [])
            manifest.write_text(manifest.read_text().replace('default = []', 'default = ["cuda"]'))
            self.assertTrue(manifest_violations(root))
            manifest.write_text(manifest.read_text() + '''
[target.'cfg(target_os = "linux")'.dependencies]
renamed = { package = "simplepir-kernel", version = "1", features = ["cuda"] }
''')
            self.assertTrue(any('dependencies belong' in p for p in manifest_violations(root)))

    def test_unrelated_registry_changes_do_not_schedule_cuda(self):
        path = 'tools/ci/test-matrix.json'
        registry = {'cpu': {'targets': ['native:server:lib:server']},
                    'cuda': {'targets': ['cuda:server:check:server']}}
        before = {path: json.dumps(registry)}
        registry['cpu']['targets'].append('native:server:test:new')
        after = {path: json.dumps(registry)}
        self.assertFalse(self.select([path], before, after)['cuda'])
        registry['cuda']['targets'].append('cuda:server:test:new')
        self.assertTrue(self.select([path], before, {path: json.dumps(registry)})['cuda'])


if __name__ == '__main__':
    unittest.main()
