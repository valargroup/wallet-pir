import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / 'ci' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fast = load('fast')
release = load('release')
timings = load('timings')
SHA = 'a' * 40


def package(name, dependencies=()):
    return {'name': name, 'manifest_path': str(ROOT / 'crates' / name / 'Cargo.toml'),
            'dependencies': [{'name': n} for n in dependencies]}


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.packages = [package('core'), package('client', ['core']), package('server', ['client']), package('other')]

    def test_transitive_reverse_dependencies(self):
        self.assertEqual(fast.select(self.packages, ['crates/core/src/lib.rs']), {'core', 'client', 'server'})
        self.assertEqual(fast.select(self.packages, ['crates/server/src/lib.rs']), {'server'})

    def test_build_and_unknown_changes_broaden(self):
        for path in ['Cargo.lock', '.cargo/config.toml', '.github/actions/rust-setup/action.yml', 'tools/ci/fast.py', 'crates/deleted/Cargo.toml', 'unknown.file']:
            with self.subTest(path=path):
                self.assertEqual(fast.select(self.packages, [path]), {'core', 'client', 'server', 'other'})

    def test_docs_and_ops_do_not_compile_rust(self):
        self.assertEqual(fast.select(self.packages, ['README.md', 'docs/ci.md', 'ops/scripts/example.py']), set())

    def test_deleted_or_renamed_source_selects_both_sides(self):
        self.assertEqual(fast.select(self.packages, ['crates/other/src/old.rs', 'crates/client/src/new.rs']), {'other', 'client', 'server'})


class ReleaseTests(unittest.TestCase):
    def bundle(self, directory, mutate=None):
        data = {name: b'binary' for name in release.BINARIES['transparent-filter']}
        data['transparent-filter-server.service'] = b'unit'
        data['revision'] = (SHA + '\n').encode()
        data['SHA256SUMS'] = ''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n' for k, v in data.items()).encode()
        if mutate:
            mutate(data)
        path = directory / 'bundle.tar.gz'
        with tarfile.open(path, 'w:gz') as archive:
            for name, value in data.items():
                member = tarfile.TarInfo(name)
                member.size = len(value)
                archive.addfile(member, io.BytesIO(value))
        return path

    def test_verified_binary_is_executable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release.extract(self.bundle(root), root / 'artifact', SHA, 'transparent-filter')
            self.assertTrue((root / 'artifact/transparent-filter-server').stat().st_mode & 0o111)

    def test_wrong_sha_tampered_missing_extra_or_path_escape_refused_before_write(self):
        mutations = [lambda d: d.update(revision=b'b' * 40),
                     lambda d: d.update({'transparent-filter-server': b'tampered'}),
                     lambda d: d.pop('transparent-filter-server.service'),
                     lambda d: d.update({'extra': b'x'}),
                     lambda d: d.update({'../outside': b'x'}),
                     lambda d: d.update(SHA256SUMS=d['SHA256SUMS'] + d['SHA256SUMS'])]
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                with self.assertRaises(ValueError):
                    release.extract(self.bundle(root, mutate), root / 'artifact', SHA, 'transparent-filter')
                self.assertFalse((root / 'artifact').exists())

    def test_only_completed_main_full_ci_is_qualified(self):
        run = dict(head_sha=SHA, head_branch='main', event='push', status='completed', conclusion='success', path='.github/workflows/ci-full.yml')
        self.assertTrue(release.qualified(run, SHA))
        for key, value in [('head_sha', 'b'*40), ('head_branch', 'feature'), ('event', 'pull_request'), ('status', 'in_progress'), ('conclusion', 'failure'), ('path', '.github/workflows/ci.yml')]:
            with self.subTest(key=key):
                self.assertFalse(release.qualified({**run, key: value}, SHA))

    def test_missing_and_expired_artifacts_refused(self):
        run = dict(id=1, head_sha=SHA, head_branch='main', event='push', status='completed', conclusion='success', path='.github/workflows/ci-full.yml')
        for artifacts in [[], [{'name': f'transparent-filter-{SHA}', 'expired': True}]]:
            with patch.dict('os.environ', {'GITHUB_REPOSITORY': 'org/repo'}), patch.object(release, 'api', side_effect=[{'workflow_runs': [run]}, {'artifacts': artifacts}]):
                with self.assertRaises(ValueError):
                    release.resolve(SHA, 'transparent-filter')

    def test_all_product_bundles_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'target/release').mkdir(parents=True)
            for name in set(sum(release.BINARIES.values(), [])):
                (root / 'target/release' / name).write_bytes(b'fake binary')
            release.assemble(SHA, root / 'target', root / 'bundles')
            # The native bundle is only assembled on request, from its own target.
            self.assertFalse((root / 'bundles' / 'enhance-pir-native.tar.gz').exists())
            release.assemble(SHA, root / 'target', root / 'bundles-native', 'enhance-pir-native')
            for kind in release.BINARIES:
                bundles = root / ('bundles-native' if kind == 'enhance-pir-native' else 'bundles')
                release.extract(bundles / f'{kind}.tar.gz', root / kind, SHA, kind)
            native = json.loads((root / 'enhance-pir-native' / 'candidate.json').read_text())
            self.assertEqual(native['protocol_revision'], 'ironwood-enhance-pir-v9-native-two-mask-m29')
            with self.assertRaises(ValueError):
                release.extract(root / 'bundles-native' / 'enhance-pir-native.tar.gz', root / 'cross', SHA, 'enhance-pir')

    def test_enhance_candidate_cannot_claim_qualification(self):
        kind = 'enhance-pir'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'target/release').mkdir(parents=True)
            for name in release.BINARIES[kind]:
                (root / 'target/release' / name).write_bytes(b'candidate binary')
            release.assemble(SHA, root / 'target', root / 'bundles', kind)
            archive = root / 'bundles' / f'{kind}.tar.gz'
            release.extract(archive, root / 'verified', SHA, kind)
            metadata = json.loads((root / 'verified/candidate.json').read_text())
            self.assertEqual(metadata['qualification'], 'unqualified')
            self.assertIsInstance(metadata['source_dirty'], bool)
            data = {p.name: p.read_bytes() for p in (root / 'verified').iterdir()}
            metadata['qualification'] = 'passed'
            data['candidate.json'] = json.dumps(metadata).encode()
            data['SHA256SUMS'] = ''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n'
                                        for k, v in data.items() if k != 'SHA256SUMS').encode()
            with tarfile.open(root / 'forged.tar.gz', 'w:gz') as forged:
                for name, value in data.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(value)
                    forged.addfile(member, io.BytesIO(value))
            with self.assertRaises(ValueError):
                release.extract(root / 'forged.tar.gz', root / 'forged', SHA, kind)
            self.assertFalse((root / 'forged').exists())


class TimingTests(unittest.TestCase):
    def test_nearest_rank_p95(self):
        self.assertEqual(timings.percentile95(list(range(1, 21))), 19)
        self.assertIsNone(timings.percentile95([]))

    def test_queue_and_execution_are_separate(self):
        run = dict(head_sha=SHA, status='completed', created_at='2026-09-14T00:00:00Z')
        jobs = {'jobs': [dict(name='test', started_at='2026-09-14T00:00:05Z', completed_at='2026-09-14T00:00:25Z', steps=[])]}
        with patch.object(timings, 'api', side_effect=[run, jobs]):
            result = timings.report('org/repo', 1)
        self.assertEqual(result['dispatch_seconds'], 25)
        self.assertEqual(result['jobs'][0]['dispatch_to_job_start_seconds'], 5)
        self.assertEqual(result['jobs'][0]['job_seconds'], 20)


    def test_rerun_uses_current_attempt_instead_of_original_creation(self):
        run = dict(head_sha=SHA, status='completed', run_attempt=2,
                   created_at='2026-09-14T00:00:00Z', run_started_at='2026-09-15T00:38:14Z')
        jobs = {'jobs': [dict(name='test', started_at='2026-09-15T00:38:17Z', completed_at='2026-09-15T00:38:40Z', steps=[])]}
        with patch.object(timings, 'api', side_effect=[run, jobs]) as api:
            result = timings.report('org/repo', 1)
        self.assertEqual(result['attempt'], 2)
        self.assertEqual(result['dispatch_seconds'], 26)
        self.assertEqual(result['jobs'][0]['dispatch_to_job_start_seconds'], 3)
        api.assert_called_with('repos/org/repo/actions/runs/1/attempts/2/jobs?per_page=100')


if __name__ == '__main__':
    unittest.main()
