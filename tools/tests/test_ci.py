from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
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
cargo_cache = load('cargo_cache')
stage = load('stage')
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
            # Native bundles are only assembled on request, from their own target.
            for kind in release.ON_REQUEST:
                self.assertFalse((root / 'bundles' / f'{kind}.tar.gz').exists())
                target = root / f'target-{kind}'
                (target / 'release').mkdir(parents=True)
                for name in release.BINARIES[kind]:
                    (target / 'release' / name).write_bytes(kind.encode())
                release.assemble(SHA, target, root / f'bundles-{kind}', kind,
                                 release.CUDA_BUILD if kind == 'enhance-pir-native-cuda' else None)
            for kind in release.BINARIES:
                bundles = root / (f'bundles-{kind}' if kind in release.ON_REQUEST else 'bundles')
                release.extract(bundles / f'{kind}.tar.gz', root / kind, SHA, kind)
                expected = kind.encode() if kind in release.ON_REQUEST else b'fake binary'
                for name in release.BINARIES[kind]:
                    self.assertEqual((root / kind / name).read_bytes(), expected)
            native = json.loads((root / 'enhance-pir-native' / 'candidate.json').read_text())
            self.assertEqual(native['protocol_revision'], 'ironwood-enhance-pir-v9-native-two-mask-m29')
            with self.assertRaises(ValueError):
                release.extract(root / 'bundles-enhance-pir-native' / 'enhance-pir-native.tar.gz', root / 'cross', SHA, 'enhance-pir')
            # The Status bundle carries the unit templates its deploy renders, and no candidate claim.
            self.assertEqual(sorted(p.name for p in (root / 'status-pir').iterdir()),
                             ['SHA256SUMS', 'revision', 'status-controller-qualification.service.in',
                              'status-pir', 'status-router.service.in', 'status-worker.service.in'])
            self.assertTrue((root / 'status-pir' / 'status-pir').stat().st_mode & 0o111)
            with self.assertRaises(ValueError):
                release.extract(root / 'bundles-status-pir' / 'status-pir.tar.gz', root / 'cross-status', SHA, 'enhance-pir-native')

    def test_cuda_metadata_and_kind_are_required_before_extraction(self):
        kind = 'enhance-pir-native-cuda'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'target/release').mkdir(parents=True)
            for name in release.BINARIES[kind]:
                (root / 'target/release' / name).write_bytes(b'CUDA binary')
            with self.assertRaises(ValueError):
                release.assemble(SHA, root / 'target', root / 'missing-metadata', kind)
            self.assertFalse((root / 'missing-metadata').exists())
            release.assemble(SHA, root / 'target', root / 'bundles', kind, release.CUDA_BUILD)
            archive = root / 'bundles' / f'{kind}.tar.gz'
            release.extract(archive, root / 'verified', SHA, kind)
            self.assertEqual(json.loads((root / 'verified/build.json').read_text()), release.CUDA_BUILD)
            for cpu_kind in ['enhance-pir', 'enhance-pir-native']:
                with self.assertRaises(ValueError):
                    release.extract(archive, root / cpu_kind, SHA, cpu_kind)
            original = {p.name: p.read_bytes() for p in (root / 'verified').iterdir()}
            mutations = [dict(release.CUDA_BUILD, **{key: value}) for key, value in
                         [('cuda', False), ('cuda', 1), ('cpu_target', 'native'),
                          ('os', 'ubuntu-24.04'), ('glibc', '2.39'), ('rust', 'stable'),
                          ('target', 'aarch64-unknown-linux-gnu')]]
            mutations += [{}, {**release.CUDA_BUILD, 'qualification': 'passed'}]
            for index, metadata in enumerate(mutations):
                with self.subTest(metadata=metadata):
                    data = dict(original)
                    data['build.json'] = json.dumps(metadata).encode()
                    data['SHA256SUMS'] = ''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n'
                                                for k, v in data.items() if k != 'SHA256SUMS').encode()
                    forged_path = root / f'forged-{index}.tar.gz'
                    with tarfile.open(forged_path, 'w:gz') as forged:
                        for name, value in data.items():
                            member = tarfile.TarInfo(name)
                            member.size = len(value)
                            forged.addfile(member, io.BytesIO(value))
                    destination = root / f'forged-{index}'
                    with self.assertRaises(ValueError):
                        release.extract(forged_path, destination, SHA, kind)
                    self.assertFalse(destination.exists())
            for wrong_sha in ['b' * 40, 'short']:
                with self.assertRaises(ValueError):
                    release.extract(archive, root / 'wrong-revision', wrong_sha, kind)
            # The checksum gate also covers CUDA build metadata.
            tampered = dict(original)
            tampered['build.json'] = b'{}'
            with tarfile.open(root / 'tampered.tar.gz', 'w:gz') as forged:
                for name, value in tampered.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(value)
                    forged.addfile(member, io.BytesIO(value))
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                release.extract(root / 'tampered.tar.gz', root / 'tampered', SHA, kind)

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
        self.assertIsNone(timings.percentile95([1, 2, 3]))

    def test_queue_and_execution_are_separate(self):
        run = dict(head_sha=SHA, status='completed', created_at='2026-09-14T00:00:00Z')
        jobs = {'jobs': [dict(name='test', started_at='2026-09-14T00:00:05Z', completed_at='2026-09-14T00:00:25Z', steps=[])]}
        with patch.object(timings, 'api', side_effect=[run, jobs]):
            result = timings.report('org/repo', 1)
        self.assertEqual(result['dispatch_seconds'], 25)
        self.assertEqual(result['jobs'][0]['dispatch_to_job_start_seconds'], 5)
        self.assertEqual(result['jobs'][0]['job_seconds'], 20)


    def test_steps_are_grouped_into_setup_work_post_and_report(self):
        run = dict(head_sha=SHA, status='completed', created_at='2026-09-14T00:00:00Z')
        step = lambda name, start, end: dict(name=name, status='completed', conclusion='success',
                                             started_at=f'2026-09-14T00:00:{start:02}Z', completed_at=f'2026-09-14T00:00:{end:02}Z')
        jobs = {'jobs': [dict(id=5, name='test', started_at='2026-09-14T00:00:05Z', completed_at='2026-09-14T00:00:59Z', steps=[
            step('Set up job', 5, 6), step('Run actions/checkout@v4', 6, 8), step('Run ./.github/actions/rust-setup', 8, 20),
            step('Run python3 tools/ci/full_packages.py --group shared', 20, 50), step('Report Cargo cache reuse', 50, 51),
            step('Post Run ./.github/actions/rust-setup', 51, 58)])]}
        log = '2026-09-14T00:00:50.1Z CI_CACHE_REPORT {"restore": "hit-main", "units": {"rebuilt": 3}}\nother\n'
        with patch.object(timings, 'api', side_effect=[run, jobs]), patch.object(timings, 'job_log', return_value=log) as fetch:
            result = timings.report('org/repo', 1, records=True)
        job = result['jobs'][0]
        self.assertEqual(job['phases'], {'setup_seconds': 15, 'work_seconds': 30, 'post_seconds': 7, 'report_seconds': 1})
        self.assertEqual(job['cache_records'], [{'type': 'CI_CACHE_REPORT', 'restore': 'hit-main', 'units': {'rebuilt': 3}}])
        fetch.assert_called_once_with('org/repo', 5)
        self.assertIn('not only runner queue', result['definitions']['dispatch_to_job_start_seconds'])

    def test_log_redirect_does_not_forward_the_token(self):
        import urllib.request
        handler = timings._DropAuthOnRedirect()
        request = urllib.request.Request('https://api.github.com/x', headers={'Authorization': 'Bearer t'})
        redirected = handler.redirect_request(request, None, 302, 'Found', {}, 'https://storage.example/log')
        self.assertFalse(redirected.has_header('Authorization'))

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


TOOLS = {
    'rustc': 'rustc 1.97.1 (abc 2026-09-01)\nbinary: rustc\ncommit-hash: ' + 'c' * 40 + '\nhost: x86_64-unknown-linux-gnu\nrelease: 1.97.1\nLLVM version: 21.1.0\n',
    'cargo': 'cargo 1.97.1', 'os': 'ubuntu 24.04', 'libc': 'glibc 2.39', 'machine': 'x86_64',
    'native': {'cc': 'cc (Ubuntu 13.3.0) 13.3.0', 'cxx': 'c++ 13.3.0', 'clang': 'clang 18.1.3', 'protoc': 'libprotoc 3.21.12'},
}
ENV = {'HOME': '/nonexistent', 'RUSTFLAGS': '-Dwarnings -C target-cpu=x86-64-v3', 'CFLAGS': '-mpclmul', 'CXXFLAGS': '-mpclmul'}


class CacheIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / '.cargo').mkdir()
        (self.root / '.cargo/config.toml').write_text('[alias]\nck = ["check"]\n')
        (self.root / 'rust-toolchain.toml').write_text('[toolchain]\nchannel = "1.97.1"\n')
        (self.root / 'Cargo.lock').write_text('version = 4\n')

    def tearDown(self):
        self.tmp.cleanup()

    def ids(self, lane='full-test', scope='enhance', tools=TOOLS, env=ENV):
        toolchain, full = cargo_cache.identity(lane, scope, tools, env, self.root)
        record = cargo_cache.sanitized(toolchain, full)
        return record['toolchain'], record['identity']

    def test_checkout_workflow_and_runtime_only_inputs_are_excluded(self):
        base = self.ids()
        noise = {'RUST_TEST_THREADS': '2', 'CARGO_TERM_COLOR': 'never', 'GITHUB_SHA': 'b' * 40,
                 'GITHUB_WORKFLOW_REF': 'org/repo/.github/workflows/other.yml@refs/heads/x',
                 'GITHUB_REF': 'refs/pull/9/merge', 'CARGO_TARGET_DIR': '/elsewhere', 'RUST_BACKTRACE': '1',
                 'QUALIFY_PUBLICATIONS': '1', 'GH_TOKEN': 'not-a-real-token'}
        for key, value in noise.items():
            with self.subTest(key=key):
                self.assertEqual(self.ids(env={**ENV, key: value}), base)
        # Unrelated workspace edits (source, docs, workflows) leave the identity alone.
        (self.root / '.github').mkdir()
        (self.root / '.github/ci.yml').write_text('env: {RUST_TEST_THREADS: "1"}\n')
        (self.root / 'README.md').write_text('changed')
        self.assertEqual(self.ids(), base)

    def test_incompatible_inputs_change_the_identity(self):
        toolchain, identity = self.ids()
        compiler = dict(TOOLS, rustc=TOOLS['rustc'].replace('c' * 40, 'd' * 40))
        changes = {
            'compiler': dict(tools=compiler),
            'os': dict(tools=dict(TOOLS, os='ubuntu 22.04', libc='glibc 2.35')),
            'native compiler': dict(tools=dict(TOOLS, native=dict(TOOLS['native'], cc='cc 14.0'))),
            'rustflags': dict(env=dict(ENV, RUSTFLAGS='-C target-cpu=native')),
            'cflags': dict(env=dict(ENV, CFLAGS='-O3')),
            'target': dict(env=dict(ENV, CARGO_BUILD_TARGET='aarch64-unknown-linux-gnu')),
            'profile env': dict(env=dict(ENV, CARGO_PROFILE_RELEASE_LTO='false')),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                changed_toolchain, changed = self.ids(**change)
                self.assertNotEqual(changed, identity)
                self.assertNotEqual(changed_toolchain, toolchain)
        # Lane, scope (features/profile) and locked dependencies split hosted
        # caches but not persistent toolchain directories, where Cargo decides.
        for name, change in {'lane': dict(lane='full-lint'), 'features': dict(scope='transparent')}.items():
            with self.subTest(name=name):
                changed_toolchain, changed = self.ids(**change)
                self.assertNotEqual(changed, identity)
                self.assertEqual(changed_toolchain, toolchain)
        (self.root / 'Cargo.lock').write_text('version = 4\n[[package]]\nname = "x"\n')
        self.assertEqual(self.ids()[0], toolchain)
        self.assertNotEqual(self.ids()[1], identity)
        (self.root / '.cargo/config.toml').write_text('[build]\nrustflags = ["-C", "target-cpu=native"]\n')
        self.assertNotEqual(self.ids()[0], toolchain)

    def test_unclassified_scope_or_missing_compiler_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'unclassified'):
            self.ids(scope='everything')
        with self.assertRaisesRegex(ValueError, 'compiler'):
            self.ids(tools=dict(TOOLS, rustc=None))

    def test_record_is_sanitized(self):
        secret = 'value-that-must-not-appear'
        toolchain, full = cargo_cache.identity('fast', 'affected', TOOLS, dict(ENV, CARGO_BUILD_RUSTC_WRAPPER=secret), self.root)
        text = json.dumps(cargo_cache.sanitized(toolchain, full))
        self.assertNotIn(secret, text)
        self.assertNotIn(str(self.root), text)
        self.assertIn('CARGO_BUILD_RUSTC_WRAPPER', text)

    def test_restore_status_names_the_source_and_accepts_missing_cache(self):
        status = cargo_cache.restore_status
        pr = 'refs/pull/7/merge'
        self.assertEqual(status('restore', 'false', ['refs/heads/main'], pr), 'miss')
        self.assertEqual(status('restore', '', None, pr), 'miss')
        self.assertEqual(status('restore', 'true', ['refs/heads/main'], pr), 'hit-main')
        self.assertEqual(status('restore', 'true', ['refs/heads/main', pr], pr), 'hit-current-ref')
        # Another PR's entry is not readable; a hit must have come from elsewhere.
        self.assertEqual(status('restore', 'true', ['refs/pull/6/merge'], pr), 'hit-unknown-ref')
        self.assertEqual(status('restore', 'true', None, pr), 'hit-unknown-ref')
        self.assertEqual(status('lookup', 'true', ['refs/heads/main'], 'refs/heads/main'), 'primed')
        self.assertEqual(status('persistent', '', None, 'refs/heads/main', existed=False), 'persistent-new')
        self.assertEqual(status('persistent', '', None, 'refs/heads/main', existed=True), 'persistent-existing')

    def test_fingerprints_classify_reused_rebuilt_and_new_units(self):
        target = self.root / 'target'
        def unit(name, content):
            directory = target / 'release-fast/.fingerprint' / name
            directory.mkdir(parents=True, exist_ok=True)
            (directory / 'lib').write_text(content)
        unit('serde-0123456789abcdef', 'a')
        unit('rocksdb-0123456789abcdef', 'a')
        unit('enhance-pir-0123456789abcdef', 'a')
        before = cargo_cache.snapshot(target)
        os.utime(target / 'release-fast/.fingerprint/enhance-pir-0123456789abcdef/lib', ns=(1, 1))
        unit('pir-control-fedcba9876543210', 'b')
        unit('tokio-fedcba9876543210', 'b')
        result = cargo_cache.classify(before, cargo_cache.snapshot(target), {'enhance-pir', 'pir-control'})
        self.assertEqual(result['units'], {'restored': 3, 'reused': 2, 'rebuilt': 1, 'new': 2, 'missing_at_end': 0})
        self.assertEqual(result['third_party'], {'rebuilt': 0, 'new': 1})
        self.assertEqual(result['workspace_names'], {'rebuilt': ['enhance-pir'], 'new': ['pir-control']})
        empty = cargo_cache.classify({}, {}, set())
        self.assertEqual(empty['units']['restored'], 0)

    def test_nested_stages_are_counted_once(self):
        records = [{'stage': 'integration:enhance-fast', 'seconds': 10, 'exit': 0, 'id': 'a', 'parent': None},
                   {'stage': 'compile', 'seconds': 6, 'exit': 0, 'id': 'b', 'parent': 'a'},
                   {'stage': 'tests', 'seconds': 3, 'exit': 1, 'id': 'c', 'parent': 'a'},
                   {'stage': 'lint:enhance', 'seconds': 4, 'exit': 0, 'id': 'd', 'parent': None}]
        result = cargo_cache.phases(records)
        self.assertEqual((result['compilation_seconds'], result['execution_seconds']), (10, 3))
        self.assertEqual(result['failed_stages'], ['tests'])

    def test_stage_log_records_parentage(self):
        log = self.root / 'stages.jsonl'
        nested = ['python3', str(ROOT / 'tools/ci/stage.py'), 'tests', '--', 'true']
        with patch.dict('os.environ', {'WALLET_PIR_STAGE_LOG': str(log)}), redirect_stdout(io.StringIO()):
            stage.run(nested, stage='integration')
        child, parent = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertEqual(child['parent'], parent['id'])
        self.assertEqual((child['stage'], parent['stage']), ('tests', 'integration'))


class CacheWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflows = {path.name: path.read_text() for path in (ROOT / '.github/workflows').glob('*.yml')}

    def test_every_setup_names_a_classified_scope(self):
        found = []
        for text in self.workflows.values():
            for block in text.split('uses: ./.github/actions/rust-setup\n')[1:]:
                lines = block.splitlines()[:5]
                lane = next(line.split(': ', 1)[1] for line in lines if line.strip().startswith('lane: '))
                scope = next(line.split(': ', 1)[1] for line in lines if line.strip().startswith('scope: '))
                found.append((lane, scope))
        self.assertGreaterEqual(len(found), 8)
        for lane, scope in found:
            if not lane.startswith('$'):
                self.assertIn((lane, scope), cargo_cache.SCOPES)
        prime = self.workflows['ci-cache.yml']
        for lane, scope in re.findall(r'\{lane: (\S+), scope: (\S+),', prime):
            self.assertIn((lane, scope), cargo_cache.SCOPES)

    def test_trusted_build_pool_and_main_cache_writes_are_main_only(self):
        full = self.workflows['ci-full.yml']
        pools = [line for line in full.splitlines() if 'runs-on' in line and 'WALLET_PIR_BUILD_RUNNER' in line]
        self.assertGreaterEqual(len(pools), 6)
        for line in pools:
            # The release job is main-only through its job condition instead.
            if 'pull_request' in line:
                self.assertIn("github.ref == 'refs/heads/main'", line)
        release = full.split('\n  release:\n', 1)[1].split('\n\n', 1)[0]
        self.assertIn("github.ref == 'refs/heads/main'", release)
        prime = self.workflows['ci-cache.yml']
        self.assertIn("if: github.ref == 'refs/heads/main'", prime)
        self.assertNotIn('pull_request', prime)
        self.assertNotIn('save-if', full + prime)

    def test_primed_identity_matches_pr_full_jobs(self):
        def flags(text):
            env = text.split('\nenv:\n', 1)[1].split('\n\n', 1)[0]
            return {key: value for key, value in re.findall(r'^  (\w+): (.+)$', env, re.M) if key in ('RUSTFLAGS', 'CFLAGS', 'CXXFLAGS')}
        self.assertEqual(flags(self.workflows['ci-cache.yml']), flags(self.workflows['ci-full.yml']))

    def test_cuda_cache_identity_sees_the_build_script_flags(self):
        script = (ROOT / 'tools/ci/build-cuda.sh').read_text()
        job = self.workflows['ci-full.yml'].split('\n  release-cuda:\n', 1)[1]
        self.assertIn("export RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq'", script)
        self.assertIn('      RUSTFLAGS: -C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq\n', job)
        self.assertIn('export CFLAGS=-mpclmul CXXFLAGS=-mpclmul', script)
        self.assertIn('      CFLAGS: -mpclmul\n      CXXFLAGS: -mpclmul\n', job)
        self.assertIn('--lane native-cuda --scope cuda', job)

    def test_priming_compiles_the_same_test_units_without_running_tests(self):
        full = load_full()
        groups = full.inventory()
        for group in groups:
            ran, primed = full.commands(group, groups), full.commands(group, groups, compile_only=True)
            self.assertEqual(ran[0][2:], primed[0][3:])
            self.assertEqual(primed[0][:3], ['cargo', 'test', '--no-run'])
            for command in primed[1:]:
                self.assertIn('--compile-only', command)


def load_full():
    import sys
    sys.path.insert(0, str(ROOT / 'tools/ci'))
    return load('full_packages')


if __name__ == '__main__':
    unittest.main()
