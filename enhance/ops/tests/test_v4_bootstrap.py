"""Isolated bootstrap contract tests; host/systemd effects are explicitly mocked."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

OPS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('v4_bootstrap', OPS / 'scripts/bootstrap-v4-worker.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SHA = 'a' * 40
FACTS = {'hostname': 'enhance-pir-v4-g02-r1', 'machine': 'x86_64', 'cpus': 4,
         'memory_controller': True, 'memory_total_bytes': 8 * module.GIB,
         'memory_available_bytes': 8 * module.GIB - 256 * module.MIB,
         'swap_total_bytes': 2 * module.GIB, 'disk_free_bytes': 40 * module.GIB,
         'boot_id': 'fixture-boot'}
LIMITS = {'memory_high_bytes': 7 * module.GIB, 'memory_max_bytes': 15 * module.GIB // 2,
          'memory_swap_max_bytes': 2 * module.GIB, 'host_reserve_bytes': 512 * module.MIB}
HEALTH = {'protocol': module.PROTOCOL, 'epoch': 0, 'revision': 0, 'candidate': None,
          'published': [], 'incarnation': 'fixture-process'}


def checksums(bundle):
    data = ''.join(f'{module.sha256(p.read_bytes())}  {p.name}\n' for p in sorted(bundle.iterdir()) if p.name != 'SHA256SUMS').encode()
    (bundle / 'SHA256SUMS').write_bytes(data)
    return module.sha256(data)


def bundle_at(path):
    path.mkdir()
    for name in module.BUNDLE_FILES:
        (path / name).write_bytes(b'fixture')
    (path / 'revision').write_text(SHA + '\n')
    (path / 'candidate.json').write_text(json.dumps({'kind': 'enhance-pir-v4-candidate',
        'source_revision': SHA, 'source_dirty': False, 'schema_version': 11,
        'protocol_revision': module.PROTOCOL, 'qualification': 'unqualified'}))
    header = bytearray(20)
    header[:6] = b'\x7fELF\x02\x01'
    header[18:20] = (62).to_bytes(2, 'little')
    (path / 'enhance-pir-v4').write_bytes(header)
    return checksums(path)


class BootstrapTests(unittest.TestCase):
    def test_clean_linux_candidate_checks_every_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'bundle'
            digest = bundle_at(path)
            identity = module.verify_bundle(path, SHA, digest)
            self.assertEqual(identity['binary_sha256'], module.sha256((path / 'enhance-pir-v4').read_bytes()))
            (path / 'v4-candidate.md').write_text('modified')
            with self.assertRaises(ValueError):
                module.verify_bundle(path, SHA, digest)

    def test_untrusted_recomputed_manifest_dirty_source_and_wrong_binary_rejected(self):
        for field, value in [('qualification', 'passed'), ('source_dirty', True), ('schema_version', 9)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / 'bundle'
                trusted = bundle_at(path)
                metadata = json.loads((path / 'candidate.json').read_text())
                metadata[field] = value
                (path / 'candidate.json').write_text(json.dumps(metadata))
                updated = checksums(path)
                with self.assertRaises(ValueError):
                    module.verify_bundle(path, SHA, trusted)
                with self.assertRaises(ValueError):
                    module.verify_bundle(path, SHA, updated)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'bundle'
            bundle_at(path)
            (path / 'enhance-pir-v4').write_bytes(b'not-a-linux-binary')
            with self.assertRaises(ValueError):
                module.verify_bundle(path, SHA, checksums(path))

    def test_symlink_and_extra_bundle_file_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'bundle'
            digest = bundle_at(path)
            (path / 'extra').write_text('')
            with self.assertRaises(ValueError):
                module.verify_bundle(path, SHA, digest)
            (path / 'extra').unlink()
            (path / 'v4-candidate.md').unlink()
            (path / 'v4-candidate.md').symlink_to(path / 'revision')
            with self.assertRaises(ValueError):
                module.verify_bundle(path, SHA, digest)

    def test_storage_probe_uses_existing_worker_mount_or_nearest_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = root / 'data'
            data.mkdir()
            worker = data / 'worker'
            self.assertEqual(module.storage_filesystem(worker), data)
            worker.mkdir()
            self.assertEqual(module.storage_filesystem(worker), worker)

    def test_measured_host_and_overhead_bound_memory(self):
        module.validate_limits(FACTS, LIMITS)
        module.validate_limits(dict(FACTS, disk_free_bytes=module.MIN_FREE_DISK_BYTES), LIMITS)
        for key, value in [('memory_total_bytes', 8_000_000_000), ('memory_available_bytes', 7 * module.GIB),
                           ('cpus', 8), ('machine', 'aarch64'), ('memory_controller', False),
                           ('disk_free_bytes', module.MIN_FREE_DISK_BYTES - 1), ('swap_total_bytes', 0)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                module.validate_limits(dict(FACTS, **{key: value}), LIMITS)
        for key, value in [('memory_max_bytes', 8 * module.GIB), ('host_reserve_bytes', 0),
                           ('memory_high_bytes', 6 * module.GIB), ('memory_swap_max_bytes', -1)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                module.validate_limits(FACTS, dict(LIMITS, **{key: value}))

    def test_unit_binds_only_private_origin_and_dedicated_identity(self):
        unit = module.unit('b' * 64, '10.0.0.4', LIMITS).decode()
        self.assertIn('User=enhance-pir-v4\n', unit)
        self.assertIn('--listen 10.0.0.4:8291 --data-dir /srv/enhance-pir-v4/worker', unit)
        self.assertIn(f'MemoryHigh={7 * module.GIB}\n', unit)
        self.assertIn('ReadWritePaths=/srv/enhance-pir-v4/worker\n', unit)
        with self.assertRaises(ValueError):
            module.unit('b' * 64, '10.0.0.4\nUser=root', LIMITS)

    def test_install_and_retry_are_bound_to_same_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'host'
            (root / 'etc/systemd/system').mkdir(parents=True)
            bundle = Path(temp) / 'bundle'
            digest = bundle_at(bundle)
            with patch.object(module, 'service_user', return_value=SimpleNamespace(pw_uid=123, pw_gid=123)), \
                    patch.object(module.os, 'chown'), patch.object(module, 'run', return_value='') as commands, \
                    patch.object(module, 'idle_health', return_value=HEALTH), \
                    patch.object(module, 'verify_service', return_value={'main_pid': 123}):
                result = module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                self.assertEqual(result['phase'], 'bootstrapped')
                self.assertEqual(result['qualification'], 'unqualified')
                receipt = root / 'srv/enhance-pir-v4/bootstrap.json'
                self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)
                commands.reset_mock()
                module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                commands.assert_not_called()
                self.assertFalse(any('restart' in call.args[0] for call in commands.call_args_list))
                changed = dict(LIMITS, memory_max_bytes=LIMITS['memory_max_bytes'] - 64 * module.MIB)
                commands.reset_mock()
                with self.assertRaisesRegex(ValueError, 'identity changed'):
                    module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', changed, FACTS, root)
                commands.assert_not_called()

    def test_assigned_state_cannot_be_bootstrapped_again(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'host'
            (root / 'etc/systemd/system').mkdir(parents=True)
            bundle = Path(temp) / 'bundle'
            digest = bundle_at(bundle)
            with patch.object(module, 'service_user', return_value=SimpleNamespace(pw_uid=123, pw_gid=123)), \
                    patch.object(module.os, 'chown'), patch.object(module, 'run', return_value='') as commands, \
                    patch.object(module, 'idle_health', return_value=HEALTH), \
                    patch.object(module, 'verify_service', return_value={'main_pid': 123}):
                module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                (root / 'srv/enhance-pir-v4/worker/worker-v4.json').write_text(json.dumps({'epoch': 1, 'revision': 1, 'published': {}, 'candidate': None}))
                commands.reset_mock()
                with self.assertRaisesRegex(ValueError, 'assigned worker'):
                    module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                commands.assert_not_called()

    def test_partial_start_failure_stops_owned_service_and_records_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'host'
            (root / 'etc/systemd/system').mkdir(parents=True)
            bundle = Path(temp) / 'bundle'
            digest = bundle_at(bundle)
            def run(args):
                if 'enable' in args:
                    raise RuntimeError('injected partial start')
                return ''
            with patch.object(module, 'service_user', return_value=SimpleNamespace(pw_uid=123, pw_gid=123)), \
                    patch.object(module.os, 'chown'), patch.object(module, 'run', side_effect=run) as commands:
                with self.assertRaises(RuntimeError):
                    module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                commands.assert_any_call(['systemctl', 'stop', module.SERVICE])
                receipt = json.loads((root / 'srv/enhance-pir-v4/bootstrap.json').read_text())
                self.assertEqual(receipt['phase'], 'failed')
                self.assertTrue(receipt['stop_confirmed'])

    def test_effective_kernel_limits_must_match_systemd(self):
        binary = Path('/opt/enhance-pir-v4/releases/fixture/enhance-pir-v4')
        properties = f'MainPID=123\nControlGroup=/system.slice/{module.SERVICE}\nMemoryHigh={LIMITS["memory_high_bytes"]}\nMemoryMax={LIMITS["memory_max_bytes"]}\nMemorySwapMax={LIMITS["memory_swap_max_bytes"]}\n'
        values = {'memory.high': str(LIMITS['memory_high_bytes']), 'memory.max': str(LIMITS['memory_max_bytes']),
                  'memory.swap.max': str(LIMITS['memory_swap_max_bytes'])}
        with patch.object(module, 'run', return_value=properties), patch.object(Path, 'resolve', return_value=binary), \
                patch.object(Path, 'read_text', autospec=True, side_effect=lambda path: values[path.name]):
            module.verify_service(binary, LIMITS)
            values['memory.max'] = 'max'
            with self.assertRaises(ValueError):
                module.verify_service(binary, LIMITS)

    def test_release_bundle_contract_matches_installer(self):
        release_spec = importlib.util.spec_from_file_location('bootstrap_release', OPS.parents[1] / 'tools/ci/release.py')
        release = importlib.util.module_from_spec(release_spec)
        release_spec.loader.exec_module(release)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = root / 'fixture'
            bundle_at(fixture)
            target = root / 'target/release'
            target.mkdir(parents=True)
            for name in release.BINARIES['enhance-pir-v4-candidate']:
                (target / name).write_bytes((fixture / 'enhance-pir-v4').read_bytes())
            with patch.object(release.subprocess, 'check_output', return_value=b''):
                release.assemble(SHA, root / 'target', root / 'bundles', 'enhance-pir-v4-candidate')
            release.extract(root / 'bundles/enhance-pir-v4-candidate.tar.gz', root / 'extracted', SHA, 'enhance-pir-v4-candidate')
            trusted = module.sha256((root / 'extracted/SHA256SUMS').read_bytes())
            module.verify_bundle(root / 'extracted', SHA, trusted)

    @unittest.skipUnless(module.platform.system() == 'Linux' and module.shutil.which('systemd-analyze'),
                         'systemd parser requires the Linux CI host')
    def test_generated_unit_parses_on_linux(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / module.SERVICE
            value = module.unit('b' * 64, '10.0.0.4', LIMITS).decode()
            # Parser validation only: the target binary is not installed on CI.
            value = value.replace('/opt/enhance-pir-v4/releases/' + 'b' * 64 + '/enhance-pir-v4', '/usr/bin/true')
            path.write_text(value)
            result = module.subprocess.run(['systemd-analyze', 'verify', str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_readiness_failure_after_success_does_not_stop_existing_service(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'host'
            (root / 'etc/systemd/system').mkdir(parents=True)
            bundle = Path(temp) / 'bundle'
            digest = bundle_at(bundle)
            with patch.object(module, 'service_user', return_value=SimpleNamespace(pw_uid=123, pw_gid=123)), \
                    patch.object(module.os, 'chown'), patch.object(module, 'run', return_value='') as commands, \
                    patch.object(module, 'idle_health', return_value=HEALTH) as health, \
                    patch.object(module, 'verify_service', return_value={'main_pid': 123}):
                module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                commands.reset_mock()
                health.side_effect = ValueError('worker received an assignment')
                with self.assertRaises(ValueError):
                    module.install(bundle, SHA, digest, FACTS['hostname'], '10.0.0.4', LIMITS, FACTS, root)
                commands.assert_not_called()
                receipt = json.loads((root / 'srv/enhance-pir-v4/bootstrap.json').read_text())
                self.assertEqual(receipt['phase'], 'bootstrapped')
