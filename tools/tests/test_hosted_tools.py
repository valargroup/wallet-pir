"""Hosted setup must reuse usable tools and fail closed after installation."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import hosted_tools


class HostedToolsTests(unittest.TestCase):
    def test_missing_native_packages_are_selected_independently(self):
        for clang, protoc, expected in [(True, True, []), (False, True, ['libclang-dev']),
                                         (True, False, ['protobuf-compiler']),
                                         (False, False, ['libclang-dev', 'protobuf-compiler'])]:
            with self.subTest(clang=clang, protoc=protoc), \
                    patch.object(hosted_tools.doctor, 'libclang_available', return_value=clang), \
                    patch.object(hosted_tools.doctor, 'probe', return_value=protoc):
                self.assertEqual(hosted_tools.missing_native_packages(), expected)

    def test_pinned_rust_components_must_all_be_usable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'rust-toolchain.toml').write_text('[toolchain]\nchannel="fixture-pinned"\n')
            for missing in [None, 'rustc', 'rustfmt', 'cargo']:
                calls = []
                def usable(command):
                    calls.append(command)
                    return command[3] != missing
                with self.subTest(missing=missing), patch.object(hosted_tools.doctor, 'ROOT', root), \
                        patch.object(hosted_tools.doctor, 'probe', side_effect=usable), \
                        patch.object(hosted_tools, 'missing_native_packages', return_value=[]):
                    status = hosted_tools.inspect()
                    self.assertEqual(status['toolchain'], 'fixture-pinned')
                    self.assertEqual(status['rust_ready'], missing is None)
                    self.assertTrue(all(command[:3] == ['rustup', 'run', 'fixture-pinned'] for command in calls))

    def test_existing_native_tools_do_not_run_package_manager(self):
        with patch.object(hosted_tools, 'missing_native_packages', return_value=[]), \
                patch.object(hosted_tools.subprocess, 'run') as run:
            hosted_tools.install_native()
            run.assert_not_called()

    def test_install_only_missing_package_and_verify_it(self):
        with patch.object(hosted_tools, 'missing_native_packages', side_effect=[['protobuf-compiler'], []]), \
                patch.object(hosted_tools.subprocess, 'run') as run:
            hosted_tools.install_native()
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(commands, [['sudo', 'apt-get', 'update', '-qq'],
                                        ['sudo', 'apt-get', 'install', '-y', '-qq', 'protobuf-compiler']])
            self.assertTrue(all(call.kwargs['check'] for call in run.call_args_list))

    def test_failed_installation_and_unusable_library_are_not_marked_ready(self):
        with patch.object(hosted_tools, 'missing_native_packages', return_value=['libclang-dev']), \
                patch.object(hosted_tools.subprocess, 'run') as run:
            run.side_effect = subprocess.CalledProcessError(1, ['apt-get'])
            with self.assertRaises(subprocess.CalledProcessError):
                hosted_tools.install_native()
            self.assertEqual(run.call_count, 1, 'do not continue after a failed update')
            run.side_effect = None
            with self.assertRaisesRegex(RuntimeError, 'libclang-dev'):
                hosted_tools.install_native()

    def test_action_outputs_report_readiness_and_keep_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'output'
            output.write_text('earlier=retained\n')
            with patch.dict(os.environ, GITHUB_OUTPUT=str(output)), \
                    patch.object(hosted_tools, 'inspect', return_value={
                        'toolchain': 'fixture-pinned', 'rust_ready': True, 'missing_native': []}), \
                    redirect_stdout(io.StringIO()):
                hosted_tools.main(['probe'])
            self.assertEqual(output.read_text(), 'earlier=retained\ntoolchain=fixture-pinned\n'
                             'rust-ready=true\nnative-ready=true\n')


if __name__ == '__main__':
    unittest.main()
