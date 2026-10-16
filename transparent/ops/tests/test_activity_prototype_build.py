"""The retained artifact build selects only service-owned Cargo targets."""
import importlib.util
from pathlib import Path
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    'activity_prototype_build', ROOT / 'transparent/ops/scripts/activity-prototype-build.py')
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


def options(command, flag):
    return [command[i + 1] for i, word in enumerate(command[:-1]) if word == flag]


def workspace_packages():
    workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']
    result = {}
    for member in workspace['members']:
        for path in ROOT.glob(member + '/Cargo.toml'):
            manifest = tomllib.loads(path.read_text())
            result[manifest['package']['name']] = (path.parent, manifest)
    return result


def targets(directory, manifest, kind):
    named = {target['name'] for target in manifest.get(kind, [])}
    folder = {'bin': 'src/bin', 'example': 'examples', 'test': 'tests'}[kind]
    named.update(path.stem for path in (directory / folder).glob('*.rs'))
    if kind == 'bin' and (directory / 'src/main.rs').exists():
        named.add(manifest['package']['name'])
    return named


class CommandPlanTests(unittest.TestCase):
    def test_every_selected_package_and_explicit_target_belongs_to_this_workspace(self):
        packages = workspace_packages()
        for profile in ['release-fast', 'release']:
            for command in build.build_commands(profile):
                with self.subTest(profile=profile, command=command):
                    self.assertEqual(command[:1], ['cargo'])
                    self.assertIn('--locked', command)
                    self.assertEqual(options(command, '--profile'), [profile])
                    selected = options(command, '-p')
                    self.assertTrue(selected)
                    self.assertFalse(set(selected) - packages.keys(), selected)
                    for kind in ['bin', 'example', 'test']:
                        for target in options(command, '--' + kind):
                            self.assertTrue(any(target in targets(*packages[name], kind)
                                                for name in selected), (kind, target))

    def test_both_profiles_build_every_retained_artifact_under_its_existing_name(self):
        packages = workspace_packages()
        for profile in ['release-fast', 'release']:
            built = set()
            for command in build.build_commands(profile):
                if command[1] != 'build':
                    continue
                selected = options(command, '-p')
                if '--bins' in command:
                    for package in selected:
                        built.update(targets(*packages[package], 'bin'))
                built.update('examples/' + name for name in options(command, '--example'))
            self.assertFalse(set(build.ARTIFACTS) - built, (profile, built))
            self.assertIn('examples/activity-reopen', built)
            self.assertNotIn('examples/engine-store-reopen', built)

    def test_fast_qualification_exercises_real_service_recovery_and_http_consumers(self):
        commands = build.build_commands('release-fast')
        tests = [command for command in commands if command[1] == 'test']
        selected = {name for command in tests for name in options(command, '--test')}
        self.assertTrue({'recovery', 'wallet_anchor', 'wallet_continuation',
                         'wallet_recut', 'wallet_sync'} <= selected)
        self.assertTrue(any('transparent-regression' in command and '--tests' in command
                            for command in tests))
        self.assertTrue(all(command[1] == 'build' for command in build.build_commands('release')))


if __name__ == '__main__':
    unittest.main()
