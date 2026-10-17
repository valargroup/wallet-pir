"""Changed inputs must invalidate real archived Cargo outputs."""
import os
import unittest

from workspace_cache_fixture import WorkspaceCacheFixture


class WorkspaceCacheInputTests(WorkspaceCacheFixture):
    def test_symlink_main_rebuilds_after_clone_and_tar(self):
        self.symlink_input('src/main.rs', 'alternate.rs', 'rust-A', 'rust-LINK',
                           'rust-LINK|include-A|build-A')

    def test_symlink_include_rebuilds_after_clone_and_tar(self):
        self.symlink_input('src/included.txt', 'alternate.txt', 'include-A', 'include-LINK',
                           'rust-A|include-LINK|build-A')

    def test_symlink_build_input_rebuilds_after_clone_and_tar(self):
        self.symlink_input('build.txt', 'alternate.txt', 'build-A', 'build-LINK',
                           'rust-A|include-A|build-LINK')

    def test_untracked_include_alias_rebuilds_after_clone_and_tar(self):
        self.untracked_alias('src/included.txt', 'alternate.txt', 'include-LINK',
                             'rust-A|include-LINK|build-A')

    def test_untracked_build_alias_rebuilds_after_clone_and_tar(self):
        self.untracked_alias('build.txt', 'alternate.txt', 'build-LINK',
                             'rust-A|include-A|build-LINK')

    def test_executable_mode_loss_reruns_generator_and_fails_after_clone_and_tar(self):
        generator = self.root / 'generator.sh'
        generator.write_text('#!/bin/sh\nprintf build-A\n')
        generator.chmod(0o755)
        (self.root / 'build.rs').write_text(
            'fn main() { println!("cargo::rerun-if-changed=generator.sh"); '
            'let out = std::process::Command::new("./generator.sh").output().unwrap(); '
            'assert!(out.status.success()); '
            'println!("cargo::rustc-env=BUILT={}", String::from_utf8(out.stdout).unwrap()); }')
        self.commit('executable generator')
        self.baseline()
        self.archive_roundtrip()
        generator.chmod(0o644)
        self.commit('generator loses executable permission')
        self.checkout()
        self.restore()
        self.build(failure='Permission denied')

    def test_source_include_and_build_inputs_recompile_and_change_behavior(self):
        self.baseline()
        changes = [('src/main.rs', 'rust-A', 'rust-B', 'rust-B|include-A|build-A'),
                   ('src/included.txt', 'include-A', 'include-B', 'rust-B|include-B|build-A'),
                   ('build.txt', 'build-A', 'build-B', 'rust-B|include-B|build-B'),
                   ('build.rs', 'BUILT={}', 'BUILT=script-{}', 'rust-B|include-B|script-build-B')]
        for name, before, after, expected in changes:
            with self.subTest(name=name):
                path = self.root / name
                path.write_text(path.read_text().replace(before, after))
                # Deliberately old timestamps must not hide changed bytes.
                os.utime(path, ns=(1, 1))
                self.restore()
                artifacts, behavior = self.build()
                self.assertTrue(any(not entry['fresh'] for entry in artifacts))
                self.assertEqual(behavior, expected)
                self.seal()
                self.git('add', '.')
                self.git('-c', 'user.name=Oracle', '-c', 'user.email=oracle@example.invalid', 'commit', '-qm', name)
                self.checkout()
                self.restore()
                artifacts, behavior = self.build()
                self.assertTrue(all(entry['fresh'] for entry in artifacts), artifacts)
                self.assertEqual(behavior, expected)



if __name__ == '__main__':
    unittest.main()
