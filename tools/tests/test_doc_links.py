"""Exercise link checks on a working tree with relocations and cached deletions."""
from pathlib import Path
import hashlib
import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

CHECKER = Path(__file__).resolve().parents[1] / "check-doc-links.sh"


class DocumentationLinks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        (self.root / "tools").mkdir()
        shutil.copy2(CHECKER, self.root / "tools/check-doc-links.sh")
        shutil.copy2(CHECKER.with_suffix(".py"), self.root / "tools/check-doc-links.py")

    def write(self, path, content):
        p = self.root / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)

    def check(self):
        return subprocess.run(["bash", "tools/check-doc-links.sh"], cwd=self.root,
                              capture_output=True, text=True)

    def test_moved_untracked_evidence_and_cached_deletion(self):
        self.write("docs/old.md", "[gone](missing.txt)\n")
        subprocess.run(["git", "add", "docs/old.md"], cwd=self.root, check=True)
        (self.root / "docs/old.md").unlink()
        self.write("transparent/evidence/run/README.md", "# Results\n[data](raw.json)\n")
        self.write("transparent/evidence/run/raw.json", "{}\n")
        self.write("docs/README.md", "[result](../transparent/evidence/run/README.md#results)\n")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("2 files", result.stdout)

    def test_missing_evidence_is_rejected(self):
        self.write("evidence/README.md", "[data](absent.json)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing target", result.stdout)

    def test_missing_heading_is_rejected(self):
        self.write("docs/README.md", "# Present\n[gate](#absent)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no heading", result.stdout)

    def test_spaces_titles_multiple_links_and_repeated_anchors(self):
        self.write("docs/spaced file.md", "# Some `Heading`!\n")
        self.write("docs/README.md", '[a](<spaced file.md#some-heading>) [b](spaced file.md#some-heading "title")\n')
        self.assertEqual(self.check().returncode, 0)
        self.write("docs/README.md", "[a](spaced file.md#some-heading) [bad](spaced file.md#absent)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("1 broken link", result.stderr)

    def test_historical_oracles_require_exact_recorded_source_and_bytes(self):
        spec = importlib.util.spec_from_file_location("doc_links", CHECKER.with_suffix(".py"))
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        source = "crates/oracle.py"
        self.write(source, "# independent oracle\n")
        subprocess.run(["git", "add", source], cwd=self.root, check=True)
        subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "-c", "commit.gpgsign=false", "commit", "-qm", "Capture oracle"],
                       cwd=self.root, check=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        digest = hashlib.sha256((self.root / source).read_bytes()).hexdigest()
        (self.root / source).unlink()
        document = "evidence/run/README.md"
        self.write(document, "[oracle](../../crates/oracle.py)\n")
        self.write("evidence/run/results.md", f"Tested `{revision}`\n")
        manifest = {"sources": {"source_sha256": {source: digest}}}
        self.write("evidence/run/manifest.json", json.dumps(manifest))
        policy = {document: (revision, frozenset({source}))}
        subprocess.run(['git', 'remote', 'add', 'origin', self.root.as_uri()], cwd=self.root, check=True)
        with patch.object(checker, 'ROOT', self.root), patch.object(checker, 'HISTORICAL_SOURCES', policy), \
                patch.object(checker, 'HISTORICAL_REPOSITORIES', frozenset()):
            def resolves(doc=document, target=source):
                return checker.historical_source_exists(doc, (self.root / target).resolve())
            self.assertTrue(resolves())
            self.assertFalse(resolves(doc="evidence/another/README.md"))
            self.assertFalse(resolves(target="crates/unrecorded.py"))
            self.write("evidence/run/results.md", "Tested a different revision\n")
            self.assertFalse(resolves())
            self.write("evidence/run/results.md", f"Tested `{revision}`\n")
            manifest['sources']['source_sha256'][source] = '0' * 64
            self.write("evidence/run/manifest.json", json.dumps(manifest))
            self.assertFalse(resolves())
            manifest['sources']['source_sha256'][source] = digest
            self.write("evidence/run/manifest.json", json.dumps(manifest))
            policy[document] = ('0' * 40, frozenset({source}))
            self.write("evidence/run/results.md", f"Tested `{'0' * 40}`\n")
            self.assertFalse(resolves())  # a recorded label without its Git blob is insufficient

    def test_shallow_checkout_fetches_only_the_pinned_capture(self):
        spec = importlib.util.spec_from_file_location("doc_links_shallow", CHECKER.with_suffix(".py"))
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        source = "crates/oracle.json"
        self.write(source, '{"answer": 42}\n')
        commit = ["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                  "-c", "commit.gpgsign=false", "commit", "-qm"]
        subprocess.run(["git", "add", source], cwd=self.root, check=True)
        subprocess.run(commit + ["Capture oracle"], cwd=self.root, check=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        digest = hashlib.sha256((self.root / source).read_bytes()).hexdigest()
        subprocess.run(["git", "rm", "-q", source], cwd=self.root, check=True)
        subprocess.run(commit + ["Move source ownership"], cwd=self.root, check=True)
        shallow = self.root / "checkout"
        subprocess.run(["git", "clone", "-q", "--depth=1", self.root.as_uri(), str(shallow)], check=True)
        self.assertNotEqual(subprocess.run(["git", "cat-file", "-e", revision], cwd=shallow,
                                          capture_output=True).returncode, 0)
        bundle = shallow / "evidence/run"
        bundle.mkdir(parents=True)
        (bundle / "results.md").write_text(f'Tested `{revision}`\n')
        (bundle / "manifest.json").write_text(json.dumps({"sources": {"source_sha256": {source: digest}}}))
        document = "evidence/run/README.md"
        with patch.object(checker, 'ROOT', shallow), \
                patch.object(checker, 'HISTORICAL_REPOSITORIES', frozenset({self.root.as_uri()})), \
                patch.object(checker, 'HISTORICAL_SOURCES', {document: (revision, frozenset({source}))}), \
                patch.object(checker.subprocess, 'run', wraps=subprocess.run) as run:
            with patch.object(checker, 'HISTORICAL_REPOSITORIES', frozenset()):
                self.assertFalse(checker.historical_source_exists(document, (shallow / source).resolve()))
            self.assertTrue(checker.historical_source_exists(document, (shallow / source).resolve()))
            fetches = [call.args[0] for call in run.call_args_list if call.args[0][:2] == ['git', 'fetch']]
            self.assertEqual(fetches, [['git', 'fetch', '--no-tags', '--depth=1', self.root.as_uri(), revision]])
            self.assertFalse((shallow / source).exists())
            # A recorded path with a digest still fails if no such blob existed.
            missing = 'crates/absent.json'
            checker.HISTORICAL_SOURCES[document] = (revision, frozenset({missing}))
            (bundle / "manifest.json").write_text(json.dumps({"sources": {"source_sha256": {missing: digest}}}))
            self.assertFalse(checker.historical_source_exists(document, (shallow / missing).resolve()))

    def test_history_url_is_not_a_local_path(self):
        self.write("docs/README.md", "[history](https://example.invalid/revision/old.md)\n")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
