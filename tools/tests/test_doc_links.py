"""Exercise link checks on a working tree with relocations and cached deletions."""
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

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

    def test_history_url_is_not_a_local_path(self):
        self.write("docs/README.md", "[history](https://example.invalid/revision/old.md)\n")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
