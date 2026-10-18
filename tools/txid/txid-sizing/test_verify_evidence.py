import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

import verify_evidence


class RetainedSourcePins(unittest.TestCase):
    def test_verifies_immutable_revision_instead_of_evolving_checkout(self):
        revision = "1" * 40
        original = b"retained source bytes\n"
        pins = {"transparent/example.rs": hashlib.sha256(original).hexdigest()}
        with patch("verify_evidence.subprocess.check_output", return_value=original) as read:
            verify_evidence.verify_source_pins(Path("/checkout"), pins, revision)
        read.assert_called_once_with(
            ["git", "show", revision + ":transparent/example.rs"], cwd=Path("/checkout")
        )

    def test_changed_retained_bytes_are_refused(self):
        with patch("verify_evidence.subprocess.check_output", return_value=b"altered"):
            with self.assertRaisesRegex(ValueError, "retained source pin mismatch"):
                verify_evidence.verify_source_pins(
                    Path("/checkout"), {"source.rs": hashlib.sha256(b"original").hexdigest()}, "1" * 40
                )

    def test_symbolic_revision_is_refused_before_git(self):
        with patch("verify_evidence.subprocess.check_output") as read:
            with self.assertRaisesRegex(ValueError, "exact commit SHA"):
                verify_evidence.verify_source_pins(Path("/checkout"), {}, "main")
        read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
