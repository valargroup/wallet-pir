"""The packaged runner must preserve candidate provenance outside a Git checkout."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/test-local.py'
spec = importlib.util.spec_from_file_location('runner', SCRIPT)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class CandidateProvenanceTests(unittest.TestCase):
    def metadata(self):
        return {'kind': 'enhance-pir', 'qualification': 'unqualified',
                'protocol_revision': 'ironwood-enhance-pir-v7', 'schema_version': 11,
                'source_revision': 'a' * 40, 'source_dirty': True}

    def test_bundle_does_not_need_git_and_preserves_dirty_status(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            (path / 'candidate.json').write_text(json.dumps(self.metadata()))
            with patch.object(runner.subprocess, 'run', side_effect=AssertionError('must not invoke Git')):
                self.assertEqual(runner.provenance(path), {
                    'revision': 'a' * 40, 'dirty': True, 'revision_source': 'candidate-metadata'})

    def test_invalid_metadata_cannot_fall_back_to_git(self):
        for field, value in [('qualification', 'passed'), ('source_dirty', 'false'),
                             ('source_revision', 'main'), ('schema_version', 9)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                path = Path(temp)
                metadata = self.metadata()
                metadata[field] = value
                (path / 'candidate.json').write_text(json.dumps(metadata))
                with patch.object(runner.subprocess, 'run', side_effect=AssertionError('must not invoke Git')):
                    with self.assertRaises(ValueError):
                        runner.provenance(path)
