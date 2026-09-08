"""Report navigation across old and current simulation output formats."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "open_report", Path(__file__).resolve().parents[1] / "open_report.py"
)
opener = importlib.util.module_from_spec(spec)
spec.loader.exec_module(opener)


class ReportNavigation(unittest.TestCase):
    def test_old_empty_failure_opens_preparation_but_current_reports_stay_at_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "report.html"
            report.write_text("main")
            batch = root / "preparation/batch-0/report.html"
            batch.parent.mkdir(parents=True)
            batch.write_text("preparation")
            data = {"users": [], "success": False, "errors": ["preparation failed"]}
            report.with_suffix(".json").write_text(json.dumps(data))
            self.assertEqual(opener.display_report(report), batch.resolve())
            data["preparation"] = [{"status": "failed"}]
            report.with_suffix(".json").write_text(json.dumps(data))
            self.assertEqual(opener.display_report(report), report)
            data.pop("preparation")
            data["users"] = [{"outcome": "failed"}]
            report.with_suffix(".json").write_text(json.dumps(data))
            self.assertEqual(opener.display_report(report), report)

    def test_missing_or_invalid_metadata_keeps_available_html(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.html"
            report.write_text("main")
            self.assertEqual(opener.display_report(report), report)
            report.with_suffix(".json").write_text("invalid")
            self.assertEqual(opener.display_report(report), report)


if __name__ == "__main__":
    unittest.main()
