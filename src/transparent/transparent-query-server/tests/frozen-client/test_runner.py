"""Hermetic orchestration controls; these do not execute client/server code."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class RunnerBudgetTests(unittest.TestCase):
    def qualify(self, inherited, fail=False):
        path = Path(__file__).with_name("run-tests.py")
        spec = importlib.util.spec_from_file_location("qualification_runner", path)
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        with tempfile.TemporaryDirectory(prefix="runner-guard-") as directory:
            root = Path(directory)
            target = root / "target"
            client = target / "release-fast/frozen-provider-client"
            client.parent.mkdir(parents=True)
            client.write_bytes(b"fake client: guard control only")
            tests, provider = root / "tests", root / "provider"
            tests.write_bytes(b"fake tests")
            provider.write_bytes(b"fake provider")
            observed = []

            def build(command, **kwargs):
                if len(command) > 1 and str(command[1]).endswith("verify-sources.py"):
                    output = Path(command[-1])
                    output.mkdir(parents=True)
                    (output / "metadata.json").write_text(json.dumps({"target_directory": str(target)}))
                messages = [{"reason": "compiler-artifact", "target": {"name": name}, "executable": str(binary)}
                            for name, binary in [("assignment", tests), ("transparent-query-server", provider)]]
                return subprocess.CompletedProcess(command, 0, stdout="\n".join(json.dumps(m) for m in messages))

            def execute(command, *, env, seconds=None, **kwargs):
                # Observe the existing guard's validated effective value, not
                # merely the receipt or the runner's intended constant.
                observed.append(runner.test_budget.case_seconds(
                    env.get("WALLET_PIR_CASE_SECONDS", 30) if seconds is None else seconds))
                if fail:
                    raise RuntimeError("guard refused case")
                return 1

            output = root / "output"
            with patch.dict(os.environ, {"WALLET_PIR_CASE_SECONDS": inherited}), \
                 patch.object(runner.subprocess, "run", build), \
                 patch.object(runner.subprocess, "check_output", lambda command, **kw: "" if kw.get("text") else b""), \
                 patch.object(runner, "host_snapshot", lambda: {"scope": "hermetic guard control"}), \
                 patch.object(runner, "source_identity", lambda: {"head": "1" * 40, "source_sha256": "fixture"}), \
                 patch.object(runner.test_budget, "install_cancellation", lambda: None), \
                 patch.object(runner.test_budget, "execute", execute):
                if fail:
                    with self.assertRaisesRegex(RuntimeError, "guard refused case"):
                        runner.qualify(argparse.Namespace(output=output))
                else:
                    self.assertEqual(runner.qualify(argparse.Namespace(output=output)), 0)
            receipt = json.loads((output / "build-receipt.json").read_text())
            result = json.loads((output / "result.json").read_text())
            return observed, receipt, result

    def test_inherited_override_cannot_change_guard_or_receipt(self):
        for inherited in ("60", "0.05", "nan"):
            with self.subTest(inherited=inherited):
                observed, receipt, result = self.qualify(inherited)
                self.assertEqual(observed, [30])
                self.assertEqual(receipt["case_deadline_seconds"], observed[0])
                self.assertTrue(result["passed"])

    def test_guard_failure_propagates_and_records_failure(self):
        observed, receipt, result = self.qualify("60", fail=True)
        self.assertEqual(observed, [30])
        self.assertEqual(receipt["case_deadline_seconds"], observed[0])
        self.assertFalse(result["passed"])


if __name__ == "__main__":
    unittest.main()
