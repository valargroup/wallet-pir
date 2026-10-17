#!/usr/bin/env python3
"""Build the isolated released client, verify its bytes, run guarded HTTP cases.

Invoke through the repository's required tool-exec wrapper. Cargo writers are
sequential and reuse its target lane. The output directory must be new.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HARNESS = Path(__file__).resolve().parent
REPO = next(p for p in HARNESS.parents if (p / "tools/ci/test_budget.py").is_file())
sys.path.insert(0, str(REPO / "tools/ci"))
import test_budget  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    subprocess.run(["cargo", "build", "--locked", "--profile", "release-fast", "--manifest-path", str(HARNESS / "Cargo.toml")], cwd=REPO, check=True)
    client_build_seconds = time.monotonic() - started
    subprocess.run([sys.executable, str(HARNESS / "verify-sources.py"), "--output", str(output / "sources")], cwd=REPO, check=True)
    metadata = json.loads((output / "sources/metadata.json").read_text())
    client = Path(metadata["target_directory"]) / "release-fast/frozen-provider-client"
    started = time.monotonic()
    compilation = subprocess.run(["cargo", "test", "--locked", "--profile", "release-fast", "-p", "transparent-shard-server", "--test", "assignment", "--no-run", "--message-format=json"], cwd=REPO, text=True, stdout=subprocess.PIPE)
    compiled = compilation.stdout
    server_build_seconds = time.monotonic() - started
    (output / "server-build.jsonl").write_text(compiled)
    messages = [json.loads(line) for line in compiled.splitlines()]
    if compilation.returncode:
        for message in messages:
            if message.get("reason") == "compiler-message":
                print(message["message"].get("rendered", ""), file=sys.stderr)
        compilation.check_returncode()
    executable = next(m["executable"] for m in reversed(messages) if m.get("reason") == "compiler-artifact" and m.get("target", {}).get("name") == "assignment" and m.get("executable"))
    receipt = {"head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
               "status": subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True),
               "client_build_seconds": client_build_seconds, "server_build_seconds": server_build_seconds,
               "client": str(client), "server_tests": executable}
    (output / "build-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    env = dict(os.environ, WALLET_PIR_FROZEN_CLIENT=str(client))
    test_budget.install_cancellation()
    test_budget.execute([executable, "provider::", "--include-ignored"], cwd=REPO, env=env, report=str(output / "cases.jsonl"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
