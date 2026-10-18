#!/usr/bin/env python3
"""Build the isolated released client, verify its bytes, run guarded HTTP cases.

Invoke through the repository's required tool-exec wrapper. Cargo writers are
sequential and reuse its target lane. The output directory must be new.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HARNESS = Path(__file__).resolve().parent
REPO = next(p for p in HARNESS.parents if (p / "tools/ci/test_budget.py").is_file())
sys.path.insert(0, str(REPO / "tools/ci"))
sys.path.insert(0, str(REPO / "tools/dev"))
import test_budget  # noqa: E402
from target_lease import local_target  # noqa: E402
from target_lease import inherited_fds  # noqa: E402
from correctness import source_identity  # noqa: E402

CASE_SECONDS = 30


def host_snapshot():
    """Raw shared-host diagnostics, never an isolation/capacity attestation."""
    paths = [Path("/proc") / name for name in
             ("meminfo", "stat", "loadavg", "pressure/cpu", "pressure/memory", "self/cgroup", "self/status")]
    group = next((line[3:] for line in Path("/proc/self/cgroup").read_text().splitlines()
                  if line.startswith("0::")), None)
    if group is not None:
        directory = Path("/sys/fs/cgroup") / group.lstrip("/")
        # Ancestor limits matter even when the task's own files say "max".
        while directory.is_relative_to("/sys/fs/cgroup"):
            paths.extend(directory / name for name in
                         ("cpu.max", "cpu.stat", "cpuset.cpus.effective", "memory.max", "memory.current", "memory.peak"))
            if directory == Path("/sys/fs/cgroup"):
                break
            directory = directory.parent
    files = {}
    for path in paths:
        try:
            files[str(path)] = path.read_text()
        except OSError as error:
            files[str(path)] = {"unavailable": str(error)}
    return {"at_unix": time.time(), "files": files,
            "processes": subprocess.check_output(["ps", "-eo", "pid,ppid,comm,pcpu,rss"], text=True)}


def identity(path):
    with path.open("rb") as binary:
        digest = hashlib.file_digest(binary, "sha256").hexdigest()
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    subprocess.run([sys.executable, str(HARNESS / "test_runner.py")], check=True, **inherited_fds())
    with local_target(REPO):
        return qualify(args)


def qualify(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = source_identity()
    head = source["head"]
    difference = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=REPO)
    (output / "source.diff").write_bytes(difference)
    (output / "host-before.json").write_text(json.dumps(host_snapshot(), indent=2) + "\n")
    started = time.monotonic()
    subprocess.run(["cargo", "build", "--locked", "--profile", "release-fast", "--manifest-path", str(HARNESS / "Cargo.toml")], cwd=REPO, check=True, **inherited_fds())
    client_build_seconds = time.monotonic() - started
    subprocess.run([sys.executable, str(HARNESS / "verify-sources.py"), "--output", str(output / "sources")], cwd=REPO, check=True, **inherited_fds())
    metadata = json.loads((output / "sources/metadata.json").read_text())
    client = Path(metadata["target_directory"]) / "release-fast/frozen-provider-client"
    started = time.monotonic()
    compilation = subprocess.run(["cargo", "test", "--locked", "--profile", "release-fast", "-p", "transparent-query-server", "--test", "assignment", "--no-run", "--message-format=json"], cwd=REPO, text=True, stdout=subprocess.PIPE, **inherited_fds())
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
    provider = next(Path(m["executable"]) for m in messages if m.get("reason") == "compiler-artifact" and
                    m.get("target", {}).get("name") == "transparent-query-server" and m.get("executable"))
    receipt = {"head": head,
               "source": source,
               "status": subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True),
               "source_diff_sha256": hashlib.sha256(difference).hexdigest(),
               "client_build_seconds": client_build_seconds, "server_build_seconds": server_build_seconds,
               "client": identity(client), "server_tests": identity(Path(executable)), "provider": identity(provider),
               "profile": "release-fast", "case_deadline_seconds": CASE_SECONDS,
               "qualification": "CPU fixture correctness; shared-host diagnostics, no application or hardware pass"}
    (output / "build-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    fixtures = output / "fixtures"
    fixtures.mkdir()
    env = dict(os.environ, WALLET_PIR_FROZEN_CLIENT=str(client), WALLET_PIR_QUALIFICATION_OUTPUT=str(fixtures))
    test_budget.install_cancellation()
    started = time.monotonic()
    passed = False
    try:
        test_budget.execute([executable, "provider::", "--include-ignored"], cwd=REPO, env=env, report=str(output / "cases.jsonl"), seconds=CASE_SECONDS)
        if source_identity() != source:
            raise RuntimeError("source changed during qualification; results do not establish this snapshot")
        passed = True
    finally:
        (output / "host-after.json").write_text(json.dumps(host_snapshot(), indent=2) + "\n")
        (output / "fixtures.json").write_text(json.dumps(
            [identity(path) for path in sorted(fixtures.rglob("*")) if path.is_file()], indent=2) + "\n")
        (output / "result.json").write_text(json.dumps({"passed": passed, "test_seconds": time.monotonic() - started,
            "client_build_seconds": client_build_seconds, "server_build_seconds": server_build_seconds,
            "application_qualification": "not_executed", "isolated_hardware_qualification": "not_executed"}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
