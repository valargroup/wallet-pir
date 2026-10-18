"""Read exact parser provenance from the executable that performs the export."""
import hashlib
from contextlib import contextmanager
import shutil
import tempfile
import json
from pathlib import Path
import re
import subprocess


def read(binary):
    binary = Path(binary).resolve()
    before = hashlib.sha256(binary.read_bytes()).hexdigest()
    result = subprocess.run([str(binary), "--source-info"], check=True,
                            capture_output=True, text=True)
    identity = json.loads(result.stdout)
    if not isinstance(identity, dict) or identity.get("schema") != "txid-sizing-export-source-v1":
        raise ValueError("unsupported export source identity")
    if identity.get("binary_sha256") != before or hashlib.sha256(binary.read_bytes()).hexdigest() != before:
        raise ValueError("export source identity does not match the executing binary")
    for field, package in (("node_parser", "zakura-chain"),
                           ("server_adapter", "zakura-server-chain"),
                           ("event_codec", "zakura-transparent-events")):
        entry = identity.get(field)
        if not isinstance(entry, dict) or entry.get("package") != package or not isinstance(entry.get("version"), str) or not entry["version"]:
            raise ValueError("missing compiled package identity: " + package)
        revision = entry.get("git_revision")
        source = entry.get("git_source", "")
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision) or not isinstance(source, str) or not source.startswith("git+") or not source.endswith("#" + revision):
            raise ValueError("missing exact compiled Git source: " + package)
    hashes = identity.get("compiled_source_file_sha256")
    if not isinstance(hashes, dict):
        raise ValueError("missing compiled source hashes")
    for value in (identity.get("cargo_lock_sha256"), hashes.get("extractor"), hashes.get("census")):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("missing compiled source hash")
    return identity


def require_same(binary, expected):
    """Refuse a replacement between verification and evidence publication."""
    if read(binary) != expected:
        raise ValueError("export binary identity changed during evidence preparation")


@contextmanager
def executable_snapshot(binary, expected):
    """Execute verified, privately owned bytes even if the source path is replaced."""
    with tempfile.TemporaryDirectory(prefix="txid-export-") as directory:
        snapshot = Path(directory) / "export"
        with Path(binary).open("rb") as source, snapshot.open("xb") as target:
            shutil.copyfileobj(source, target)
        snapshot.chmod(0o500)
        if read(snapshot) != expected:
            raise ValueError("export binary identity changed before extraction")
        yield snapshot
