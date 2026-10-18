#!/usr/bin/env python3
"""Retain resolved graph and verify registry bytes; run through task tool-exec."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib
import tarfile

ROOT = Path(__file__).resolve().parent
EXPECTED = {
    "zakura-pir-transparent": ("0.0.1-rc0", "391d0b0247f0e07cfdea855ce87fcdf8ba376aae"),
    **{name: ("0.1.0-rc.3", "c0852aa558abc6cfd97640965fdda8d3e27b7334") for name in
       ("transparent-wallet", "transparent-shard", "transparent-native", "transparent-filter", "transparent-events", "transparent-txid-client")},
}
ADAPTER = "ad10db262413b0502e4782710af4eb5a76333d6a3d89adc0c50c3c130947ddf2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path", str(ROOT / "Cargo.toml")]))
    lock_bytes = (ROOT / "Cargo.lock").read_bytes()
    lock = tomllib.loads(lock_bytes.decode())
    receipts = []
    for name, (version, vcs) in EXPECTED.items():
        packages = [p for p in metadata["packages"] if p["name"] == name]
        assert len(packages) == 1, (name, "missing or substituted graph")
        package = packages[0]
        assert package["version"] == version and package["source"] == "registry+https://github.com/rust-lang/crates.io-index", name
        source = Path(package["manifest_path"]).parent
        actual_vcs = json.loads((source / ".cargo_vcs_info.json").read_text())["git"]["sha1"]
        assert actual_vcs == vcs, (name, actual_vcs)
        locked = next(p for p in lock["package"] if p["name"] == name and p["version"] == version)
        archive = source.parents[2] / "cache" / source.parent.name / f"{name}-{version}.crate"
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        assert checksum == locked["checksum"], name
        if name == "zakura-pir-transparent":
            assert checksum == ADAPTER
        files_verified = 0
        with tarfile.open(archive) as original:
            for member in original.getmembers():
                if not member.isfile():
                    continue
                relative = Path(member.name).relative_to(f"{name}-{version}")
                assert ".." not in relative.parts
                assert (source / relative).read_bytes() == original.extractfile(member).read(), (name, str(relative))
                files_verified += 1
        receipts.append({"package": name, "version": version, "source": package["source"], "vcs": vcs, "crate_sha256": checksum, "manifest": str(source / "Cargo.toml"), "files_verified": files_verified})
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (args.output / "provenance.json").write_text(json.dumps({"lock_sha256": hashlib.sha256(lock_bytes).hexdigest(), "packages": receipts}, indent=2) + "\n")
    print(f"Verified {len(receipts)} original released packages; provenance in {args.output}")


if __name__ == "__main__":
    main()
