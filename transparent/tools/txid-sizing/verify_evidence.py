#!/usr/bin/env python3
"""Verify sanitized artifacts and deterministic report regeneration offline."""
import hashlib
import json
from pathlib import Path
import analyze
import geometry


def main():
    root=Path(__file__).resolve().parents[3]
    evidence=root/"transparent/evidence/txid-sizing"
    for line in (evidence/"SHA256SUMS").read_text().splitlines():
        digest,name=line.split("  ",1)
        if hashlib.sha256((evidence/name).read_bytes()).hexdigest()!=digest:
            raise ValueError("evidence checksum mismatch: "+name)
    data=json.loads((evidence/"canonical-sample.json").read_bytes())
    report=analyze.analyze(data)
    report["input_sha256"]=hashlib.sha256((evidence/"canonical-sample.json").read_bytes()).hexdigest()
    if report!=json.loads((evidence/"analysis.json").read_bytes()):
        raise ValueError("analysis regeneration mismatch")
    if geometry.report()!=json.loads((evidence/"geometry-projections.json").read_bytes()):
        raise ValueError("geometry regeneration mismatch")
    pins=json.loads((evidence/"sources.json").read_bytes())
    for name,digest in pins["source_files"].items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:
            raise ValueError("pinned source changed: "+name)
    print("checksums, source pins, analysis and geometry regenerate exactly; qualification remains UNQUALIFIED")


if __name__=="__main__": main()
