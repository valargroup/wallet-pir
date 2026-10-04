#!/usr/bin/env python3
"""Verify sanitized artifacts and deterministic report regeneration offline."""
import hashlib
import json
import subprocess
from pathlib import Path
import analyze
import geometry
import survey


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
        source=(root/name).read_bytes()
        if name == "transparent/tools/txid-sizing/export/Cargo.lock":
            # The vector oracle predates the standalone disk-UTXO dependency.
            # Verify its original lock at the exact retained PR #124 head;
            # the resume receipt separately pins the current exporter lock.
            source=subprocess.check_output(["git","show",
                "7d46f7618b94f2a889dcfebd5b3dc9229bd98b5a:"+name],cwd=root)
        if hashlib.sha256(source).hexdigest()!=digest:
            raise ValueError("pinned source changed: "+name)
    current=evidence/"archive-survey-statistics.json"
    if current.exists():
        new_report=survey.report(json.loads(current.read_bytes()))
        new_report["input_sha256"]=hashlib.sha256(current.read_bytes()).hexdigest()
        if new_report!=json.loads((evidence/"archive-survey-analysis.json").read_bytes()):
            raise ValueError("survey regeneration mismatch")
        if geometry.survey_projection(new_report)!=json.loads((evidence/"archive-survey-geometry.json").read_bytes()):
            raise ValueError("survey geometry regeneration mismatch")
        new_pins=json.loads((evidence/"archive-survey-sources.json").read_bytes())
        for name,digest in new_pins["source_files"].items():
            if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:
                raise ValueError("survey source changed: "+name)
    print("checksums, source pins and deterministic reports verified; retained vector and incomplete census qualification limits remain explicit")


if __name__=="__main__": main()
