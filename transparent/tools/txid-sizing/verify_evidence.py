#!/usr/bin/env python3
"""Verify sanitized artifacts and deterministic report regeneration offline."""
import hashlib
import json
import subprocess
from pathlib import Path
import analyze
import geometry
import survey
import day_analysis


def verify_source_pins(root,pins,revision):
    import re
    if re.fullmatch(r"[0-9a-f]{40}",revision) is None:
        raise ValueError("source pin needs an exact commit SHA")
    for name,digest in pins.items():
        source=subprocess.check_output(["git","show",revision+":"+name],cwd=root)
        if hashlib.sha256(source).hexdigest()!=digest:
            raise ValueError("retained source pin mismatch: "+name)


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
    # The retained oracle belongs to PR #124, not the current checkout.
    # Check all of its source bytes at that exact immutable revision so new
    # analysis tools and normal main evolution cannot rewrite provenance.
    verify_source_pins(root,pins["source_files"],"7d46f7618b94f2a889dcfebd5b3dc9229bd98b5a")
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
    day=evidence/"day-statistics.json.gz"
    if day.exists():
        report=day_analysis.report(day_analysis.read(day))
        day_analysis.apply_bootstrap(report,day_analysis.read(evidence/"day-bootstrap.json"))
        if report!=day_analysis.read(evidence/"day-analysis.json"):
            raise ValueError("one-day report regeneration mismatch")
        if day_analysis.geometry_projection(report)!=day_analysis.read(evidence/"day-geometry.json"):
            raise ValueError("one-day geometry regeneration mismatch")
        pins=day_analysis.read(evidence/"day-sources.json")
        verify_source_pins(root,pins["source_files"],pins["analysis_and_final_crosscheck_code_commit"])
        for name,digest in pins["data_files"].items():
            if hashlib.sha256((evidence/name).read_bytes()).hexdigest()!=digest:
                raise ValueError("one-day data changed: "+name)
        data=day_analysis.read(day)
        receipt=day_analysis.read(evidence/"day-receipt.json.gz")
        if data["design"]!=receipt["design"] or len(receipt["blocks"])!=len(data["blocks"]):
            raise ValueError("one-day design/receipt mismatch")
        if data["design"]!=day_analysis.read(evidence/"day-plan.json"):
            raise ValueError("one-day fixed plan changed")
        expected={h for s in data["design"]["strata"] for h in s["heights"]}
        raw={b["height"]:b for b in receipt["blocks"]}
        heights={b["height"] for b in data["blocks"]}
        if expected!=set(raw) or heights!=expected or len(heights)!=len(data["blocks"]):
            raise ValueError("one-day selection/inventory mismatch")
        extraction=day_analysis.read(evidence/"day-extraction-receipt.json.gz")
        record_pins={p["height"]:p["canonical_records_sha256"] for p in extraction["pins"]}
        if set(record_pins)!=expected or extraction["blocks"]!=len(expected):
            raise ValueError("one-day canonical inventory mismatch")
        if sum(b["transactions"] for b in receipt["blocks"])!=extraction["distinct_all_txids"]:
            raise ValueError("one-day distinct identity total mismatch")
        for b in data["blocks"]:
            if b["canonical_records_sha256"]!=record_pins[b["height"]]:
                raise ValueError("one-day canonical record pin mismatch")
            if raw[b["height"]]["hash"]!=b["hash"] or raw[b["height"]]["frame_sha256"]!=b["source_sha256"]:
                raise ValueError("one-day block source pin mismatch")
    print("checksums, source pins and deterministic reports verified; retained vector and incomplete census qualification limits remain explicit")


if __name__=="__main__": main()
