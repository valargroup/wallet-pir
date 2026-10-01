#!/usr/bin/env python3
"""Publish compact sanitized sufficient statistics; leave public raw data outside Git."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import census
import geometry
import survey
import verify_stream

ROOT=Path(__file__).resolve().parents[3]

def sample_packing(checkpoint,binary,analysis):
    records=[]
    for path in sorted((Path(checkpoint)/"records").glob("*.json")):
        records.extend(json.loads(path.read_text())["records"])
    if len(records)>100000 or sum(len(r["display_v1_hex"])//2 for r in records)>128*1024*1024:
        raise ValueError("bounded exact sample-packing oracle capacity exceeded")
    process=subprocess.Popen([str(binary),"--pack-stream"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    try:
        for r in records:process.stdin.write(json.dumps({k:r[k] for k in ("txid_internal","display_v1_hex")})+"\n")
        process.stdin.close();oracle=json.loads(process.stdout.read())
        if process.wait():raise RuntimeError("implemented sample table verifier failed")
    finally:
        if process.poll() is None:process.terminate();process.wait()
    reports={}
    for codec in survey.a.CODECS:
        cutoffs=sorted(set(survey.a.THRESHOLDS)|{t for t in analysis["codecs"][codec]["weighted_frontiers_bytes"].values() if t<=4044})
        reports[codec]=[{k:v for k,v in survey.a.packing(records,codec,t).items() if k!="per_record"} for t in cutoffs]
    implemented=next(p for p in reports["display-v1"] if p["threshold"]==128)
    for k in ("transactions","payload_bytes","directory_segments","page_segments","occupied_directory_rows","occupied_page_rows","occupied_bytes"):
        if implemented[k]!=oracle[k]:raise ValueError("sample Python/Rust packing disagreement: "+k)
    return dict(schema="txid-sizing-probability-sample-packing-v2",basis="exact unweighted sampled records sorted by txid, artificially packed in bucket 0/recent-4k; not a population or live layout replay",implemented_oracle=oracle,codecs=reports)


def publish(checkpoint,binary,output):
    checkpoint=Path(checkpoint);output=Path(output)
    receipt=json.loads((checkpoint/"receipt.json").read_text())
    replay=json.loads((checkpoint/"final-replay.json").read_text())
    data=survey.sufficient(checkpoint)
    summary_path=output/"archive-survey-statistics.json"
    census.atomic_json(summary_path,data)
    analysis=survey.report(data)
    analysis["input_sha256"]=census.digest(summary_path)
    census.atomic_json(output/"archive-survey-analysis.json",analysis)
    census.atomic_json(output/"archive-survey-geometry.json",geometry.survey_projection(analysis))
    census.atomic_json(output/"stream-conformance.json",verify_stream.verify(checkpoint,binary))
    census.atomic_json(output/"archive-sample-packing.json",sample_packing(checkpoint,binary,analysis))
    source_files={}
    for path in sorted((ROOT/"transparent/tools/txid-sizing").rglob("*")):
        if path.is_file() and path.suffix in (".py",".rs",".toml") or path.is_file() and path.name=="Cargo.lock":
            source_files[str(path.relative_to(ROOT))]=census.digest(path)
    old=json.loads((output/"sources.json").read_text())
    source_files.update(old["source_files"])
    node=json.loads((checkpoint/"node-info.json").read_text())
    source=dict(schema="txid-sizing-archive-survey-sources-v2",evidence_generated_at_utc=datetime.now(timezone.utc).isoformat(),
        acquisition_completed_at_utc=datetime.fromtimestamp((checkpoint/"receipt.json").stat().st_mtime,timezone.utc).isoformat(),
        source=receipt["source"],base_source_sha="7d46f7618b94f2a889dcfebd5b3dc9229bd98b5a",
        analysis_tools_sha=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
        parser_revision="af944f5194ef2e9921bc96af017629450375013c",
        metadata_dependency="3cfbc4848b92b1385b9215174251f8e51fbfa804",
        node_chain=node.get("chain"),node_pruned=node.get("pruned"),node_reported_upgrades=node.get("upgrades"),node_build_version="unavailable from allowed RPC methods; not inferred from source",
        anchor_height=census.ANCHOR_HEIGHT,anchor_hash=census.ANCHOR_HASH,
        complete_census=False,selected_blocks=len(receipt["blocks"]),selection=receipt["design"],blocks=receipt["blocks"],
        canonical_recheck=receipt["canonical_recheck"],canonical_consensus="trusted archive canonical height/hash RPC; no independent consensus replay or continuity check across unsampled blocks",
        coverage="all transparent-bearing confirmed transactions in selected blocks, including coinbase; no missing-prevout exclusions; shielded-only inventory retained separately",
        raw_retention=dict(location=str(checkpoint/"blocks"),format="read-only per-block JSON with raw block and complete external-parent raw bytes plus decoded RPC oracle fields",receipt="per-frame and raw-block SHA256 in this artifact; canonical record checkpoints retained outside repository"),
        node_info_sha256=receipt["node_info_sha256"],acquisition_seconds=receipt["acquisition_seconds"],payload_rpc_calls=receipt["new_rpc_calls"],pin_recheck_rpc_calls=len(receipt["blocks"])+1,initial_anchor_and_info_rpc_calls=2,
        final_replay=replay,source_files=source_files)
    census.atomic_json(output/"archive-survey-sources.json",source)
    sums=[]
    for path in sorted(output.glob("*.json")):sums.append(census.digest(path)+"  "+path.name)
    (output/"SHA256SUMS").write_text("\n".join(sums)+"\n")
    return analysis

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint",type=Path);parser.add_argument("binary",type=Path);parser.add_argument("output",type=Path)
    args=parser.parse_args();publish(args.checkpoint,args.binary,args.output)
