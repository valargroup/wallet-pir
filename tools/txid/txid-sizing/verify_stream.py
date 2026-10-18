#!/usr/bin/env python3
"""Focused canonical streaming refusal cases on retained public raw checkpoints."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import census

def verify(checkpoint,binary):
    frames=[]
    for path in sorted((Path(checkpoint)/"blocks").glob("*.json")):
        frame=json.loads(path.read_text())
        if frame["parents"]:
            frames.append(frame);break
    if not frames:raise ValueError("no external-parent checkpoint available for refusal oracle")
    frame=frames[0]
    canonical={k:frame[k] for k in ("height","hash","raw_block")}
    canonical["parents"]=[dict(txid=p["txid"],hex=p["hex"]) for p in frame["parents"]]
    def run(value):return subprocess.run([str(binary),"--stream"],input=json.dumps(value)+"\n",text=True,capture_output=True)
    good=run(canonical)
    if good.returncode:raise ValueError("valid canonical streaming checkpoint refused: "+good.stderr)
    records=json.loads(good.stdout)
    checks={"valid_complete_external_parents":True}
    cases=[]
    bad=copy.deepcopy(canonical);bad["hash"]="00"*32;cases.append(("wrong_block_hash",bad,"raw block hash"))
    bad=copy.deepcopy(canonical);bad["raw_block"]+="00";cases.append(("trailing_block_bytes",bad,"trailing canonical bytes"))
    bad=copy.deepcopy(canonical);bad["parents"][0]["txid"]="00"*32;cases.append(("wrong_parent_txid",bad,"raw parent hash"))
    bad=copy.deepcopy(canonical);bad["parents"][0]["hex"]+="00";cases.append(("trailing_parent_bytes",bad,"trailing canonical bytes"))
    bad=copy.deepcopy(canonical);bad["parents"]=[];cases.append(("missing_external_prevouts",bad,"MissingPreviousOutput"))
    for name,value,expected in cases:
        result=run(value)
        if result.returncode==0 or expected not in result.stderr:raise ValueError("refusal oracle failed: "+name)
        checks[name]=True
    # Restarting the exporter and replaying the same complete frame is deterministic.
    if run(canonical).stdout!=good.stdout:raise ValueError("restart replay changed canonical extraction")
    checks["restart_replay_identical"]=True
    return dict(schema="txid-sizing-stream-conformance-v2",height=frame["height"],hash=frame["hash"],
        raw_sha256=__import__("hashlib").sha256(bytes.fromhex(frame["raw_block"])).hexdigest(),
        eligible=len(records["records"]),checks=checks)

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint",type=Path);parser.add_argument("binary",type=Path);parser.add_argument("output",type=Path)
    args=parser.parse_args();census.atomic_json(args.output,verify(args.checkpoint,args.binary))
