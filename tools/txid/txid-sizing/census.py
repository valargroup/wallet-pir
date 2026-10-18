#!/usr/bin/env python3
"""Resumable, single-session canonical stratified sample acquisition.

Public raw inputs, parent cache and checkpoints must live outside the repository.
The RPC transport is injected: one session, sequential requests, <=40 requests/s.
"""
import hashlib
import json
import os
from pathlib import Path
import random
import sqlite3
import time

ANCHOR_HEIGHT = 3502662
ANCHOR_HASH = "0000000000228603173bfeb3650b51ceb92b1ac6b8b6392fa1fa9e9bac3acf7f"
SEED = "wallet-pir-txid-sizing-v2/" + ANCHOR_HASH

def plan(anchor=ANCHOR_HEIGHT, per_stratum=256):
    boundaries = [0,347500,419200,653600,903000,1046400,1687104,2726400,anchor-9999,anchor+1]
    names = ["Sprout","Overwinter","Sapling","Blossom","Heartwood","Canopy","NU5","NU6+ archive","recent 10000 blocks"]
    rng = random.Random(SEED)
    strata = []
    for name, lo, hi in zip(names,boundaries,boundaries[1:]):
        if hi <= lo: raise ValueError("invalid anchor/stratum boundaries")
        n = min(per_stratum,hi-lo)
        strata.append(dict(name=name,lo=lo,hi_exclusive=hi,N=hi-lo,n=n,
            inclusion_probability=n/(hi-lo),heights=sorted(rng.sample(range(lo,hi),n))))
    return dict(schema="txid-sizing-probability-plan-v2",anchor_height=anchor,anchor_hash=ANCHOR_HASH,
        seed=SEED,selection="independent simple random samples without replacement of blocks within disjoint strata",
        strata=strata)

def digest(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):h.update(chunk)
    return h.hexdigest()

def atomic_json(path,value):
    temp=path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(value,separators=(",",":"))+"\n")
    os.replace(temp,path)

def run(checkpoint, rpc, status_path):
    checkpoint=Path(checkpoint).resolve()
    if checkpoint.is_relative_to(Path(__file__).resolve().parents[3]):raise ValueError("raw checkpoints must remain outside repository")
    checkpoint.mkdir(parents=True,exist_ok=True)
    blocks=checkpoint/"blocks";blocks.mkdir(exist_ok=True)
    manifest=checkpoint/"plan.json"
    if not manifest.exists():atomic_json(manifest,plan())
    design=json.loads(manifest.read_text())
    if design != plan():raise ValueError("checkpoint design differs from pinned selection")
    def call(method,params):
        response=rpc(method,params)
        if response.get("error") is not None:raise RuntimeError("gateway RPC error: "+json.dumps(response["error"],sort_keys=True))
        if "result" not in response:raise RuntimeError("gateway missing result")
        return response["result"]
    if call("getblockhash",[ANCHOR_HEIGHT]) != ANCHOR_HASH:raise RuntimeError("canonical anchor changed")
    info=call("getblockchaininfo",[])
    atomic_json(checkpoint/"node-info.json",info)
    db=sqlite3.connect(checkpoint/"parents.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS parents (txid TEXT PRIMARY KEY, response TEXT NOT NULL)")
    start=time.monotonic();completed=0;requests=0
    total=sum(s["n"] for s in design["strata"])
    pins=[]
    try:
        for stratum in design["strata"]:
            for height in stratum["heights"]:
                dest=blocks/f"{height}.json"
                if not dest.exists():
                    block=call("getblock",[str(height),2]);requests+=1
                    if block["height"]!=height:raise ValueError("RPC height mismatch")
                    raw=call("getblock",[block["hash"],0]);requests+=1
                    own={t["txid"] for t in block["tx"]}
                    ids=sorted({v["txid"] for t in block["tx"] for v in t["vin"] if "txid" in v}-own)
                    parents=[]
                    for txid in ids:
                        cached=db.execute("SELECT response FROM parents WHERE txid=?",(txid,)).fetchone()
                        if cached is None:
                            parent=call("getrawtransaction",[txid,1]);requests+=1
                            if parent["txid"]!=txid:raise ValueError("RPC parent identity mismatch")
                            db.execute("INSERT INTO parents VALUES (?,?)",(txid,json.dumps(parent,separators=(",",":"))))
                        else:parent=json.loads(cached[0])
                        parents.append(parent)
                    # Raw canonical bytes and decoded RPC fields form independent cross-check inputs.
                    atomic_json(dest,dict(height=height,hash=block["hash"],raw_block=raw,
                        rpc_block=block,parents=parents,stratum=stratum["name"]))
                    db.commit()
                frame=json.loads(dest.read_text())
                if frame["height"]!=height or frame["stratum"]!=stratum["name"]:raise ValueError("checkpoint identity mismatch")
                pins.append(dict(height=height,hash=frame["hash"],previousblockhash=frame["rpc_block"].get("previousblockhash"),
                    raw_sha256=hashlib.sha256(bytes.fromhex(frame["raw_block"])).hexdigest(),
                    frame_sha256=digest(dest),raw_bytes=len(frame["raw_block"])//2,
                    transactions=frame["rpc_block"]["nTx"],parents=len(frame["parents"]),stratum=stratum["name"]))
                completed+=1
                elapsed=time.monotonic()-start
                eta=elapsed/max(1,completed)*(total-completed)
                atomic_json(Path(status_path),dict(summary=f"Canonical stratified sample acquisition: {completed}/{total} blocks; {requests} new RPC calls; next canonical extraction and weighted analysis",eta=f"{int(eta/60)+1}m",needs_you="",state="working"))
        if call("getblockhash",[ANCHOR_HEIGHT]) != ANCHOR_HASH:raise RuntimeError("canonical anchor changed at completion")
        # Every selected block is checked again by height, including old cached blocks.
        for pin in pins:
            if call("getblockhash",[pin["height"]])!=pin["hash"]:raise RuntimeError("sample block no longer canonical")
        receipt=dict(design=design,source="read-only pir-census SSH JSON-RPC gateway at 167.99.42.60 to production zakurad archive; single sequential session",
            blocks=pins,acquisition_seconds=time.monotonic()-start,new_rpc_calls=requests,
            canonical_recheck="all selected height/hash pins and anchor rechecked after acquisition",
            node_info_sha256=digest(checkpoint/"node-info.json"))
        atomic_json(checkpoint/"receipt.json",receipt)
        return receipt
    except Exception as error:
        atomic_json(Path(status_path),dict(summary=f"Census acquisition stopped at {completed}/{total} sampled blocks",eta="",needs_you=str(error),state="blocked"))
        raise
    finally:db.close()


class Gateway:
    """Own exactly one SSH process and erase the temporary credential on exit."""
    def __enter__(self):
        import subprocess
        import tempfile
        key=os.environ.get("WALLET_PIR_CENSUS_RPC_SSH_KEY")
        if not key:raise RuntimeError("WALLET_PIR_CENSUS_RPC_SSH_KEY is not configured")
        fd,self.key_path=tempfile.mkstemp(prefix="wallet-pir-census-key-",dir="/tmp")
        os.fchmod(fd,0o600)
        with os.fdopen(fd,"w") as f:f.write(key if key.endswith("\n") else key+"\n")
        try:
            self.process=subprocess.Popen(["ssh","-i",self.key_path,"-o","IdentitiesOnly=yes",
                "-o","StrictHostKeyChecking=accept-new","-o","ConnectTimeout=20","-o","ServerAliveInterval=15","-o","ServerAliveCountMax=4",
                "pir-census@167.99.42.60"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,text=True,bufsize=1)
        except BaseException:
            os.unlink(self.key_path);raise
        self.next_id=0;self.last=0
        return self
    def __call__(self,method,params):
        import select
        allowed={"getblock","getrawtransaction","getblockhash","getblockheader","getblockcount","getbestblockhash","getblockchaininfo"}
        if method not in allowed:raise ValueError("method outside read-only census allowlist")
        time.sleep(max(0,.025-(time.monotonic()-self.last)))
        self.next_id+=1;self.last=time.monotonic()
        self.process.stdin.write(json.dumps(dict(id=self.next_id,method=method,params=params))+"\n")
        self.process.stdin.flush()
        ready,_,_=select.select([self.process.stdout],[],[],30)
        if not ready:raise RuntimeError("gateway response timed out after 30 seconds")
        line=self.process.stdout.readline()
        if not line:
            self.process.wait(timeout=5)
            raise RuntimeError(self.process.stderr.read().strip() or "gateway closed stdout")
        response=json.loads(line)
        if response.get("id")!=self.next_id:raise RuntimeError("gateway response ID mismatch")
        return response
    def __exit__(self,*exc):
        import subprocess
        try:
            if self.process.poll() is None:self.process.terminate()
            try:self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
        finally:os.unlink(self.key_path)

if __name__=="__main__":
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint",type=Path)
    parser.add_argument("--status",type=Path,required=True)
    args=parser.parse_args()
    try:
        with Gateway() as gateway:run(args.checkpoint,gateway,args.status)
    except Exception as error:
        atomic_json(args.status,dict(summary="Canonical census gateway/acquisition blocked",eta="",needs_you=str(error),state="blocked"))
        raise
