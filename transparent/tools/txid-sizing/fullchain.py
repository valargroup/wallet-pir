#!/usr/bin/env python3
"""Full-chain single-session pipelined acquisition; public inputs stay in cache."""
import argparse
import hashlib
import json
import queue
import threading
import subprocess
import sqlite3
import os
import time
from pathlib import Path
from census import Gateway, ANCHOR_HEIGHT, ANCHOR_HASH, atomic_json, digest

class SessionDropped(RuntimeError):
    """An admitted stream dropped after returning public block data."""

class Pipeline:
    """Bound outstanding IDs and pace writes; exactly one Gateway owns SSH."""
    def __init__(self, gateway, rate=45, window=200):
        if not 0 < rate <= 50 or not 1 <= window <= 200:
            raise ValueError("invalid rate/window")
        self.gateway, self.rate, self.window = gateway, rate, window

    def blocks(self, start, stop):
        pending = threading.Semaphore(self.window)
        completed = queue.Queue(maxsize=self.window)
        cancelled = threading.Event()
        failures = []
        received = [0]
        first_id = self.gateway.next_id + 1
        def writer():
            try:
                last = self.gateway.last
                for height in range(start, stop):
                    while not pending.acquire(timeout=.1):
                        if cancelled.is_set(): return
                    if cancelled.is_set(): return
                    time.sleep(max(0, 1/self.rate-(time.monotonic()-last)))
                    request_id = first_id + height-start
                    self.gateway.process.stdin.write(json.dumps(dict(id=request_id,method="getblock",params=[str(height),0]))+"\n")
                    self.gateway.process.stdin.flush()
                    last = time.monotonic()
                    self.gateway.last = last
                    self.gateway.next_id = request_id
            except BaseException as error:
                failures.append(error)
                cancelled.set()
        def reader():
            try:
                for _ in range(start, stop):
                    line = self.gateway.process.stdout.readline()
                    if not line:
                        self.gateway.process.wait(timeout=5)
                        detail=self.gateway.process.stderr.read().strip() or "gateway closed stdout"
                        if received[0] and not any(word in detail.lower() for word in ("refused","denied","unreachable")):
                            raise SessionDropped(detail)
                        raise RuntimeError(detail)
                    response = json.loads(line)
                    if response.get("error") is not None:
                        raise RuntimeError("gateway RPC error: "+json.dumps(response["error"],sort_keys=True))
                    height = response.get("id", -1)-first_id+start
                    if not start <= height < stop: raise RuntimeError("gateway response ID outside request window")
                    received[0]+=1
                    pending.release()
                    while True:
                        try: completed.put((height,response["result"]),timeout=.1); break
                        except queue.Full:
                            if cancelled.is_set(): return
            except BaseException as error:
                failures.append(error)
                cancelled.set()
        threads = [threading.Thread(target=writer),threading.Thread(target=reader)]
        for thread in threads: thread.start()
        seen = set()
        try:
            for _ in range(start,stop):
                while True:
                    try: height,raw=completed.get(timeout=.5); break
                    except queue.Empty:
                        if failures: raise failures[0]
                if height in seen: raise RuntimeError("duplicate gateway response ID")
                seen.add(height)
                yield height, bytes.fromhex(raw)
        finally:
            cancelled.set()
            if any(thread.is_alive() for thread in threads):
                # A cancelled reader must be unblocked before another session starts.
                self.gateway.process.terminate()
            for thread in threads: thread.join(timeout=25)
            if any(thread.is_alive() for thread in threads): raise RuntimeError("gateway threads failed to stop")

def verify_raw_cache(cache):
    manifest=cache/"raw-manifest.jsonl"
    pins=[]
    if not manifest.exists(): return pins
    for line in manifest.read_text().splitlines():
        pin=json.loads(line)
        if pin["height"] != len(pins): raise ValueError("nonsequential retained raw cache")
        path=cache/"raw"/f'{pin["height"]}.bin'
        if path.stat().st_size != pin["raw_bytes"] or digest(path) != pin["raw_sha256"]:
            raise ValueError("retained raw checksum mismatch")
        pins.append(pin)
    return pins

def preflight(cache,status,duration=180):
    cache=cache.resolve(); cache.mkdir(parents=True,exist_ok=True)
    pins=verify_raw_cache(cache)
    start_height=len(pins)
    report=None
    with Gateway() as gateway:
        response=gateway("getblockhash",[ANCHOR_HEIGHT])
        if response.get("error") is not None or response.get("result") != ANCHOR_HASH:
            raise RuntimeError("canonical anchor verification failed: "+json.dumps(response.get("error")))
        info=gateway("getblockchaininfo",[])
        if info.get("error") is not None: raise RuntimeError("gateway RPC error: "+json.dumps(info["error"]))
        atomic_json(cache/"node-info.json",info["result"])
        started=time.monotonic(); count=0; raw_bytes=0
        # A complete bounded request batch leaves no outstanding RPC on completion.
        stop=min(ANCHOR_HEIGHT+1,start_height+int(duration*40))
        batch=cache/f"preflight-{start_height}-{stop-1}.raw"
        manifest=cache/f"preflight-{start_height}-{stop-1}.jsonl"
        with batch.open("xb") as raw_file,manifest.open("x") as inventory:
            for height,raw in Pipeline(gateway).blocks(start_height,stop):
                offset=raw_file.tell();raw_file.write(raw)
                inventory.write(json.dumps(dict(height=height,offset=offset,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest()),separators=(",",":"))+"\n")
                count+=1;raw_bytes+=len(raw)
                elapsed=time.monotonic()-started
                if count%100==0:
                    rate=count/elapsed;hours=(ANCHOR_HEIGHT+1-count-start_height)/rate/3600
                    atomic_json(status,dict(summary=f"Pipelined raw preflight: {count} new blocks; {rate:.2f} blocks/s; parsed census pending",eta=f"{hours:.1f}h",needs_you="",state="working"))
        seconds=time.monotonic()-started
        rate=count/seconds;hours=(ANCHOR_HEIGHT+1-count-start_height)/rate/3600
        report=dict(schema="txid-census-pipeline-preflight-v1",anchor_height=ANCHOR_HEIGHT,anchor_hash=ANCHOR_HASH,
            retained_raw_verified=len(pins),new_raw_blocks=count,seconds=seconds,blocks_per_second=rate,
            projected_remaining_hours=hours,exceeds_limit=hours>72,raw_bytes=raw_bytes,
            raw_bundle=batch.name,raw_bundle_sha256=digest(batch),inventory_sha256=digest(manifest),
            transport="one persistent SSH session; height-string raw getblock; 45 requests/s paced; window 200",
            qualification="RAW ACQUISITION ONLY; canonical parsing, UTXO and aggregates pending")
        atomic_json(cache/"pipeline-throughput.json",report)
    atomic_json(status,dict(summary=f"Pipelined preflight complete: {count} blocks at {rate:.2f}/s; canonical scan next",eta=f"{hours:.1f}h",needs_you=f"Measured {rate:.2f} blocks/s projects {hours:.1f}h, exceeding 72h" if hours>72 else "",state="blocked" if hours>72 else "working"))
    return report


def bundles(cache):
    """Verify complete receipts before reuse; preserve immutable raw bytes."""
    old=verify_raw_cache(cache)
    if old: yield ("legacy",old)
    for path in sorted(cache.glob("*.jsonl")):
        if not path.name.startswith(("preflight-","chain-")): continue
        raw_path=path.with_suffix(".raw")
        if not raw_path.exists(): continue
        rows=[json.loads(line) for line in path.read_text().splitlines()]
        if not rows: continue
        seen=set()
        with raw_path.open("rb") as f:
            for row in rows:
                if row["height"] in seen: raise ValueError("duplicate retained block height")
                seen.add(row["height"])
                f.seek(row["offset"]);raw=f.read(row["raw_bytes"])
                if hashlib.sha256(raw).hexdigest()!=row["raw_sha256"]: raise ValueError("raw bundle checksum mismatch")
        yield (raw_path,rows)

def extract_bundle(cache,binary,bundle,rows,status):
    db_path=cache/"census.sqlite"
    process=subprocess.Popen([str(binary),"--census",str(db_path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
    started=time.monotonic();count=0
    try:
        raw_file=None if bundle=="legacy" else bundle.open("rb")
        try:
            for row in sorted(rows,key=lambda r:r["height"]):
                if raw_file:
                    raw_file.seek(row["offset"]);raw=raw_file.read(row["raw_bytes"])
                else: raw=(cache/"raw"/f'{row["height"]}.bin').read_bytes()
                frame=dict(height=row["height"],raw_block=raw.hex())
                if "hash" in row: frame["hash"]=row["hash"]
                process.stdin.write(json.dumps(frame,separators=(",",":"))+"\n");process.stdin.flush()
                line=process.stdout.readline()
                if not line: raise RuntimeError("canonical extraction: "+process.stderr.read().strip())
                summary=json.loads(line)
                if summary["height"]!=row["height"] or summary["raw_sha256"]!=row["raw_sha256"]: raise ValueError("extractor frame identity mismatch")
                if summary["eligible"]+summary["shielded_only"]!=summary["transactions"]: raise ValueError("eligibility inventory mismatch")
                count+=1
                if count%100==0:
                    rate=count/(time.monotonic()-started)
                    atomic_json(status,dict(summary=f'Canonical UTXO census through height {row["height"]}; local extraction {rate:.2f} blocks/s',eta="22h",needs_you="",state="working"))
        finally:
            if raw_file: raw_file.close()
        process.stdin.close()
        if process.wait()!=0: raise RuntimeError("canonical extraction: "+process.stderr.read().strip())
        return dict(blocks=count,seconds=time.monotonic()-started)
    finally:
        if process.poll() is None: process.terminate();process.wait()

def scan(cache,binary,status,evidence):
    cache=cache.resolve();binary=binary.resolve();evidence=evidence.resolve()
    checkpoint=cache/"checkpoint.json"
    if checkpoint.exists():
        saved=json.loads(checkpoint.read_text())
        if digest(cache/"census.sqlite")!=saved["database_sha256"]: raise ValueError("census database checksum differs from checkpoint")
        if saved["anchor_hash"]!=ANCHOR_HASH: raise ValueError("checkpoint anchor differs")
    started=time.monotonic(); scanned=0
    for bundle,rows in sorted(bundles(cache),key=lambda p:min(r["height"] for r in p[1])):
        receipt=extract_bundle(cache,binary,bundle,rows,status);scanned+=receipt["blocks"]
    def snapshot():
        with sqlite3.connect(cache/"census.sqlite") as db:
            last=db.execute("SELECT height,hash FROM blocks ORDER BY height DESC LIMIT 1").fetchone()
            counts=db.execute("SELECT count(*),min(height),max(height) FROM blocks").fetchone()
            txids=db.execute("SELECT count(*) FROM txids").fetchone()[0]
            if counts[1]!=0 or counts[0]!=last[0]+1: raise ValueError("noncontinuous database height count")
        value=dict(schema="txid-full-chain-checkpoint-v1",anchor_height=ANCHOR_HEIGHT,anchor_hash=ANCHOR_HASH,
            scanned_blocks=counts[0],last_height=last[0],last_hash=last[1],transactions=txids,
            database_sha256=digest(cache/"census.sqlite"),binary_sha256=digest(binary),
            parser_sha="af944f5194ef2e9921bc96af017629450375013c",
            extractor_sha256=digest(Path("transparent/services/transparent-filter-server/src/extract.rs")),
            census_source_sha256=digest(Path("transparent/tools/txid-sizing/export/src/census.rs")),
            coverage="INCOMPLETE" if counts[0]!=ANCHOR_HEIGHT+1 else "FULL_CHAIN")
        atomic_json(checkpoint,value);atomic_json(evidence/"full-chain-checkpoint.json",value)
        return value
    current=snapshot()
    initial=current["scanned_blocks"]
    atomic_json(cache/"extraction-preflight.json",dict(blocks=scanned,seconds=time.monotonic()-started,blocks_per_second=scanned/(time.monotonic()-started)))
    with Gateway() as gateway:
        if gateway("getblockhash",[ANCHOR_HEIGHT]).get("result")!=ANCHOR_HASH: raise RuntimeError("canonical anchor changed")
        while current["scanned_blocks"]<=ANCHOR_HEIGHT:
            lo=current["scanned_blocks"];hi=min(lo+10000,ANCHOR_HEIGHT+1)
            bundle=cache/f"chain-{lo:07d}-{hi-1:07d}.raw";manifest=bundle.with_suffix(".jsonl")
            with bundle.open("xb") as raw_file,manifest.open("x") as output:
                for height,raw in Pipeline(gateway).blocks(lo,hi):
                    offset=raw_file.tell();raw_file.write(raw)
                    output.write(json.dumps(dict(height=height,offset=offset,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest()),separators=(",",":"))+"\n")
                    if (height-lo)%500==0:
                        atomic_json(status,dict(summary=f'Full-chain raw acquisition at height {height}; parsed through {lo-1}',eta="22h",needs_you="",state="working"))
            rows=[json.loads(line) for line in manifest.read_text().splitlines()]
            extract_bundle(cache,binary,bundle,rows,status)
            current=snapshot()
            elapsed=time.monotonic()-started
            rate=(current["scanned_blocks"]-initial)/elapsed
            hours=(ANCHOR_HEIGHT+1-current["scanned_blocks"])/rate/3600
            atomic_json(status,dict(summary=f'Full-chain census: {current["scanned_blocks"]}/{ANCHOR_HEIGHT+1} blocks; {rate:.2f} blocks/s',eta=f"{hours:.1f}h",needs_you="",state="working"))
            print(json.dumps(dict(height=current["last_height"],blocks_per_second=rate,remaining_hours=hours)),flush=True)
            if hours>72:
                raise RuntimeError(f"Measured end-to-end {rate:.2f} blocks/s projects {hours:.1f}h, exceeding 72h")
            subprocess.run(["git","add",str(evidence/"full-chain-checkpoint.json")],check=True)
            subprocess.run(["git","commit","-m",f'Census checkpoint through height {current["last_height"]}'],check=True)
            subprocess.run(["git","push","origin","HEAD"],check=True)
            # The active task checks the inbox between batches (about four minutes).
            inbox=status.parent/"inbox"
            if inbox.exists() and any(inbox.iterdir()):
                raise RuntimeError("Roman message arrived in workspace inbox; inspect before continuing")
        if current["last_hash"]!=ANCHOR_HASH or gateway("getblockhash",[ANCHOR_HEIGHT]).get("result")!=ANCHOR_HASH:
            raise RuntimeError("full-chain anchor mismatch")
    return current

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache",type=Path);parser.add_argument("--status",type=Path,required=True)
    parser.add_argument("--binary",type=Path);parser.add_argument("--evidence",type=Path,default=Path("transparent/evidence/txid-sizing"))
    args=parser.parse_args()
    try:
        if args.binary:
            for attempt in range(5):
                try:
                    print(json.dumps(scan(args.cache,args.binary,args.status,args.evidence)))
                    break
                except SessionDropped:
                    if attempt==4: raise
                    time.sleep(2**attempt)
        else: print(json.dumps(preflight(args.cache,args.status)))
    except Exception as error:
        atomic_json(args.status,dict(summary="Full-chain census blocked; retained checkpoints remain incomplete",eta="",needs_you=str(error),state="blocked"))
        raise
