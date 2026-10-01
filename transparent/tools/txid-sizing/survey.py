#!/usr/bin/env python3
"""Streaming canonical extraction and design-based block-cluster estimators.

No independent-Bernoulli transaction confidence intervals. Routing models are
counterfactual frozen transcripts, never live privacy or minimum qualification.
"""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sqlite3
import time
import analyze as a
import census


def estimate(values, design, denominator=None, proportion=False):
    """Stratified SRSWOR HT total or linearized ratio; pointwise normal 95% CI."""
    total=den=variance=0.0
    sample=0
    for s in design["strata"]:
        xs=values[s["name"]]
        if len(xs)!=s["n"]:raise ValueError("incomplete probability sample")
        total+=s["N"]*sum(xs)/s["n"];sample+=sum(xs)
        if denominator is not None:den+=s["N"]*sum(denominator[s["name"]])/s["n"]
    point=total if denominator is None else (total/den if den else None)
    if point is None:return dict(estimate=None,ci95=None,sample_sum=sample)
    for s in design["strata"]:
        xs=values[s["name"]]
        residuals=xs if denominator is None else [x-point*y for x,y in zip(xs,denominator[s["name"]])]
        mean=sum(residuals)/s["n"]
        v=sum((x-mean)**2 for x in residuals)/(s["n"]-1) if s["n"]>1 else 0
        variance+=s["N"]**2*(1-s["n"]/s["N"])*v/s["n"]
    se=math.sqrt(variance)/(den if denominator is not None else 1)
    lo,hi=max(0,point-1.96*se),point+1.96*se
    if proportion:hi=min(1,hi)
    # A zero observed tail cannot supply an upper bound via a zero Wald variance.
    zero_tail=variance==0 and any(s["n"]<s["N"] for s in design["strata"]) and (sample==0 or (proportion and point in (0,1)))
    return dict(estimate=point,standard_error=se,ci95=None if zero_tail else [lo,hi],
        ci_method="stratified block-cluster linearization, FPC, pointwise normal 1.96" if not zero_tail else "unobserved tail: Wald interval withheld",
        sample_sum=sample)


def hist_quantile(hist,p,strict=False):
    total=sum(hist.values());cumulative=0
    for size,count in sorted(hist.items()):
        cumulative+=count
        if cumulative>p*total if strict else cumulative>=p*total:return size
    return None


def estimate_moments(moments,design):
    """Same HT total/variance from sparse per-stratum (sum, sum of squares)."""
    total=variance=observed=0.0
    for s in design["strata"]:
        x,xx=moments.get(s["name"],(0,0))
        total+=s["N"]*x/s["n"];observed+=x
        v=max(0,(xx-x*x/s["n"])/(s["n"]-1)) if s["n"]>1 else 0
        variance+=s["N"]**2*(1-s["n"]/s["N"])*v/s["n"]
    se=math.sqrt(variance)
    return dict(estimate=total,standard_error=se,ci95=[max(0,total-1.96*se),total+1.96*se],
        ci_method="stratified block-cluster linearization, FPC, pointwise normal 1.96",sample_sum=observed)


def sufficient(checkpoint):
    checkpoint=Path(checkpoint)
    receipt=json.loads((checkpoint/"receipt.json").read_text())
    blocks=[];moments=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:[0,0])))
    for s in receipt["design"]["strata"]:
        for h in s["heights"]:
            b=json.loads((checkpoint/"summaries"/f"{h}.json").read_text())
            for name,counts in b.pop("routes").items():
                for key,count in counts.items():
                    pair=moments[name][key][s["name"]];pair[0]+=count;pair[1]+=count*count
            blocks.append(b)
    return dict(schema="txid-sizing-survey-sufficient-statistics-v2",design=receipt["design"],blocks=blocks,route_moments=moments)


def frontier_intervals(groups,design,codec,category="all"):
    """Invert pointwise normal CDF bands; no simultaneous-band qualification."""
    events=defaultdict(list);state={};den=0
    for s in design["strata"]:
        ys=[sum(n for _,n in b["hist"][codec][category]) for b in groups[s["name"]]]
        state[s["name"]]=dict(x=[0]*s["n"],y=ys,sx=0,sxx=0,sxy=0,sy=sum(ys),syy=sum(y*y for y in ys))
        den+=s["N"]*sum(ys)/s["n"]
        for i,b in enumerate(groups[s["name"]]):
            for size,n in b["hist"][codec][category]:events[size].append((s["name"],i,n))
    if not den:return {}
    curves=[]
    for size in sorted(events):
        for name,i,n in events[size]:
            st=state[name];old=st["x"][i];st["x"][i]+=n
            st["sx"]+=n;st["sxx"]+=2*old*n+n*n;st["sxy"]+=st["y"][i]*n
        numerator=sum(s["N"]*state[s["name"]]["sx"]/s["n"] for s in design["strata"])
        ratio=numerator/den;variance=0
        for s in design["strata"]:
            st=state[s["name"]]
            ss=st["sxx"]-2*ratio*st["sxy"]+ratio*ratio*st["syy"]-(st["sx"]-ratio*st["sy"])**2/s["n"]
            if abs(ss)<1e-12*max(1,st["sxx"],st["syy"]):ss=0
            variance+=s["N"]**2*(1-s["n"]/s["N"])*max(0,ss)/(s["n"]*(s["n"]-1))
        se=math.sqrt(variance)/den
        curves.append((size,ratio,max(0,ratio-1.96*se),min(1,ratio+1.96*se)))
    frontiers={}
    for target in (.5,.8,.85,.9,.95,.99,.999):
        def crossed(v):return v>target if target==.8 else v>=target
        point=next((size for size,r,lo,hi in curves if crossed(r)),None)
        lower=next((size for size,r,lo,hi in curves if crossed(hi)),None)
        upper=next((size for size,r,lo,hi in curves if r<1 and crossed(lo)),None)
        frontiers[str(target)]=dict(estimated_bytes=point,approximate_ci95_bytes=[lower,upper],
            method="inverted pointwise stratified block-cluster normal CDF bands; sparse/extreme-tail coverage limited; null upper means unresolved beyond observed support")
    return frontiers


def block_summary(frame, extracted):
    if extracted["hash"]!=frame["hash"] or extracted["height"]!=frame["height"]:raise ValueError("canonical extraction pin mismatch")
    block=frame["rpc_block"]
    if extracted["transactions"]!=block["nTx"]:raise ValueError("raw/decoded transaction inventory mismatch")
    records=extracted["records"]
    if len({r["txid_internal"] for r in records})!=len(records):raise ValueError("duplicate real txid")
    rpc_eligible=[t for t in block["tx"] if t["vin"] or t["vout"]]
    if len(rpc_eligible)!=len(records):raise ValueError("raw/decoded eligibility mismatch")
    totals=Counter(all_transactions=block["nTx"],eligible=len(records),shielded_only=extracted["shielded_only"],outputs=0,transparent_inputs=0)
    hist={codec:{"all":Counter(),"coinbase":Counter(),"non_coinbase":Counter()} for codec in a.CODECS}
    scripts=Counter();routes=defaultdict(Counter)
    parents={p["txid"]:p for p in frame["parents"]}
    parents.update({t["txid"]:t for t in block["tx"]})
    fee_oracles=0
    for r in records:
        tx=block["tx"][r["transaction_index"]]
        if bytes.fromhex(tx["txid"])[::-1].hex()!=r["txid_internal"]:raise ValueError("raw/decoded txid mismatch")
        decoded=[dict(value=o.get("valueZat",int(Decimal(str(o["value"]))*100000000)),script=o["scriptPubKey"]["hex"]) for o in tx["vout"]]
        if r["outputs"]!=decoded:raise ValueError("raw/decoded exact output mismatch")
        coinbase=any("coinbase" in v for v in tx["vin"])
        ni=sum("txid" in v for v in tx["vin"])
        if coinbase!=r["coinbase"] or ni!=r["input_count"]:raise ValueError("raw/decoded input shape mismatch")
        if a.payload(r).hex()!=r["display_v1_hex"]:raise ValueError("Rust/Python display-v1 byte mismatch")
        # Independent transparent-only RPC accounting oracle, never extraction input.
        if not coinbase and not r["shielded_components"]:
            incoming=0
            for vin in tx["vin"]:
                out=parents[vin["txid"]]["vout"][vin["vout"]]
                incoming+=out.get("valueZat",int(Decimal(str(out["value"]))*100000000))
            if incoming-sum(o["value"] for o in decoded)!=r["fee"]:raise ValueError("independent transparent fee oracle mismatch")
            fee_oracles+=1
        group="coinbase" if coinbase else "non_coinbase"
        totals[group]+=1;totals["outputs"]+=len(decoded);totals["transparent_inputs"]+=ni
        totals["shielded_components"]+=r["shielded_components"]
        totals["input_only"]+=ni>0 and not decoded
        totals["external_unshielding"]+=not coinbase and ni==0 and bool(decoded) and r["shielded_components"]
        for o in decoded:
            raw=bytes.fromhex(o["script"]);tag=a.script_encode(raw)[0]
            scripts[str(tag)]+=1;totals["raw_escape_outputs"]+=tag==0
            totals["empty_outputs"]+=not raw;totals["opreturn_outputs"]+=bool(raw) and raw[0]==106
        sizes={codec:len(a.payload(r,codec)) for codec in a.CODECS}
        for codec,size in sizes.items():hist[codec]["all"][size]+=1;hist[codec][group][size]+=1
        size=sizes["display-v1"]
        h=r["height"];txid=r["txid_internal"]
        lh=int.from_bytes(hashlib.sha256(b"txid-sizing/lookup/"+bytes.fromhex(txid)).digest()[:8],"little")
        oh=int.from_bytes(hashlib.sha256(b"txid-sizing/overflow/"+bytes.fromhex(txid)).digest()[:8],"little")
        for threshold in (128,192,256,384,512,768,1024):
            count=len(a.fragments(size)) if size>threshold else 0
            for lookup,overflow,lb,ob in (("temporal","global",1,1),("coarse","global",1,1),("hash","global",4,1),("global","global",1,1),("hash","broad",4,1),("hash","hash",4,4),("hash","global",16,1),("hash","hash",16,16),("hash","global",64,1)):
                l=h//50000 if lookup=="temporal" else (h//1000000 if lookup=="coarse" else (lh%lb if lookup=="hash" else 0))
                for cover in (0,3):
                    queries=max(count,cover)
                    o=(oh%ob if overflow=="hash" else (h//1000000 if overflow=="broad" else 0)) if queries else "none"
                    initial=2 if cover else len(set(a.choices(txid,4096,bucket=l)))
                    name=f"{threshold}:{lookup}/{overflow}:{lb}/{ob}:cover{cover}"
                    key=json.dumps([l,o,"frozen",[1,1 if queries else 0],initial+queries,queries,"unmodeled"],separators=(",",":"))
                    routes[name][key]+=1
                    if threshold==128 and lookup=="hash" and lb==4 and overflow=="global" and cover==3:
                        for label,width in (("refresh100",100),("timing10",10)):
                            key=json.dumps([l,o,h//width if label=="refresh100" else "frozen",[1,1],initial+queries,queries,h//width if label=="timing10" else "unmodeled"],separators=(",",":"))
                            routes[name+":"+label][key]+=1
    totals["transparent_fee_oracles"]=fee_oracles
    return dict(height=frame["height"],hash=frame["hash"],stratum=frame["stratum"],totals=dict(totals),scripts=dict(scripts),
        hist={c:{g:sorted(v.items()) for g,v in groups.items()} for c,groups in hist.items()},
        routes={name:dict(counts) for name,counts in routes.items()})


def extract(checkpoint,binary,status=None,follow=False):
    """Each accepted block result is atomic; restart skips checksum-bound blocks."""
    checkpoint=Path(checkpoint);out=checkpoint/"summaries";out.mkdir(exist_ok=True)
    records_dir=checkpoint/"records";records_dir.mkdir(exist_ok=True)
    identities=sqlite3.connect(checkpoint/"identities.sqlite")
    identities.execute("CREATE TABLE IF NOT EXISTS txids (txid TEXT PRIMARY KEY, height INTEGER NOT NULL)")
    design=json.loads((checkpoint/"plan.json").read_text())
    process=subprocess.Popen([str(binary),"--stream"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
    try:
        for s in design["strata"]:
            for h in s["heights"]:
                source=checkpoint/"blocks"/f"{h}.json";dest=out/f"{h}.json"
                while not source.exists():
                    if not follow:raise ValueError("acquisition incomplete")
                    time.sleep(1)
                sha=census.digest(source)
                if dest.exists():
                    if json.loads(dest.read_text())["source_sha256"]!=sha:raise ValueError("checkpoint source changed")
                    continue
                frame=json.loads(source.read_text())
                canonical={k:frame[k] for k in ("height","hash","raw_block")}
                canonical["parents"]=[dict(txid=p["txid"],hex=p["hex"]) for p in frame["parents"]]
                process.stdin.write(json.dumps(canonical,separators=(",",":"))+"\n");process.stdin.flush()
                line=process.stdout.readline()
                if not line:raise RuntimeError("canonical streaming extractor stopped; inspect stderr")
                extracted=json.loads(line)
                result=block_summary(frame,extracted);result["source_sha256"]=sha
                for r in extracted["records"]:
                    existing=identities.execute("SELECT height FROM txids WHERE txid=?",(r["txid_internal"],)).fetchone()
                    if existing is not None and existing[0]!=h:raise ValueError("duplicate transaction identity across sampled blocks")
                    identities.execute("INSERT OR IGNORE INTO txids VALUES (?,?)",(r["txid_internal"],h))
                identities.commit()
                census.atomic_json(records_dir/f"{h}.json",extracted)
                result["canonical_records_sha256"]=census.digest(records_dir/f"{h}.json")
                census.atomic_json(dest,result)
        process.stdin.close()
        if process.wait()!=0:raise RuntimeError("canonical extractor failed")
    finally:
        identities.close()
        if process.poll() is None:process.terminate();process.wait()


def group_blocks(blocks,design):
    result={s["name"]:[] for s in design["strata"]}
    for b in blocks:result[b["stratum"]].append(b)
    for s in design["strata"]:
        if sorted(b["height"] for b in result[s["name"]])!=s["heights"]:raise ValueError("incomplete/duplicate selected block membership")
    return result


def report(data):
    design=data["design"];groups=group_blocks(data["blocks"],design)
    def vals(getter):return {s:[getter(b) for b in bs] for s,bs in groups.items()}
    denom=vals(lambda b:b["totals"]["eligible"])
    totals={k:estimate(vals(lambda b:b["totals"].get(k,0)),design) for k in sorted({k for b in data["blocks"] for k in b["totals"]})}
    result=dict(schema="txid-sizing-survey-analysis-v2",qualification="weighted probability-sample estimates; full census and minimum-population qualification unavailable",
        ci_limitations="Pointwise normal block-cluster/FPC intervals; asymptotic, not simultaneous. Sparse/heavy-tail classes may have poor normal coverage. Zero-observed-tail Wald intervals withheld. No true maximum/minimum or absence proof.",
        blocks=len(data["blocks"]),totals=totals,codecs={},routing={})
    for codec in a.CODECS:
        weighted=Counter()
        for s in design["strata"]:
            for b in groups[s["name"]]:
                for size,count in b["hist"][codec]["all"]:weighted[size]+=count*s["N"]/s["n"]
        frontiers={str(p):hist_quantile(weighted,p,strict=p==.8) for p in (.5,.8,.85,.9,.95,.99,.999)}
        cutoffs=sorted(set(a.THRESHOLDS)|{v for v in frontiers.values() if v is not None})
        thresholds={}
        for t in cutoffs:
            def metric(b,kind):
                value=0
                for size,count in b["hist"][codec]["all"]:
                    inline=size<=t;fr=a.fragments(size,codec!="display-v1") if not inline else []
                    if kind=="inline":v=int(inline)
                    elif kind=="overflow":v=int(not inline)
                    elif kind=="fragments":v=len(fr)
                    elif kind=="cover3_requests":v=max(3,len(fr))
                    elif kind=="payload_bytes":v=size
                    elif kind=="overflow_entry_bytes":v=sum(2+header+chunk for _,chunk,header in fr)
                    elif codec=="display-v1":v=48+(size if inline else 0)
                    else:
                        v=35+len(a.uleb(size))+(size if inline else len(a.uleb(len(fr)))+(1 if kind=="directory_entry_bytes_min" else 5))
                    value+=v*count
                return value
            row={kind:estimate(vals(lambda b,k=kind:metric(b,k)),design) for kind in ("inline","overflow","fragments","cover3_requests","payload_bytes","directory_entry_bytes_min","directory_entry_bytes_max","overflow_entry_bytes")}
            row["coverage"]=estimate(vals(lambda b:metric(b,"inline")),design,denom,proportion=True)
            row["attainable_single_directory_row"]=t<=4044
            row["overflow_row_requests_per_open"]=estimate(vals(lambda b:metric(b,"fragments")),design,denom)
            thresholds[str(t)]=row
        result["codecs"][codec]=dict(weighted_frontiers_bytes=frontiers,thresholds=thresholds,
            frontier_intervals=frontier_intervals(groups,design,codec),
            coinbase_frontiers=frontier_intervals(groups,design,codec,"coinbase"),
            non_coinbase_frontiers=frontier_intervals(groups,design,codec,"non_coinbase"),
            maximum_observed_bytes=max(weighted),qualified_true_maximum_bytes=None,
            packing_basis="additive exact envelopes/fragments; compact locator 1..5 bytes. Full-chain row occupancy/segments/slack not replayed.")
    for name,class_moments in sorted(data["route_moments"].items()):
        keys=sorted(class_moments)
        populations=[];observed=[];below={str(f):0 for f in (1000,10000)};upper_below={str(f):0 for f in (1000,10000)}
        largest_small=[]
        for key in keys:
            e=estimate_moments(class_moments[key],design)
            populations.append(e["estimate"]);observed.append(e["sample_sum"])
            for f in (1000,10000):
                below[str(f)]+=e["estimate"]<f
                upper_below[str(f)]+=e["ci95"] is not None and e["ci95"][1]<f
            largest_small.append(dict(transcript=json.loads(key),**e))
        candidates=sorted(largest_small,key=lambda e:e["estimate"])
        result["routing"][name]=dict(observed_classes=len(keys),sample_minimum_distinct_txids=min(observed),
            class_weighted_estimated_population_percentiles=a.percentiles(populations),
            transaction_weighted_estimated_population_percentiles={str(p):hist_quantile(Counter({v:sum(x for x in populations if x==v) for v in populations}),p) for p in (.01,.05,.5,.95,.99)},
            observed_classes_estimated_below_policy=below,observed_classes_ci_upper_below_policy=upper_below,
            estimated_transactions_in_observed_classes_below_policy={str(f):sum(e["estimate"] for e in candidates if e["estimate"]<f) for f in (1000,10000)},
            smallest_observed_class_estimates=candidates[:5],qualified_true_minimum=None,
            transcript_assumptions="frozen uniform segment vector unless named refresh100/timing10; exact txid lookup coincidences, canonical size-derived contiguous fragment requests; no prior-history conditioning, no live-layout replay")
    result["strata"]={s["name"]:dict(population_blocks=s["N"],sample_blocks=s["n"],
        sampled_transactions=sum(b["totals"]["all_transactions"] for b in groups[s["name"]]),
        sampled_eligible=sum(b["totals"]["eligible"] for b in groups[s["name"]]),
        sampled_outputs=sum(b["totals"]["outputs"] for b in groups[s["name"]]),
        coinbase=sum(b["totals"].get("coinbase",0) for b in groups[s["name"]]),
        frontiers={}) for s in design["strata"]}
    for s in design["strata"]:
        result["strata"][s["name"]]["frontiers"]={}
        for c in a.CODECS:
            hist=Counter()
            for b in groups[s["name"]]:hist.update(dict(b["hist"][c]["all"]))
            result["strata"][s["name"]]["frontiers"][c]={str(p):hist_quantile(hist,p,strict=p==.8) for p in (.5,.8,.85,.9,.95,.99)}
    return result

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest="command",required=True)
    ex=sub.add_parser("extract");ex.add_argument("checkpoint",type=Path);ex.add_argument("binary",type=Path);ex.add_argument("--follow",action="store_true")
    re=sub.add_parser("report");re.add_argument("input",type=Path);re.add_argument("output",type=Path)
    args=parser.parse_args()
    if args.command=="extract":extract(args.checkpoint,args.binary,follow=args.follow)
    else:census.atomic_json(args.output,report(json.loads(args.input.read_text())))
