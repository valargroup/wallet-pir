#!/usr/bin/env python3
"""Exact-fee sizing bounds and joint-transcript estimates from clustered blocks."""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import time
import math
import random
from statistics import NormalDist
import analyze as a
import census
import survey

THRESHOLDS=(128,192,256,384,512,768,1024)
CODECS=a.CODECS
MAX_FEE_BYTES=len(a.uleb(a.MAX_MONEY))

def fee_size_bounds(record, codec='display-v1'):
    baseline=len(a.payload(record,codec))
    if record['fee']=='unknown': return baseline+1,baseline+MAX_FEE_BYTES
    return baseline,baseline

def json_gz(path, data):
    path.write_bytes(gzip.compress((json.dumps(data,separators=(',',':'),sort_keys=True)+'\n').encode(),mtime=0))

def read(path):
    b=path.read_bytes()
    return json.loads(gzip.decompress(b) if path.suffix=='.gz' else b)

def block_summary(frame, extracted):
    block=frame['rpc_block'];records=extracted['records']
    if (extracted['height'],extracted['hash'])!=(frame['height'],frame['hash']):raise ValueError('block pin')
    if extracted['transactions']!=block['nTx'] or len(extracted['all_txids'])!=block['nTx']:raise ValueError('transaction inventory')
    if block.get('previousblockhash') is not None and block['previousblockhash']!=extracted['previousblockhash']:raise ValueError('parent pin')
    if extracted['all_txids'] != [bytes.fromhex(t['txid'])[::-1].hex() for t in block['tx']]:raise ValueError('all transaction identities')
    if len(set(extracted['all_txids']))!=block['nTx']:raise ValueError('duplicate transaction')
    if len([t for t in block['tx'] if t['vin'] or t['vout']])!=len(records):raise ValueError('eligibility')
    totals=Counter(all_transactions=block['nTx'],eligible=len(records),shielded_only=extracted['shielded_only'])
    hist={f'{c}/{bound}':{g:Counter() for g in ('all','coinbase','non_coinbase')} for c in CODECS for bound in ('lower','upper')}
    scripts=Counter();shapes=[];pairs={c:Counter() for c in CODECS}
    for r in records:
        t=block['tx'][r['transaction_index']]
        if bytes.fromhex(t['txid'])[::-1].hex()!=r['txid_internal']:raise ValueError('record identity')
        cb=any('coinbase' in v for v in t['vin']);ni=sum('txid' in v for v in t['vin'])
        if (cb,ni)!=(r['coinbase'],r['input_count']):raise ValueError('input/coinbase shape')
        outputs=[dict(value=o.get('valueZat',int(Decimal(str(o['value']))*100000000)),script=o['scriptPubKey']['hex']) for o in t['vout']]
        if outputs!=r['outputs']:raise ValueError('exact raw outputs')
        if a.payload(r).hex()!=r['display_v1_hex']:raise ValueError('Rust/Python codec bytes')
        # RPC pool fields independently check presence, without decoding raw txs.
        shielded=bool(t.get('vjoinsplit') or t.get('vShieldedSpend') or t.get('vShieldedOutput') or t.get('orchard',{}).get('actions'))
        if shielded!=r['shielded_components']:raise ValueError('shared shielded-component flag')
        g='coinbase' if cb else 'non_coinbase'
        totals[g]+=1;totals['outputs']+=len(outputs);totals['transparent_inputs']+=ni
        totals['input_only']+=ni>0 and not outputs;totals['shielded_components']+=r['shielded_components']
        totals['fee_unknown']+=r['fee']=='unknown';totals['fee_not_applicable']+=r['fee']=='not_applicable'
        totals['fee_exact_zero']+=r['fee']==0;totals['fee_exact_nonzero']+=isinstance(r['fee'],int) and r['fee']>0
        for o in outputs:
            raw=bytes.fromhex(o['script']);tag=a.script_encode(raw)[0]
            scripts[str(tag)]+=1;totals['empty_scripts']+=not raw;totals['op_return_scripts']+=bool(raw) and raw[0]==106
            totals['raw_escape_outputs']+=tag==0
        sizes={c:fee_size_bounds(r,c) for c in CODECS}
        for c,(lo,hi) in sizes.items():
            pairs[c][(lo,hi)]+=1
            for bound,size in (('lower',lo),('upper',hi)):
                hist[f'{c}/{bound}']['all'][size]+=1;hist[f'{c}/{bound}'][g][size]+=1
        # Public identity hash only; raw scripts remain in retained external inputs.
        lh=int.from_bytes(hashlib.sha256(b'txid-sizing/lookup/'+bytes.fromhex(r['txid_internal'])).digest()[:8],'little')
        oh=int.from_bytes(hashlib.sha256(b'txid-sizing/overflow/'+bytes.fromhex(r['txid_internal'])).digest()[:8],'little')
        shapes.append([lh,oh,len(set(a.choices(r['txid_internal'],4096))),*sizes['display-v1']])
    return dict(height=frame['height'],hash=frame['hash'],stratum=frame['stratum'],totals=dict(totals),scripts=dict(scripts),
        hist={c:{g:sorted(v.items()) for g,v in gs.items()} for c,gs in hist.items()},shapes=shapes,
        size_pairs={c:[[lo,hi,n] for (lo,hi),n in sorted(ps.items())] for c,ps in pairs.items()})

def extract(root,binary,status):
    root=Path(root);design=read(root/'plan.json');(root/'summaries').mkdir(exist_ok=True);(root/'records').mkdir(exist_ok=True)
    process=subprocess.Popen([str(binary),'--shape-stream'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
    completed=0;seen=set();start=time.monotonic();pins=[]
    try:
        for s in design['strata']:
            for h in s['heights']:
                path=root/'blocks'/f'{h}.json';dest=root/'summaries'/f'{h}.json';rp=root/'records'/f'{h}.json'
                while not path.exists():time.sleep(.25)
                frame=read(path);sha=census.digest(path)
                # Always replay canonical extraction, including after a restart.
                process.stdin.write(json.dumps({k:frame[k] for k in ('height','hash','raw_block')},separators=(',',':'))+'\n');process.stdin.flush()
                line=process.stdout.readline()
                if not line:raise RuntimeError('canonical shape exporter stopped')
                out=json.loads(line)
                for identity in out['all_txids']:
                    if identity in seen:raise ValueError('duplicate across sampled blocks')
                    seen.add(identity)
                b=block_summary(frame,out);b['source_sha256']=sha
                if rp.exists() and read(rp)!=out:raise ValueError('canonical replay changed')
                if not rp.exists():census.atomic_json(rp,out)
                b['canonical_records_sha256']=census.digest(rp)
                if dest.exists() and read(dest)!=b:raise ValueError('summary replay changed')
                if not dest.exists():census.atomic_json(dest,b)
                pins.append({'height':h,'canonical_records_sha256':b['canonical_records_sha256'],'summary_sha256':census.digest(dest)})
                completed+=1
                if completed%256==0:print(json.dumps({'extracted_blocks':completed,'transactions':len(seen),'seconds':round(time.monotonic()-start,2)}),flush=True)
        process.stdin.close()
        if process.wait():raise RuntimeError('canonical extractor failed')
    finally:
        if process.poll() is None:process.terminate();process.wait()
    result=dict(blocks=completed,distinct_all_txids=len(seen),extraction_seconds=time.monotonic()-start,binary_sha256=census.digest(Path(binary)),pins=pins)
    census.atomic_json(root/'extraction-receipt.json',result)
    return result

# Lookup and overflow domain hashes are independent. Segments are fixed across
# each placement bucket; actual layouts, workload timing and history unmodeled.
GEOMETRIES=(('temporal','global',1,1),('coarse','global',1,1),('global','global',1,1),('hash','global',4,1),('hash','global',16,1),('hash','global',64,1),('hash','broad',4,1),('hash','hash',4,4),('hash','hash',16,16))

def transcript_keys(shape,height,t,geometry,cover=3,cohort=None):
    lh,oh,initial,lo,hi=shape;lookup,overflow,lb,ob=geometry
    l=height//50000 if lookup=='temporal' else (height//1000000 if lookup=='coarse' else (lh%lb if lookup=='hash' else 0))
    keys=set()
    for size in (lo,hi):
        count=(size+a.FRAGMENT-1)//a.FRAGMENT if size>t else 0
        q=max(cover,count)
        o=(oh%ob if overflow=='hash' else (height//1000000 if overflow=='broad' else 0)) if q else 'none'
        revision=height//100 if cohort=='refresh100' else 'frozen'
        timing=height//10 if cohort=='timing10' else 'unmodeled'
        keys.add(json.dumps([l,o,revision,[1,1 if q else 0],(2 if cover else initial)+q,q,timing],separators=(',',':')))
    return keys

def route_report(data,thresholds):
    design=data['design'];moments=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:[0,0,0,0])))
    scenarios=[]
    for t in thresholds:
        for geometry in GEOMETRIES:
            for cover in (0,3):scenarios.append((t,geometry,cover,None))
        if t==128 or t==192:
            for cohort in ('refresh100','timing10'):scenarios.append((t,('hash','global',4,1),3,cohort))
    for b in data['blocks']:
        for t,geo,cover,cohort in scenarios:
            name=f'{t}:{geo[0]}/{geo[1]}:{geo[2]}/{geo[3]}:cover{cover}'+(':'+cohort if cohort else '')
            guaranteed=Counter();possible=Counter()
            for shape in b['shapes']:
                keys=transcript_keys(shape,b['height'],t,geo,cover,cohort)
                possible.update(keys)
                if len(keys)==1:guaranteed.update(keys)
            for key,n in possible.items():
                g=guaranteed[key];pair=moments[name][key][b['stratum']];pair[0]+=g;pair[1]+=g*g;pair[2]+=n;pair[3]+=n*n
    results={}
    for name,classes in sorted(moments.items()):
        candidates=[]
        for key,ms in sorted(classes.items()):
            lo=survey.estimate_moments({s:v[:2] for s,v in ms.items()},design)
            hi=survey.estimate_moments({s:v[2:] for s,v in ms.items()},design)
            if lo['sample_sum']==0:lo['ci95']=None
            candidates.append(dict(transcript=json.loads(key),guaranteed=lo,possible=hi))
        candidates.sort(key=lambda v:v['guaranteed']['estimate'])
        results[name]=dict(observed_possible_classes=len(candidates),qualified_population_minimum=None,
            sample_distinct_guaranteed_min=min(v['guaranteed']['sample_sum'] for v in candidates),
            sample_distinct_possible_min=min(v['possible']['sample_sum'] for v in candidates),
            smallest_classes=candidates[:5],
            guaranteed_estimated_class_percentiles=a.percentiles([v['guaranteed']['estimate'] for v in candidates]),
            possible_estimated_class_percentiles=a.percentiles([v['possible']['estimate'] for v in candidates]),
            observed_classes_possible_estimate_below_policy={str(k):sum(v['possible']['estimate']<k for v in candidates) for k in (5,1000,10000)},
            observed_classes_possible_ci_upper_below_policy={str(k):sum(v['possible']['ci95'][1]<k for v in candidates) for k in (5,1000,10000)},
            limit='Observed classes only; sample cannot bound true population minima or unseen classes; normal intervals sparse-tail limited. Fixed uniform segment vector, no actual time/history replay.')
    return results

def statistics(root):
    root=Path(root);receipt=read(root/'receipt.json');design=receipt['design']
    blocks=[read(root/'summaries'/f'{h}.json') for s in design['strata'] for h in s['heights']]
    info=read(root/'node-info.json')
    eras=[dict(name='Sprout',height=0)]+sorted([dict(name=u['name'],height=u['activationheight']) for u in info['upgrades'].values() if u['activationheight']<=design['anchor_height']],key=lambda u:u['height'])
    return dict(schema='txid-sizing-one-day-statistics-v1',design=design,era_boundaries=eras,blocks=blocks)

def summarize(root):
    """Refresh derived statistics after helper edits; never modify raw inputs."""
    root=Path(root);design=read(root/'plan.json');pins=[]
    for s in design['strata']:
        for h in s['heights']:
            raw=root/'blocks'/f'{h}.json';rp=root/'records'/f'{h}.json';dest=root/'summaries'/f'{h}.json'
            old=read(dest)
            if census.digest(raw)!=old['source_sha256'] or census.digest(rp)!=old['canonical_records_sha256']:raise ValueError('retained source changed')
            b=block_summary(read(raw),read(rp));b['source_sha256']=old['source_sha256'];b['canonical_records_sha256']=old['canonical_records_sha256']
            census.atomic_json(dest,b);pins.append({'height':h,'summary_sha256':census.digest(dest)})
    census.atomic_json(root/'summary-receipt.json',{'blocks':len(pins),'pins':pins})

def packing_extents(size,threshold,compact=False):
    inline=size<=threshold
    fr=a.fragments(size,compact) if not inline else []
    if compact:
        # Locator range for proposed envelope, not an implemented table layout.
        directory_min=35+len(a.uleb(size))+(size if inline else len(a.uleb(len(fr)))+1)
        directory_max=directory_min+(0 if inline else 4)
    else:directory_min=directory_max=48+(size if inline else 0)
    return dict(directory_entry_bytes_min=directory_min,directory_entry_bytes_max=directory_max,
        overflow_entry_bytes=sum(2+header+chunk for _,chunk,header in fr),fragments=len(fr))

def packing_bound(block,codec,threshold,metric,upper):
    total=0
    for lo,hi,n in block['size_pairs'][codec]:
        # At most eight integer lengths for an unknown fee. Inspect all of them
        # to handle the nonmonotone directory jump at the inline cutoff.
        candidates=[packing_extents(size,threshold,codec!='display-v1')[metric] for size in range(lo,hi+1)]
        total+=n*(max(candidates) if upper else min(candidates))
    return total

def report(data):
    design=data['design'];groups=survey.group_blocks(data['blocks'],design)
    def vals(fn):return {s:[fn(b) for b in bs] for s,bs in groups.items()}
    denom=vals(lambda b:b['totals']['eligible'])
    result=dict(schema='txid-sizing-one-day-analysis-v1',qualification='Probability-sample sizing bounds; no full-population anonymity minimum qualification',blocks=len(data['blocks']),fee_size_policy={'unknown_exact_fee_uleb_bytes':[1,MAX_FEE_BYTES],'max_money':a.MAX_MONEY,'unknown_is_actual_exact_size':False},totals={},codecs={})
    for k in sorted({k for b in data['blocks'] for k in b['totals']}):result['totals'][k]=survey.estimate(vals(lambda b:b['totals'].get(k,0)),design)
    for c in CODECS:
        weighted={bound:Counter() for bound in ('lower','upper')}
        for s in design['strata']:
            for b in groups[s['name']]:
                for bound in weighted:
                    for size,n in b['hist'][c+'/'+bound]['all']:weighted[bound][size]+=n*s['N']/s['n']
        frontiers={str(p):[survey.hist_quantile(weighted['lower'],p),survey.hist_quantile(weighted['upper'],p)] for p in (.85,.9,.95,.99)}
        cutoffs=sorted(set(THRESHOLDS)|{v for pair in frontiers.values() for v in pair})
        rows={}
        for t in cutoffs:
            row={}
            for bound in ('lower','upper'):
                def metric(b,kind):
                    value=0
                    for size,n in b['hist'][c+'/'+bound]['all']:
                        inline=size<=t;fr=a.fragments(size,c!='display-v1') if not inline else []
                        if kind=='inline':v=int(inline)
                        elif kind=='overflow':v=int(not inline)
                        elif kind=='fragments':v=len(fr)
                        elif kind=='payload_bytes':v=size
                        elif kind=='overflow_entry_bytes':v=sum(2+header+chunk for _,chunk,header in fr)
                        elif c=='display-v1':v=48+(size if inline else 0)
                        else:v=35+len(a.uleb(size))+(size if inline else len(a.uleb(len(fr)))+(1 if kind=='directory_entry_bytes_min' else 5))
                        value+=n*v
                    return value
                side={kind:survey.estimate(vals(lambda b,k=kind:metric(b,k)),design) for kind in ('inline','overflow','fragments','payload_bytes','overflow_entry_bytes','directory_entry_bytes_min','directory_entry_bytes_max')}
                side['coverage']=survey.estimate(vals(lambda b:metric(b,'inline')),design,denom,proportion=True)
                row[bound+'_size_population']=side
            row['packing_bounds']={kind:{bound:survey.estimate(vals(lambda b,k=kind,up=bound=='upper':packing_bound(b,c,t,k,up)),design) for bound in ('lower','upper')} for kind in ('directory_entry_bytes_min','directory_entry_bytes_max','overflow_entry_bytes','fragments')}
            rows[str(t)]=row
        result['codecs'][c]=dict(frontier_size_bounds_bytes=frontiers,frontier_ci95={bound:survey.frontier_intervals(groups,design,c+'/'+bound) for bound in ('lower','upper')},thresholds=rows,observed_size_range=[min(weighted['lower']),max(weighted['upper'])],true_maximum=None)
    result['domains']={}
    domains=[('all',lambda b:True)]
    for i,e in enumerate(data['era_boundaries']):
        lo=e['height'];hi=data['era_boundaries'][i+1]['height'] if i+1<len(data['era_boundaries']) else design['anchor_height']+1
        domains.append((e['name'],lambda b,lo=lo,hi=hi:lo<=b['height']<hi))
    for name,inside in domains:
        result['domains'][name]={}
        for category in ('all','coinbase','non_coinbase'):
            den=vals(lambda b:sum(n for _,n in b['hist']['display-v1/lower'][category]) if inside(b) else 0)
            dh={bound:Counter() for bound in ('lower','upper')}
            for s in design['strata']:
                for b in groups[s['name']]:
                    if inside(b):
                        for bound in dh:
                            for size,n in b['hist']['display-v1/'+bound][category]:dh[bound][size]+=n*s['N']/s['n']
            coverage={str(t):{bound:survey.estimate(vals(lambda b:sum(n for size,n in b['hist']['display-v1/'+bound][category] if size<=t) if inside(b) else 0),design,den,proportion=True) for bound in ('lower','upper')} for t in THRESHOLDS}
            result['domains'][name][category]=dict(eligible=survey.estimate(den,design),sample_blocks=sum(inside(b) for b in data['blocks']),coverage=coverage,frontier_size_bounds_bytes={str(p):[survey.hist_quantile(dh['lower'],p),survey.hist_quantile(dh['upper'],p)] for p in (.85,.9,.95,.99)})
        result['domains'][name]['totals']={k:survey.estimate(vals(lambda b:b['totals'].get(k,0) if inside(b) else 0),design) for k in ('all_transactions','eligible','shielded_only','outputs','input_only','raw_escape_outputs')}
    routed_thresholds=sorted(set(THRESHOLDS)|{v for pair in result['codecs']['display-v1']['frontier_size_bounds_bytes'].values() for v in pair if v<=4044})
    result['routing']=route_report(data,routed_thresholds)
    # Select from the seven prespecified cutoffs with a one-sided Bonferroni
    # normal lower bound, using worst-case fee membership. Still asymptotic.
    z=NormalDist().inv_cdf(1-.05/len(THRESHOLDS))
    selected=None;selection={}
    for t in THRESHOLDS:
        coverage=result['codecs']['display-v1']['thresholds'][str(t)]['upper_size_population']['coverage']
        lower=max(0,coverage['estimate']-z*coverage['standard_error'])
        selection[str(t)]=dict(worst_case_estimate=coverage['estimate'],familywise_normal_lower95=lower)
        if selected is None and lower>.8:selected=t
    result['threshold_decision']=dict(selected_bytes=selected,requirement='strictly greater than 80% of distinct eligible records',candidates=selection,z=z,method='one-sided 95% Bonferroni normal lower bounds over seven prespecified cutoffs; block clustering/FPC; conservative unknown-fee membership; asymptotic, not a full census guarantee')
    result['ci_limitations']='HT totals; block-cluster ratio linearization with stratum FPC; pointwise normal 95% except explicit seven-cutoff Bonferroni selection. Asymptotic, not formal guarantees. Sparse/extreme tails and heavy block counts can have poor coverage. Zero-observed tails unresolved.'
    result['packing_limitations']='Additive payload/envelope/fragment estimates, not a full-chain packing replay. packing_bounds evaluates per-record fee-length possibilities including the nonmonotone inline jump. No measured native latency/RSS.'
    return result

def bootstrap_selection(data,repetitions=2000):
    """Sensitivity check: stratified rescaled cluster bootstrap, no tx IID draws."""
    design=data['design'];groups=survey.group_blocks(data['blocks'],design)
    clusters=[]
    for s in design['strata']:
        vectors=[]
        for b in groups[s['name']]:
            hist=b['hist']['display-v1/upper']['all']
            vectors.append([b['totals']['eligible']]+[sum(n for size,n in hist if size<=t) for t in THRESHOLDS])
        means=[sum(v[k] for v in vectors)/s['n'] for k in range(8)]
        clusters.append((s,vectors,means,math.sqrt(1-s['n']/s['N'])))
    rng=random.Random('wallet-pir/day/cluster-bootstrap/v1')
    ratios=[[] for _ in THRESHOLDS]
    for _ in range(repetitions):
        totals=[0.0]*8
        for s,vectors,means,fpc in clusters:
            sums=[0]*8
            for i in rng.choices(range(s['n']),k=s['n']):
                vector=vectors[i]
                for k in range(8):sums[k]+=vector[k]
            for k in range(8):totals[k]+=s['N']*(means[k]+fpc*(sums[k]/s['n']-means[k]))
        for k in range(7):ratios[k].append(totals[k+1]/totals[0])
    out={}
    for t,rs in zip(THRESHOLDS,ratios):
        rs.sort()
        out[str(t)]=dict(ci95=[rs[int(.025*repetitions)],rs[min(repetitions-1,int(.975*repetitions))]],bonferroni_lower95=rs[int(.05/7*repetitions)])
    return dict(schema='txid-sizing-cluster-bootstrap-v1',seed='wallet-pir/day/cluster-bootstrap/v1',repetitions=repetitions,method='resample whole blocks within original strata; centered stratum means rescaled by sqrt(1-f); percentile sensitivity, asymptotic',worst_case_fee_coverage=out)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    ex=sub.add_parser('extract');ex.add_argument('checkpoint',type=Path);ex.add_argument('binary',type=Path);ex.add_argument('--status',type=Path,required=True)
    re=sub.add_parser('report');re.add_argument('input',type=Path);re.add_argument('output',type=Path)
    st=sub.add_parser('statistics');st.add_argument('checkpoint',type=Path);st.add_argument('output',type=Path)
    sm=sub.add_parser('summarize');sm.add_argument('checkpoint',type=Path)
    bs=sub.add_parser('bootstrap');bs.add_argument('input',type=Path);bs.add_argument('output',type=Path)
    args=p.parse_args()
    if args.command=='extract':print(json.dumps(extract(args.checkpoint,args.binary,args.status)))
    elif args.command=='summarize':summarize(args.checkpoint)
    elif args.command=='bootstrap':census.atomic_json(args.output,bootstrap_selection(read(args.input)))
    elif args.command=='statistics':json_gz(args.output,statistics(args.checkpoint))
    else:census.atomic_json(args.output,report(read(args.input)))
