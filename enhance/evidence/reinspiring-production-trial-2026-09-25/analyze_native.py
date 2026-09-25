#!/usr/bin/env python3
"""Compare saved router histogram deltas. Quantiles interpolate histogram buckets."""
import datetime,json,pathlib,re,tarfile
root=pathlib.Path(__file__).resolve().parent

def metric(s,name):
    m=re.search(r'^'+re.escape(name)+r' (\S+)$',s,re.M)
    return float(m[1]) if m else 0

def summarize(rows,start,end):
    rows=[r for r in rows if start<=r['at']<=end]
    if len(rows)<2:return None
    a,b=rows[0],rows[-1];ma,mb=a['metrics'],b['metrics']
    result={'start_utc':datetime.datetime.fromtimestamp(a['at'],datetime.timezone.utc).isoformat(),'seconds':b['at']-a['at'],'samples':len(rows),'peak_memory_mib':max(int(r['memory.peak']) for r in rows)/2**20}
    cpu=lambda r:int(dict(line.split() for line in r['cpu.stat'].splitlines())['usage_usec'])
    result['mean_cpu_cores']=(cpu(b)-cpu(a))/1e6/result['seconds']
    result['stages']={}
    for stage in ['packing','worker','total']:
        name='enhance_query_stage_duration_seconds_'
        count=metric(mb,name+'count{stage="'+stage+'"}')-metric(ma,name+'count{stage="'+stage+'"}')
        total=metric(mb,name+'sum{stage="'+stage+'"}')-metric(ma,name+'sum{stage="'+stage+'"}')
        def buckets(s):
            out={}
            for line in s.splitlines():
                if line.startswith(name+'bucket{') and 'stage="'+stage+'"' in line:
                    bound=float(re.search(r'le="([^"]+)"',line)[1]);out[bound]=float(line.rsplit(' ',1)[1])
            return out
        ba,bb=buckets(ma),buckets(mb)
        deltas=sorted((k,v-ba.get(k,0)) for k,v in bb.items())
        def quantile(q):
            prev_x=prev_n=0
            for x,n in deltas:
                if n>=count*q and n>prev_n:return 1000*(prev_x+(x-prev_x)*(count*q-prev_n)/(n-prev_n))
                prev_x,prev_n=x,n
        result['stages'][stage]={'count':count,'mean_ms':total/count*1000 if count else None,'p50_ms_estimated':quantile(.5),'p99_ms_estimated':quantile(.99)}
    for name in ['enhance_packing_router_artifact_loads_total','enhance_packing_router_artifact_load_microseconds_total','enhance_packing_router_artifact_load_failures_total','enhance_packing_preparations_total']:
        result[name]=metric(mb,name)-metric(ma,name)
    result['memory_events_end']=dict(line.split() for line in b['memory.events'].splitlines())
    return result

baseline=[]
with tarfile.open(root/'raw/production-resources.tar.gz') as t:
    for m in t:
        if m.name=='apm-load-15qps-20260925-0456/production-resources.jsonl':baseline=[json.loads(s) for s in t.extractfile(m) if s.strip()]
native=[json.loads(s) for s in (root/'raw/native-resources.jsonl').read_text().splitlines()]
stamp=lambda s:datetime.datetime.fromisoformat(s).timestamp()
output={'baseline_20qps':summarize(baseline,stamp('2026-09-25T06:15:00+00:00'),stamp('2026-09-25T06:22:00+00:00')),'native_batches':[]}
with tarfile.open(root/'raw/native-load-early.tar.gz') as t:
    batches=[];reports={}
    for m in t:
        if m.name.endswith('batches.jsonl'):batches=[json.loads(s) for s in t.extractfile(m)]
        elif re.search(r'/load-\d+-\d+qps.json$',m.name):reports[pathlib.Path(m.name).name]=json.load(t.extractfile(m))
    for b in batches:
        b['report_data']=reports.get(pathlib.Path(b['report']).name)
        b['router_window']=summarize(native,stamp(b['utc_start']),stamp(b['utc_end']))
        output['native_batches'].append(b)
print(json.dumps(output,indent=2))
