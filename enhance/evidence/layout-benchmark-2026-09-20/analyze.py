import pathlib,json,statistics,math,re
root=pathlib.Path(__file__).resolve().parent
rows=[];total=0;allq=[]
for folder in ['results','supplemental']:
 m=json.loads((root/folder/'manifest.json').read_text());assert 'ended' in m and 'stopped' not in m
 assert all(c['exit_code']==0 for c in m['cases'])
 for p in sorted((root/folder).glob('*.jsonl')):
  events=[json.loads(l) for l in p.read_text().splitlines()];s=events[0];q=[e for e in events if e['event']=='query'];allq+=q
  assert all(e['exact'] and e['noise_error']<e['noise_threshold']/4 for e in q)
  measured=[e for e in q if not e['warmup']];total+=len(measured)
  d={'case':p.stem,**{k:s[k] for k in ['records','records_per_row','shard_rows','logical_rows','instances','shards']},'queries':len(measured)}
  rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',p.with_suffix('.time').read_text())[1])*1024;d['process_peak_rss_bytes']=rss
  if measured:
   init=next(e for e in events if e['event']=='packing_ready');d.update({k:init[k] for k in ['serialized_init_bytes','public_params_bytes','base64_public_params_bytes']})
   d.update({k:measured[0][k] for k in ['upload_bytes','response_bytes']});d['total_query_bytes']=d['upload_bytes']+d['response_bytes']
   for key in ['server_ms','scan_ms','pack_ms','prepare_ms','decode_ms']:
    vals=sorted(e[key] for e in measured);d[key+'_p50']=statistics.median(vals);d[key+'_p95']=vals[math.ceil(.95*len(vals))-1]
   d['client_ms_p50']=statistics.median(e['prepare_ms']+e['decode_ms'] for e in measured)
   d['worst_noise_fraction']=max(e['noise_error']/e['noise_threshold'] for e in q)
  else:
   d['retention_events']=[e for e in events if e['event'] in ['retention_baseline','retained_revision']]
  rows.append(d)
summary={'measured_queries':total,'including_warmup_queries':len(allq),'all_exact':True,'worst_noise_fraction':max(e['noise_error']/e['noise_threshold'] for e in allq),'cases':rows}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for r in rows:
 if r['queries']:print(r['case'],*[round(r[k]/1024,3) for k in ['upload_bytes','response_bytes','total_query_bytes','serialized_init_bytes']],*[round(r[k],2) for k in ['server_ms_p50','server_ms_p95','scan_ms_p50','pack_ms_p50','client_ms_p50']])
 else:print(r['case'],'peak GiB',r['process_peak_rss_bytes']/2**30,'final RSS',r['retention_events'][-1]['memory']['VmRSS']/2**30)
print({k:v for k,v in summary.items() if k!='cases'})
