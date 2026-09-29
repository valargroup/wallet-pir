import json,pathlib,sys
base=pathlib.Path(sys.argv[1])
old=json.loads(pathlib.Path('transparent/evidence/v9-cutover-2026-09-28/load/v9-pinned-live-2.json').read_text())
new=json.loads((base/'load-4.json').read_text())
rows=[]
for name,current in new['steps'][0]['classes'].items():
 def summarize(c):
  q=[v for k,v in c['stages'].items() if k.startswith('query_')]
  calls=sum(v['calls'] for v in q);up=sum(v['bytes_up'] for v in q)
  return {'attempts':c['n'],'completed':c['completed'],'exact':c['exact'],'failed':c['failed'],'incomplete_by_reason':c['incomplete_by_reason'],'latency_all_attempts':c['sync_seconds'],'logical_queries':calls,'key_upload_bytes_derived':calls*27648,'non_key_query_upload_bytes_derived':up-calls*27648,'private_response_bytes':sum(v['bytes_down'] for v in q),'cacheable_setup_payload_bytes':sum(v['bytes_down'] for k,v in c['stages'].items() if k.startswith('setup_')),'filter_payload_bytes':sum(v['bytes_down'] for k,v in c['stages'].items() if k.startswith('filters')),'stage_payload_bytes_all_attempts':sum(v['bytes_up']+v['bytes_down'] for v in c['stages'].values())}
 a=summarize(old['steps'][0]['classes'][name]);b=summarize(current)
 rows.append({'class':name,'v9':a,'v10':b,'logical_payload_per_attempt_change_percent':100*((b['stage_payload_bytes_all_attempts']/b['attempts'])/(a['stage_payload_bytes_all_attempts']/a['attempts'])-1)})
result={'baseline':'transparent/evidence/v9-cutover-2026-09-28/load/v9-pinned-live-2.json','new':'load-4.json','byte_accounting':'Logical wallet-stage payloads. All attempts included. Excludes HTTP/TLS overhead and retransmissions hidden by transport retries. Key upload derived as 27648 bytes per native query; non-key upload is the remainder. Cached public setup distinguished from uploads/responses.','completion_definition':'Synthetic range coverage with exact digest; unresolved-spends-only outcomes count as completed while the wallet withholds its anchor. Query-budget outcomes are incomplete.','comparison_limits':'Same sample and bounded four-client flags, different dates/tips/background workloads; not a controlled latency or capacity comparison.','classes':rows}
(base/'comparison-v9.json').write_text(json.dumps(result,indent=2)+'\n')
for r in rows:
 a,b=r['v9'],r['v10'];print(r['class'],str(b['exact'])+'/'+str(b['attempts']), 'p50',a['latency_all_attempts']['p50'],b['latency_all_attempts']['p50'],'payload/attempt %',round(r['logical_payload_per_attempt_change_percent'],2))
