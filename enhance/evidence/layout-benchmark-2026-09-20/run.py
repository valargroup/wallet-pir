import pathlib,subprocess,json,datetime,time,os,hashlib
root=pathlib.Path('/tmp/wallet-pir-layout-20260920');out=root/'results';out.mkdir(exist_ok=False)
exe=root/'target/release/enhance-layout-benchmark'
env=dict(os.environ,RAYON_NUM_THREADS='4',BENCH_INIT_TEMPLATE=str(root/'init-template.json'))
manifest={'start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary_sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'cases':[],'rayon_threads':4,'cpu_limit':'AllowedCPUs=0-3; CPUQuota=400%','memory_limit':'10G','workload':'deterministic synthetic 737-byte records, real production shard runtime/combiner, fresh randomized queries, first/last/poly/shard boundaries plus distributed interior rows'}
(out/'hardware.txt').write_text(subprocess.check_output(['lscpu'],text=True)+subprocess.check_output(['uname','-a'],text=True)+pathlib.Path('/proc/meminfo').read_text())
cases=[(f'n450163-k{k}',k,450163,100,0) for k in [9,58,19,77,29,38,14]]+ [('n450163-k9-repeat',9,450163,100,0)]+[(f'n1000000-k{k}',k,1000000,60,0) for k in [9,29,63]]+[(f'retention-k{k}',k,450163,0,r) for k,r in [(19,3),(38,3),(58,8),(77,2)]]
for name,k,n,q,r in cases:
 active=subprocess.check_output(['ps','-eo','comm='],text=True)
 if 'Runner.Worker' in active:
  manifest['stopped']='CI job became active';break
 command=['/usr/bin/time','-v','-o',str(out/(name+'.time')),str(exe),str(k),str(n),str(q),str(r)]
 step={'name':name,'command':command,'started':datetime.datetime.now(datetime.timezone.utc).isoformat()};manifest['cases'].append(step);t=time.monotonic()
 with open(out/(name+'.jsonl'),'w') as stdout,open(out/(name+'.stderr'),'w') as stderr:
  result=subprocess.run(command,env=env,stdout=stdout,stderr=stderr,timeout=900)
 step.update(exit_code=result.returncode,wall_seconds=time.monotonic()-t);(out/'manifest.json').write_text(json.dumps(manifest,indent=2));print(json.dumps(step),flush=True)
 if result.returncode:manifest['stopped']='case failed';break
manifest['ended']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'manifest.json').write_text(json.dumps(manifest,indent=2));print('FINISHED',flush=True)
