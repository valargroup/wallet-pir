import pathlib,subprocess,json,datetime,time,os
root=pathlib.Path('/tmp/wallet-pir-layout-20260920')
while subprocess.run(['systemctl','is-active','--quiet','wallet-pir-layout-benchmark-20260920']).returncode==0:time.sleep(5)
out=root/'supplemental';out.mkdir(exist_ok=False);env=dict(os.environ,RAYON_NUM_THREADS='4',BENCH_INIT_TEMPLATE=str(root/'init-template.json'))
cases=[('current-k9',9,466986,100,0,8192),('current-k29',29,466986,100,0,8192),('current-k58',58,466986,100,0,8192),('current-k58-shard4096',58,466986,100,0,4096),('current-k58-shard2048',58,466986,100,0,2048),('retention-k58-shard4096',58,466986,0,8,4096),('retention-k58-shard2048',58,466986,0,8,2048)]
manifest={'start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cases':[]}
for name,k,n,q,r,shard_rows in cases:
 if 'Runner.Worker' in subprocess.check_output(['ps','-eo','comm='],text=True):manifest['stopped']='CI job became active';break
 command=['/usr/bin/time','-v','-o',str(out/(name+'.time')),str(root/'target/release/enhance-layout-benchmark'),str(k),str(n),str(q),str(r),str(shard_rows)]
 step={'name':name,'command':command,'started':datetime.datetime.now(datetime.timezone.utc).isoformat()};manifest['cases'].append(step);t=time.monotonic()
 with open(out/(name+'.jsonl'),'w') as stdout,open(out/(name+'.stderr'),'w') as stderr:r=subprocess.run(command,env=env,stdout=stdout,stderr=stderr,timeout=900)
 step.update(exit_code=r.returncode,wall_seconds=time.monotonic()-t);(out/'manifest.json').write_text(json.dumps(manifest,indent=2));print(json.dumps(step),flush=True)
 if r.returncode:manifest['stopped']='case failed';break
manifest['ended']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'manifest.json').write_text(json.dumps(manifest,indent=2));print('FINISHED',flush=True)
