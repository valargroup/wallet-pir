import pathlib,time,json,subprocess,urllib.request,datetime,hashlib
p=pathlib.Path(__file__).resolve().parent;root=p.parents[1]
while 'COMPLETE' not in (p/'run-probes-continuation.log').read_text():time.sleep(5)
binary=root/'target/release/enhance-pir-load-test'
manifest={'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'steps':[]}
def fetch(url):
 with urllib.request.urlopen(url,timeout=5) as response:return response.read()
for c in [1,8]:
 name=f'load-c{c}'
 before=fetch('https://enhance-pir.valargroup.dev/v1/health');(p/f'{name}-health-before.json').write_bytes(before)
 if json.loads(before)['phase']['phase'] not in ['serving','building']:raise SystemExit('unhealthy preflight')
 (p/f'{name}-metrics-before.txt').write_bytes(fetch('http://127.0.0.1:18080/metrics'))
 args=[str(binary),'--server','https://enhance-pir.valargroup.dev','--parallelism',str(c),'--duration','60s','--warmup','10s','--seed','42','--max-error-rate','0','--slo-p99-ms','5000','--json-out',str(p/f'{name}-summary.json')]
 step={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'command':args};manifest['steps'].append(step);failures=0
 with open(p/f'{name}.log','w') as log,open(p/f'{name}-health-probes.jsonl','w') as probes:
  proc=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT)
  while proc.poll() is None:
   try:
    health=json.loads(fetch('https://enhance-pir.valargroup.dev/v1/health'));good=health['phase']['phase'] in ['serving','building'];event={'health':health}
   except Exception as error:good=False;event={'error':str(error)}
   event['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();probes.write(json.dumps(event)+'\n');probes.flush();failures=0 if good else failures+1
   if failures>=2:proc.terminate();step['stop']='two consecutive failed health probes';break
   time.sleep(5)
  step['exit_code']=proc.wait(timeout=15)
 step['ended_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 (p/f'{name}-metrics-after.txt').write_bytes(fetch('http://127.0.0.1:18080/metrics'))
 (p/'load-manifest.json').write_text(json.dumps(manifest,indent=2));print(name,step['exit_code'],flush=True)
 if step['exit_code']:break
 if c==1:time.sleep(30)
print('COMPLETE',flush=True)
