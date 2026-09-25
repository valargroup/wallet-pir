import concurrent.futures,datetime,json,pathlib,subprocess,threading,time,urllib.request
root=pathlib.Path('/root/reinspiring-trial-20260925/native-load');root.mkdir(exist_ok=True)
origin='https://enhance-pir.valargroup.dev'
deadline=datetime.datetime(2026,9,25,10,56,32,tzinfo=datetime.timezone.utc).timestamp()
stop=threading.Event()
def background():
    n=0
    while time.time()<deadline and not stop.is_set():
        started=time.time();sample={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
        urls={'init':origin+'/v1/enhance/init'}
        if n%5==0:urls.update({'router_metrics':'http://10.142.0.14:8093/internal/metrics','router_health':'http://10.142.0.14:8093/internal/health','coordinator_metrics':'http://127.0.0.1:8080/metrics','ingress_metrics':'http://127.0.0.1:8083/internal/metrics'})
        for name,url in urls.items():
            try:
                with urllib.request.urlopen(url,timeout=5) as r:body=r.read().decode()
                if name!='init':sample[name]=body
            except Exception as e:sample[name+'_error']=str(e)
        if len(sample)>1:
            with (root/'samples.jsonl').open('a') as f:f.write(json.dumps(sample)+'\n')
        n+=1;stop.wait(max(0,1-(time.time()-started)))
thread=threading.Thread(target=background);thread.start()
try:
    n=0
    while time.time()<deadline:
        n+=1;rate=8 if n==1 else 15 if n==2 else 20
        duration=min(60,int(deadline-time.time()))
        if duration<=0:break
        began=datetime.datetime.now(datetime.timezone.utc).isoformat()
        report=root/f'load-{n:04d}-{rate}qps.json'
        cmd=['/root/reinspiring-trial-20260925/enhance-pir-load-test','--server',origin,'--oracle','/root/architecture2-deploy/public-oracle.json','--parallelism','8','--rate',str(rate),'--duration',f'{duration}s','--warmup','0s','--max-error-rate','0','--json-out',str(report)]
        try:
            r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=duration+45)
            with (root/'load.log').open('a') as f:f.write(r.stdout+'\n')
            status={'batch':n,'utc_start':began,'utc_end':datetime.datetime.now(datetime.timezone.utc).isoformat(),'rate':rate,'exit':r.returncode,'report':str(report)}
        except subprocess.TimeoutExpired:status={'batch':n,'utc_start':began,'rate':rate,'exit':124,'error':'load timeout'}
        print(json.dumps(status),flush=True)
        with (root/'batches.jsonl').open('a') as f:f.write(json.dumps(status)+'\n')
        if status['exit']!=0:raise SystemExit('Stopped trial load on failure; no rollback performed')
finally:stop.set();thread.join()
