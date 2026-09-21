import json,subprocess,datetime,urllib.request,os
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
for key,args in [('hostname',['hostname']),('kernel',['uname','-a']),('cpu',['lscpu','-J']),('memory',['cat','/proc/meminfo'])]:
 out[key]=subprocess.check_output(args,text=True)
for name in ['id','region']:
 try:out['droplet_'+name]=urllib.request.urlopen('http://169.254.169.254/metadata/v1/'+name,timeout=2).read().decode()
 except Exception as e:out['droplet_'+name]=str(e)
out['binaries']={}
for name in ['enhance-pir-worker','enhance-pir-server','transparent-shard-server']:
 path='/usr/local/bin/'+name
 if os.path.exists(path):out['binaries'][name]=subprocess.check_output(['sha256sum',path],text=True).strip()
out['services']={}
for name in ['enhance-pir-worker','enhance-pir-server','transparent-shard-server']:
 out['services'][name]=subprocess.run(['systemctl','show',name,'-p','MainPID','-p','MemoryCurrent','-p','NRestarts','-p','ActiveEnterTimestamp','-p','ExecStart'],capture_output=True,text=True).stdout
print(json.dumps(out))
