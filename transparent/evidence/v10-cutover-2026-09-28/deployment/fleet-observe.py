import concurrent.futures,datetime,json,pathlib,subprocess,urllib.request
root=pathlib.Path('/opt/transparent-publisher')
fleet=json.loads((root/'fleet.json').read_text())
roster=json.loads((root/'roster.json').read_text())
def one(w):
 ssh=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host']]
 command="df -B1 /srv/transparent-pir /; free -b; systemctl show transparent-shard-server -p ActiveState -p NRestarts -p MemoryCurrent -p MemoryPeak; cat /proc/loadavg"
 p=subprocess.run(ssh+[command],capture_output=True,text=True,check=True)
 with urllib.request.urlopen('http://'+w['upstream']+'/v1/ready',timeout=10) as r: ready=json.load(r)
 return {'worker':w['id'],'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'host':p.stdout,'ready':ready}
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex: result=list(ex.map(one,roster))
print(json.dumps(result,indent=2))
