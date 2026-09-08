import concurrent.futures, datetime, json, pathlib, subprocess, sys
root = pathlib.Path(__file__).parent
roster = json.loads(subprocess.check_output(['gh','api','repos/valargroup/enhance-pir/environments/production/variables/TRANSPARENT_FLEET_JSON','--jq','.value'], text=True))
remote = '''import datetime,json,os,pathlib,subprocess,urllib.request,urllib.error
result={"observed_at":datetime.datetime.now(datetime.timezone.utc).isoformat()}
try:
 with urllib.request.urlopen("http://127.0.0.1:8093/v1/ready",timeout=10) as r: result["ready"]=json.load(r)
except urllib.error.HTTPError as e: result["ready"]=json.load(e)
except Exception as e: result["ready_error"]=str(e)
result["systemd"]=dict(line.split("=",1) for line in subprocess.check_output(["systemctl","show","transparent-shard-server.service","-p","MainPID,ActiveState,SubState,MemoryCurrent,MemoryPeak,ExecMainStartTimestamp,InvocationID,ControlGroup"],text=True).splitlines() if "=" in line)
try:
 status=(pathlib.Path("/proc")/result["systemd"]["MainPID"]/"status").read_text().splitlines()
 result["process_memory_bytes"]={line.split(":",1)[0]:int(line.split()[1])*1024 for line in status if line.startswith(("VmRSS:","VmHWM:"))}
except OSError: pass
try:
 with urllib.request.urlopen("http://127.0.0.1:8093/metrics",timeout=10) as r:
  result["runtime_metrics"]=[line for line in r.read().decode().splitlines() if line.startswith(("transparent_shard_disk_","transparent_shard_builds_total"))]
except Exception: pass
cgroup=pathlib.Path("/sys/fs/cgroup") / result["systemd"].get("ControlGroup","").lstrip("/")
for name in ["memory.current","memory.peak","memory.events","memory.swap.current"]:
 try: result[name]=(cgroup/name).read_text().strip()
 except OSError: pass
s=os.statvfs("/srv/transparent-pir")
result["filesystem"]={"bytes":s.f_blocks*s.f_frsize,"available_bytes":s.f_bavail*s.f_frsize}
for name in ["current-release","current-unit-digest"]:
 try: result[name]=(pathlib.Path("/opt/transparent-pir")/name).read_text().strip()
 except OSError: pass
print(json.dumps(result))
'''
def capture(worker):
 host=worker['ssh_host']
 proc=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/tmp/transparent-fleet-known-hosts','-o','ConnectTimeout=10','-J','root@167.99.42.60','root@'+host,'python3','-'],input=remote,text=True,capture_output=True)
 if proc.returncode: return worker['id'], {'error':proc.stderr, 'exit_code':proc.returncode}
 return worker['id'],json.loads(proc.stdout)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 result=dict(pool.map(capture,roster))
path=root/(sys.argv[1]+'.json')
path.write_text(json.dumps(result,indent=2)+'\n')
print(path)
for name,r in result.items():
 print(name, 'ready='+str(r.get('ready',{}).get('ready')), 'pid='+str(r.get('systemd',{}).get('MainPID')), 'error='+str(r.get('error')))
