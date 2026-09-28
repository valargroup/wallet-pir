#!/usr/bin/env python3
"""Read-only resource and metrics timeline, including expected cutover errors."""
import concurrent.futures,datetime,json,pathlib,subprocess,time,urllib.request,urllib.error
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads((root/'pre-roster.json').read_text())
def one(w):
 result={'worker':w['id'],'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'-o','ConnectTimeout=5','root@'+w['ssh_host'],'cat /proc/meminfo; systemctl show transparent-shard-server -p ActiveState -p NRestarts -p MemoryCurrent -p MemoryPeak -p ExecMainStatus; cat /sys/fs/cgroup/system.slice/transparent-shard-server.service/memory.events; df -B1 /srv/transparent-pir; cat /proc/loadavg']
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=15)
  result.update(host=p.stdout,ssh_exit=p.returncode,ssh_error=p.stderr)
 except Exception as error: result['ssh_error']=str(error)
 for endpoint in ['v1/ready','metrics']:
  try:
   with urllib.request.urlopen('http://'+w['upstream']+'/'+endpoint,timeout=5) as response:
    result[endpoint]={'http':response.status,'body':response.read().decode()}
  except urllib.error.HTTPError as error:
   result[endpoint]={'http':error.code,'body':error.read().decode()}
  except Exception as error: result[endpoint]={'error':str(error)}
 return result
with (root/'resource-timeline.jsonl').open('a') as stream:
 for _ in range(480):
  with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: items=list(pool.map(one,roster))
  stream.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'workers':items})+'\n');stream.flush()
  time.sleep(30)
