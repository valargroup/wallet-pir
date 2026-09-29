import concurrent.futures,datetime,json,pathlib,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads((root/'pre-roster.json').read_text())
command='uname -srv; cat /etc/os-release; lscpu; free -b; lsblk -b -o NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS; df -B1 / /srv/transparent-pir'
def one(w):
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host'],command]
 p=subprocess.run(args,capture_output=True,text=True,check=True)
 return {'worker':w['id'],'facts':p.stdout}
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: workers=list(pool.map(one,roster))
p=subprocess.run(['bash','-c',command.replace('/srv/transparent-pir','/srv/zakura')],capture_output=True,text=True,check=True)
(root/'host-facts.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'coordinator':p.stdout,'workers':workers},indent=2)+'\n')
print('Saved coordinator and six worker host facts')
