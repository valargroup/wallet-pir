import subprocess,json,datetime
run=lambda a:subprocess.check_output(a,text=True).strip()
mount=json.loads(run(['findmnt','-J','-T','/srv/transparent-pir/runtime-cache']))['filesystems'][0]
assert mount['source']=='/dev/vda1' and mount['fstype']=='ext4' and 'discard' in mount['options'].split(','),mount
assert run(['systemctl','is-active','transparent-shard-server.service'])=='active'
subprocess.run(['systemd-run','--unit=transparent-m1-discard-restore','--on-active=30min','/usr/bin/mount','-o','remount,discard','/'],check=True)
print(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'before':mount}),flush=True)
subprocess.run(['mount','-o','remount,nodiscard','/'],check=True)
after=json.loads(run(['findmnt','-J','-T','/srv/transparent-pir/runtime-cache']))['filesystems'][0]
assert 'discard' not in after['options'].split(','),after
print(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'after':after}),flush=True)
