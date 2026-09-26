import argparse, hashlib, json, os, pathlib, shlex, shutil, subprocess, time
p=argparse.ArgumentParser();p.add_argument('mode',choices=['stage','apply','rollback']);p.add_argument('unit');p.add_argument('source');p.add_argument('sha256');p.add_argument('revision');p.add_argument('--variant',choices=['native','q48'],default='native');a=p.parse_args()
allowed={'enhance-pir-coordinator','enhance-pir-query-ingress','enhance-pir-packing-router','enhance-pir-worker','enhance-pir-gpu-worker','status-controller-qualification','status-worker','status-router','status-pir','pir-apm','pir-monitor'}
assert a.unit in allowed and len(a.revision)==40 and all(c in '0123456789abcdef' for c in a.revision)
root=pathlib.Path('/opt/wallet-pir/releases')/a.revision/a.variant;root.mkdir(parents=True,exist_ok=True)
backup=pathlib.Path('/root/wallet-pir-rollout')/a.revision/a.unit;backup.mkdir(parents=True,exist_ok=True);backup.chmod(0o700)
unit=a.unit+'.service'; drop=pathlib.Path('/etc/systemd/system')/(unit+'.d')/'zzzzzzzzzzzzzzzz-main.conf'
def run(*cmd):return subprocess.check_output(cmd,text=True).strip()
if a.mode=='rollback':
 if (backup/'override-before').exists():shutil.copy2(backup/'override-before',drop)
 elif drop.exists():drop.unlink()
 subprocess.run(['systemctl','daemon-reload'],check=True);subprocess.run(['systemctl','restart',unit],check=True);print('rollback',a.unit);raise SystemExit
b=pathlib.Path(a.source).read_bytes();assert hashlib.sha256(b).hexdigest()==a.sha256
pid=run('systemctl','show',unit,'-p','MainPID','--value');assert int(pid)>0
old=[v.decode() for v in pathlib.Path('/proc',pid,'cmdline').read_bytes().split(b'\0') if v][0]
name=pathlib.Path(old).name;assert name in ['enhance-pir-server','status-pir','pir-apm','pir-monitor']
new=root/name
if new.exists():assert hashlib.sha256(new.read_bytes()).hexdigest()==a.sha256
else:
 temp=new.with_suffix('.next');temp.write_bytes(b);temp.chmod(0o755);os.replace(temp,new)
link=subprocess.run(['ldd',str(new)],text=True,capture_output=True);assert link.returncode==0 and 'not found' not in link.stdout, 'shared library incompatibility'
if name!='pir-monitor':subprocess.run([str(new),'--help'],stdout=subprocess.DEVNULL,check=True,timeout=20)
cat=run('systemctl','cat',unit)
lines=cat.splitlines();commands=[l[len('ExecStart='):] for l in lines if l.startswith('ExecStart=') and l!='ExecStart='];command=commands[-1]
assert old in command and '\n' not in command and '%' not in command
newcommand=command.replace(old,str(new),1)
extra=''
if a.unit=='status-pir':
 import pwd
 args=[v.decode() for v in pathlib.Path('/proc',pid,'cmdline').read_bytes().split(b'\0') if v]
 oldstate=args[args.index('--state-dir')+1]
 state=pathlib.Path('/var/lib')/('status-pir-synthetic-'+a.revision)
 state.mkdir(parents=True,exist_ok=True)
 user=run('systemctl','show',unit,'-p','User','--value') or 'root';pw=pwd.getpwnam(user);os.chown(state,pw.pw_uid,pw.pw_gid)
 assert oldstate in newcommand
 newcommand=newcommand.replace(oldstate,str(state),1)
 extra='ReadWritePaths='+str(state)+'\n'

if not (backup/'unit-before').exists():
 (backup/'unit-before').write_text(cat+'\n');(backup/'unit-before').chmod(0o600)
 if drop.exists():shutil.copy2(drop,backup/'override-before')
 oldhash=hashlib.sha256(pathlib.Path('/proc',pid,'exe').read_bytes()).hexdigest()
 (backup/'before.json').write_text(json.dumps({'unit':unit,'pid':pid,'binary':old,'sha256':oldhash}))
print(json.dumps({'mode':a.mode,'unit':unit,'revision':a.revision,'binary':str(new),'sha256':a.sha256}),flush=True)
if a.mode=='stage':raise SystemExit
assert not (backup/'applied.json').exists(), 'already applied; verify before repeating'
drop.parent.mkdir(parents=True,exist_ok=True);drop.write_text('[Service]\nExecStart=\nExecStart='+newcommand+'\n'+extra)
subprocess.run(['systemctl','daemon-reload'],check=True);subprocess.run(['systemctl','restart',unit],check=True)
(backup/'applied.json').write_text(json.dumps({'at':time.time(),'revision':a.revision,'sha256':a.sha256}))
