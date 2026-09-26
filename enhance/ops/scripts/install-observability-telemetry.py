#!/usr/bin/env python3
"""Install the qualified additive telemetry release while preserving ExecStart arguments."""
import json, os, pathlib, shlex, shutil, subprocess, sys, time
unit, source = sys.argv[1:]
assert unit in ('enhance-pir-coordinator', 'enhance-pir-packing-router')
override = pathlib.Path('/etc/systemd/system')/(unit+'.service.d')/'zzzzzzzzzzzz-observability.conf'
assert not override.exists(), 'override already exists; inspect before reinstalling'
root=pathlib.Path('/opt/enhance-pir/releases/observability-20260926')
root.mkdir(parents=True,exist_ok=True)
new=root/'enhance-pir-server'
shutil.copy2(source,new);new.chmod(0o755)
subprocess.run([str(new),'--help'],check=True,stdout=subprocess.DEVNULL,timeout=15)
pid=subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True).strip()
args=[v.decode() for v in pathlib.Path('/proc/'+pid+'/cmdline').read_bytes().split(b'\0') if v]
assert args and args[0].endswith('enhance-pir-server')
backup=pathlib.Path('/root/observability-rollout');backup.mkdir(mode=0o700,exist_ok=True)
(backup/(unit+'.previous-command.json')).write_text(json.dumps(args)+'\n')
path=pathlib.Path('/etc/systemd/system')/(unit+'.service.d')/'zzzzzzzzzzzz-observability.conf'
assert not path.exists(), 'override already exists; inspect before reinstalling'
path.parent.mkdir(exist_ok=True)
args[0]=str(new)
path.write_text('[Service]\nExecStart=\nExecStart='+shlex.join(args)+'\n')
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','restart',unit],check=True)
print(unit,'restarted with additive telemetry; rollback removes',path)
