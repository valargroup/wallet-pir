#!/usr/bin/env python3
"""Install the qualified 2026-09-26 APM rollout and preserve prior configuration."""
import pathlib, shutil, subprocess, hashlib, sys
root=pathlib.Path('/opt/enhance-pir/releases/observability-20260926');root.mkdir(parents=True,exist_ok=True)
source=pathlib.Path('/root/observability-rollout/pir-apm')
assert len(sys.argv)==2, 'pass the qualified pir-apm SHA-256'
assert hashlib.sha256(source.read_bytes()).hexdigest()==sys.argv[1]
shutil.copy2(source,root/'pir-apm');(root/'pir-apm').chmod(0o755)
shutil.copy2('/root/observability-rollout/credential-exec.py',root/'credential-exec.py')
backup=pathlib.Path('/root/observability-rollout')
(backup/'apm-before.service').write_bytes(subprocess.check_output(['systemctl','cat','pir-apm']))
shutil.copy2('/etc/default/pir-apm',backup/'apm-before.env');(backup/'apm-before.env').chmod(0o600)
p=pathlib.Path('/etc/systemd/system/pir-apm.service.d/zzzzzzzzzzzz-observability.conf')
assert not p.exists()
s=(backup/'apm-observability.conf').read_text()
s+='\nEnvironment=PIR_APM_ORACLE_ANCHOR_HEIGHT=3494753\nEnvironment=PIR_APM_ORACLE_ANCHOR_HASH=00000000002d515daf3dc7f8f6b3d6e9471800ac8cfa435bccf93c0bdda53338\n'
s+='ExecStart=\nExecStart=/usr/bin/python3 '+str(root/'credential-exec.py')+' '+str(root/'pir-apm')+'\n'
p.write_text(s)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','restart','pir-apm'],check=True)
print('APM started in shadow mode; rollback removes',p)
