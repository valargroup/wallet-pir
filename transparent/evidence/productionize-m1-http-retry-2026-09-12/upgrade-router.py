"""One-time guarded router upgrade; run on the coordinator."""
import asyncio, importlib.util, json, time
from pathlib import Path
OPS=Path('/opt/transparent-publisher-build/archive-cache-upgrade-20260911/ops/scripts')
OUT=Path('/opt/transparent-publisher-build/caddy-http-retry-20260912/router-upgrade')
SPEC=importlib.util.spec_from_file_location('upgrade',OPS/'upgrade-transparent-fleet.py')
U=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(U)
SHA='b7105518e3ed1c0761f232e44fc09345535533c9cb0abf0e12809416c7ac64d9'
INSTALL=r'''
from pathlib import Path
import hashlib,subprocess,shutil
candidate=Path('/opt/transparent-publisher-build/caddy-2.11.4-test/caddy')
assert hashlib.sha256(candidate.read_bytes()).hexdigest()=='b7105518e3ed1c0761f232e44fc09345535533c9cb0abf0e12809416c7ac64d9'
override=Path('/etc/systemd/system/caddy.service.d/transparent-version.conf')
assert not override.exists(), 'existing override requires separate review'
subprocess.run([str(candidate),'validate','--config','/etc/caddy/Caddyfile','--adapter','caddyfile'],check=True)
target=Path('/usr/local/lib/transparent-router/caddy-2.11.4');target.parent.mkdir(parents=True,exist_ok=True)
shutil.copy2(candidate,target);target.chmod(0o755)
override.parent.mkdir(parents=True,exist_ok=True)
override.write_text('[Service]\nExecStart=\nExecStart='+str(target)+' run --environ --config /etc/caddy/Caddyfile\nExecReload=\nExecReload='+str(target)+' reload --config /etc/caddy/Caddyfile --force\n')
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','restart','caddy'],check=True)
pid=subprocess.check_output(['systemctl','show','caddy','-p','MainPID','--value'],text=True).strip()
assert hashlib.sha256(Path('/proc/'+pid+'/exe').read_bytes()).hexdigest()=='b7105518e3ed1c0761f232e44fc09345535533c9cb0abf0e12809416c7ac64d9'
print('verified running binary',pid)
'''
ROLLBACK=r'''
from pathlib import Path
import subprocess
Path('/etc/systemd/system/caddy.service.d/transparent-version.conf').unlink(missing_ok=True)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','restart','caddy'],check=True)
'''
async def main():
    OUT.mkdir(parents=True,exist_ok=False)
    fleet=U.L.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
    host=fleet.c['router_host']
    # Verify rollback has no pre-existing version override before any mutation.
    await fleet.ssh(host,'test ! -e /etc/systemd/system/caddy.service.d/transparent-version.conf')
    (OUT/'unit-before.txt').write_bytes(await fleet.ssh(host,'systemctl cat caddy'))
    await U.maintenance(fleet,OUT)
    attempted=False
    try:
        attempted=True
        (OUT/'install.log').write_bytes(await fleet.ssh(host,"python3 -",INSTALL.encode()))
        deadline=time.monotonic()+180
        while True:
            try:
                await U.reopen(fleet,OUT)
                break
            except Exception:
                if time.monotonic()>deadline:raise
                await asyncio.sleep(2)
        await U.verify_public(fleet)
        (OUT/'result.json').write_text(json.dumps(dict(passed=True,binary_sha256=SHA)))
    except BaseException as error:
        (OUT/'failure.json').write_text(json.dumps(dict(passed=False,error=repr(error))))
        # Reinstate guards if public verification failed after reopening.
        async with fleet.lock('routing'):
            U.L.atomic_json(fleet.root/'maintenance.json',{'enabled':True})
            await fleet.route([])
            U.apply_coordinator((OUT/'Caddyfile.guarded').read_bytes())
        if attempted:
            (OUT/'rollback.log').write_bytes(await fleet.ssh(host,'python3 -',ROLLBACK.encode()))
        await U.reopen(fleet,OUT)
        await U.verify_public(fleet)
        raise
asyncio.run(main())
