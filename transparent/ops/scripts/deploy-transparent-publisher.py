#!/usr/bin/env python3
"""Install, canary, activate or roll back continuous transparent publication.

Run on the coordinator from the production GitHub Environment. Secrets arrive
through that environment, never through command arguments or artifact files.
"""
import argparse
import asyncio
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time
import urllib.request

SCRIPT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('live_fleet', SCRIPT/'transparent-live-fleet.py')
LIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LIVE)
ROOT = Path('/opt/transparent-publisher')


def execute(args, **kwargs):
    subprocess.run(list(map(str,args)),check=True,**kwargs)


def read_json(url):
    with urllib.request.urlopen(url,timeout=5) as response:
        return json.load(response)


def secret_file(path, value):
    # GitHub secret values may omit the final LF required by OpenSSH's parser.
    if not value.endswith('\n'):
        value += '\n'
    fd = os.open(path, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600)
    with os.fdopen(fd,'w') as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path,0o600)


async def save_baseline(fleet, saved, coordinator=Path('/etc/caddy/Caddyfile')):
    saved.mkdir(exist_ok=True)
    # Each file is independent: a failed SSH connection after the first copy
    # must not make the next attempt skip the unfinished backup.
    if not (saved/'Caddyfile.coordinator').exists():
        atomic_bytes(saved/'Caddyfile.coordinator',coordinator.read_bytes())
    if not (saved/'Caddyfile.router').exists():
        data=await fleet.ssh(fleet.c['router_host'],'cat /etc/caddy/Caddyfile')
        atomic_bytes(saved/'Caddyfile.router',data)
    if not (saved/'controller.json').exists():
        atomic_bytes(saved/'controller.json',(ROOT/'controller.json').read_bytes())


def atomic_bytes(path, data):
    temporary=path.with_suffix('.tmp')
    with temporary.open('wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary,path)
    directory=os.open(path.parent,os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def align_filter_origin(initial):
    """Make the static predecessor coherent before saving rollout backups."""
    execute(['/usr/local/bin/transparent-filter-server','--check-shard-dir','--shard-dir',initial,'--zakura-cookie','/root/.cache/zakura/.cookie'])
    path=Path('/etc/systemd/system/transparent-filter-server.service')
    text=path.read_text()
    lines=text.splitlines()
    for i,line in enumerate(lines):
        if line.startswith('ExecStart='):
            args=shlex.split(line.removeprefix('ExecStart='))
            index=args.index('--shard-dir')+1
            args[index]=str(initial)
            lines[i]='ExecStart='+shlex.join(args)
    updated='\n'.join(lines)+'\n'
    if updated!=text:
        shutil.copy2(path,ROOT/'filter-unit-before-alignment')
        path.write_text(updated)
        execute(['systemctl','daemon-reload'])
        execute(['systemctl','restart','transparent-filter-server'])


def route_coordinator():
    path=Path('/etc/caddy/Caddyfile')
    text=path.read_text()
    if '# Continuous transparent publication' in text:
        return
    marker='\thandle /v1/filters/* {'
    if text.count(marker)!=1:
        raise RuntimeError('unexpected coordinator Caddyfile; cannot identify transparent filter handler')
    block='''\t# Continuous transparent publication: both origins read this authority.
\t@transparent_publication path /v1/shards /v1/shards/init /v1/shards/*/revisions/*/manifest /v1/filters/shards /v1/filters/shards/*
\thandle @transparent_publication {
\t\treverse_proxy 127.0.0.1:8094
\t}
\t@legacy_transparent_filters path /v1/filters/*
\thandle @legacy_transparent_filters {'''
    candidate=path.with_suffix('.publication-next')
    candidate.write_text(text.replace(marker,block))
    execute(['caddy','validate','--config',candidate,'--adapter','caddyfile'])
    os.replace(candidate,path)
    execute(['systemctl','reload','caddy'])


STORAGE_PATH = '/usr/local/lib/transparent-pir/storage-policy.py'
STORAGE_PRESTART = 'ExecStartPre=+/usr/bin/python3 '+STORAGE_PATH+' --apply'

HEADLESS_PATH = '/usr/local/lib/transparent-pir/headless-console.py'
HEADLESS_PRESTART = 'ExecStartPre=+/usr/bin/python3 '+HEADLESS_PATH+' --apply'


async def install_worker(fleet,worker,artifacts,rollback,stage_only=False,warm_seconds=1800):
    host=worker['ssh_host']
    before=read_json('http://'+worker['upstream']+'/v1/ready')
    if not before.get('ready') or before.get('mode')!='warm':
        raise RuntimeError(worker['id']+' is not a warm rollout predecessor')
    unit=(await fleet.ssh(host,'cat /etc/systemd/system/transparent-shard-server.service')).decode()
    args=None
    for line in unit.splitlines():
        if line.startswith('ExecStart='):
            args=shlex.split(line.removeprefix('ExecStart='))
    if not args:
        raise RuntimeError('worker unit lacks a simple ExecStart')
    # Older units predate the fleet generator's persistent runtime cache.
    # Preserve explicit cache settings, but repair an absent cache before the
    # next restart so archive workers can restore instead of rebuilding.
    cache_flags = ('--runtime-cache-dir', '--runtime-cache-max-bytes')
    present = [any(arg == flag or arg.startswith(flag+'=') for arg in args)
               for flag in cache_flags]
    if any(present) and not all(present):
        raise ValueError('worker unit has incomplete runtime cache configuration')
    if not any(present) and 'cache_bytes' in worker:
        cache_bytes = worker['cache_bytes']
        if type(cache_bytes) is not int or cache_bytes <= 0:
            raise ValueError('cache_bytes must be a positive integer')
        args += ['--runtime-cache-dir', '/srv/transparent-pir/runtime-cache',
                 '--runtime-cache-max-bytes', str(cache_bytes * 2)]
    if '--control-socket' not in args:
        args += ['--control-socket','/run/transparent-pir/control.sock','--active-record','/opt/transparent-publisher/active.json']
    if 'build_slots' in worker:
        slots = worker['build_slots']
        if type(slots) is not int or not 1 <= slots <= 99:
            raise ValueError('build_slots must be an integer from 1 to 99')
        # Replace both accepted CLI spellings; never leave a stale duplicate.
        updated = []
        index = 0
        while index < len(args):
            if args[index] == '--build-slots':
                if index + 1 >= len(args):
                    raise ValueError('worker unit has an incomplete --build-slots')
                index += 2
            elif args[index].startswith('--build-slots='):
                index += 1
            else:
                updated.append(args[index])
                index += 1
        args = updated + ['--build-slots', str(slots)]
    new_unit='\n'.join('ExecStart='+shlex.join(args) if line.startswith('ExecStart=') else line for line in unit.splitlines())+'\n'
    # RuntimeDirectory is created before the binary opens its control socket.
    if 'RuntimeDirectory=transparent-pir' not in new_unit.splitlines():
        new_unit=new_unit.replace('[Service]','[Service]\nRuntimeDirectory=transparent-pir',1)
    if worker['role'] in ('recent-replica', 'archive-owner'):
        # Reclaim file cache before transient admission reaches the 7 GiB
        # hard limit. The four-replica target is measured with a 5.5 GiB high
        # threshold; anonymous allocations still obey the work/cache guards.
        new_unit='\n'.join(line for line in new_unit.splitlines() if not line.startswith('MemoryHigh='))+'\n'
        # Archive hosts otherwise retain ~10 GiB of file cache on top of their
        # ~46 GiB anonymous working set, exceeding the cgroup headroom gate.
        high = 5905580032 if worker['role'] == 'recent-replica' else 51539607552
        new_unit=new_unit.replace('[Service]',f'[Service]\nMemoryHigh={high}',1)
    headless = fleet.c.get('headless_console', False)
    if type(headless) is not bool:
        raise ValueError('headless_console must be a boolean')
    new_unit='\n'.join(line for line in new_unit.splitlines() if line != HEADLESS_PRESTART)+'\n'
    if headless:
        new_unit=new_unit.replace('[Service]', '[Service]\n'+HEADLESS_PRESTART, 1)
    storage = fleet.c.get('storage_nodiscard', False)
    if type(storage) is not bool:
        raise ValueError('storage_nodiscard must be a boolean')
    new_unit='\n'.join(line for line in new_unit.splitlines() if line != STORAGE_PRESTART)+'\n'
    if storage:
        new_unit=new_unit.replace('[Service]', '[Service]\n'+STORAGE_PRESTART, 1)
    remote='/opt/transparent-publisher/staged'
    await fleet.ssh(host,'mkdir -p '+remote+' '+shlex.quote(rollback))
    if headless:
        helper=(SCRIPT/'transparent-headless-console.py').read_bytes()
        await fleet.ssh(host,'cat > '+remote+'/headless-console.py',helper)
        await fleet.ssh(host,'python3 '+remote+'/headless-console.py --preflight')
    if storage:
        storage_helper=(SCRIPT/'transparent-storage-policy.py').read_bytes()
        await fleet.ssh(host,'cat > '+remote+'/storage-policy.py',storage_helper)
        await fleet.ssh(host,'python3 '+remote+'/storage-policy.py --preflight')
    for name in ['transparent-shard-server','shard-control']:
        await LIVE.run(['rsync','-a','-e',shlex.join(fleet.ssh_args),str(artifacts/name),'root@'+host+':'+remote+'/'+name],timeout=120)
    verify=[remote+'/transparent-shard-server']+args[1:]+['--verify-only']
    await fleet.ssh(host,shlex.join(verify),timeout=300,multiplex=False)
    await fleet.ssh(host,'cat > '+remote+'/worker.service',new_unit.encode())
    if stage_only:
        return
    headless_install = ('install -Dm755 '+remote+'/headless-console.py '+HEADLESS_PATH) if headless else ':'
    storage_install = ('install -Dm755 '+remote+'/storage-policy.py '+STORAGE_PATH) if storage else ':'
    storage_backup = ('findmnt -n -o OPTIONS / > '+shlex.quote(rollback)+'/storage-mount-options\ncp '+remote+'/storage-policy.py '+shlex.quote(rollback)+'/restore-storage-policy.py') if storage else ':'
    command=f'''set -eu
if [ ! -f {shlex.quote(rollback)}/worker.service ]; then
 {storage_backup}
 if [ -f {STORAGE_PATH} ]; then cp {STORAGE_PATH} {shlex.quote(rollback)}/storage-policy.py; fi
 cp /etc/systemd/system/transparent-shard-server.service {shlex.quote(rollback)}/worker.service
 cp /usr/local/bin/transparent-shard-server {shlex.quote(rollback)}/transparent-shard-server
 cp /usr/local/bin/shard-control {shlex.quote(rollback)}/shard-control
 if [ -f {HEADLESS_PATH} ]; then cp {HEADLESS_PATH} {shlex.quote(rollback)}/headless-console.py; fi
fi
{headless_install}
{storage_install}
install -m755 {remote}/transparent-shard-server /usr/local/bin/transparent-shard-server.next
mv /usr/local/bin/transparent-shard-server.next /usr/local/bin/transparent-shard-server
install -m755 {remote}/shard-control /usr/local/bin/shard-control
install -m644 {remote}/worker.service /etc/systemd/system/transparent-shard-server.service
systemctl daemon-reload
systemctl restart transparent-shard-server
'''
    await fleet.ssh(host,command)
    expected_binary=hashlib.sha256((artifacts/'transparent-shard-server').read_bytes()).hexdigest()
    deadline=time.monotonic()+warm_seconds
    while time.monotonic()<deadline:
        try:
            ready=await asyncio.to_thread(read_json,'http://'+worker['upstream']+'/v1/ready')
            if ready.get('ready') and ready.get('binary_sha256')==expected_binary:
                status=await fleet.control(worker,{'operation':'status'})
                if status['active']['map_sha256']!=ready['map_sha256'] or not status['warm']:
                    continue
                if headless:
                    evidence=json.loads(await fleet.ssh(host,'python3 '+HEADLESS_PATH+' --check'))
                    if evidence.get('helper_sha256') != hashlib.sha256(helper).hexdigest() or evidence.get('persistent') is not True:
                        raise RuntimeError('headless helper does not attest the installed configuration')
                if storage:
                    evidence=json.loads(await fleet.ssh(host,'python3 '+STORAGE_PATH+' --check'))
                    if evidence.get('helper_sha256') != hashlib.sha256(storage_helper).hexdigest() or evidence.get('persistent') is not True or evidence.get('online_discard') is not False:
                        raise RuntimeError('storage helper does not attest installed policy')
                print(worker['id']+': warm with publication control',flush=True)
                return
        except Exception:
            pass
        await asyncio.sleep(5)
    raise RuntimeError(worker['id']+' failed to warm after upgrade')


async def rollback(fleet, saved):
    subprocess.run(['systemctl','stop','transparent-replica-reconciler'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    subprocess.run(['systemctl','stop','transparent-publish-controller'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    # A binary rollback must never resurrect an orphaned publication. Validate
    # the static predecessor against the node before changing routing or units.
    initial=Path(json.loads((saved/'controller.json').read_text())['initial_publication'])
    end=json.loads((initial/'shards.json').read_text())['shards'][-1]
    import base64
    cookie=Path('/root/.cache/zakura/.cookie').read_text().strip()
    request=urllib.request.Request('http://127.0.0.1:8232',json.dumps({'jsonrpc':'2.0','id':1,'method':'getblockhash','params':[end['end_height']]}).encode(),{'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
    with urllib.request.urlopen(request,timeout=10) as response:
        canonical=json.load(response)['result']
    if canonical!=end['terminal_block_hash']:
        await fleet.route([])
        raise RuntimeError('rollback predecessor is orphaned; routing remains withdrawn for automatic recovery')
    for worker in fleet.roster:
        remote=str(saved)
        if await fleet.ssh(worker['ssh_host'],f'test -f {shlex.quote(remote)}/worker.service && echo yes || true')!=b'yes\n':
            continue
        await fleet.ssh(worker['ssh_host'],f'''set -eu
cp {shlex.quote(remote)}/transparent-shard-server /usr/local/bin/transparent-shard-server.rollback
chmod 755 /usr/local/bin/transparent-shard-server.rollback
mv /usr/local/bin/transparent-shard-server.rollback /usr/local/bin/transparent-shard-server
cp {shlex.quote(remote)}/worker.service /etc/systemd/system/transparent-shard-server.service
systemctl daemon-reload
systemctl restart transparent-shard-server
''')
    shutil.copy2(saved/'Caddyfile.coordinator','/etc/caddy/Caddyfile')
    execute(['systemctl','reload','caddy'])
    await fleet.ssh(fleet.c['router_host'],'cat > /etc/caddy/Caddyfile.rollback',(saved/'Caddyfile.router').read_bytes())
    await fleet.ssh(fleet.c['router_host'],'caddy validate --config /etc/caddy/Caddyfile.rollback --adapter caddyfile >&2 && mv /etc/caddy/Caddyfile.rollback /etc/caddy/Caddyfile && systemctl reload caddy')


async def main():
    cli=argparse.ArgumentParser()
    cli.add_argument('mode',choices=['shadow','activate','rollback'])
    cli.add_argument('--artifacts',type=Path,required=True)
    cli.add_argument('--initial-publication',type=Path,default=Path('/srv/zakura/transparent-shards-v7-full'))
    cli.add_argument('--source-sha',required=True)
    cli.add_argument('--range-profile',default='zcash-transparent-range-v1',
                     help='Range-filter profile the controller publishes under (shadow mode); must match the initial publication')
    cli.add_argument('--recent-geometry',default='recent-8k',
                     help='Recent geometry the controller publishes (shadow mode); must match the initial publication')
    cli.add_argument('--data-dir',type=Path,default=Path('/srv/zakura/transparent-event-data-v2'),
                     help='Event journal the controller publishes from (shadow mode); version 2 since schema v9')
    cli.add_argument('--directory-choice',choices=['off','sealed','all'],default='off',
                     help='Which newly built shards publish a directory choice table (shadow mode)')
    args=cli.parse_args()
    if os.geteuid()!=0:
        raise RuntimeError('run on the coordinator as root')
    ROOT.mkdir(exist_ok=True)
    for name in ['credentials','state','rollback']:
        (ROOT/name).mkdir(exist_ok=True,mode=0o700)
    previous_files={}
    if args.mode=='shadow':
        # Keep what is running so a failure before any worker changes can put
        # it back: the credentials the live controller uses and its config.
        for name in ['credentials/deploy-ssh','credentials/known_hosts','controller.json','fleet.json','roster.json']:
            if (ROOT/name).exists():
                previous_files[name]=(ROOT/name).read_bytes()
        # Reuse the environment's existing deployment identity at runtime.
        secret_file(ROOT/'credentials/deploy-ssh',os.environ['WALLET_PIR_DEPLOY_SSH_KEY'])
        secret_file(ROOT/'credentials/known_hosts',os.environ['TRANSPARENT_SSH_KNOWN_HOSTS'])
        LIVE.atomic_json(ROOT/'roster.json',json.loads(os.environ['TRANSPARENT_FLEET_JSON']))
        fleet_config=dict(roster=str(ROOT/'roster.json'),state_dir=str(ROOT/'state'),ssh_key=str(ROOT/'credentials/deploy-ssh'),known_hosts=str(ROOT/'credentials/known_hosts'),
                          assign_binary='/usr/local/bin/shard-assign',public_host='transparent-pir.valargroup.dev',router_host=os.environ['TRANSPARENT_ROUTER_HOST'],
                          authority_upstream='https://enhance-pir.valargroup.dev',internal_listen=os.environ['TRANSPARENT_ROUTER_HOST']+':8080')
        # Operators and rollouts own these settings. Regenerating the file
        # without them once disabled status forwarding and replica catch-up.
        if 'fleet.json' in previous_files:
            fleet_config=LIVE.carry_operational(fleet_config,json.loads(previous_files['fleet.json']))
        LIVE.atomic_json(ROOT/'fleet.json',fleet_config)
        config=dict(data_dir=str(args.data_dir),publication_root='/srv/zakura/transparent-publications',initial_publication=str(args.initial_publication),
                    recent_from=3262749,recent_geometry=args.recent_geometry,archive_geometry='archive-wide',rpc_url='http://127.0.0.1:8232',rpc_cookie='/root/.cache/zakura/.cookie',
                    fleet_command=str(ROOT/'transparent-live-fleet.py'),fleet_config=str(ROOT/'fleet.json'),listen='127.0.0.1:8094',source_sha=args.source_sha,shadow=True)
        # Written only when enabled, so a controller built before the field
        # (whose config refuses unknown fields) can still read a default config.
        if args.directory_choice!='off':
            config['directory_choice']=args.directory_choice
        if args.range_profile!='zcash-transparent-range-v1':
            config['range_profile']=args.range_profile
        LIVE.atomic_json(ROOT/'controller.json',config)
    fleet=LIVE.Fleet(json.loads((ROOT/'fleet.json').read_text()))
    saved=ROOT/'rollback'/args.source_sha
    if args.mode=='rollback':
        await rollback(fleet,saved)
        return
    if args.mode=='shadow':
        def restore_previous():
            for name,data in previous_files.items():
                atomic_bytes(ROOT/name,data)
            if (ROOT/'credentials/deploy-ssh').exists():
                os.chmod(ROOT/'credentials/deploy-ssh',0o600)
        # Every host must accept the deployment identity before anything
        # stops: a rotated secret that the fleet does not authorise would
        # otherwise leave the live controller stopped with credentials that
        # cannot reach its own workers.
        hosts=[fleet.c['router_host']]+[w['ssh_host'] for w in fleet.roster]
        try:
            for host in hosts:
                await fleet.ssh(host,'true',multiplex=False)
        except Exception as error:
            restore_previous()
            raise RuntimeError('deployment identity is not accepted by '+host+'; nothing was changed') from error
        saved.mkdir(parents=True,exist_ok=True)
        if 'controller.json' in previous_files and not (saved/'controller.previous.json').exists():
            atomic_bytes(saved/'controller.previous.json',previous_files['controller.json'])
        subprocess.run(['systemctl','stop','transparent-publish-controller'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            align_filter_origin(args.initial_publication)
            await save_baseline(fleet,saved)
        except Exception:
            # No worker has changed yet: resume the previous controller.
            restore_previous()
            subprocess.run(['systemctl','start','transparent-publish-controller'],check=False)
            raise
        for name in ['transparent-publish-controller','shard-assign']:
            shutil.copy2(args.artifacts/name,'/usr/local/bin/'+name+'.next')
            os.chmod('/usr/local/bin/'+name+'.next',0o755)
            os.replace('/usr/local/bin/'+name+'.next','/usr/local/bin/'+name)
        shutil.copy2(SCRIPT/'transparent-live-fleet.py',ROOT/'transparent-live-fleet.py')
        shutil.copy2(SCRIPT.parent/'deploy/transparent-publish-controller.service','/etc/systemd/system/transparent-publish-controller.service')
        shutil.copy2(SCRIPT.parent/'deploy/transparent-replica-reconciler.service','/etc/systemd/system/transparent-replica-reconciler.service')
        shutil.copy2(SCRIPT.parent/'deploy/transparent-control-sessions.service','/etc/systemd/system/transparent-control-sessions.service')
        Path('/srv/zakura/transparent-publications').mkdir(exist_ok=True)
        # Recent canary first; archive owners follow serially, then other replicas.
        recent=[w for w in fleet.roster if w['role']=='recent-replica']
        owners=[w for w in fleet.roster if w['role']=='archive-owner']
        try:
            for worker in recent[:1]+owners+recent[1:]:
                await install_worker(fleet,worker,args.artifacts,str(saved))
            execute(['systemctl','daemon-reload'])
            execute(['systemctl','enable','--now','transparent-publish-controller'])
            deadline=time.monotonic()+1800
            while time.monotonic()<deadline:
                try:
                    status=read_json('http://127.0.0.1:8094/v1/status')
                    if status.get('phase')=='shadow_verified':
                        print(json.dumps(status,indent=2));return
                except Exception:
                    pass
                await asyncio.sleep(5)
            raise RuntimeError('controller did not produce a verified shadow candidate')
        except Exception:
            await rollback(fleet,saved)
            raise
    else:
        config=json.loads((ROOT/'controller.json').read_text())
        if config['source_sha']!=args.source_sha:
            raise RuntimeError('activation must name the shadow-tested source SHA')
        config['shadow']=False
        LIVE.atomic_json(ROOT/'controller.json',config)
        route_coordinator()
        execute(['systemctl','restart','transparent-publish-controller'])
        execute(['systemctl','enable','transparent-replica-reconciler'])
        # Restart rather than start: a reconciler that is already running keeps
        # the fleet code it loaded, and would re-route with the old router
        # policy after this activation (2026-09-28).
        execute(['systemctl','restart','transparent-replica-reconciler'])
        deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            try:
                status=read_json('http://127.0.0.1:8094/v1/status')
                if status.get('phase')=='serving':
                    maps=[]
                    for url in ['https://transparent-pir.valargroup.dev/v1/shards','https://enhance-pir.valargroup.dev/v1/filters/shards']:
                        with urllib.request.urlopen(url,timeout=10) as response:
                            maps.append(response.read())
                    if maps[0]!=maps[1]:
                        raise RuntimeError('public origins disagree after activation')
                    print(json.dumps(status,indent=2));return
            except Exception as exc:
                print('waiting for activation: '+str(exc),flush=True)
            await asyncio.sleep(2)
        raise RuntimeError('activation deadline exceeded; controller retains retry/recovery state')


if __name__=='__main__':
    asyncio.run(main())
