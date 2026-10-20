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
import sys
import time
import urllib.request

SCRIPT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('live_fleet', SCRIPT/'transparent-live-fleet.py')
LIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LIVE)
INVENTORY_SPEC = importlib.util.spec_from_file_location('fleet_inventory', SCRIPT/'transparent-fleet-inventory.py')
INVENTORY = importlib.util.module_from_spec(INVENTORY_SPEC)
INVENTORY_SPEC.loader.exec_module(INVENTORY)
ROOT = Path('/opt/transparent-publisher')
# The shared primitives live in the same checkout; this script is never shipped alone.
LIB = str(SCRIPT.parents[2]/'tools/lib')
if LIB not in sys.path:
    sys.path.insert(0, LIB)
from wallet_pir_ops import inherited_lock, transparent_unit  # noqa: E402



def publisher_service(data_dir, publication_root, initial_publication, *, state_dir=None, config_path=None):
    """Render the publisher and its sandbox from the same journal/publication paths."""
    from wallet_pir_ops.paths import ROOT as _resources
    from tools.transparent.lib.roles import publisher
    config = dict(data_dir=str(data_dir), publication_root=str(publication_root),
                  initial_publication=str(initial_publication))
    return publisher(config, state_dir=ROOT/'state' if state_dir is None else state_dir,
                     config_path=ROOT/'controller.json' if config_path is None else config_path).unit


def argument_values(args, flag):
    values = []
    for index, value in enumerate(args):
        if value == flag:
            if index+1 >= len(args) or args[index+1].startswith('--'):
                raise ValueError('worker unit has an incomplete '+flag)
            values.append(args[index+1])
        elif value.startswith(flag+'='):
            if not value[len(flag)+1:]:
                raise ValueError('worker unit has an incomplete '+flag)
            values.append(value[len(flag)+1:])
    if len(values) > 1:
        raise ValueError('worker unit has duplicate '+flag)
    return values


def replace_argument(args, flag, value):
    argument_values(args, flag)
    output, index = [], 0
    while index < len(args):
        if args[index] == flag:
            index += 2
        elif args[index].startswith(flag+'='):
            index += 1
        else:
            output.append(args[index])
            index += 1
    return output+[flag, str(value)]


def worker_control_args(args, config):
    """Bind control/persistence to the selected schema without reusing v10 state."""
    args = list(args)
    socket = argument_values(args, '--control-socket')
    active = argument_values(args, '--active-record')
    if bool(socket) != bool(active):
        raise ValueError('worker unit has incomplete publication control')
    schema = config.get('worker_schema')
    if schema is None:
        if not socket:
            args += ['--control-socket','/run/transparent-pir/control.sock',
                     '--active-record','/opt/transparent-publisher/active.json']
        return args
    if schema != 'transparent-shard-v11':
        raise ValueError('unsupported worker schema namespace')
    expected = {'worker_active_record': '/opt/transparent-publisher/v11/active.json',
                'worker_runtime_cache_dir': '/srv/transparent-pir/v11/runtime-cache',
                'worker_root': '/srv/transparent-pir/v11/publications'}
    # These fixed public namespaces keep every new collector away from v10
    # rollback data. A future schema needs its own reviewed namespace contract.
    if any(config.get(key) != value for key,value in expected.items()):
        raise ValueError('v11 requires all separate worker persistence namespaces')
    publication = argument_values(args, '--shard-dir')
    if len(publication) != 1 or not Path(publication[0]).is_relative_to(Path(expected['worker_root'])):
        raise ValueError('v11 worker unit must already select its staged v11 publication namespace')
    if '..' in Path(publication[0]).parts:
        raise ValueError('v11 worker publication path contains traversal')
    args = replace_argument(args, '--control-socket', '/run/transparent-pir/control.sock')
    args = replace_argument(args, '--active-record', expected['worker_active_record'])
    return replace_argument(args, '--runtime-cache-dir', expected['worker_runtime_cache_dir'])


def execute(args, **kwargs):
    subprocess.run(list(map(str,args)),check=True,**inherited_lock.options(),**kwargs)


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
    path=Path('/etc/systemd/system/transparent-filter-server.service')
    text=path.read_text()
    lines=text.splitlines()
    args=next(shlex.split(line.removeprefix('ExecStart=')) for line in lines if line.startswith('ExecStart='))
    if len(args)!=3 or args[1]!='--config' or Path(args[0]).name!='pir-publisher':
        raise RuntimeError('filter service must use pir-publisher with its installed host configuration')
    executable,_,config_path=args
    execute([executable,'verify-filters','--shard-dir',initial])
    config_path=Path(config_path)
    config=json.loads(config_path.read_text())
    settings=config['transparent_filters']['settings']
    if settings.get('shard_dir')!=str(initial):
        atomic_bytes(ROOT/'filter-config-before-alignment',config_path.read_bytes())
        settings['shard_dir']=str(initial)
        atomic_bytes(config_path,(json.dumps(config,indent=2)+'\n').encode())
        execute(['systemctl','restart','transparent-filter-server'])


# The worker runs the release's pir-query-server over its generated
# configuration. Hosts installed before it run a retired executable with flags;
# their unit, executable and (absent) configuration are what a rollback restores.
WORKER_BINARY = transparent_unit.SERVER
WORKER_CONFIG = transparent_unit.CONFIG
RETIRED_WORKER_BINARIES = ' '.join(transparent_unit.RETIRED_NAMES)

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
    try:
        current=(await fleet.ssh(host,'cat '+WORKER_CONFIG)).decode() if transparent_unit.configured(unit) else None
        # The running settings as flags, from a retired executable's command
        # line or the configuration pir-query-server runs.
        args=[WORKER_BINARY]+transparent_unit.server_flags(unit, current)
    except ValueError as error:
        raise RuntimeError('worker unit lacks a simple ExecStart') from error
    # pir-query-server refuses the unspecified address a retired unit may
    # name; it listens on the worker's own VPC upstream.
    args = replace_argument(args, '--listen', worker['upstream'])
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
    args = worker_control_args(args, fleet.c)
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
    new_unit, new_config = transparent_unit.worker_exec(unit, args[1:], WORKER_BINARY)
    # RuntimeDirectory is created before the binary opens its control socket.
    if 'RuntimeDirectory=transparent-pir' not in new_unit.splitlines():
        new_unit=new_unit.replace('[Service]','[Service]\nRuntimeDirectory=transparent-pir',1)
    if worker['role'] in transparent_unit.MEMORY_HIGH:
        # Reclaim file cache before the hard limit; anonymous allocations
        # still obey the work/cache guards. The values are per role.
        new_unit=transparent_unit.set_memory_high(new_unit, worker['role'])
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
    active_parent = Path(argument_values(args, '--active-record')[0]).parent
    await fleet.ssh(host,'mkdir -p '+remote+' '+shlex.quote(rollback)+' '+shlex.quote(str(active_parent)))
    if headless:
        helper=(SCRIPT/'transparent-headless-console.py').read_bytes()
        await fleet.ssh(host,'cat > '+remote+'/headless-console.py',helper)
        await fleet.ssh(host,'python3 '+remote+'/headless-console.py --preflight')
    if storage:
        storage_helper=(SCRIPT/'transparent-storage-policy.py').read_bytes()
        await fleet.ssh(host,'cat > '+remote+'/storage-policy.py',storage_helper)
        await fleet.ssh(host,'python3 '+remote+'/storage-policy.py --preflight')
    for name in ['pir-query-server','shard-control']:
        await LIVE.run(['rsync','-a','-e',shlex.join(fleet.ssh_args),str(artifacts/name),'root@'+host+':'+remote+'/'+name],timeout=120)
    await verify_worker_artifacts(fleet, host, artifacts, remote)
    await fleet.ssh(host,'cat > '+remote+'/shard-server.json',new_config.encode())
    # The staged server checks the configuration statically, then loads and
    # verifies it through its one-shot path, which binds no listener.
    check=[remote+'/pir-query-server','--config',remote+'/shard-server.json']
    await fleet.ssh(host,shlex.join(check+['--check']),timeout=60,multiplex=False)
    await fleet.ssh(host,shlex.join(check+['--verify-only']),timeout=300,multiplex=False)
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
 # Keep whichever executable the saved unit runs, retired names included,
 # and the configuration a pir-query-server unit reads.
 for name in pir-query-server {RETIRED_WORKER_BINARIES}; do
  if [ -f /usr/local/bin/$name ]; then cp /usr/local/bin/$name {shlex.quote(rollback)}/$name; fi
 done
 if [ -f {WORKER_CONFIG} ]; then cp {WORKER_CONFIG} {shlex.quote(rollback)}/shard-server.json; fi
 cp /usr/local/bin/shard-control {shlex.quote(rollback)}/shard-control
 if [ -f {HEADLESS_PATH} ]; then cp {HEADLESS_PATH} {shlex.quote(rollback)}/headless-console.py; fi
fi
{headless_install}
{storage_install}
install -m755 {remote}/pir-query-server {WORKER_BINARY}.next
mv {WORKER_BINARY}.next {WORKER_BINARY}
install -d -m755 {shlex.quote(str(Path(WORKER_CONFIG).parent))}
install -m644 {remote}/shard-server.json {WORKER_CONFIG}.next
mv {WORKER_CONFIG}.next {WORKER_CONFIG}
install -m755 {remote}/shard-control /usr/local/bin/shard-control
install -m644 {remote}/worker.service /etc/systemd/system/transparent-shard-server.service
systemctl daemon-reload
systemctl restart transparent-shard-server
'''
    await fleet.ssh(host,command)
    expected_binary=hashlib.sha256((artifacts/'pir-query-server').read_bytes()).hexdigest()
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


def restore_config(backup):
    """Shell restoring the saved worker configuration; a unit saved before
    pir-query-server had none, so the generated one is removed."""
    saved = shlex.quote(str(backup)) + '/shard-server.json'
    return (f'if [ -f {saved} ]; then install -D -m644 {saved} {WORKER_CONFIG}.rollback && '
            f'mv {WORKER_CONFIG}.rollback {WORKER_CONFIG}; else rm -f {WORKER_CONFIG}; fi')


async def verify_worker_artifacts(fleet, host, artifacts, remote):
    hashes = (await fleet.ssh(host, 'sha256sum '+remote+'/pir-query-server '+remote+'/shard-control')).decode().splitlines()
    expected_hashes = [hashlib.sha256((artifacts/name).read_bytes()).hexdigest()+'  '+remote+'/'+name
                       for name in ['pir-query-server','shard-control']]
    if hashes != expected_hashes:
        raise RuntimeError('worker transferred artifact checksum mismatch')


async def rollback(fleet, saved):
    subprocess.run(['systemctl','stop','transparent-replica-reconciler'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,**inherited_lock.options())
    subprocess.run(['systemctl','stop','transparent-publish-controller'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,**inherited_lock.options())
    # A binary rollback must never resurrect an orphaned publication. Validate
    # the static predecessor against the node before changing routing or units.
    initial=Path(json.loads((saved/'controller.json').read_text())['transparent_history']['settings']['initial_publication'])
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
for name in pir-query-server {RETIRED_WORKER_BINARIES}; do
 if [ -f {shlex.quote(remote)}/$name ]; then
  cp {shlex.quote(remote)}/$name /usr/local/bin/$name.rollback
  chmod 755 /usr/local/bin/$name.rollback
  mv /usr/local/bin/$name.rollback /usr/local/bin/$name
 fi
done
{restore_config(remote)}
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
                     help='Event journal the controller publishes from (shadow mode); use a separate v3 journal for schema v11')
    cli.add_argument('--publication-root',type=Path,default=Path('/srv/zakura/transparent-publications'),
                     help='Writable publication parent (shadow mode); keep it on the journal filesystem')
    cli.add_argument('--directory-choice',choices=['off','sealed','all'],default='off',
                     help='Which newly built shards publish a directory choice table (shadow mode)')
    args=cli.parse_args()
    service = publisher_service(args.data_dir, args.publication_root, args.initial_publication) if args.mode=='shadow' else None
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
        if (ROOT/'state/inventory.json').exists():
            # The inventory owns membership once it exists: keep its roster
            # (elastic replicas, pinned archive ranges) and its host keys.
            INVENTORY.Inventory(ROOT/'state', ROOT/'roster.json', ROOT/'credentials/known_hosts').project(
                INVENTORY.Inventory(ROOT/'state').last_good())
        else:
            LIVE.atomic_json(ROOT/'roster.json',json.loads(os.environ['TRANSPARENT_FLEET_JSON']))
        fleet_config=dict(roster=str(ROOT/'roster.json'),state_dir=str(ROOT/'state'),ssh_key=str(ROOT/'credentials/deploy-ssh'),known_hosts=str(ROOT/'credentials/known_hosts'),
                          assign_binary='/usr/local/bin/shard-assign',public_host='transparent-pir.valargroup.dev',router_host=os.environ['TRANSPARENT_ROUTER_HOST'],
                          authority_upstream='https://enhance-pir.valargroup.dev',internal_listen=os.environ['TRANSPARENT_ROUTER_HOST']+':8080')
        # Operators and rollouts own these settings. Regenerating the file
        # without them once disabled status forwarding and replica catch-up.
        if 'fleet.json' in previous_files:
            fleet_config=LIVE.carry_operational(fleet_config,json.loads(previous_files['fleet.json']))
        LIVE.atomic_json(ROOT/'fleet.json',fleet_config)
        config=dict(data_dir=str(args.data_dir),publication_root=str(args.publication_root),initial_publication=str(args.initial_publication),
                    recent_from=3262749,recent_geometry=args.recent_geometry,archive_geometry='archive-wide',rpc_url='http://127.0.0.1:8232',rpc_cookie='/root/.cache/zakura/.cookie',
                    fleet_command=str(ROOT/'transparent-live-fleet.py'),fleet_config=str(ROOT/'fleet.json'),listen='127.0.0.1:8094',source_sha=args.source_sha,shadow=True)
        # Written only when enabled, so a controller built before the field
        # (whose config refuses unknown fields) can still read a default config.
        if args.directory_choice!='off':
            config['directory_choice']=args.directory_choice
        if args.range_profile!='zcash-transparent-range-v1':
            config['range_profile']=args.range_profile
        from tools.transparent.lib.roles import publisher
        rendered = publisher(config, state_dir=ROOT/'state', config_path=ROOT/'controller.json',
                             executable=str(ROOT/'releases'/args.source_sha/'pir-publisher'))
        service = rendered.unit
        atomic_bytes(ROOT/'controller.json', rendered.files[str(ROOT/'controller.json')].encode())
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
        subprocess.run(['systemctl','stop','transparent-publish-controller'],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,**inherited_lock.options())
        try:
            align_filter_origin(args.initial_publication)
            await save_baseline(fleet,saved)
        except Exception:
            # No worker has changed yet: resume the previous controller.
            restore_previous()
            subprocess.run(['systemctl','start','transparent-publish-controller'],check=False,**inherited_lock.options())
            raise
        release_dir=ROOT/'releases'/args.source_sha
        release_dir.mkdir(parents=True,exist_ok=True)
        shutil.copy2(args.artifacts/'pir-publisher',release_dir/'pir-publisher')
        os.chmod(release_dir/'pir-publisher',0o755)
        shutil.copy2(args.artifacts/'shard-assign','/usr/local/bin/shard-assign.next')
        os.chmod('/usr/local/bin/shard-assign.next',0o755)
        os.replace('/usr/local/bin/shard-assign.next','/usr/local/bin/shard-assign')
        shutil.copy2(SCRIPT/'transparent-live-fleet.py',ROOT/'transparent-live-fleet.py')
        shutil.copy2(SCRIPT/'transparent-fleet-inventory.py',ROOT/'transparent-fleet-inventory.py')
        args.publication_root.mkdir(parents=True,exist_ok=True)
        Path('/etc/systemd/system/transparent-publish-controller.service').write_text(service)
        shutil.copy2(SCRIPT.parent/'deploy/transparent-replica-reconciler.service','/etc/systemd/system/transparent-replica-reconciler.service')
        shutil.copy2(SCRIPT.parent/'deploy/transparent-control-sessions.service','/etc/systemd/system/transparent-control-sessions.service')
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
        settings=config['transparent_history']['settings']
        if settings['source_sha']!=args.source_sha:
            raise RuntimeError('activation must name the shadow-tested source SHA')
        settings['shadow']=False
        LIVE.atomic_json(ROOT/'controller.json',config)
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
