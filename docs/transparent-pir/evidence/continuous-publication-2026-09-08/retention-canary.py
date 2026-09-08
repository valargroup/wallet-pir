import asyncio,datetime,importlib.util,json,pathlib,time,urllib.request,os,shutil,tempfile,hashlib
BASE=pathlib.Path('/opt/transparent-publisher-build/6e0c65c')
spec=importlib.util.spec_from_file_location('deploy',BASE/'ops/scripts/deploy-transparent-publisher.py'); d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)
ROOT=pathlib.Path('/opt/transparent-publisher')
def emit(**kw): print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),**kw)),flush=True)
async def main():
    fleet=d.LIVE.Fleet(json.loads((ROOT/'fleet.json').read_text()))
    worker=next(w for w in fleet.roster if w['id']=='transparent-pir-recent-01')
    # The live router currently selects recent-02 alone. Fail before touching a routed canary.
    route=(await fleet.ssh(fleet.c['router_host'],'cat /etc/caddy/Caddyfile')).decode()
    assert worker['upstream'] not in route,'canary is routed; withdraw it before isolated churn'
    emit(event='install_start',worker=worker['id'],source_sha='6e0c65c1b2cb4f4e4bae1fb4e1b34e282ea19ea2')
    ready=d.read_json('http://'+worker['upstream']+'/v1/ready')
    expected_binary=hashlib.sha256((BASE/'artifacts/transparent-shard-server').read_bytes()).hexdigest()
    if ready.get('binary_sha256')!=expected_binary:
        await d.install_worker(fleet,worker,pathlib.Path('/opt/transparent-publisher-build/6e0c65c/artifacts'),'/opt/transparent-publisher/rollback/6e0c65c1b2cb4f4e4bae1fb4e1b34e282ea19ea2')
    baseline=(await fleet.ssh(worker['ssh_host'],'systemctl show transparent-shard-server -p NRestarts; cat /sys/fs/cgroup/system.slice/transparent-shard-server.service/memory.events')).decode()
    counters=lambda raw: {line.split()[0]:line.split()[1] for line in raw.replace('NRestarts=','NRestarts ').splitlines() if line.startswith(('NRestarts','oom_kill '))}
    emit(event='installed',counters=counters(baseline))
    # Exercise the real journal's candidate generations without changing public routing or authority.
    fleet.roster=[worker]
    active=json.loads(pathlib.Path('/srv/zakura/transparent-publications/active.json').read_text())
    by_height={}
    for directory in pathlib.Path('/srv/zakura/transparent-publications').glob('candidate-*'):
        try:
            b=(directory/'shards.json').read_bytes();m=json.loads(b);tail=m['shards'][-1]
            if tail['end_height']>active['height']:
                by_height[tail['end_height']]=(directory,hashlib.sha256(json.dumps(m,separators=(",",":")).encode()).hexdigest(),tail['terminal_block_hash'])
        except (OSError,ValueError):pass
    assert len(by_height)>=2, 'need distinct real-block revisions'
    frozen=pathlib.Path(tempfile.mkdtemp(prefix='memory-canary-6e0-',dir='/srv/zakura'))
    for height,(directory,digest,block_hash) in list(by_height.items()):
        target=frozen/str(height);shutil.copytree(directory,target,copy_function=os.link)
        by_height[height]=(target,digest,block_hash)
    cases=(sorted(by_height.items())*6)[:12]
    emit(event='workload',distinct_heights=sorted(by_height),generations=len(cases),repeat=True)
    for height,(directory,digest,block_hash) in cases:
        fleet.canonical.clear()
        assert await fleet.canonical_hash(height)==block_hash
        began=time.monotonic()
        request=dict(directory=str(directory),map_sha256=digest,recent_from=3262749,source_sha='c676fb69ea9d2dd32d141be4ca86778a0608b650')
        for attempt in range(3):
            try:
                prepared=await fleet.prepare(request);break
            except RuntimeError as error:
                emit(event='prepare_retry',height=height,attempt=attempt+1,error=str(error))
                if attempt==2:raise
                await asyncio.sleep(5)
        before=prepared['workers'][worker['id']]['expected']
        fleet.canonical.clear()
        assert await fleet.canonical_hash(height)==block_hash
        await fleet.control(worker,dict(operation='activate',expected=before,map_sha256=digest))
        await fleet.control(worker,dict(operation='collect'))
        status=await fleet.control(worker,dict(operation='status'))
        assert status['retired_snapshots']<=3, 'idle generations were retained'
        ready=d.read_json('http://'+worker['upstream']+'/v1/ready')
        assert ready['ready'] and ready['map_sha256']==digest
        with urllib.request.urlopen('http://'+worker['upstream']+'/metrics') as r:metrics=r.read().decode()
        memory=(await fleet.ssh(worker['ssh_host'],'systemctl show transparent-shard-server -p NRestarts -p MemoryCurrent -p MemoryPeak; cat /sys/fs/cgroup/system.slice/transparent-shard-server.service/memory.events')).decode()
        emit(event='generation',height=height,map_sha256=digest,seconds=time.monotonic()-began,ready=ready,metrics=metrics,memory=memory,control_status=status)
        assert counters(memory)==counters(baseline), 'worker restarted or suffered an OOM during churn'
    emit(event='result',passed=True,generations=len(cases),public_routing_unchanged=(await fleet.ssh(fleet.c['router_host'],'cat /etc/caddy/Caddyfile')).decode()==route)
asyncio.run(main())
