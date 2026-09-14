import asyncio,base64,datetime,hashlib,importlib.util,json,pathlib,subprocess,urllib.request
ROOT=pathlib.Path('/opt/transparent-publisher')
spec=importlib.util.spec_from_file_location('fleet',ROOT/'transparent-live-fleet.py'); module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def get(url):
    try:
        with urllib.request.urlopen(url,timeout=8) as r: return r.read()
    except urllib.error.HTTPError as e: return e.read()
def json_get(url):
    raw=get(url)
    try:return json.loads(raw)
    except Exception:return {'raw':raw.decode()}
def rpc(method,params=[]):
    cookie=pathlib.Path('/root/.cache/zakura/.cookie').read_text().strip()
    req=urllib.request.Request('http://127.0.0.1:8232',json.dumps(dict(jsonrpc='2.0',id=1,method=method,params=params)).encode(),{'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
    return json.load(urllib.request.urlopen(req,timeout=8))['result']
async def main():
    fleet=module.Fleet(json.loads((ROOT/'fleet.json').read_text()))
    async def worker(w):
        ready=await asyncio.to_thread(json_get,'http://'+w['upstream']+'/v1/ready')
        metrics=(await asyncio.to_thread(get,'http://'+w['upstream']+'/metrics')).decode()
        facts=(await fleet.ssh(w['ssh_host'],'uname -a; lscpu; free -b; df -B1 /srv/transparent-pir; systemctl show transparent-shard-server -p NRestarts -p MemoryCurrent -p MemoryPeak -p MemoryMax -p ActiveEnterTimestamp; sha256sum /usr/local/bin/transparent-shard-server; cat /sys/fs/cgroup/system.slice/transparent-shard-server.service/memory.events')).decode()
        return w['id'],dict(ready=ready,metrics=metrics,facts=facts,control_status=await fleet.control(w,{'operation':'status'}))
    height=rpc('getblockcount')
    out=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),node_height=height,node_hash=rpc('getblockhash',[height]),genesis=rpc('getblockhash',[0]),controller=json_get('http://127.0.0.1:8094/v1/status'),controller_metrics=get('http://127.0.0.1:8094/metrics').decode(),active=json.loads(pathlib.Path('/srv/zakura/transparent-publications/active.json').read_text()),workers=dict(await asyncio.gather(*(worker(w) for w in fleet.roster))))
    out['publication_record']=json.loads((pathlib.Path(out['active']['directory'])/'publication.json').read_text())
    out['origins']={}
    for u in ['https://transparent-pir.valargroup.dev/v1/shards','https://enhance-pir.valargroup.dev/v1/filters/shards']:
        b=get(u);out['origins'][u]=dict(sha256=hashlib.sha256(b).hexdigest(),map=json.loads(b))
    out['coordinator_facts']=subprocess.check_output(['sh','-c','df -B1 /srv/zakura /; systemctl show transparent-publish-controller -p NRestarts -p MemoryCurrent -p MemoryPeak -p MemoryMax; sha256sum /usr/local/bin/transparent-publish-controller'],text=True)
    j=pathlib.Path('/srv/zakura/transparent-event-data');out['journal']=dict(meta=json.loads((j/'meta.json').read_text()),checkpoint_hex=(j/'checkpoint.bin').read_bytes().hex(),files={name:(j/name).stat().st_size for name in ['blocks.bin','events.bin']})
    print(json.dumps(out,indent=2))
asyncio.run(main())
