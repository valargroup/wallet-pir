import asyncio,datetime,importlib.util,json,pathlib,urllib.request
p=pathlib.Path('/opt/transparent-publisher-build/a671e4e/ops/scripts/transparent-live-fleet.py');s=importlib.util.spec_from_file_location('f',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 f=m.Fleet(json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text()))
 f.roster=[w for w in f.roster if w['role']=='archive-owner' or w['id']=='transparent-pir-recent-01']
 active=json.loads(pathlib.Path('/srv/zakura/transparent-publications/active.json').read_text())
 assert await f.canonical_hash(active['height'])==active['hash']
 request=dict(directory=active['directory'],map_sha256=active['map_sha256'],recent_from=3262749,source_sha='c676fb69ea9d2dd32d141be4ca86778a0608b650')
 prepared=await f.prepare(request)
 f.canonical.clear();assert await f.canonical_hash(active['height'])==active['hash']
 result=await f.activate(dict(request,prepared=prepared))
 maps=[urllib.request.urlopen(u).read() for u in ['https://transparent-pir.valargroup.dev/v1/shards','https://enhance-pir.valargroup.dev/v1/filters/shards']]
 assert maps[0]==maps[1]
 print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),event='takeover',result=result,public_map_sha256=active['map_sha256'],height=active['height'])),flush=True)
asyncio.run(main())
