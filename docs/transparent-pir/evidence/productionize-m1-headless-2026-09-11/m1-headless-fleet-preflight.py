import asyncio,hashlib,importlib.util,json
from pathlib import Path
spec=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M)
async def main():
 fleet=M.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
 helper=Path('/tmp/m1-headless-console.py').read_bytes();digest=hashlib.sha256(helper).hexdigest()
 slots=asyncio.Semaphore(3)
 async def check(worker):
  async with slots:
   await fleet.ssh(worker['ssh_host'],'cat > /tmp/m1-headless-console.py',helper)
   value=json.loads(await fleet.ssh(worker['ssh_host'],'python3 /tmp/m1-headless-console.py --preflight',timeout=10))
   assert value['helper_sha256']==digest
   return {'worker':worker['id'],**value}
 result={'source_sha256':digest,'workers':await asyncio.gather(*(check(w) for w in fleet.roster))}
 root=Path('/opt/transparent-publisher-build/headless-20260911');root.mkdir(exist_ok=True)
 (root/'preflight.json').write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(result,indent=2))
asyncio.run(main())
