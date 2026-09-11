import asyncio,importlib.util,json,datetime
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
 out={'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'last_sample_utc':'2026-09-11T03:31:59.177127+00:00','served_endpoint':{'digest': '55e197161ea52fda8f2a65b45b654e109b961bdb1d77de913cdd463bb0d0ae86', 'end_height': 3479236, 'terminal_block_hash': '000000000055fddecdc0f968f66b39f9e9ad81e8fe8f5df4f1eb9e0d9baf946a'},'canonical_hash_now':await f.canonical_hash(3479236),'candidate_height_canonical_hash_now':await f.canonical_hash(3479237)}
 print(json.dumps(out,indent=2))
asyncio.run(main())
