import asyncio,importlib.util,json
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()));w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
 cmd="journalctl -u ssh -u systemd-logind --since '2026-09-10 04:51:40 UTC' --until '2026-09-10 04:52:20 UTC' --no-pager -o short-iso-precise"
 b=await f.ssh(w['ssh_host'],cmd,multiplex=False)
 p=Path('/opt/transparent-publisher-build/status-latency-20260910/ssh-failure.log');p.write_bytes(b);print(b.decode()[-6000:])
asyncio.run(main())
