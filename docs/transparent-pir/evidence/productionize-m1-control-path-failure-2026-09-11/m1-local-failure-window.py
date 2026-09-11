import json
from pathlib import Path
out={}
for name in ['unix','http']:
 rows=[json.loads(x) for x in Path('/run/tp-publication-io-probe',name+'.ndjson').read_text().splitlines()]
 out[name]=[r for r in rows if '2026-09-11T00:08:18' <= r['utc'] <= '2026-09-11T00:08:31']
print(json.dumps(out,indent=2))
