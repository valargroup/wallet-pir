import json,subprocess,hashlib,sys,urllib.request
from pathlib import Path
label,url,binary=sys.argv[1:]
root=Path('/srv/enhance-pir-v11/validation')/label
root.mkdir(parents=True,exist_ok=False)
with urllib.request.urlopen(url+'/v1/enhance/init',timeout=30) as r:m=json.load(r)
assert m['schema_version']==11,m
(root/'manifest.json').write_text(json.dumps(m,indent=2))
count=m['coverage']['records'];assert count>570000,count
positions=[0,1,32,33,270335,270336,540671,540672,count-1]
entries=[]
with open('/srv/enhance-pir-v11/canonical/enhance/records.bin','rb') as f:
 for p in positions:
  f.seek(p*653);v=f.read(653);assert len(v)==653
  entries.append({'position':p,'record_hex':v.hex()})
oracle=root/'oracle.json';oracle.write_text(json.dumps(entries))
(root/'provenance.json').write_text(json.dumps({'source':'fresh schema-11 canonical journal; exact PIR comparison, not independent transaction extraction','oracle_sha256':hashlib.sha256(oracle.read_bytes()).hexdigest()}))
cmd=[binary,'--v4','--server',url,'--oracle',str(oracle),'--parallelism','2','--warmup','3s','--duration','30s','--seed','20260923','--max-error-rate','0','--json-out',str(root/'load.json')]
(root/'command.json').write_text(json.dumps(cmd))
with (root/'load.log').open('w') as f: subprocess.run(cmd,stdout=f,stderr=f,check=True,timeout=180)
print(label,'passed',count,flush=True)
