import datetime,hashlib,json,shutil,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/publication-io-20260910')
out=root/'start-evidence';out.mkdir()
for name in ['routing-audit-install/result.json','rollout/status.json','rollout/canary-upgrade/result.json','rollout/canary-upgrade/verified.json','rollout/canary-upgrade/deployment.json','rollout/canary/samples.ndjson','rollout/canary/query-0.ndjson','rollout/canary/query-1.ndjson','qualification-summary.json']:
 target=out/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/name,target)
units=subprocess.check_output(['systemctl','show','transparent-m1-publication-io-rollout.service','transparent-m1-install-routing-audit.service','transparent-m1-publication-io-control-build.service','-p','Id','-p','MainPID','-p','ActiveState','-p','ExecMainStatus','-p','ExecMainStartTimestamp','-p','ExecMainExitTimestamp'],text=True)
(out/'units.txt').write_text(units)
live=Path('/opt/transparent-publisher');c=json.loads((live/'fleet.json').read_text())
record={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'acceptance_complete':False,'source':'043c051','hashes':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [live/'transparent-live-fleet.py',live/'fleet.json',live/'roster.json',*sorted((root/'artifacts').iterdir())]},'routing_availability':json.loads((Path(c['state_dir'])/'routing-availability.json').read_text())}
(out/'capture.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps({'utc':record['utc'],'routing_availability':record['routing_availability'],'units':units},indent=2))
