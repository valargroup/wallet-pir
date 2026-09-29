import concurrent.futures,json,pathlib,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75');dest=root/'prewarm-evidence';dest.mkdir(exist_ok=False)
assert (root/'staging.complete').exists()
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text());roster=json.loads((root/'pre-roster.json').read_text())
def one(w):
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host']]
 for suffix in ['original','retry'] if w['id']=='transparent-pir-archive-02' else ['original']:
  d='v10-prewarm-'+('retry-' if suffix=='retry' else '')+'8e69ea75'
  data=subprocess.check_output(args+['tar -czf - -C /opt/transparent-publisher '+d])
  (dest/(w['id']+'-'+suffix+'.tgz')).write_bytes(data)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(one,roster))
print('Captured all private prewarm attempts')
