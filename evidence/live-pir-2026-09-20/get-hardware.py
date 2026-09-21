import concurrent.futures,pathlib,json,subprocess
p=pathlib.Path(__file__).resolve().parent
hosts={'coordinator':'167.99.42.60','enhance-01':'10.142.0.15','enhance-02':'10.142.0.16'}
for r in json.loads((p/'roster.json').read_text()):hosts[r['id']]=r['ssh_host']
def get(pair):
 name,ip=pair;args=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','-o','StrictHostKeyChecking=accept-new','-o','UserKnownHostsFile=/tmp/wallet-pir-bench-known-hosts']
 if name!='coordinator':args+=['-J','root@167.99.42.60']
 args+=['root@'+ip,'python3 -'];r=subprocess.run(args,input=(p/'hardware.py').read_bytes(),capture_output=True,timeout=30)
 (p/(name+'-hardware.json')).write_bytes(r.stdout);(p/(name+'-hardware.stderr')).write_bytes(r.stderr)
 return name,r.returncode
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:print(list(ex.map(get,hosts.items())))
