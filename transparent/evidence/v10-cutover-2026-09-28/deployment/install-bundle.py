import hashlib,json,pathlib,shutil,subprocess,tarfile
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
archive=root/'release.tgz'
assert hashlib.sha256(archive.read_bytes()).hexdigest()=='24a86054f6ea74e0b6fbc55e669dc8185f962a11171ab4bae2d10edf82a5c7a7'
bins=root/'bin';bins.mkdir(exist_ok=False)
with tarfile.open(archive) as tar:
 assert all(x.isfile() and pathlib.Path(x.name).name==x.name for x in tar.getmembers())
 tar.extractall(bins,filter='data')
subprocess.run(['sha256sum','-c','SHA256SUMS'],cwd=bins,check=True)
sha=(bins/'revision').read_text().strip()
assert sha=='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
for name in ['shard-publish','transparent-shard-server','transparent-filter-server','transparent-publish-controller','shard-assign','shard-verify','regression-export','transparent-regression','transparent-loadtest']:
 subprocess.run([str(bins/name),'--help'],stdout=subprocess.DEVNULL,check=True)
inventory={'transparent-filter':['transparent-filter-server'],'transparent-shard':['transparent-shard-server','shard-assign','shard-prune'],'transparent-publisher':['transparent-publish-controller','transparent-shard-server','shard-control','shard-assign']}
extras={'transparent-filter':['transparent-filter-server.service'],'transparent-shard':['transparent-shard-server.service','transparent-Caddyfile'],'transparent-publisher':[]}
for kind,names in inventory.items():
 dest=root/'artifacts'/kind;dest.mkdir(parents=True,exist_ok=False)
 for name in names: shutil.copy2(bins/name,dest/name)
 for name in extras[kind]: shutil.copy2(root/'source/transparent/ops/deploy'/name,dest/name)
 (dest/'revision').write_text(sha+'\n')
 (dest/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in sorted(dest.iterdir())))
print('Bundle checksums, revision, executable startup and deployment inventories verified.')
