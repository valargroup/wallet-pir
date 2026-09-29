#!/usr/bin/env python3
"""Relocate only the new candidate, preserving v9 and both disk reserves."""
import datetime,hashlib,json,os,pathlib,shutil,subprocess,time
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
while not (root/'qualification/complete.json').exists():
 if subprocess.run(['systemctl','is-active','--quiet','wallet-pir-v10-qualification']).returncode!=0: raise RuntimeError('qualification failed')
 time.sleep(10)
assert json.loads((root/'qualification/complete.json').read_text())['source_sha']==sha
source=pathlib.Path('/srv/zakura/transparent-shards-v10-full')
target=pathlib.Path('/srv/transparent-data-v10/publications/initial')
temporary=pathlib.Path('/srv/zakura/transparent-shards-v10-full-relocated-original')
assert source.is_dir() and not source.is_symlink() and not target.exists() and not temporary.exists()
files=sorted(p for p in source.rglob('*') if p.is_file())
assert not any(p.is_symlink() for p in source.rglob('*'))
size=sum(p.stat().st_size for p in files)
space=shutil.disk_usage(root)
assert space.free-size>space.total//5, 'destination would have under 20% disk reserve'
record={'source':str(source),'target':str(target),'source_sha':sha,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'logical_bytes':size,'files':len(files),'destination_space_before':space._asdict()}
target.parent.mkdir(parents=True,exist_ok=False)
subprocess.run(['rsync','-a',str(source)+'/',str(target)+'/'],check=True)
def digest(path,sync=False):
 h=hashlib.sha256()
 with path.open('rb') as stream:
  while block:=stream.read(4*1024*1024): h.update(block)
  if sync: os.fsync(stream.fileno())
 return h.hexdigest()
with (root/'candidate-relocation-files.jsonl').open('w') as stream:
 for p in files:
  relative=p.relative_to(source);expected=digest(p);actual=digest(target/relative,True)
  assert actual==expected, ('copy hash mismatch',str(relative))
  stream.write(json.dumps({'path':str(relative),'bytes':p.stat().st_size,'sha256':actual})+'\n')
assert sorted(p.relative_to(target) for p in target.rglob('*') if p.is_file())==[p.relative_to(source) for p in files]
def sync_dir(path):
 fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
 try: os.fsync(fd)
 finally: os.close(fd)
for directory in sorted((p for p in target.rglob('*') if p.is_dir()),reverse=True): sync_dir(directory)
for directory in [target,target.parent,target.parent.parent,target.parent.parent.parent]: sync_dir(directory)
source.rename(temporary)
source.symlink_to(target,target_is_directory=True)
assert source.resolve()==target and digest(source/'shards.json')==digest(target/'shards.json')
sync_dir(source.parent)
# Only the verified duplicate of this task's new candidate is removed. Old
# schema data and unrelated evidence are never touched.
shutil.rmtree(temporary)
sync_dir(source.parent)
record.update(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),destination_space_after=shutil.disk_usage(root)._asdict(),journal_volume_space_after=shutil.disk_usage('/srv/zakura')._asdict(),all_files_sha256_equal=True)
for field in ['destination_space_after','journal_volume_space_after']:
 assert record[field]['free']>record[field]['total']//5, field
(root/'candidate-relocation.json').write_text(json.dumps(record,indent=2)+'\n')
(root/'candidate-storage.complete').write_text(sha+'\n')
print(json.dumps(record),flush=True)
