"""Keep Enhance's installed service identity and prepare only new cache roots."""
from dataclasses import replace
import json
from pathlib import PurePosixPath
import re

from wallet_pir_ops.role_config import set_service
from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.engine import DeployError, SYSTEM, unit_files


# Runs as the pinned SSH operator. Permission checks run in a child with the
# same uid, primary group and supplementary groups systemd gives the service.
# Credential environment files are checked by the operator, who reads them
# for systemd, without reading their contents or requiring service-user access.
PROGRAM = '''import grp,json,os,pwd,sys
from pathlib import Path
value=json.loads(sys.argv[1]); prepare=value['prepare']
user=pwd.getpwnam(value['user'])
gid=grp.getgrnam(value['group']).gr_gid if value['group'] else user.pw_gid
directories=[Path(p) for p in value['directories']]
checks=[]
for path in directories:
 assert path.is_absolute() and path.resolve()==path, 'cache path must not contain symlinks'
 parent=path
 while not parent.exists(): parent=parent.parent
 assert parent.is_dir(), 'cache parent is not a directory'
 if path.exists():
  info=path.stat()
  assert path.is_dir() and info.st_uid==user.pw_uid, 'existing cache has a foreign owner'
  assert not info.st_mode & 2, 'existing cache is world writable'
  checks.append((str(path),os.W_OK|os.X_OK))
 else:
  checks.append((str(parent),os.X_OK))
for name in value['credentials']:
 optional=name.startswith('-'); path=Path(name[1:] if optional else name)
 assert path.is_absolute(), 'credential file must be absolute'
 if optional and not path.exists(): continue
 assert path.is_file() and os.access(path,os.R_OK), 'credential environment file is unavailable'
def check_access():
 pid=os.fork()
 if pid==0:
  try:
   if os.geteuid()==0:
    os.setgroups(os.getgrouplist(user.pw_name,gid)); os.setgid(gid); os.setuid(user.pw_uid)
   else:
    assert os.geteuid()==user.pw_uid and os.getegid()==gid
   assert all(os.access(path,mode) for path,mode in checks), 'service account cannot access its cache path'
  except Exception:
   os._exit(1)
  os._exit(0)
 assert os.waitpid(pid,0)[1]==0, 'service account cannot access its cache path'
check_access()
if prepare:
 for path in directories:
  if path.exists(): continue
  missing=[]; parent=path.parent
  while not parent.exists(): missing.append(parent); parent=parent.parent
  for item in reversed(missing):
   item.mkdir(mode=0o755); item.chmod(0o755)
  path.mkdir(mode=0o750); path.chmod(0o750)
  os.chown(path,user.pw_uid,gid)
  fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
  try: os.fsync(fd)
  finally: os.close(fd)
 checks=[(str(path),os.W_OK|os.X_OK) for path in directories]
 check_access()
print(json.dumps({'uid':user.pw_uid,'gid':gid}))
'''


class Guard:
    manages_directories = True

    def rendered_role(self, target, state, rendered):
        """Installed defaults survive a binary/configuration update.

        Explicit inventory limits and credential references remain authoritative.
        A fresh host uses the template defaults and must provision that account.
        """
        if state['load_state'] != 'loaded':
            return rendered
        previous = units.effective(text for _, text in unit_files(state)).get('Service', {})
        limits = target.settings.get('limits', {})
        inherited = {}
        if 'User' not in limits:
            inherited['User'] = previous.get('User', 'root')
        if ('Group' not in limits and previous.get('Group')
                and limits.get('User', previous.get('User', 'root')) == previous.get('User', 'root')):
            inherited['Group'] = previous['Group']
        if 'credentials' not in target.settings and previous.get('EnvironmentFile'):
            inherited['EnvironmentFile'] = previous['EnvironmentFile']
        return replace(rendered, unit=set_service(rendered.unit, inherited)) if inherited else rendered

    def check(self, deployer, host, text, directories, *, prepare=False):
        service = units.effective([text]).get('Service', {})
        value = {'user': service.get('User', 'root'), 'group': service.get('Group', ''),
                 'credentials': service.get('EnvironmentFile', []),
                 'directories': list(directories), 'prepare': prepare}
        code, output = deployer.ex.run(host, ['python3', '-c', PROGRAM, json.dumps(value)], 30)
        return [] if code == 0 else ['service account, credential files or cache access refused: ' + output[-1000:]]

    def desired_unit(self, deployer, record):
        path = SYSTEM + '/' + record['unit']
        text = next((c['new'] for c in record['changes'] if c['path'] == path), None)
        text = text if text is not None else deployer.ex.read(record['host'], path)
        if text is None:
            raise DeployError('serving unit disappeared: ' + path)
        return text

    def plan(self, deployer, plan):
        path = SYSTEM + '/' + plan.target.unit
        text = next((c['new'] for c in plan.changes if c['path'] == path), plan.state['fragment_text'])
        return self.check(deployer, plan.target.host, text, plan.directories)

    def before_restart(self, deployer, record):
        return self.check(deployer, record['host'], self.desired_unit(deployer, record), record['directories'])

    def prepare_directories(self, deployer, journal, record):
        deployer.lock.verify()
        journal.event('prepare service-owned cache directories', host=record['host'], unit=record['unit'])
        problems = self.check(deployer, record['host'], self.desired_unit(deployer, record),
                              record['directories'], prepare=True)
        if problems:
            raise DeployError(record['key'] + ': ' + '; '.join(problems))

    def preflight(self, deployer, journal, record, sha):
        problems = self.before_restart(deployer, record)
        if problems:
            raise DeployError(record['key'] + ': ' + '; '.join(problems))
        deployer.configuration_check(journal, record, sha)

    def ready(self, deployer, record, expected, document):
        return True, 'ready'

    def restored_ready(self, record, expected, document):
        """The restored legacy CUDA worker reports holdings instead of `ready`.

        Only its recorded executable and restoration phase may use that older
        contract. Autonomous query hosts always need their normal readiness.
        """
        previous = record.get('previous', {})
        if (record.get('phase') not in ('restoring', 'restore-failed')
                or record.get('role') != 'worker'
                or expected != previous.get('exe_sha256')
                or PurePosixPath(previous.get('exec_binary', '')).name != 'enhance-pir-server'
                or not isinstance(document, dict) or 'ready' in document
                or document.get('binary_sha256') != expected
                or document.get('protocol') != 'ironwood-enhance-pir-v9-native-two-mask-m29'
                or document.get('matvec') != {'matvec_backend': 'cuda', 'cuda_device': 0}):
            return False
        published = document.get('published')
        digests = document.get('published_manifest_digests')
        resident = document.get('resident_database_bytes')
        return (isinstance(published, list) and bool(published)
                and all(type(shard) is int and shard >= 0 for shard in published)
                and len(set(published)) == len(published)
                and isinstance(digests, dict) and set(digests) == {str(shard) for shard in published}
                and all(isinstance(digest, str) and re.fullmatch('[a-f0-9]{64}', digest)
                        for digest in digests.values())
                and type(resident) is int and resident > 0)

    def before_restore(self, deployer, journal, record):
        # The predecessor's directories and ownership were never changed.
        return []
