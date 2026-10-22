"""Repair serving identities in place without restarting the publisher."""
import json
from pathlib import Path

from .. import durable
from ..role_config import set_service
from . import units
from .engine import mutation_fence
from .publisher import ENHANCE_HTTPS_SCHEMA, UNIT, require
from .remote import production_lock

IDENTITY = '''import json,pwd,grp,sys
from pathlib import Path
user=pwd.getpwnam(sys.argv[1])
gid=grp.getgrnam(sys.argv[2]).gr_gid if sys.argv[2] else user.pw_gid
paths=json.loads(sys.argv[3]); result=[]
for name in paths:
 p=Path(name)
 assert p.is_dir() and not p.is_symlink() and p.resolve()==p
 s=p.stat(); assert s.st_uid in (0,user.pw_uid)
 result.append({'path':name,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode & 511})
print(json.dumps({'uid':user.pw_uid,'gid':gid,'directories':result}))
'''

OWNERSHIP = '''import json,os,sys
from pathlib import Path
value=json.loads(sys.argv[1])
for item in value['directories']:
 p=Path(item['path']); assert p.is_dir() and not p.is_symlink() and p.resolve()==p
 s=p.stat(); assert (s.st_uid,s.st_gid) in ((item['uid'],item['gid']),(value['uid'],value['gid']))
 assert s.st_mode & 511 == item['mode']
 os.chown(p,value['uid'],value['gid'])
'''


def plan(owner, parent):
    require(owner.request['schema'] == ENHANCE_HTTPS_SCHEMA and parent['phase'] == 'verifying'
            and parent['request'] == owner.request, 'repair requires this interrupted Enhance verification')
    require(owner.ex.probe_unit(owner.host, UNIT)['exe_sha256'] == owner.request['release']['binary_sha256'],
            'repair requires the candidate publisher to remain running')
    owner.serving.restore_guard(parent)
    repairs = []
    for kind, group in parent['enhance_journals'].items():
        for index, record in enumerate(group['hosts']):
            previous = parent['plan']['serving']['group_units'][kind][index]
            service = units.effective([previous['fragment_text'],
                                      *(d['text'] for d in previous['drop_ins'])])['Service']
            user, group_name = service.get('User', 'root'), service.get('Group', '')
            path = '/etc/systemd/system/' + record['unit']
            change = next(c for c in record['changes'] if c['path'] == path)
            current = owner.ex.read(record['host'], path)
            require(current == change['new'], 'serving unit changed outside this transaction: ' + path)
            identity = {'User': user, **({'Group': group_name} if group_name else {})}
            if service.get('EnvironmentFile'):
                identity['EnvironmentFile'] = service['EnvironmentFile']
            replacement = set_service(current, identity)
            code, output = owner.ex.run(record['host'], ['python3', '-c', IDENTITY, user, group_name,
                json.dumps(record['directories'])], 30)
            require(code == 0, 'serving identity or cache directory refused on ' + record['host'] + ': ' + output[-500:])
            repairs.append({'kind': kind, 'index': index, 'host': record['host'], 'unit': record['unit'],
                            'path': path, 'before': current, 'after': replacement,
                            'ownership': json.loads(output)})
    return repairs


def dispatch(owner, args):
    with production_lock(owner.inventory, owner.ex) as owner.lock:
        parent = owner.latest()
        require(parent is not None, 'no Enhance transaction to repair')
        from .cutover_operator import status, operator_decision
        supervision = status(args.state_dir, args.request_sha256)
        require(supervision is not None and supervision['forward']['MainPID'] == '0'
                and supervision['watchdog']['MainPID'] == '0', 'stop the transaction-owned operator jobs before manual repair')
        repairs = plan(owner, parent)
        digest = durable.digest(repairs)
        if args.command.endswith('-plan'):
            return {'plan_sha256': digest, 'repairs': repairs}
        require(digest == args.expect_plan_sha256, 'identity repair plan is stale')
        mutation_fence(owner.inventory, owner.ex, owner.lock)
        parent['enhance_identity_repair'] = {'plan_sha256': digest, 'repairs': repairs}
        owner.save(parent)
        for repair in repairs:
            host, unit = repair['host'], repair['unit']
            record = parent['enhance_journals'][repair['kind']]['hosts'][repair['index']]
            change = next(c for c in record['changes'] if c['path'] == repair['path'])
            # Save the intended bytes before changing the unit; the original
            # predecessor bytes remain in the normal rollback journal.
            change['new'] = repair['after']
            owner.save(parent)
            owner.effect(parent, 'repair identity ' + host + '/' + unit,
                         lambda r=repair: owner.ex.write(r['host'], r['path'], r['after'].encode(), 0o644))
            def ownership(r=repair):
                code, output = owner.ex.run(r['host'], ['python3', '-c', OWNERSHIP, json.dumps(r['ownership'])], 30)
                require(code == 0, 'cache ownership repair failed: ' + output[-500:])
            owner.effect(parent, 'repair cache ownership ' + host + '/' + unit, ownership)
            owner.effect(parent, 'repair reload ' + host, lambda h=host: owner.ex.systemctl(h, 'daemon-reload'))
            owner.effect(parent, 'repair restart ' + host + '/' + unit,
                         lambda h=host, u=unit: owner.ex.systemctl(h, 'restart', u))
        owner.supervision = operator_decision(
            Path(args.state_dir) / 'operator-supervision' / parent['id'], parent['id'])
        owner.supervision.guard()
        owner.ready(True)
        owner.serving.verify(parent)
        owner.serving.verify_advancement(parent)
        owner.exact(parent)
        owner.require_stopped_load()
        owner.supervision.accept({'products': {name: {'ready': True, 'advanced': True, 'exact': True}
                                  for name in owner.request['checks']}, 'progress': parent['product_progress'],
                                  'request_sha256': durable.digest(owner.request)})
        owner.save(parent, 'committed')
        return parent
