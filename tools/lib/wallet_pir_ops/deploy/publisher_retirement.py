"""Retire the explicitly approved v4/v6 Enhance state under the publisher lock.

The plan pins directory identities and sizes, the active publisher and its
committed rollback baseline. Retirement records intent before deleting either
directory. It never changes the publisher's rollback journal.
"""
import json

from .. import durable
from .publisher import CONFIG, ROOT_DIR, UNIT, require, stable_unit
from .remote import production_lock
from .transaction import new_id

ROOTS = ('/srv/enhance-pir-v4', '/srv/enhance-pir-v6')
INSPECT = r'''
import json, os, pathlib, subprocess
roots = ('/srv/enhance-pir-v4', '/srv/enhance-pir-v6')
inspectors = set()
pid = os.getpid()
while pid and pid not in inspectors:
    inspectors.add(pid)
    status = pathlib.Path('/proc', str(pid), 'status').read_text()
    pid = int(next(line.split()[1] for line in status.splitlines() if line.startswith('PPid:')))
for proc in pathlib.Path('/proc').glob('[0-9]*'):
    if int(proc.name) in inspectors: continue
    values = []
    for child in [proc/'cwd', proc/'exe', *list((proc/'fd').glob('*'))]:
        try: values.append(os.readlink(child))
        except OSError: pass
    for name in ('maps', 'cmdline'):
        try: values.append((proc/name).read_bytes().decode(errors='replace'))
        except OSError: pass
    for root in roots:
        if any(root in value for value in values):
            raise SystemExit('retired state still used by process ' + proc.name)
result = {}
for root in roots:
    path = pathlib.Path(root)
    if not path.exists():
        result[root] = None
        continue
    if path.is_symlink() or path.resolve() != path or not path.is_dir():
        raise SystemExit('retired root is not a plain directory')
    stat = path.stat()
    size = int(subprocess.check_output(['du', '-sb', root]).split()[0])
    result[root] = {'device': stat.st_dev, 'inode': stat.st_ino, 'bytes': size}
print(json.dumps(result))
'''


def capture(target):
    require(json.loads(target.read(CONFIG)) == target.request['configuration'],
            'publisher configuration changed before state retirement')
    current = stable_unit(target.ex.probe_unit(target.host, UNIT))
    require(current['active_state'] == 'active'
            and current['exe_sha256'] == target.request['release']['binary_sha256'],
            'state retirement requires the pinned active publisher')
    latest = target.latest()
    require(latest and latest['phase'] == 'committed', 'state retirement requires a committed publisher baseline')
    # Both active settings and the retained rollback configuration must avoid
    # these roots. Historical inactive unit files may still name them.
    for body in (target.read(CONFIG), json.dumps(latest['request']['configuration'])):
        require(not any(root in body for root in ROOTS), 'active or rollback publisher uses retired state')
    for unit in latest.get('plan', {}).get('units', {}).values():
        require(unit['active_state'] != 'active' or not any(root in json.dumps(unit) for root in ROOTS),
                'rollback would start a service using retired state')
    resources = target.resources()
    code, output = target.ex.run(target.host, ['python3', '-c', INSPECT], 120)
    require(code == 0, 'retired-state ownership check failed: ' + output[-1000:])
    roots = json.loads(output)
    require(set(roots) == set(ROOTS), 'retired-state inspection returned different paths')
    return {'machine_id': resources['machine_id'], 'publisher': current,
            'baseline': latest['id'], 'roots': roots}


def dispatch(target, operation, expected=None):
    if operation != 'retire-state-deploy':
        plan = capture(target)
        return {'plan': plan, 'plan_sha256': durable.digest(plan), 'irreversible': True}
    with production_lock(target.inventory, target.ex) as target.lock:
        plan = capture(target)
        require(durable.digest(plan) == expected, 'state-retirement plan is stale')
        identifier = new_id('retired-state', durable.digest(plan))
        receipt = {'id': identifier, 'plan': plan, 'completed': [], 'intent': None}
        path = ROOT_DIR + '/retirements/' + identifier + '.json'
        def save():
            target.lock.verify()
            durable.atomic_json(target.state_dir / (identifier + '.json'), receipt, mode=0o600)
            target.ex.mkdir(target.host, ROOT_DIR + '/retirements', 0o700)
            target.ex.write(target.host, path, durable.canonical(receipt), 0o600)
        target.state_dir.mkdir(parents=True, exist_ok=True)
        for root, identity in plan['roots'].items():
            receipt['intent'] = root
            save()
            if identity is not None:
                program = ('import json,pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]);'
                           's=p.stat(); expected=json.loads(sys.argv[2]);'
                           'assert not p.is_symlink() and p.resolve()==p and '
                           '(s.st_dev,s.st_ino)==(expected["device"],expected["inode"]);shutil.rmtree(p)')
                code, output = target.ex.run(target.host, ['python3', '-c', program, root, json.dumps(identity)], 300)
                require(code == 0, 'approved retirement failed: ' + output[-1000:])
            receipt['completed'].append(root)
            receipt['intent'] = None
            save()
        receipt['phase'] = 'retired'
        save()
        return receipt
