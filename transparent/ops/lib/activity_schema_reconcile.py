"""Adopt one retained predecessor activation after a failed, unstaged cutover.

This is recovery at a newer revision, never restoration of a collected baseline.
All paths come from the original captured coordinator plan and controller. The
original recipe and baseline remain immutable; the wrapper retains the exact
adoption plan and previous destination bytes before any replacement.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
from importlib import util

spec = util.spec_from_file_location('reconcile_host', Path(__file__).with_name('activity_schema_host.py'))
H = util.module_from_spec(spec)
spec.loader.exec_module(H)


def inspect(product, record, target):
    # Reuse the concrete product's bounded parsers and independently pinned
    # real-client routing verifier, rather than accepting caller-selected code.
    h = H
    h.require(isinstance(target, str) and h.HEX.fullmatch(target), 'invalid reconciliation map')
    h.require(not any(e.get('group') == 'steps' and e.get('name') != 'preserve-v10'
                      for e in record['events']), 'newer v10 adoption requires an unstaged cutover')
    product.bound(record['id'])
    product.local.identity()
    product.local.quiet(h.WRITERS['coordinator'])
    baseline, _ = product.local.saved()
    old = h.load(h.ROOT/'controller.json')
    publication = h.B.safe_path(old['publication_root'])
    retention = next((i for i in baseline['plan']['retained']
                      if Path(i['path']).resolve() == publication.resolve()), None)
    h.require(retention is not None, 'adoption publication namespace was not captured')
    suffix = '.schema-displaced-'+baseline['plan_sha256'][:12]
    state = h.ROOT/('state'+suffix)
    native = publication/('active.json'+suffix)
    h.require(state.is_dir() and not state.is_symlink(), 'displaced adoption state is not a retained directory')
    sources = {name:state/name for name in ('active.json', target+'.request.json',
               target+'.assignment.json', target+'.prepared.json', target+'.roster.json')}
    sources['native-active'] = native
    files = {}
    for name, path in sources.items():
        h.require(path.is_file() and not path.is_symlink() and path.stat().st_uid == os.geteuid(),
                  'adoption requires retained owned regular records')
        h.load(path)  # Enforce the same bounded, duplicate-key rejecting parser.
        info = path.stat()
        files[name] = {'path':str(path), 'sha256':h.checksum(path),
                       'mode':info.st_mode & 0o777, 'uid':info.st_uid, 'gid':info.st_gid}
    active = h.load(sources['active.json']); request = h.load(sources[target+'.request.json'])
    assignment = h.load(sources[target+'.assignment.json'])
    prepared = h.load(sources[target+'.prepared.json']); roster = h.load(sources[target+'.roster.json'])
    nat = h.load(native)
    directory = h.B.safe_path(request['directory'])
    h.require(publication.resolve() in directory.resolve().parents and not directory.is_symlink(),
              'adoption publication escaped captured namespace')
    mapping = h.load(directory/'shards.json')
    h.require(h.transparent_map.served_sha256(mapping) == target and
              active['map_sha256'] == request['map_sha256'] == nat['map_sha256'] == target and
              nat['directory'] == request['directory'] and
              nat['height'] == mapping['shards'][-1]['end_height'] and
              nat['hash'] == mapping['shards'][-1]['terminal_block_hash'], 'adoption publication identities disagree')
    assignment_path = str(h.ROOT/'state'/(target+'.assignment.json'))
    names = {w['id'] for w in roster}
    h.require(active['assignment'] == request['assignment'] == assignment_path and
              assignment['set']['map_sha256'] == target and assignment['set']['shard_schema'] == 'transparent-shard-v10' and
              not assignment['unassigned'] and names == set(active['workers']) == set(prepared) ==
              {w['id'] for w in assignment['workers']}, 'adoption assignment is incomplete')
    fleet = product.routing.fleet('v10', read_only=True)
    h.require(names == {w['id'] for w in fleet.roster} and roster == fleet.roster,
              'adoption roster differs from captured fleet')
    product.routing.pins('v10')
    async def workers():
        result = []
        # Native control uses the protocol's serialized assignment identity.
        assignment_sha = product.routing.__class__.live.__globals__['assignment_digest'](assignment)
        rows = {w['id']:w for w in assignment['workers']}
        for worker in fleet.roster:
            status = await fleet.control(worker, {'operation':'status'})
            ready = product.routing.fetch('http://'+worker['upstream']+'/v1/ready')
            row = rows[worker['id']]
            pin = product.routing.worker_pins['v10'][worker['id']]
            h.require(status.get('warm') is True and status.get('invalidated') is False and
                      status.get('candidate') is None and status.get('preparing') is None and
                      status.get('active',{}).get('map_sha256') == target and
                      ready.get('ready') is True and ready.get('mode') == 'warm' and
                      ready.get('map_sha256') == target and ready.get('assignment_sha256') == assignment_sha and
                      ready.get('binary_sha256') == pin and ready.get('worker_id') == worker['id'] and
                      ready.get('role') == worker['role'] == row['role'] and
                      row['upstream'] == worker['upstream'] and ready.get('assigned_shards') == len(row['shards']),
                      'adoption worker is not the pinned complete warm predecessor')
            revisions = status.get('revisions')
            h.require(isinstance(revisions, list) and revisions, 'adoption worker omitted anchors')
            for revision in revisions:
                h.require(await fleet.canonical_hash(revision['end_height']) == revision['terminal_block_hash'],
                          'adoption advertised anchor is not canonical')
            result.append({'id':worker['id'], 'binary_sha256':pin})
        for shard in mapping['shards']:
            h.require(await fleet.canonical_hash(shard['end_height']) == shard['terminal_block_hash'],
                      'adoption publication anchor is not canonical')
        return result
    pins = asyncio.run(workers())
    return {'version':1, 'transaction':record['id'], 'recipe_sha256':record['recipe_sha256'],
            'map_sha256':target, 'baseline_sha256':h.checksum(product.local.root/'complete.json'),
            'files':files, 'publication':{'path':str(directory), 'sha256':h.checksum(directory/'shards.json')},
            'workers':pins}


def install(product, record):
    """Replace only the six activation records, keeping all prior bytes."""
    B, require, ROOT = H.B, H.require, H.ROOT
    plan = record['v10_reconciliation']['plan']
    require(plan['transaction'] == record['id'] and plan['recipe_sha256'] == record['recipe_sha256'],
            'adoption intent differs from original transaction')
    product.local.quiet(H.WRITERS['coordinator'])
    product.local.withdrawn()
    product.local.saved()
    require(H.checksum(product.local.root/'complete.json') == plan['baseline_sha256'], 'adoption baseline changed')
    for info in plan['files'].values():
        p = Path(info['path']); s = p.stat()
        require(p.is_file() and not p.is_symlink() and H.checksum(p) == info['sha256'] and
                (s.st_mode & 0o777,s.st_uid,s.st_gid) == (info['mode'],info['uid'],info['gid']),
                'adoption retained record changed')
    pub = plan['publication']
    require(H.checksum(Path(pub['path'])/'shards.json') == pub['sha256'], 'adoption publication changed')
    directory = product.root/'reconciliation-before'
    require(not directory.exists(), 'partial adoption requires explicit reconciliation')
    directory.mkdir(parents=True, mode=0o700)
    target = plan['map_sha256']
    native = Path(plan['files']['native-active']['path'])
    destinations = {name:ROOT/'state'/name for name in plan['files'] if name != 'native-active'}
    destinations['native-active'] = native.with_name('active.json')
    # desired is explicitly the adopted request, never the displaced pending tail.
    destinations['desired.json'] = ROOT/'state/desired.json'
    payloads = {name:Path(info['path']).read_bytes() for name,info in plan['files'].items()}
    payloads['desired.json'] = payloads[target+'.request.json']
    receipt = {}
    for name,path in destinations.items():
        require(not path.is_symlink(), 'adoption destination is a symlink')
        previous = path.read_bytes() if path.exists() else None
        receipt[name] = {'destination':str(path), 'previous_sha256':None if previous is None else hashlib.sha256(previous).hexdigest()}
        if previous is not None:
            B.atomic(directory/name, previous)
    B.atomic(directory/'intent.json', json.dumps({'plan':plan,'destinations':receipt},sort_keys=True).encode())
    for name,path in destinations.items():
        info = plan['files'][target+'.request.json' if name == 'desired.json' else name]
        B.atomic(path, payloads[name], info['mode'])
        os.chown(path, info['uid'], info['gid'])
    B.atomic(ROOT/'state/maintenance.json', b'{"enabled":true}')
    B.atomic(directory/'complete.json', json.dumps({'map_sha256':target,'status':'installed'},sort_keys=True).encode())
