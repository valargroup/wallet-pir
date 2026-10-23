"""Replace an interrupted cutover's publisher release, preserving its rollback baseline.

Only the executable may change. Configuration, prepared roots, serving releases,
checker mode and acceptance probes remain exactly those already reviewed. The
existing journal records both unit versions before any effect; failure holds the
candidate for diagnosis and never triggers an automatic rollback.
"""
import copy
import json
from pathlib import Path
import tempfile

from .. import durable, release
from .engine import file_sha256, mutation_fence
from .publisher import ROOT_DIR, SERVING_SCHEMA, UNIT, UNIT_PATH, require
from .remote import production_lock


def plan(owner, journal):
    require(journal is not None and journal['phase'] == 'verifying'
            and owner.request['schema'] == SERVING_SCHEMA and not owner.resume_load,
            'release repair requires an interrupted verification with checker stopped')
    owner.resources()
    owner.require_stopped_load()
    saved = journal.get('release_repair')
    require(journal['intent'] is None or saved and journal['intent'].startswith('release repair '),
            'another transaction effect is pending')
    if saved:
        require(saved['candidate_request_sha256'] == durable.digest(owner.request)
                and journal['request'] == owner.request, 'another release repair owns this transaction')
        repair = saved
    else:
        previous = journal['request']
        comparison = copy.deepcopy(owner.request)
        comparison['release'] = previous['release']
        require(comparison == previous and owner.request['release'] != previous['release'],
                'release repair may change only the publisher executable')
        old = ROOT_DIR + '/releases/' + previous['release']['binary_sha256'] + '/pir-publisher'
        new = ROOT_DIR + '/releases/' + owner.request['release']['binary_sha256'] + '/pir-publisher'
        before = {path: text for path, text in journal['plan']['desired'].items()
                  if path == UNIT_PATH or path.startswith('/etc/systemd/system/' + UNIT + '.d/')}
        require(old in before[UNIT_PATH], 'recorded publisher command differs')
        after = {path: text.replace(old, new) for path, text in before.items()}
        repair = {'base_request_sha256': durable.digest(previous),
                  'candidate_request_sha256': durable.digest(owner.request),
                  'previous_release': previous['release'], 'candidate_release': owner.request['release'],
                  'previous_binary': old, 'candidate_binary': new, 'before': before, 'after': after}
    for path, desired in journal['plan']['desired'].items():
        allowed = (repair['before'][path], repair['after'][path]) if path in repair['before'] else (desired,)
        require(owner.read(path) in allowed, 'managed file changed before release repair: ' + path)
    state = owner.ex.probe_unit(owner.host, UNIT)
    allowed_hashes = {repair['previous_release']['binary_sha256']}
    if saved:
        allowed_hashes.add(repair['candidate_release']['binary_sha256'])
    require(state['active_state'] == 'active' and state['exe_sha256'] in allowed_hashes
            or saved is not None and not state['main_pid'], 'publisher owner changed before release repair')
    expected_drops = [dict(entry, text=journal['plan']['desired'].get(entry['path'], entry['text']))
                      for entry in journal['plan']['units'][UNIT]['drop_ins']]
    require([entry['path'] for entry in state['drop_ins']] == [entry['path'] for entry in expected_drops]
            and all(entry['text'] in ((repair['before'][entry['path']], repair['after'][entry['path']])
                                     if saved and entry['path'] in repair['before'] else (expected['text'],))
                    for entry, expected in zip(state['drop_ins'], expected_drops)),
            'publisher drop-ins changed before release repair')
    return repair


def apply(owner, journal, repair, binary):
    """Apply the validated plan while the caller holds the production lock."""
    require(file_sha256(binary) == owner.request['release']['binary_sha256']
            and Path(binary).stat().st_size == owner.request['release']['binary_bytes'],
            'repair executable differs from request')
    mutation_fence(owner.inventory, owner.ex, owner.lock)
    journal['release_repair'] = repair
    journal['request'] = owner.request
    journal['plan']['request_sha256'] = durable.digest(owner.request)
    journal['plan']['desired'].update(repair['after'])
    journal['plan']['binary'] = repair['candidate_binary']
    owner.save(journal)
    def step(name, action):
        name = 'release repair ' + name
        if name not in journal['completed']:
            owner.effect(journal, name, action)
    step('stage publisher', lambda: owner.stage(binary, repair['candidate_binary']))
    step('stop publisher', lambda: owner.stop(UNIT))
    starting = journal['intent'] == 'release repair start publisher'
    if ('release repair start publisher' not in journal['completed']
            and not (starting and owner.ex.probe_unit(owner.host, UNIT)['main_pid'])):
        owner.unlocked()
    for path, text in repair['after'].items():
        step('install ' + path, lambda p=path, t=text: owner.write(p, t, 0o644))
    step('reload services', lambda: owner.ex.systemctl(owner.host, 'daemon-reload'))
    step('start publisher', lambda: owner.ex.systemctl(owner.host, 'start', UNIT))


def dispatch(owner, args):
    with tempfile.TemporaryDirectory(prefix='publisher-release-repair-') as directory:
        bundle = Path(directory) / 'bundle'
        release.extract(Path(args.archive), bundle, owner.request['release']['git_sha'], 'pir-publisher')
        require(json.loads((bundle / 'candidate.json').read_text()).get('source_dirty') is False,
                'repair executable must come from clean source')
        binary = bundle / 'pir-publisher'
        require(file_sha256(binary) == owner.request['release']['binary_sha256']
                and binary.stat().st_size == owner.request['release']['binary_bytes'],
                'repair archive differs from request')
        with production_lock(owner.inventory, owner.ex) as owner.lock:
            journal = owner.latest()
            repair = plan(owner, journal)
            digest = durable.digest(repair)
            if args.command.endswith('-plan'):
                return {'plan_sha256': digest, 'repair': repair}
            require(digest == args.expect_plan_sha256, 'release repair plan is stale')
            apply(owner, journal, repair, binary)
    return owner.verify()
