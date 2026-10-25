"""Refresh only a stale Status probe client after the fleet has been restored.

The original request stays immutable. Its staged publisher supplies the updated
probe executable; the oracle, harness, load and lookup deadlines are unchanged.
Verification records the client identity and repeats all four exact-answer gates
without stopping, starting or rewriting any service.
"""
import ast
import copy
import re

from .. import durable
from .publisher import CONFIG, LOAD, ROOT_DIR, UNIT, require, sha, stable_unit
from .publisher_serving import CADDY
from .remote import production_lock


def replacement(owner):
    check = owner.request['checks']['status']
    argv = check['argv']
    require(check['host'] == owner.host and argv[:2] == ['python3', '-c']
            and argv.count('--client') == 1,
            'Status probe refresh requires the coordinator harness with one --client')
    position = argv.index('--client') + 1
    require(position < len(argv) and argv[position].startswith('/'),
            'Status probe client must be an absolute path')
    release = owner.request['release']
    path = ROOT_DIR + '/releases/' + release['binary_sha256'] + '/pir-publisher'
    require(argv[position] != path, 'Status probe already uses the staged publisher')
    return {'transaction_request_sha256': durable.digest(owner.request),
            'original_check_sha256': durable.digest(check), 'original_client': argv[position],
            'client_argument': position, 'client': path,
            'client_sha256': release['binary_sha256'], 'source_revision': release['git_sha']}


def checks_for(owner, journal):
    checks = copy.deepcopy(owner.request['checks'])
    saved = journal.get('status_probe_refresh')
    if saved is not None:
        require(saved.get('replacement') == replacement(owner),
                'recorded Status probe refresh differs from the pinned request')
        change = saved['replacement']
        require(owner.ex.sha256(owner.host, change['client']) == change['client_sha256'],
                'staged Status probe executable changed')
        checks['status']['argv'][change['client_argument']] = change['client']
        checks['status']['argv'][2] = refreshed_harness(checks['status']['argv'][2],
                                                      change['client_sha256'])
    return checks


def refreshed_harness(source, client_sha256):
    """Change only the digest literal guarding the client executable."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        require(False, 'Status probe harness is not valid Python')
    expected = ast.dump(ast.parse('hashlib.sha256(args.client.read_bytes()).hexdigest()',
                                 mode='eval').body)
    pins = [node.comparators[0] for node in ast.walk(tree)
            if isinstance(node, ast.Compare) and len(node.ops) == 1
            and isinstance(node.ops[0], ast.NotEq) and ast.dump(node.left) == expected
            and isinstance(node.comparators[0], ast.Constant)
            and isinstance(node.comparators[0].value, str)
            and re.fullmatch('[a-f0-9]{64}', node.comparators[0].value)]
    require(len(pins) == 1, 'Status probe harness must pin exactly one client digest')
    pin = pins[0]
    lines = source.encode().splitlines(keepends=True)
    start = sum(map(len, lines[:pin.lineno - 1])) + pin.col_offset
    end = sum(map(len, lines[:pin.end_lineno - 1])) + pin.end_col_offset
    encoded = source.encode()
    literal = encoded[start:end]
    require(literal in (('"' + pin.value + '"').encode(), ("'" + pin.value + "'").encode()),
            'Status probe client digest must be a plain string literal')
    updated = literal[:1] + client_sha256.encode() + literal[-1:]
    return (encoded[:start] + updated + encoded[end:]).decode()


def restored_state(owner, journal):
    """Observe only the completed restoration, binding the actual invocations."""
    require(journal is not None and journal['request'] == owner.request
            and journal['phase'] == 'restoring' and journal['intent'] is None
            and not owner.resume_load,
            'probe refresh requires this restored transaction with its checker stopped')
    require('restore start ' + UNIT in journal['completed'],
            'publisher restoration has not reached its recorded start')
    owner.require_stopped_load()
    plan = journal['plan']
    require(all(owner.read(path) == before['text'] for path, before in plan['before'].items()),
            'managed files do not match the restored configuration')
    require(owner.read(CADDY) == plan['serving']['edge_before'], 'wallet edge is not restored')
    observed = {}
    for name, before in plan['units'].items():
        state = stable_unit(owner.ex.probe_unit(owner.host, name))
        require(all(state[key] == before[key] for key in
                    ('load_state', 'fragment_path', 'fragment_text', 'drop_ins', 'unit_file_state'))
                and not state['need_daemon_reload'], 'restored unit configuration changed: ' + name)
        if before['active_state'] == 'active' and name != LOAD:
            require(state['active_state'] == 'active' and state['main_pid']
                    and state['exe_sha256'] == before['exe_sha256'],
                    'restored process identity differs: ' + name)
        else:
            require(state['active_state'] in ('inactive', 'failed') and not state['main_pid'],
                    'a stopped predecessor is running: ' + name)
        observed[name] = state
    serving = journal.get('serving_journal', {})
    require(serving.get('hosts') and all(r['phase'] == 'restored' for r in serving['hosts']),
            'serving restoration has not passed its identity checks')
    for record in serving['hosts']:
        state = stable_unit(owner.ex.probe_unit(record['host'], record['unit']))
        require(state['active_state'] == 'active' and state['main_pid']
                and state['exe_sha256'] == record['previous']['exe_sha256']
                and not state['need_daemon_reload'],
                'restored serving process identity differs: ' + record['key'])
        require(all(owner.ex.read(record['host'], change['path']) == change['previous']
                    for change in record['changes']),
                'restored serving files changed: ' + record['key'])
        observed[record['key']] = state
    return observed


def plan(owner, journal):
    observed = restored_state(owner, journal)
    change = replacement(owner)
    require(owner.ex.sha256(owner.host, change['client']) == change['client_sha256'],
            'staged Status probe executable changed')
    saved = journal.get('status_probe_refresh')
    require(saved is None or saved.get('replacement') == change,
            'another Status probe refresh owns this transaction')
    harness = refreshed_harness(owner.request['checks']['status']['argv'][2], change['client_sha256'])
    return {'transaction': journal['id'], 'replacement': change, 'restored_units': observed,
            'refreshed_harness_sha256': sha(harness),
            'publisher_config_sha256': sha(owner.read(CONFIG))}


def dispatch(owner, args):
    with production_lock(owner.inventory, owner.ex) as owner.lock:
        journal = owner.latest()
        saved = journal and journal.get('status_probe_refresh')
        if (args.command.endswith('-verify') and journal is not None
                and journal['phase'] == 'rolled-back'):
            require(journal['request'] == owner.request and saved is not None
                    and saved['plan_sha256'] == args.expect_plan_sha256
                    and saved['replacement'] == replacement(owner)
                    and all(journal.get('checks', {}).get(name, {}).get('passed') is True
                            for name in owner.request['checks']),
                    'completed probe refresh differs from this request')
            return journal
        reviewed = plan(owner, journal)
        digest = durable.digest(reviewed)
        if args.command.endswith('-plan'):
            return {'plan': reviewed, 'plan_sha256': digest}
        require(digest == args.expect_plan_sha256, 'probe refresh plan is stale')
        journal['status_probe_refresh'] = {'replacement': reviewed['replacement'],
                                         'plan_sha256': digest}
        owner.save(journal)
        owner.ready()
        owner.exact(journal)
        require(restored_state(owner, journal) == reviewed['restored_units'],
                'restored processes changed during probe verification')
        owner.save(journal, 'rolled-back')
        return journal
