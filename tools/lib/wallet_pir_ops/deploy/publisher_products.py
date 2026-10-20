"""Adopt the running Enhance and Status applications without changing their policy.

Only known standalone arguments are translated. Referenced configuration files
are pinned by the cutover plan; credentials remain in their existing files.
"""
import json
import shlex

from . import units
from .engine import DeployError

STATUS_CONFIG = '/etc/status-pir-control/controller-native.json'


def require(condition, message):
    if not condition:
        raise DeployError(message)


def effective(state):
    return units.effective([state['fragment_text'], *(d['text'] for d in state['drop_ins'])])


def arguments(state, mode, defaults, flags=(), repeated=()):
    command = shlex.split(units.exec_start(effective(state)))
    require(command.count(mode) == 1, 'predecessor command has an unknown mode')
    command.remove(mode)
    result = dict(defaults)
    seen = set()
    args = iter(command[1:])
    for option in args:
        name = option.removeprefix('--').replace('-', '_')
        require(option.startswith('--') and name in defaults, 'unhandled predecessor argument: ' + option)
        require(name not in seen or name in repeated, 'duplicate predecessor argument: ' + option)
        seen.add(name)
        if name in flags:
            result[name] = True
            continue
        value = next(args, None)
        require(value is not None and not value.startswith('--'), 'missing predecessor argument value')
        if name in repeated:
            result[name] = [*result[name], value]
        elif isinstance(defaults[name], (int, float)):
            result[name] = type(defaults[name])(value)
        else:
            result[name] = value
    return result


def enhance_settings(state):
    values = arguments(state, 'coordinator', {
        'status_config': None, 'packing_router_config': None, 'query_ingress': [],
        'pool_placement': False, 'frontier_replicas': 2, 'pool_policy': None,
        'artifact_listen': None, 'listen': '127.0.0.1:8080', 'data_dir': None,
        'worker_config': None, 'zakura_rpc_url': 'http://127.0.0.1:8232', 'zakura_cookie': None,
        'isolated_fixture': False, 'fixture_records': 67, 'fixture_append_records': 1,
        'poll_seconds': 10, 'prevout_cache_outputs': 250000,
        'capacity_fallback_rows_per_second': 1.0, 'capacity_readiness_seconds': 21600.0,
        'capacity_burst_rows': 4096, 'sealed_shards': 6,
    }, flags=('pool_placement', 'isolated_fixture'), repeated=('query_ingress',))
    require(values.pop('status_config') is None, 'embedded Status must not run alongside the adopted Status publisher')
    require(not values['isolated_fixture'] and values['zakura_cookie'], 'Enhance must retain its canonical source')
    return values


def status_settings(state, read):
    values = arguments(state, 'serve-distributed', {
        'config': None, 'rpc_url': 'http://127.0.0.1:8232', 'cookie': None, 'public_listen': None,
    })
    require(values['config'] == STATUS_CONFIG and values['cookie'], 'unknown Status source configuration')
    text = read(STATUS_CONFIG)
    require(text is not None, 'missing Status configuration')
    return {'publisher': json.loads(text), 'rpc_url': values['rpc_url'],
            'cookie': values['cookie'], 'public_listen': values['public_listen']}


def inputs(config):
    enhance = config['enhance']['settings']
    return [STATUS_CONFIG, *(enhance[k] for k in
            ('worker_config', 'packing_router_config', 'pool_policy') if enhance.get(k))]


def locks(config):
    return [config['enhance']['settings']['data_dir'] + '/publisher.lock',
            config['status']['settings']['publisher']['state_dir'] + '/publisher.lock']


def verify_adoption(cutover, states, installed):
    from .publisher import ADOPTED, EXPANDED_PRODUCTS, PRODUCTS
    config = cutover.request['configuration']
    require(set(installed) in ({'management_listen', *PRODUCTS}, {'management_listen', *EXPANDED_PRODUCTS}),
            'installed publisher has unknown product ownership')
    for product in PRODUCTS:
        require(config[product] == installed[product], 'Transparent settings must remain unchanged')
    already_adopted = 'enhance' in installed
    if already_adopted:
        require(config == installed, 'publisher updates must preserve every product setting')
    else:
        require(all(states[name]['active_state'] == 'active' and states[name]['main_pid'] for name in ADOPTED),
                'both predecessor publishers must be running')
        require(config['enhance']['settings'] == enhance_settings(states[ADOPTED[0]]),
                'Enhance configuration changes more than process ownership')
        require(config['status']['settings'] == status_settings(states[ADOPTED[1]], cutover.read),
                'Status configuration changes more than process ownership')
    if already_adopted:
        require(all(states[name]['active_state'] == 'inactive' and not states[name]['main_pid'] for name in ADOPTED),
                'standalone and unified publishers both own state')


def monitoring(sampler, old):
    for target in sampler.get('targets', []):
        if target.get('unit') in old:
            target['unit'] = 'pir-publisher.service'
        if 'units' in target:
            target['units'] = list(dict.fromkeys('pir-publisher.service' if u in old else u for u in target['units']))


def render(config, binary, states):
    from .publisher import ADOPTED, UNIT
    # Retain the deployed Transparent sandbox, then grant only the adopted roots.
    text = states[UNIT]['fragment_text']
    old_command = units.exec_start(effective(states[UNIT]))
    text = text.replace(old_command, binary + ' --config /etc/wallet-pir/publisher.json')
    text = text.replace('MemoryMax=12G', 'MemoryMax=40G')
    require('MemoryMax=40G' in text and 'MemorySwapMax=0' in text, 'unknown publisher resource policy')
    text = text.replace('Description=Transparent history and transaction-detail publisher',
                        'Description=Unified Enhance, Status and Transparent publisher')
    writable = [config['enhance']['settings']['data_dir'], config['status']['settings']['publisher']['state_dir']]
    for path in writable:
        require(path.startswith('/') and all(c.isalnum() or c in '/._-' for c in path), 'unsafe publisher state path')
    extra = ['ReadWritePaths=' + ' '.join(writable)]
    limits = [effective(states[name]).get('Service', {}).get('LimitNOFILE') for name in (UNIT, *ADOPTED)]
    limits = [int(value) for value in limits if value is not None]
    if limits:
        extra.append('LimitNOFILE=' + str(max(limits)))
    environment = {}
    files = []
    for name in (UNIT, *ADOPTED):
        service = effective(states[name]).get('Service', {})
        require(service.get('User', 'root') == 'root', 'publisher user change requires separate review')
        require(not any(service.get(k) for k in ('ExecStartPre', 'ExecStartPost', 'ExecStop', 'ExecStopPost',
                                                'LoadCredential', 'LoadCredentialEncrypted', 'SetCredential')),
                'predecessor has unhandled lifecycle or credential directives')
        for key, value in service.get('Environment', {}).items():
            require(key in ('RUST_LOG', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'),
                    'inline application environment must be moved to an existing credential file before adoption')
            require(key not in environment or environment[key] == value, 'conflicting publisher environment')
            environment[key] = value
        files.extend(service.get('EnvironmentFile', []))
    for key, value in environment.items():
        require('\n' not in value and '"' not in value and '%' not in value, 'unsafe publisher environment')
        extra.append('Environment="' + key + '=' + value + '"')
    for path in dict.fromkeys(files):
        require(path.lstrip('-').startswith('/') and not any(c.isspace() or c in '%"' for c in path),
                'unsafe environment file reference')
        extra.append('EnvironmentFile=' + path)
    return text.replace('\n[Install]', '\n' + '\n'.join(extra) + '\n\n[Install]')
