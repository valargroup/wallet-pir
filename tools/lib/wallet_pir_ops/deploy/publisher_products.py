"""Product settings, locks and unit rendering of the unified publisher."""
import copy

from . import units
from .engine import DeployError

# Values required by the retired publisher for the same live HTTPS behavior.
# None names a field that was ignored once HTTP-only mode was enabled.
_RETIRED_TRANSPARENT_SETTINGS = {
    'transparent_history': {'http_only': True, 'distribute_prepared': True, 'ship_runtimes': True,
                            'shadow': False, 'fleet_command': None, 'fleet_config': None},
    'transaction_details': {'http_only': True, 'ship_runtimes': True, 'workers': None, 'mode': 'live',
                            'blocks_per_step': None, 'step_interval_ms': None, 'replay_end': None,
                            'exit_when_idle': False},
}


def require(condition, message):
    if not condition:
        raise DeployError(message)


def require_current_configuration(config):
    """New requests omit removed fields; saved rollback requests keep their bytes."""
    for product, fields in _RETIRED_TRANSPARENT_SETTINGS.items():
        retired = set(config[product]['settings']) & fields.keys()
        require(not retired, 'new publisher requests contain retired Transparent settings: '
                + product + ': ' + ', '.join(sorted(retired)))


def configuration_for_update(installed):
    """Remove only obsolete settings from an already-live, prepared HTTPS publisher.

    The caller compares the entire result with the reviewed candidate, so every
    other setting remains exact. The original installed bytes are left intact
    for the deployment journal, drift checks and rollback.
    """
    updated = copy.deepcopy(installed)
    refusal = 'publisher updates must preserve every product setting; retiring settings requires live prepared HTTPS'
    for product, fields in _RETIRED_TRANSPARENT_SETTINGS.items():
        settings = updated[product]['settings']
        if not (settings.keys() & fields.keys()):
            continue
        required = ['http_only', 'ship_runtimes']
        if product == 'transparent_history':
            required.append('distribute_prepared')
        require(all(settings.get(name) is True for name in required), refusal)
        for name, expected in fields.items():
            if expected is not None:
                actual = settings.get(name, expected)
                require(type(actual) is type(expected) and actual == expected, refusal)
            settings.pop(name, None)
    return updated


def locks(config):
    return [config['enhance']['settings']['data_dir'] + '/publisher.lock',
            config['status']['settings']['publisher']['state_dir'] + '/publisher.lock']


def render(config, binary, states):
    """Replace the executable while preserving the installed sandbox and credentials."""
    from .publisher import UNIT
    state = states[UNIT]
    text = state['fragment_text']
    service = units.effective([text])
    require(service.get('Service', {}).get('User', 'root') == 'root', 'unknown publisher user')
    require('MemoryMax=40G' in text and 'MemorySwapMax=0' in text, 'unknown publisher resource policy')
    command = units.exec_start(service)
    return text.replace(command, binary + ' --config /etc/wallet-pir/publisher.json')
