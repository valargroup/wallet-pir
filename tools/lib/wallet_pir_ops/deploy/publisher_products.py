"""Locks and unit rendering of the already-unified four-product publisher."""
from . import units
from .engine import DeployError


def require(condition, message):
    if not condition:
        raise DeployError(message)


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
