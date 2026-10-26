"""Product settings, locks and unit rendering of the unified publisher."""
import json
from pathlib import PurePosixPath

from . import units
from .engine import DeployError

def require(condition, message):
    if not condition:
        raise DeployError(message)


def locks(config):
    return [config['enhance']['settings']['data_dir'] + '/publisher.lock',
            config['status']['settings']['publisher']['state_dir'] + '/publisher.lock',
            *([config['receiver']['settings']['data_dir'] + '/publisher.lock'] if 'receiver' in config else [])]


def render(config, binary, states, receiver_environments=()):
    """Replace the executable while preserving the installed sandbox and credentials."""
    from .publisher import UNIT
    state = states[UNIT]
    text = state['fragment_text']
    service = units.effective([text])
    require(service.get('Service', {}).get('User', 'root') == 'root', 'unknown publisher user')
    require(service.get('Service', {}).get('MemoryMax') in ('40G', '47G')
            and service['Service'].get('MemorySwapMax') == '0', 'unknown publisher resource policy')
    if 'receiver' in config:
        text = text.replace('MemoryMax=40G', 'MemoryMax=47G')
    existing = service.get('Service', {}).get('EnvironmentFile', [])
    for path in receiver_environments:
        if path not in existing:
            text += '\n[Service]\nEnvironmentFile=' + path + '\n'
    command = units.exec_start(service)
    return storage_sandbox(text.replace(command, binary + ' --config /etc/wallet-pir/publisher.json'),
                           config, [d['text'] for d in state['drop_ins']])


def storage_sandbox(text, config, drop_ins=()):
    """Allow the exact publication roots while preserving the installed sandbox.

    A prepared root can be a sibling of the old root, outside its writable bind
    mount. Never widen access to their common parent to accommodate that move.
    """
    from ..role_config import directory
    roots = [directory(config['transparent_history']['settings']['publication_root']),
             directory(config['transaction_details']['settings']['root']),
             *([directory(config['receiver']['settings']['data_dir'])] if 'receiver' in config else [])]
    def covered(fragment):
        paths = units.effective([fragment, *drop_ins]).get('Service', {}).get('ReadWritePaths', [])
        return [root for root in roots if any(
            path.startswith('/') and PurePosixPath(root).is_relative_to(PurePosixPath(path))
            for path in paths)]
    missing = [root for root in roots if root not in covered(text)]
    if missing:
        text = text.rstrip() + '\n\n[Service]\nReadWritePaths=' + ' '.join(map(json.dumps, missing)) + '\n'
    require(len(covered(text)) == len(roots), 'publisher drop-in resets required storage write permissions')
    return text
