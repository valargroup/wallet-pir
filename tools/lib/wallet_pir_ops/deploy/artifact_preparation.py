"""Stage public migration inputs while all predecessor services keep serving."""
import hashlib
import json
from pathlib import PurePosixPath

from . import descriptors
from .engine import DeployError

PRODUCTS = {'enhance', 'status', 'transparent-history', 'transparent-display'}


def require(condition, message):
    if not condition:
        raise DeployError(message)


def validate(source):
    if set(source) == {'product', 'inventory', 'inventory_sha256', 'checkpoints'}:
        require(source['product'] in PRODUCTS and isinstance(source['inventory'], str)
                and len(source['inventory'].encode()) <= 16 << 20, 'invalid predecessor inventory')
        require(hashlib.sha256(source['inventory'].encode()).hexdigest() == source['inventory_sha256'],
                'predecessor inventory pin differs')
        require(descriptors.PLAIN_PATH.fullmatch(source['checkpoints']) is not None,
                'shared checkpoint path must be absolute')
    else:
        require(set(source) == {'publisher_host', 'publisher_stage'}
                and descriptors.PLAIN_PATH.fullmatch(source['publisher_stage']) is not None,
                'cache source must name a prepared publisher stage')


def read_public(owner, host, path):
    raw = owner.ex.read(host, str(path))
    require(isinstance(raw, str) and len(raw.encode()) <= 16 << 20,
            'prepared public metadata is missing or exceeds its bound')
    return raw


def run(owner, entry):
    source = entry['source']
    host, stage = entry['host'], PurePosixPath(entry['stage'])
    owner.held.verify()
    owner.ex.mkdir(host, str(stage), 0o700)
    if 'product' in source:
        inventory = stage / 'predecessor-input.json'
        owner.held.verify()
        owner.ex.write(host, str(inventory), source['inventory'].encode(), 0o600)
        return owner.command(entry, 'prepare', '--product', source['product'],
            '--root', entry['root'], '--inventory', str(inventory),
            '--inventory-sha256', source['inventory_sha256'], '--stage', str(stage),
            '--checkpoints', source['checkpoints'])
    publisher_host, publisher_stage = source['publisher_host'], PurePosixPath(source['publisher_stage'])
    raw = read_public(owner, publisher_host, publisher_stage / 'prepared.json')
    prepared = json.loads(raw)
    require(prepared.get('schema') == 'artifact-checksum-preparation/1'
            and prepared.get('stage') == str(publisher_stage)
            and prepared.get('inventory_file') == str(publisher_stage / 'inventory.json'),
            'publisher preparation names unexpected metadata paths')
    inputs = stage / 'publisher-input'
    owner.held.verify()
    owner.ex.mkdir(host, str(inputs), 0o700)
    files = {name:read_public(owner, publisher_host, publisher_stage / name)
             for name in ('predecessor-inventory.json', 'inventory.json')}
    prepared.update(stage=str(inputs), inventory_file=str(inputs / 'inventory.json'))
    files['prepared.json'] = json.dumps(prepared)
    for name, body in files.items():
        owner.held.verify()
        owner.ex.write(host, str(inputs / name), body.encode(), 0o600)
    return owner.command(entry, 'prepare-cache', '--preparation', str(inputs / 'prepared.json'),
                         '--root', entry['root'], '--stage', str(stage))
