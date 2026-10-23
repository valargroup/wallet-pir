"""Create one approved data volume in the query host's existing DO project.

Creation is recoverable by its unique name and request digest. A saved receipt
records the volume before project assignment or attachment; retries reuse it.
Credentials come only from the operator's runtime environment.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.request

from .. import durable
from ..digitalocean import DigitalOcean
from . import descriptors
from .engine import DeployError, mutation_fence
from .remote import SSHExecutor, production_lock


class VolumeAPI(DigitalOcean):
    def write(self, method, path, body):
        request = urllib.request.Request(self.API + path, data=json.dumps(body).encode(), method=method,
                                         headers={'Authorization': 'Bearer ' + self.token,
                                                  'Content-Type': 'application/json'})
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *_args, **_kwargs):
                return None
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            return json.load(response)


def require(condition, message):
    if not condition:
        raise DeployError(message)


def review(request, inventory, api):
    require(set(request) == {'host', 'project_id', 'droplet_id', 'region', 'name', 'size_gib'},
            'volume request needs exact host, project, droplet, region, name and size')
    require(type(request['size_gib']) is int and 1 <= request['size_gib'] <= 16384, 'invalid volume size')
    require(re.fullmatch('[a-z][a-z0-9-]{1,62}', request['name']), 'invalid volume name')
    require(request['host'] in inventory.hosts, 'volume host is absent from inventory')
    team = api.get('/v2/account')['account'].get('team', {}).get('name', '')
    require(team.lower() == 'valargroup', 'volume provisioning requires the Valargroup account')
    droplet = api.droplet(request['droplet_id'])
    require(droplet and droplet['region']['slug'] == request['region'], 'droplet or region differs from request')
    addresses = {network['ip_address'] for network in droplet['networks']['v4']}
    require(inventory.hosts[request['host']]['address'] in addresses, 'provider droplet differs from pinned inventory host')
    project = api.get('/v2/projects/' + request['project_id'])['project']
    resources = api.get('/v2/projects/' + request['project_id'] + '/resources?per_page=200')['resources']
    require(any(item['urn'] == 'do:droplet:' + str(droplet['id']) for item in resources),
            'droplet does not belong to the approved project')
    return {'request': request, 'team': team, 'project_name': project['name'], 'droplet_name': droplet['name'],
            'request_sha256': durable.digest(request), 'filesystem': 'ext4'}


def provision(plan, api, save):
    request = plan['request']
    marker = 'Wallet PIR Details query cache; request ' + plan['request_sha256']
    volumes = api.get('/v2/volumes?region=' + request['region'])['volumes']
    matches = [volume for volume in volumes if volume['name'] == request['name']]
    require(len(matches) <= 1, 'ambiguous existing volume name')
    if matches:
        volume = matches[0]
        require(volume['description'] == marker and volume['size_gigabytes'] == request['size_gib']
                and volume['filesystem_type'] == 'ext4', 'existing volume is not this approved candidate')
    else:
        save({'phase': 'creating', 'plan': plan})
        volume = api.write('POST', '/v2/volumes', {'region': request['region'], 'name': request['name'],
                           'size_gigabytes': request['size_gib'], 'initial_filesystem_type': 'ext4',
                           'description': marker})['volume']
    state = {'phase': 'created', 'plan': plan, 'volume_id': volume['id']}
    save(state)
    api.write('POST', '/v2/projects/' + request['project_id'] + '/resources',
              {'resources': ['do:volume:' + volume['id']]})
    volume = api.get('/v2/volumes/' + volume['id'])['volume']
    require(not set(volume['droplet_ids']) - {request['droplet_id']}, 'volume is attached to another droplet')
    if request['droplet_id'] not in volume['droplet_ids']:
        state['phase'] = 'attaching'
        save(state)
        action = api.write('POST', '/v2/volumes/' + volume['id'] + '/actions',
                           {'type': 'attach', 'droplet_id': request['droplet_id'], 'region': request['region']})['action']
        state['action_id'] = action['id']
        save(state)
        deadline = time.monotonic() + 240
        while action['status'] == 'in-progress' and time.monotonic() < deadline:
            time.sleep(3)
            action = api.get('/v2/actions/' + str(action['id']))['action']
        require(action['status'] == 'completed', 'volume attachment did not complete; inspect the retained action receipt')
    volume = api.get('/v2/volumes/' + volume['id'])['volume']
    require(volume['droplet_ids'] == [request['droplet_id']], 'volume attachment was not observed')
    state.update(phase='attached', volume=volume)
    save(state)
    return state


def register(commands):
    for operation in ('plan', 'provision'):
        command = commands.add_parser('cache-volume-' + operation, allow_abbrev=False)
        command.add_argument('--request', type=Path, required=True)
        command.add_argument('--request-sha256', required=True)
        if operation == 'provision':
            command.add_argument('--expect-plan-sha256', required=True)


def dispatch(args, executor=None):
    raw = args.request.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == args.request_sha256, 'volume request digest differs')
    request = json.loads(raw)
    inventory = descriptors.load_inventory(args.inventory)
    token = os.environ.get('DIGITALOCEAN_ACCESS_TOKEN')
    require(token, 'provide the Valargroup production DO credential in DIGITALOCEAN_ACCESS_TOKEN at runtime')
    api = VolumeAPI(token)
    executor = executor or SSHExecutor(inventory)
    with production_lock(inventory, executor) as lock:
        mutation_fence(inventory, executor, lock)
        plan = review(request, inventory, api)
        digest = durable.digest(plan)
        if args.command.endswith('-plan'):
            return {'plan': plan, 'plan_sha256': digest}
        require(digest == args.expect_plan_sha256, 'volume provisioning plan is stale')
        lock.verify()
        receipt = Path(args.state_dir) / ('volume-' + plan['request_sha256'][:16] + '.json')
        receipt.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        return provision(plan, api, lambda state: durable.atomic_json(receipt, state, mode=0o600))
