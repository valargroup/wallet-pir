"""Provision one approved query node, retaining the installed fleet source.

Run on the coordinator through its encrypted runtime credential loader. A fresh
saved plan must create exactly the approved node's six resources. The first node
adds the query module, its root declaration and its enabled setting. A later
node replaces that module and declaration with reviewed source and adds its own
enabled setting. Other Terraform files, inputs, backend and resource addresses
remain in place.
"""
import json
import os
from pathlib import Path
import re
import tempfile

from .. import durable
from ..hostlock import PinnedHostLock
from ..query_infra import NODES, initial_plan
from ..terraform import Terraform, clean_environment, file_sha256

CANONICAL = Path('/opt/enhance-pir/infra/production')
LOCK = Path('/run/lock/wallet-pir-production.lock')
PRODUCTION_VARIABLES = Path('/etc/enhance-pir/production.tfvars')
AUTOSCALE_STATE = Path('/srv/enhance-pir/autoscale/state.json')
MODULE_FILES = {'main.tf', 'variables.tf', 'outputs.tf', 'versions.tf', 'cloud-init.yaml.tftpl'}
ENABLED = 'query_serving_enabled = true\n'
# Each node's enabled setting and the file that holds it.
SETTINGS = {'01': ('query-serving.auto.tfvars', ENABLED),
            '02': ('query-serving-02.auto.tfvars', 'query_serving_02_enabled = true\n')}


def inputs(root):
    """Hash installed configuration and operator inputs, excluding provider caches."""
    result = {}
    for directory, children, files in os.walk(root):
        children[:] = [name for name in children if not name.startswith('.')]
        for name in files:
            if name.endswith(('.tf', '.tfvars', '.tftpl', '.yaml', '.yml', '.sh')) or name == '.terraform.lock.hcl':
                path = Path(directory) / name
                if path.is_symlink():
                    raise ValueError('Terraform source must not redirect outside its root')
                result[path.relative_to(root).as_posix()] = file_sha256(path)
    return result


def group_count(value):
    """Legacy Terraform variables may serialize the approved count as a string."""
    if type(value) is int and 1 <= value <= 4:
        return value
    if isinstance(value, str) and value in ('1', '2', '3', '4'):
        return int(value)
    raise ValueError('autoscaling group count must be between one and four')


def environment():
    """Read the coordinator's runtime credentials without persisting or printing them."""
    return clean_environment(
        TF_VAR_digitalocean_token=os.environ['DO_TOKEN_NEW_ORG'],
        TF_VAR_cloudflare_api_token=os.environ['CF_API_TOKEN'],
        AWS_ACCESS_KEY_ID=os.environ['WALLET_PIR_TF_STATE_ACCESS_KEY'],
        AWS_SECRET_ACCESS_KEY=os.environ['WALLET_PIR_TF_STATE_SECRET_KEY'],
        AWS_EC2_METADATA_DISABLED='true')


def identities(state):
    """Return only the durable managed resource bindings, never their other values."""
    return {(r.get('module', ''), r['type'], r['name'], str(i.get('index_key', ''))): i['attributes']['id']
            for r in state['resources'] if r['mode'] == 'managed' for i in r['instances']}


def prepare(request, *, canonical=None, terraform=None):
    """Validate the saved plan and bind every source/input byte before installation."""
    canonical = CANONICAL if canonical is None else Path(canonical)
    if set(request) != {'machine_id', 'source_sha', 'staged_root', 'saved_plan', 'saved_plan_sha256',
                        'project_id', 'baseline', 'operator_variables_sha256', 'desired_groups', 'node'}:
        raise ValueError('unexpected query provisioning request fields')
    if request['node'] not in NODES:
        raise ValueError('unknown query node')
    first = request['node'] == '01'
    if not isinstance(request['source_sha'], str) or not re.fullmatch('[0-9a-f]{40}', request['source_sha']):
        raise ValueError('query extension needs an exact source revision')
    source = Path(request['staged_root']).resolve()
    saved = Path(request['saved_plan']).resolve()
    if not source.is_dir() or file_sha256(saved) != request['saved_plan_sha256']:
        raise ValueError('query source or saved plan changed')
    if inputs(canonical) != request['baseline']:
        raise ValueError('installed Terraform configuration or inputs changed')
    if (file_sha256(PRODUCTION_VARIABLES) != request['operator_variables_sha256']
            or group_count(json.loads(AUTOSCALE_STATE.read_text())['desired_groups'])
            != group_count(request['desired_groups'])):
        raise ValueError('production variables or autoscaling inputs changed')
    copied = inputs(source)
    if copied != {**request['baseline'], 'query-serving.tf': file_sha256(source / 'query-serving.tf')}:
        raise ValueError('staged fleet source differs beyond the query extension')
    module = source.parent / 'query-serving'
    for name in MODULE_FILES:
        if not (module / name).is_file() or (module / name).is_symlink():
            raise ValueError('query module source is incomplete or redirected')
    declared = (canonical / 'query-serving.tf', canonical.parent / 'query-serving')
    if not first and any(not path.exists() or path.is_symlink() for path in declared):
        raise ValueError('a later query node needs the installed query module')
    enabled = canonical / SETTINGS[request['node']][0]
    for destination in (*(declared if first else ()), enabled):
        if destination.exists() or destination.is_symlink():
            raise ValueError('query source already exists; inspect the recorded transaction')
    tf = terraform or Terraform(source, env=dict(os.environ))
    document = tf.show_json(saved)
    addresses = initial_plan(document, request['project_id'], request['node'])
    if group_count(document['variables']['enhance_group_count']['value']) != group_count(request['desired_groups']):
        raise ValueError('saved plan differs from current autoscaling inputs')
    hashes = {'query-serving.tf': file_sha256(source / 'query-serving.tf'),
              **{'query-serving/' + name: file_sha256(module / name) for name in sorted(MODULE_FILES)}}
    return {'request_sha256': durable.digest(request), 'source_sha': request['source_sha'], 'node': request['node'],
            'saved_plan_sha256': request['saved_plan_sha256'], 'source_files': hashes,
            'creates': addresses, 'existing_fleet_actions': 0, 'baseline': request['baseline'],
            'operator_variables_sha256': request['operator_variables_sha256'],
            'desired_groups': request['desired_groups']}


def provision(request, expected, receipt, *, canonical=None, terraform_factory=Terraform, lock=None):
    """Install the additive source and apply its pinned plan under the production lock.

    Once apply is attempted, failures retain source and the transaction for
    inspection. Never remove a partially created volume or guess a rollback.
    Existing transactions are refused; use status after an interrupted call.
    """
    canonical = CANONICAL if canonical is None else Path(canonical)
    source = Path(request['staged_root']).resolve()
    receipt = Path(receipt)
    if receipt.exists():
        raise ValueError('query provisioning transaction already exists; use status')
    config = {'type': 'pinned_host', 'machine_id': request['machine_id']}
    with (lock or PinnedHostLock(config, path=LOCK)) as held:
        tf = terraform_factory(source, env=environment(), lock=held)
        plan = prepare(request, canonical=canonical, terraform=tf)
        if durable.digest(plan) != expected:
            raise ValueError('query provisioning plan changed')
        before = tf.state_pull()
        record = {'plan': plan, 'phase': 'installing-source', 'lineage': before['lineage'],
                  'serial_before': before['serial']}
        receipt.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        durable.atomic_json(receipt, record, mode=0o600)
        try:
            # Complete the new module before making its root declaration visible.
            # A later node keeps the replaced module beside it for inspection.
            if plan['node'] != '01':
                record['previous_module'] = str(canonical.parent / ('.query-serving-before-' + plan['node']))
                durable.atomic_json(receipt, record, mode=0o600)
                os.rename(canonical.parent / 'query-serving', record['previous_module'])
            with tempfile.TemporaryDirectory(prefix='.query-module-', dir=canonical.parent) as temporary:
                staged = Path(temporary) / 'query-serving'
                staged.mkdir()
                for name in MODULE_FILES:
                    durable.atomic_text(staged / name,
                                        (source.parent / 'query-serving' / name).read_text(), mode=0o644)
                if any(file_sha256(staged / name) != plan['source_files']['query-serving/' + name]
                       for name in MODULE_FILES):
                    raise ValueError('query module changed during installation')
                os.rename(staged, canonical.parent / 'query-serving')
                directory = os.open(canonical.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            declaration = (source / 'query-serving.tf').read_text()
            if file_sha256(source / 'query-serving.tf') != plan['source_files']['query-serving.tf']:
                raise ValueError('query declaration changed during installation')
            durable.atomic_text(canonical / 'query-serving.tf', declaration, mode=0o644)
            name, setting = SETTINGS[plan['node']]
            durable.atomic_text(canonical / name, setting, mode=0o600)
            installed = terraform_factory(canonical, env=environment(), lock=held)
            installed.run(['init', '-input=false', '-lockfile=readonly'])
            record['phase'] = 'applying'
            durable.atomic_json(receipt, record, mode=0o600)
            tf.apply(request['saved_plan'], request['saved_plan_sha256'])
            after = installed.state_pull()
            previous, current = identities(before), identities(after)
            if before['lineage'] != after['lineage'] or any(current.get(k) != v for k, v in previous.items()):
                raise ValueError('existing fleet resource identities changed')
            output = 'query_serving_node' + ('' if plan['node'] == '01' else '_' + plan['node'])
            record.update(phase='committed', serial_after=after['serial'],
                          node=installed.output_json()[output]['value'])
            durable.atomic_json(receipt, record, mode=0o600)
        except BaseException:
            record['phase'] = 'needs-reconcile'
            durable.atomic_json(receipt, record, mode=0o600)
            raise
    return record


def register(commands):
    for action in ('plan', 'preflight', 'provision', 'status'):
        parser = commands.add_parser('query-node-' + action, help='additive query-node infrastructure on the coordinator')
        parser.add_argument('--request', type=Path, required=True)
        parser.add_argument('--request-sha256', required=True)
        if action == 'provision':
            parser.add_argument('--expect-plan-sha256', required=True)
        parser.add_argument('--receipt', type=Path, required=True)


def dispatch(args):
    request = json.loads(args.request.read_text())
    if durable.digest(request) != args.request_sha256:
        raise ValueError('query provisioning request changed')
    action = args.command.removeprefix('query-node-')
    if action == 'status':
        return json.loads(args.receipt.read_text()) if args.receipt.exists() else {'phase': 'not-started'}
    if action == 'provision':
        record = provision(request, args.expect_plan_sha256, args.receipt)
        # Receipts retain inputs privately; presentation contains no credentials.
        return {k: record[k] for k in ('phase', 'serial_before', 'serial_after', 'node')}
    plan = prepare(request)
    return {'plan_sha256': durable.digest(plan), 'plan': plan}
