"""Per-host APM observers rendered from the maintained deployment inventory.

The deployment inventory (`/etc/wallet-pir/fleet.json`) is the one host list.
Each managed host has an `apm-sidecar.observer` role entry whose settings name
its private monitoring address, required volumes, units and hooks. From it:

- `sidecar` renders the sidecar's unit and `/etc/pir-apm-sidecar/sidecar.json`
  for the deploy engine (install, update, status and rollback under the lock);
- `observers` renders the central APM's observer inventory, carrying each
  host's expected TLS name, configuration digest and check names, so the APM
  rejects a sidecar running any other configuration;
- `coverage` compares provisioned hosts (Terraform outputs, declared external
  hosts and the dynamic Transparent roster) and required volumes with that
  enrollment, listing every missing declaration. Without every source it reports
  `partial` and never claims full cloud coverage.

Retired inventory hosts become explicit `decommissioned` entries, the only way
the APM forgets an enrolled host.
"""
import hashlib
import ipaddress
import json
import re

from .paths import ROOT
from .role_config import RenderedRole, directory, render, settings_of

TEMPLATE = ROOT / 'tools/shared/pir-apm-sidecar/deploy/pir-apm-sidecar.service.in'
CONFIG = '/etc/pir-apm-sidecar/sidecar.json'
STATE = '/var/lib/pir-apm-sidecar/state.json'
CREDENTIALS = '/etc/pir-apm-sidecar/credentials'
COLLECTOR_IDENTITY = 'pir-apm-collector'
TLS_DOMAIN = 'pir-monitoring'
PORT = 9443
SERVICE = 'apm-sidecar'
ROLE = 'observer'
NAME = re.compile(r'^[a-z0-9@._:/-]{1,80}$')
UUID = re.compile(r'^[0-9A-Fa-f-]{1,64}$')
UNIT = re.compile(r'^[A-Za-z0-9@._-]+\.(service|socket|timer)$')
# Every coordinator keeps its chain, activity and txid genesis data on separate volumes.
COORDINATOR_VOLUMES = ('/srv/zakura', '/srv/transparent-activity', '/srv/txid-display-genesis')


def tls_name(host_id):
    """The DNS name the host's serving certificate carries and the APM expects."""
    return '%s.%s' % (host_id, TLS_DOMAIN)


def _volumes(value):
    if not isinstance(value, list):
        raise ValueError('required_volumes must be a list')
    seen = set()
    for volume in value:
        if (not isinstance(volume, dict) or set(volume) != {'path', 'uuid'}
                or not isinstance(volume['path'], str) or not volume['path'].startswith('/')
                or volume['path'] == '/' or '..' in volume['path'] or volume['path'] in seen
                or not isinstance(volume['uuid'], str) or not UUID.match(volume['uuid'])):
            raise ValueError('each required volume is a unique {"path", "uuid"} below /')
        seen.add(volume['path'])
    return value


def _hooks(value):
    if not isinstance(value, list) or len(value) > 16:
        raise ValueError('hooks must be a list of at most 16')
    for hook in value:
        if (not isinstance(hook, dict) or not set(hook) <= {'name', 'argv', 'timeout_seconds'}
                or not isinstance(hook.get('name'), str) or not re.fullmatch(r'[a-z0-9._-]{1,48}', hook['name'])
                or not isinstance(hook.get('argv'), list) or not hook['argv']
                or not all(isinstance(a, str) and a and not any(ord(c) < 32 for c in a) for a in hook['argv'])
                or not hook['argv'][0].startswith('/')):
            raise ValueError('each hook is {"name", "argv": [absolute executable, ...]} with fixed arguments')
    return value


def sidecar_document(settings):
    """The sidecar configuration for one host, validated, as the file will hold it."""
    config = settings_of(settings, ('host_id', 'address'),
                         ('required_volumes', 'units', 'hooks', 'edge_metrics_url', 'port'))
    if not NAME.match(config['host_id']):
        raise ValueError('host_id must be a lowercase monitoring name')
    address = ipaddress.ip_address(config['address'])
    if not address.is_private:
        raise ValueError('a sidecar listens on its private address only')
    port = config.get('port', PORT)
    if not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError('port must be an unprivileged TCP port')
    units = config.get('units', [])
    if not isinstance(units, list) or not all(isinstance(u, str) and UNIT.match(u) for u in units):
        raise ValueError('units must be systemd unit names')
    edge = config.get('edge_metrics_url')
    if edge is not None and not re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}/[A-Za-z0-9/_-]*', edge):
        raise ValueError('edge_metrics_url is a loopback HTTP URL')
    document = {
        'host_id': config['host_id'],
        'listen': '%s:%d' % (address, port) if address.version == 4 else '[%s]:%d' % (address, port),
        'tls': {'cert': 'server.pem', 'key': 'server.key', 'client_ca': 'monitoring-ca.pem',
                'collector_identity': COLLECTOR_IDENTITY},
        'required_volumes': _volumes(config.get('required_volumes', [])),
        'units': units,
        'hooks': _hooks(config.get('hooks', [])),
        'state_path': STATE,
    }
    if edge is not None:
        document['edge_metrics_url'] = edge
    return document


def config_bytes(document):
    """Exactly the bytes the deploy engine installs (role_config.render's encoding)."""
    return (json.dumps(document, sort_keys=True, indent=2) + '\n').encode()


def config_digest(document):
    return 'sha256:' + hashlib.sha256(config_bytes(document)).hexdigest()


def expected_checks(document):
    """Check names the sidecar reports on every collection (pir-apm-sidecar `expected_checks`)."""
    checks = ['memory', 'swap', 'filesystems', 'certificate']
    checks += ['volume:' + v['path'] for v in document['required_volumes']]
    for unit in document['units']:
        checks += ['unit:%s:%s' % (unit.lower(), kind) for kind in ('active', 'restarts', 'oom')]
    checks += ['hook:' + h['name'] for h in document['hooks']]
    if 'edge_metrics_url' in document:
        checks.append('edge')
    return checks


def sidecar(settings, *, release, template=None):
    """Render the sidecar unit and configuration for the deploy engine."""
    document = sidecar_document(settings)
    rendered = render(TEMPLATE.read_text() if template is None else template,
                      directory(release) + '/pir-apm-sidecar', ['--config', CONFIG],
                      {CONFIG: document})
    if rendered.files[CONFIG].encode() != config_bytes(document):
        raise ValueError('rendered sidecar configuration differs from its digest encoding')
    return RenderedRole(rendered.unit, rendered.files, (), ('--config', CONFIG, '--check'))


def observer_entries(inventory):
    """`(inventory host, settings)` for every observer role in the deploy inventory."""
    roles = inventory.get('services', {}).get(SERVICE, {}).get('roles', {}).get(ROLE, [])
    entries = []
    for entry in roles:
        settings = entry.get('settings', {})
        if settings.get('host_id') != entry.get('host'):
            raise ValueError('observer on %s: settings.host_id must equal the inventory host name'
                             % entry.get('host'))
        if entry['host'] not in inventory.get('hosts', {}):
            raise ValueError('observer on %s: not an inventory host' % entry['host'])
        entries.append((entry['host'], settings))
    names = [host for host, _ in entries]
    if len(names) != len(set(names)):
        raise ValueError('a host has more than one observer')
    return entries


def observers(inventory, *, mode, edge_outputs=None):
    """The central APM's observer inventory (`PIR_APM_OBSERVERS`)."""
    if mode not in ('shadow', 'active'):
        raise ValueError('mode is shadow or active')
    edge_outputs = edge_outputs or {}
    hosts = []
    for host, settings in observer_entries(inventory):
        document = sidecar_document(settings)
        entry = {'host_id': host, 'address': document['listen'], 'tls_name': tls_name(host),
                 'config_digest': config_digest(document), 'checks': expected_checks(document)}
        if host in edge_outputs:
            entry['edge_output'] = edge_outputs[host]
        hosts.append(entry)
    return {'mode': mode,
            'collector': {'cert': 'collector.pem', 'key': 'collector.key', 'ca': 'monitoring-ca.pem'},
            'hosts': hosts,
            'decommissioned': sorted(inventory.get('retired_hosts', {}))}


def _addresses(record):
    """IP addresses named by one host record's `*ipv4*` or `ssh_host` fields."""
    found = set()
    for key, value in record.items():
        if isinstance(value, str) and ('ipv4' in key or key == 'ssh_host'):
            try:
                found.add(str(ipaddress.ip_address(value)))
            except ValueError:
                pass
    return frozenset(found)


def provisioned_hosts(outputs):
    """Hosts in `terraform output -json`, each as `(label, addresses)`.

    A host is any object carrying `*ipv4*` fields (worker groups, routers,
    roster members in a JSON-string output) or a family of scalar or list
    outputs sharing a prefix (`X_public_ipv4`, `X_private_ipv4`, `X_ipv4`),
    whose list elements pair by position. One host's public and private
    addresses stay together, so enrollment by either covers it.
    """
    hosts = []
    families = {}

    def walk(label, value):
        if isinstance(value, str) and value[:1] in '[{':
            try:
                value = json.loads(value)
            except ValueError:
                return
        if isinstance(value, dict):
            addresses = _addresses(value)
            if addresses:
                hosts.append((value.get('name') or value.get('id') or label, addresses))
            for key, child in value.items():
                if isinstance(child, (dict, list)):
                    walk(label, child)
        elif isinstance(value, list):
            for child in value:
                walk(label, child)

    for name, output in sorted(outputs.items()):
        value = output.get('value') if isinstance(output, dict) else None
        match = re.fullmatch(r'(.+?)_(public_|private_)?ipv4', name)
        if match and (isinstance(value, str) or (isinstance(value, list) and all(isinstance(v, str) for v in value))):
            values = value if isinstance(value, list) else [value]
            family = families.setdefault(match.group(1), {})
            for index, address in enumerate(values):
                family.setdefault(index, set()).add(address)
        else:
            walk(name, value)
    for prefix, members in sorted(families.items()):
        for index, addresses in sorted(members.items()):
            parsed = _addresses({'ipv4_%d' % i: a for i, a in enumerate(sorted(addresses))})
            if parsed:
                hosts.append(('%s[%d]' % (prefix, index) if len(members) > 1 else prefix, parsed))
    return hosts


def coverage(inventory, *, terraform_outputs=None, external_hosts=(), roster=None, expected_volumes=None):
    """Compare provisioned hosts and expected volumes with observer enrollment.

    `terraform_outputs` is `terraform output -json` of the fleet root,
    `external_hosts` maps declared non-Terraform hosts (an external GPU or a
    checker) to their addresses, and `roster` is the Transparent dynamic
    roster. `expected_volumes` maps an enrolled host to mount paths it must
    check; coordinators always need their three data volumes. Each missing
    source makes the report `partial`: it then proves nothing about the cloud.
    """
    entries = observer_entries(inventory)
    enrolled = {host: sidecar_document(settings) for host, settings in entries}
    covered = {}
    for host, document in enrolled.items():
        addresses = {document['listen'].rsplit(':', 1)[0].strip('[]')}
        entry = inventory['hosts'].get(host) or {}
        if entry.get('address'):
            addresses.add(entry['address'])
        for address in addresses:
            covered[address] = host
    provisioned = provisioned_hosts(terraform_outputs or {})
    provisioned += [(name, frozenset(addresses)) for name, addresses in dict(external_hosts).items()]
    provisioned += [(m['id'], _addresses(m)) for m in roster or []
                    if m.get('intent', 'enrolled') in ('enrolled', 'draining')]
    missing = sorted({label for label, addresses in provisioned if not addresses & covered.keys()})
    seen = {covered[a] for _, addresses in provisioned for a in addresses if a in covered}
    volumes = dict(expected_volumes or {})
    # The coordinator is where the central APM runs.
    apm = inventory.get('services', {}).get('apm', {}).get('roles', {}).get('server', [])
    for host in {entry['host'] for entry in apm}:
        volumes.setdefault(host, list(COORDINATOR_VOLUMES))
    missing_volumes = {}
    for host, paths in sorted(volumes.items()):
        declared = {v['path'] for v in enrolled.get(host, {}).get('required_volumes', [])}
        absent = sorted(set(paths) - declared)
        if absent:
            missing_volumes[host] = absent
    sources = {'terraform': terraform_outputs is not None, 'roster': roster is not None}
    partial = not all(sources.values())
    return {
        'enrolled': sorted(enrolled),
        'missing_declarations': missing,
        'not_provisioned': [] if partial else sorted(set(enrolled) - seen),
        'missing_volumes': missing_volumes,
        'decommissioned': sorted(inventory.get('retired_hosts', {})),
        'partial': partial,
        'sources': sources,
    }
