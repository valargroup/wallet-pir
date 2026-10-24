"""The central APM and the independent monitor as rendered deploy roles.

`apm` renders `pir-apm.service` for the `apm.central` role and `monitor`
renders `pir-monitor.service` for the `monitor.checker` role, each from its
inventory entry's settings, so a fresh host and an adopted one run the same
reviewed unit under the production lock (install, update, status, rollback).

Neither unit carries a secret. Credentials are root-only files that
`wallet-pir-monitoring.py install-credentials` installs from a private
directory filled from Infisical, and each reaches only its own unit through
`LoadCredential=`: the collector identity and the observer inventory for the
APM, and one Slack webhook per process, named to it by `*_SLACK_WEBHOOK_FILE`.
An environment setting cannot name a webhook, so no rendered byte holds one.
"""
import json
import re

from . import monitoring_certs
from .role_config import RenderedRole, directory, limits_of, render, set_service, settings_of

APM_UNIT = 'pir-apm.service'
APM_LISTEN = '127.0.0.1:3002'
APM_STATE = '/var/lib/pir-apm/incidents.sqlite'
APM_OBSERVERS = '/etc/pir-apm/observers.json'
# Kept apart from the collector's directory: each install journal covers one directory.
APM_ALERTING = '/etc/pir-apm/alerting'
APM_CHAIN_COOKIE = '/root/.cache/zakura/.cookie'
MONITOR_UNIT = 'pir-monitor.service'
MONITOR_LISTEN = '127.0.0.1:3003'
MONITOR_STATE = '/var/lib/pir-monitor/incidents.sqlite'
MONITOR_CREDENTIALS = '/etc/pir-monitor/credentials'
MONITOR_PROBES = '/etc/pir-monitor/service-probes.json'
SLACK = 'slack-webhook'
CHAIN_RPC_COOKIE = 'chain-rpc-cookie'
NAME = re.compile(r'^[A-Z][A-Z0-9_]*$')
# No quote, backslash, `%` or `$`: a value can neither end its word nor reach a
# systemd specifier or variable expansion.
VALUE = re.compile(r'^[^"\\%$\x00-\x1f\x7f]{1,512}$')
# Set by the renderer or holding a secret; never an inventory setting.
APM_RESERVED = {'PIR_APM_SLACK_WEBHOOK_URL', 'PIR_APM_SLACK_WEBHOOK_FILE', 'PIR_APM_OBSERVERS',
                'PIR_APM_STATE_PATH', 'PIR_APM_LISTEN', 'PIR_APM_CHAIN_RPC_COOKIE'}
MONITOR_REQUIRED = ('PIR_MONITOR_ORIGIN', 'PIR_MONITOR_APM_STATUS_URL', 'PIR_MONITOR_ORACLE',
                    'PIR_MONITOR_ORACLE_SHA256')
MONITOR_OPTIONAL = ('PIR_MONITOR_ALERT_MODE', 'PIR_MONITOR_PUBLIC_STATUS_URL', 'PIR_MONITOR_SERVICE_ALERT_MODE')
MODES = ('shadow', 'active')


def _environment(value, prefix, allowed=None, reserved=()):
    if not isinstance(value, dict):
        raise ValueError('environment must map variable names to values')
    for name, setting in value.items():
        if not isinstance(name, str) or not NAME.match(name) or not name.startswith(prefix):
            raise ValueError('environment names must be %s* variables' % prefix)
        if name in reserved or (allowed is not None and name not in allowed):
            raise ValueError('%s is rendered by the role or not configurable here' % name)
        if not isinstance(setting, str) or not VALUE.match(setting) or setting != setting.strip():
            raise ValueError('%s must be a plain single-line value' % name)
        if 'hooks.slack.com' in setting.lower():
            raise ValueError('%s looks like a Slack webhook; webhooks are systemd credentials' % name)
        if name.endswith('ALERT_MODE') and setting not in MODES:
            raise ValueError('%s is shadow or active' % name)
    return dict(value)


def _quoted(environment):
    return ['"%s=%s"' % (name, value) for name, value in sorted(environment.items())]


def _flag(settings, name, default):
    value = settings.get(name, default)
    if type(value) is not bool:
        raise ValueError('%s is true or false' % name)
    return value


def apm(settings, *, release, template):
    """The central APM: collector, observer inventory, alert evaluation and Slack delivery.

    Settings: `environment` (non-secret `PIR_APM_*` configuration such as
    scrape URLs, alert modes, the peer status URL and oracle anchor),
    `slack` (default true), `chain_cookie` (the node cookie path loaded as
    the `chain-cookie` credential, default the coordinator node's; null for
    none), `writable` (paths besides its state directory it writes, such as
    edge metric outputs) and `limits`.
    """
    s = settings_of(settings, (), ('environment', 'slack', 'chain_cookie', 'writable'))
    writable = s.get('writable', [])
    if not isinstance(writable, list):
        raise ValueError('writable is a list of paths')
    environment = _environment(s.get('environment', {}), 'PIR_APM_', reserved=APM_RESERVED)
    environment.update(PIR_APM_LISTEN=APM_LISTEN, PIR_APM_STATE_PATH=APM_STATE, PIR_APM_OBSERVERS=APM_OBSERVERS)
    credentials = ['%s:%s/%s' % (name, monitoring_certs.COLLECTOR_CREDENTIALS, name)
                   for name in monitoring_certs.COLLECTOR_FILES]
    if _flag(s, 'slack', True):
        credentials.append('%s:%s/%s' % (SLACK, APM_ALERTING, SLACK))
        environment['PIR_APM_SLACK_WEBHOOK_FILE'] = SLACK
    cookie = s.get('chain_cookie', APM_CHAIN_COOKIE)
    if cookie is not None:
        credentials.append('chain-cookie:' + directory(cookie))
        environment['PIR_APM_CHAIN_RPC_COOKIE'] = '/run/credentials/%s/chain-cookie' % APM_UNIT
    rendered = render(template, directory(release) + '/pir-apm', [], {}, writable)
    unit = set_service(rendered.unit, {'Environment': _quoted(environment), 'LoadCredential': credentials})
    return RenderedRole(_limited(unit, s), {}, ())


def monitor(settings, *, release, template):
    """The independent monitor on its own host and region.

    Settings: `environment` (`PIR_MONITOR_ORIGIN`, `PIR_MONITOR_APM_STATUS_URL`,
    `PIR_MONITOR_ORACLE` and `PIR_MONITOR_ORACLE_SHA256` required; alert
    modes and the public status URL optional), `slack` (default true),
    `service_probes` (the probe records, rendered as
    `/etc/pir-monitor/service-probes.json`), `chain_rpc_cookie` (default
    false: load the probes' node cookie credential) and `limits`.
    """
    s = settings_of(settings, ('environment',), ('slack', 'service_probes', 'chain_rpc_cookie'))
    environment = _environment(s['environment'], 'PIR_MONITOR_', allowed=MONITOR_REQUIRED + MONITOR_OPTIONAL)
    missing = [name for name in MONITOR_REQUIRED if name not in environment]
    if missing:
        raise ValueError('monitor environment is missing %s' % missing)
    if not re.fullmatch('[0-9a-f]{64}', environment['PIR_MONITOR_ORACLE_SHA256']):
        raise ValueError('PIR_MONITOR_ORACLE_SHA256 is the oracle file\'s lowercase sha256')
    directory(environment['PIR_MONITOR_ORACLE'])
    environment.update(PIR_MONITOR_LISTEN=MONITOR_LISTEN, PIR_MONITOR_STATE_PATH=MONITOR_STATE)
    credentials = []
    if _flag(s, 'slack', True):
        credentials.append('%s:%s/%s' % (SLACK, MONITOR_CREDENTIALS, SLACK))
        environment['PIR_MONITOR_SLACK_WEBHOOK_FILE'] = SLACK
    if _flag(s, 'chain_rpc_cookie', False):
        credentials.append('%s:%s/%s' % (CHAIN_RPC_COOKIE, MONITOR_CREDENTIALS, CHAIN_RPC_COOKIE))
    files = {}
    probes = s.get('service_probes')
    if probes is not None:
        files[MONITOR_PROBES] = _probes(probes)
        environment['PIR_MONITOR_SERVICE_PROBES_CONFIG'] = MONITOR_PROBES
    rendered = render(template, directory(release) + '/pir-monitor', [], {})
    extra = {'Environment': _quoted(environment)}
    if credentials:
        extra['LoadCredential'] = credentials
    unit = set_service(rendered.unit, extra)
    return RenderedRole(_limited(unit, s), files, ())


def _probes(records):
    """Unique known services, absolute commands and 1–45 second timeouts, as pir-monitor requires."""
    if not isinstance(records, list) or not records:
        raise ValueError('service_probes is a non-empty list')
    names = set()
    for record in records:
        if (not isinstance(record, dict) or set(record) != {'service', 'command', 'timeout_seconds'}
                or record['service'] not in ('status', 'transparent', 'receiver', 'txid')
                or record['service'] in names or not isinstance(record['command'], list) or not record['command']
                or not all(isinstance(a, str) and a and not any(ord(c) < 32 for c in a) for a in record['command'])
                or not record['command'][0].startswith('/')
                or type(record['timeout_seconds']) is not int or not 1 <= record['timeout_seconds'] <= 45):
            raise ValueError('each service probe is {"service", "command": [absolute, ...], "timeout_seconds"}')
        names.add(record['service'])
    return json.dumps(records, indent=2) + '\n'


def _limited(unit, settings):
    limits = limits_of(settings)
    return set_service(unit, limits) if limits else unit
