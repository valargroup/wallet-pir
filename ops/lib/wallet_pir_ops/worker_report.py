"""A private, read-only report of external workers from their existing endpoints.

Each worker is reported on separate axes that must not be conflated:

- `process`: systemd's state and the running executable's digest;
- `executable`: the identity the process reports (binary digest, incarnation);
- `publication`: what is assigned and what is loaded, as the product reports it;
- `preparation`: the latest preparation outcome or refusal;
- `readiness`: `verified` (its inputs passed the product loader), `warm`
  (prepared runtimes ready), `authorized` (the product controller activated
  it) and `ready` (the product's own readiness answer);
- `encrypted_correctness`: never inferred here; only an exact-answer check
  observes it.

A concept a product does not expose is the string "unsupported", and one it
could expose but did not answer is "unobserved": neither is guessed. Public
response schemas are untouched; this reads the private health routes the
deploy already verifies against.
"""
import importlib
import json

UNSUPPORTED = 'unsupported'
UNOBSERVED = 'unobserved'
# Product-local extractors, by service: `module:function(service, role, health, ready_status)`.
EXTRACTORS = {
    'enhance': 'ops.enhance.lib.roles:report',
    'status': 'ops.enhance.lib.roles:report',
    'transparent-history': 'ops.transparent.lib.roles:report',
    'transparent-display': 'ops.transparent.lib.roles:report',
    'receiver': 'ops.receiver.lib.roles:report',
}


def extractor(service):
    module, function = EXTRACTORS[service].split(':')
    return getattr(importlib.import_module(module), function)


def collect(service, targets, executor):
    """One report per target. An unreachable host or endpoint is a result, never an omission."""
    reports = []
    for target in targets:
        entry = {'service': service.name, 'role': target.role.name, 'host': target.host, 'unit': target.unit,
                 'encrypted_correctness': 'unobserved; run the exact-answer check'}
        try:
            state = executor.probe_unit(target.host, target.unit)
        except Exception as error:  # noqa: BLE001 - each host's failure is its own result
            entry['process'] = {'alive': None, 'error': str(error)}
            reports.append(entry)
            continue
        entry['process'] = {'alive': state['active_state'] == 'active' and bool(state['main_pid']),
                            'load_state': state['load_state'], 'active_state': state['active_state'],
                            'exe_sha256': state['exe_sha256']}
        health = ready_status = None
        if entry['process']['alive']:
            status, body = executor.http_get(target.host, target.url(target.role.health))
            try:
                health = json.loads(body) if status == 200 else None
            except ValueError:
                health = None
            if health is None:
                entry['health_error'] = 'health answered %s' % status
            if target.role.ready_url:
                ready_status, _ = executor.http_get(target.host, target.url(target.role.ready_url))
        entry.update(extractor(service.name)(service.name, target.role.name, health, ready_status))
        executable = entry.setdefault('executable', {})
        if entry['process'].get('exe_sha256') and executable.get('binary_sha256') not in (None, UNOBSERVED):
            executable['matches_process'] = executable['binary_sha256'] == entry['process']['exe_sha256']
        reports.append(entry)
    return reports
