"""Install and enroll a sidecar on a host a controller created itself.

The deploy engine installs sidecars on deployment-inventory hosts. A controller
that provisions hosts (the Transparent fleet actuator) calls `install_and_enroll`
instead, with the same renderer: it issues the host's serving certificate from
the monitoring CA it receives as Infisical runtime references
(`PIR_MONITORING_CA_PEM`, `PIR_MONITORING_CA_KEY` in its environment, never in
its configuration or on the new host), installs the pinned binary, root-only
credential files, configuration and unit, and waits until the sidecar reports
ready with the expected configuration digest and binary. Only then does it
return the enrollment record the APM expects; a host without one is reported
as unenrolled, never as covered.

`transport` is the controller's host access: `run(host, command, data=None,
timeout=...)` returning stdout and `copy(host, sources, destination)`.
"""
import hashlib
import json
import os
from pathlib import Path
import shlex
import tempfile
import time

from . import monitoring, monitoring_certs

CA_PEM = 'PIR_MONITORING_CA_PEM'
CA_KEY = 'PIR_MONITORING_CA_KEY'
RELEASES = '/opt/pir-apm-sidecar/releases'
UNIT_PATH = '/etc/systemd/system/' + monitoring_certs.SIDECAR_UNIT


def issue_from_environment(host_id, environ=os.environ):
    """A fresh serving certificate for `host_id`, signed by the runtime-provided CA."""
    if not environ.get(CA_PEM) or not environ.get(CA_KEY):
        raise RuntimeError('monitoring CA runtime references %s and %s are not set' % (CA_PEM, CA_KEY))
    with tempfile.TemporaryDirectory() as scratch:
        ca = Path(scratch) / 'ca'
        ca.mkdir(mode=0o700)
        (ca / 'ca.pem').write_text(environ[CA_PEM])
        key = ca / 'ca.key'
        key.touch(mode=0o600)
        key.write_text(environ[CA_KEY])
        out = Path(scratch) / 'out'
        monitoring_certs.issue_leaf(ca, out, monitoring.tls_name(host_id), 'server')
        return {name: (out / name).read_bytes() for name in monitoring_certs.SIDECAR_FILES}


def _put(transport, host, path, data, mode):
    quoted = shlex.quote(path)
    transport.run(host, 'install -d -m 755 %s && cat > %s.next && chmod %o %s.next && mv %s.next %s'
                  % (shlex.quote(os.path.dirname(path)), quoted, mode, quoted, quoted, quoted), data=data)


def install_and_enroll(transport, host, settings, *, binary, credentials, ready_timeout=60, sleep=time.sleep):
    """Install the sidecar for `settings` on `host`; returns its enrollment record."""
    binary = Path(binary)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    release = '%s/%s' % (RELEASES, digest)
    rendered = monitoring.sidecar(settings, release=release)
    record = monitoring.enrollment_record(settings)
    transport.run(host, 'install -d -m 755 ' + shlex.quote(release))
    transport.copy(host, [str(binary)], release + '/pir-apm-sidecar.next')
    transport.run(host, 'cd %s && echo "%s  pir-apm-sidecar.next" | sha256sum -c --quiet '
                        '&& chmod 755 pir-apm-sidecar.next && mv pir-apm-sidecar.next pir-apm-sidecar'
                  % (shlex.quote(release), digest))
    transport.run(host, 'install -d -m 700 ' + monitoring_certs.SIDECAR_CREDENTIALS)
    for name in monitoring_certs.SIDECAR_FILES:
        _put(transport, host, '%s/%s' % (monitoring_certs.SIDECAR_CREDENTIALS, name), credentials[name], 0o600)
    for path, text in rendered.files.items():
        _put(transport, host, path, text.encode(), 0o644)
    _put(transport, host, UNIT_PATH, rendered.unit.encode(), 0o644)
    transport.run(host, 'systemctl daemon-reload && systemctl enable %s && systemctl restart %s'
                  % (monitoring_certs.SIDECAR_UNIT, monitoring_certs.SIDECAR_UNIT))
    health = monitoring_certs.SIDECAR_HEALTH
    port = settings.get('health_port')
    if port:
        health = 'http://127.0.0.1:%d/healthz' % port
    deadline = time.monotonic() + ready_timeout
    while True:
        try:
            report = json.loads(transport.run(host, 'curl -s --max-time 3 ' + shlex.quote(health)))
        except Exception:  # noqa: BLE001 - any transport or parse failure is "not ready yet"
            report = {}
        if (report.get('ready') is True and report.get('config_digest') == record['config_digest']
                and report.get('binary_sha256') == digest):
            return record
        if time.monotonic() >= deadline:
            raise RuntimeError('sidecar on %s is not ready with the expected configuration' % host)
        sleep(2)
