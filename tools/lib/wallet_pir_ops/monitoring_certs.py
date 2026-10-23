"""Monitoring CA, per-host serving certificates and the collector identity.

Issuance runs on an operator machine with `openssl`; there is no unattended
issuer. `issue_ca` creates the dedicated monitoring CA, `issue_leaf` a host's
serving certificate (`<host>.pir-monitoring`, serverAuth) or the central
collector's client certificate (`pir-apm-collector`, clientAuth). The operator
stores the results in Infisical, the source of truth; nothing here prints key
material, and Terraform, cloud-init and configuration reports never carry it.

`install` places one host's credential files as root-only files under the
production lock, restarts the unit that loads them through systemd
credentials, and waits for its readiness. A failed start restores the previous
files and restarts again, so a rotation is all-or-nothing on each host. The
central APM's observer inventory is installed the same way.
"""
import datetime
import os
from pathlib import Path
import subprocess
import tempfile
import time


WARN_DAYS = 30
CRITICAL_DAYS = 7
LEAF_DAYS = 397
CA_DAYS = 3650
SIDECAR_CREDENTIALS = '/etc/pir-apm-sidecar/credentials'
SIDECAR_UNIT = 'pir-apm-sidecar.service'
SIDECAR_HEALTH = 'http://127.0.0.1:9444/healthz'
SIDECAR_FILES = ('server.pem', 'server.key', 'monitoring-ca.pem')
COLLECTOR_CREDENTIALS = '/etc/pir-apm/credentials'
COLLECTOR_FILES = ('collector.pem', 'collector.key', 'monitoring-ca.pem')
USAGES = {'server': 'serverAuth', 'client': 'clientAuth'}


def _openssl(*args, cwd):
    # Errors name the failing step only; openssl output can echo key paths.
    result = subprocess.run(['openssl', *args], cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError('openssl %s failed' % args[0])
    return result.stdout


def issue_ca(directory, *, days=CA_DAYS):
    """Create `ca.pem` and a 0600 `ca.key` in a new private directory."""
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    _openssl('req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
             '-keyout', 'ca.key', '-out', 'ca.pem', '-days', str(days), '-subj', '/CN=PIR monitoring CA',
             '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
             cwd=directory)
    os.chmod(directory / 'ca.key', 0o600)
    return directory / 'ca.pem'


def issue_leaf(ca_directory, output, name, usage, *, days=LEAF_DAYS):
    """Sign `<output>/<stem>.pem` and `.key` for DNS `name`; returns the cert path.

    The stem is `server` or `collector`. Each issuance makes a fresh key, so a
    rotation never reuses one.
    """
    if usage not in USAGES:
        raise ValueError('usage is server or client')
    ca_directory, output = Path(ca_directory), Path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    stem = 'server' if usage == 'server' else 'collector'
    with tempfile.TemporaryDirectory() as scratch:
        extensions = Path(scratch) / 'ext.cnf'
        extensions.write_text('subjectAltName=DNS:%s\nextendedKeyUsage=%s\nbasicConstraints=critical,CA:FALSE\n'
                              'keyUsage=critical,digitalSignature\n' % (name, USAGES[usage]))
        _openssl('req', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
                 '-keyout', str(output / (stem + '.key')), '-out', str(Path(scratch) / 'leaf.csr'),
                 '-subj', '/CN=' + name, cwd=scratch)
        os.chmod(output / (stem + '.key'), 0o600)
        _openssl('x509', '-req', '-in', 'leaf.csr', '-CA', str(ca_directory / 'ca.pem'),
                 '-CAkey', str(ca_directory / 'ca.key'), '-CAcreateserial', '-out', str(output / (stem + '.pem')),
                 '-days', str(days), '-extfile', str(extensions), cwd=scratch)
    (output / 'monitoring-ca.pem').write_bytes((ca_directory / 'ca.pem').read_bytes())
    return output / (stem + '.pem')


def not_after(pem):
    """Expiry of a PEM certificate as a UTC datetime."""
    with tempfile.NamedTemporaryFile('w', suffix='.pem') as handle:
        handle.write(pem)
        handle.flush()
        text = _openssl('x509', '-in', handle.name, '-noout', '-enddate', cwd='/')
    value = text.strip().split('=', 1)[1]
    return datetime.datetime.strptime(value, '%b %d %H:%M:%S %Y %Z').replace(tzinfo=datetime.timezone.utc)


def expiry_status(pem, now=None):
    """`healthy`, `warning` (under 30 days) or `critical` (under 7 days)."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    remaining = not_after(pem) - now
    if remaining < datetime.timedelta(days=CRITICAL_DAYS):
        return 'critical'
    if remaining < datetime.timedelta(days=WARN_DAYS):
        return 'warning'
    return 'healthy'


def _ready(executor, host, health, deadline):
    import json
    while time.monotonic() < deadline:
        status, body = executor.http_get(host, health)
        if status == 200:
            try:
                if json.loads(body).get('ready') is True:
                    return True
            except ValueError:
                pass
        time.sleep(1)
    return False


def install(executor, lock, host, files, *, directory=SIDECAR_CREDENTIALS, unit=SIDECAR_UNIT,
            health=SIDECAR_HEALTH, ready_timeout=60, mode=0o600):
    """Install files on one host and restart its unit, or restore and raise.

    `lock` is the production lock as a context manager (`remote.production_lock`).
    `files` maps file names in `directory` to bytes. The previous contents are
    read first; if the restarted unit is not ready in time they are put back
    and the unit restarted again. Returns the names whose contents changed.
    """
    with lock as held:
        previous = {name: executor.read(host, '%s/%s' % (directory, name)) for name in files}
        changed = sorted(name for name, data in files.items()
                         if previous[name] is None or previous[name].encode() != data)
        if not changed:
            return []
        held.verify()
        executor.mkdir(host, directory, 0o700)
        for name in changed:
            staged = '%s/.%s.next' % (directory, name)
            executor.write(host, staged, files[name], mode)
            if previous[name] is not None:
                executor.remove(host, '%s/%s' % (directory, name))
            executor.rename(host, staged, '%s/%s' % (directory, name))
        executor.systemctl(host, 'restart', unit)
        if _ready(executor, host, health, time.monotonic() + ready_timeout):
            return changed
        held.verify()
        for name in changed:
            executor.remove(host, '%s/%s' % (directory, name))
            if previous[name] is not None:
                executor.write(host, '%s/%s' % (directory, name), previous[name].encode(), mode)
        executor.systemctl(host, 'restart', unit)
        raise RuntimeError('%s did not become ready after the change on %s; previous files restored'
                           % (unit, host))
