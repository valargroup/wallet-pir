"""Monitoring CA, per-host serving certificates and the collector identity.

Issuance runs on an operator machine with `openssl`; there is no unattended
issuer. `issue_ca` creates the dedicated monitoring CA, `issue_leaf` a host's
serving certificate (`<host>.pir-monitoring`, serverAuth) or the central
collector's client certificate (`pir-apm-collector`, clientAuth). The operator
stores the results in Infisical, the source of truth; nothing here prints key
material, and Terraform, cloud-init and configuration reports never carry it.

`install` places one host's credential files as root-only files under the
production lock, restarts the unit that loads them through systemd
credentials, and waits for that role's own readiness contract, as a journaled
transaction with `reconcile` and `rollback`. A failed start restores the
previous files and restarts again, so a rotation is all-or-nothing on each
host. The central APM's observer inventory is installed the same way.

On a host whose unit is not installed yet (a rebuild, before the role's first
deploy), `install` stages the files under the same journal without a restart;
the role's install then starts the unit with them.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from .deploy import remote, transaction


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
COLLECTOR_UNIT = 'pir-apm.service'
COLLECTOR_READY = 'http://127.0.0.1:3002/observers/ready'
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


def sidecar_ready(health=SIDECAR_HEALTH):
    """The sidecar's readiness contract: its health JSON reports `ready: true`."""
    def ready(executor, host):
        status, body = executor.http_get(host, health)
        return status == 200 and json.loads(body).get('ready') is True
    return ready


def collector_ready(revision, url=COLLECTOR_READY):
    """Require the loaded revision and fresh authenticated collection from every host."""
    def ready(executor, host):
        status, body = executor.http_get(host, url)
        if status != 200:
            return False
        report = json.loads(body)
        expected = report.get('expected_hosts', 0)
        evaluated = report.get('evaluated_at', 0)
        now = time.time()
        return (report.get('ready') is True and report.get('revision') == revision
                and type(expected) is int and expected > 0 and report.get('valid_hosts') == expected
                and type(evaluated) in (int, float) and now - 45 <= evaluated <= now + 5)
    return ready


def collector_restore_ready(executor, host, record):
    """Check the previous inventory revision saved before the transaction touched files."""
    return collector_ready(record['identity']['previous_revision'])(executor, host)


def _wait(executor, host, ready, timeout, sleep, clock):
    deadline = clock() + timeout
    while True:
        try:
            if ready(executor, host):
                return True
        except (OSError, ValueError, RuntimeError, AttributeError):
            pass  # an unreachable or malformed answer is "not ready yet"
        if clock() >= deadline:
            return False
        sleep(1)


def _digest(data):
    return None if data is None else hashlib.sha256(data).hexdigest()


def _service(host, directory):
    """One journal sequence per host and file set, as the deploy engine keeps one per service."""
    return 'monitoring-files-%s-%s' % (host, directory.strip('/').replace('/', '-'))


def install(executor, lock, host, files, *, state_dir, directory=SIDECAR_CREDENTIALS, unit=SIDECAR_UNIT,
            ready=None, restore_ready=None, identity=None, ready_timeout=60, mode=0o600,
            sleep=time.sleep, clock=time.monotonic):
    """Replace files on one host and restart its unit as one journaled transaction.

    `lock` is the production lock as a context manager (`remote.production_lock`)
    and `files` maps names in `directory` to bytes. The runner journal under
    `state_dir` (one per host, as the deploy engine keeps one per service) is
    written before every effect and records only digests: previous contents
    are backed up on the host itself, root-only, in `.previous-<transaction>`.
    Each file is replaced atomically, never removed first. Any failure after
    the first effect, an unready unit included, restores the backup and
    restarts again; if that cannot finish (the lock was lost or the host is
    unreachable), the journal stays open and the next `install` or `reconcile`
    on the host completes the restoration before anything else.
    Where `unit` is not installed, the files are staged without a restart.
    Returns the names whose contents changed.
    """
    ready = ready or sidecar_ready()
    with lock as held:
        _reconcile(executor, held, host, directory, state_dir, ready, restore_ready, ready_timeout, sleep, clock)
        previous = {name: executor.read(host, '%s/%s' % (directory, name)) for name in files}
        if identity is not None and 'observers.json' in previous:
            old = previous['observers.json']
            identity = dict(identity, previous_revision=json.loads(old)['revision'] if old else None)
        changed = sorted(name for name, data in files.items()
                         if previous[name] is None or previous[name].encode() != data)
        previous_unit = executor.probe_unit(host, unit)
        if not changed:
            if previous_unit['load_state'] != 'not-found' and not _wait(
                    executor, host, ready, ready_timeout, sleep, clock):
                raise RuntimeError('%s has unchanged files but is not ready' % unit)
            return []
        prior = transaction.Journal.load(state_dir, _service(host, directory))
        staged = previous_unit['load_state'] == 'not-found'
        record = {'key': '%s:%s' % (host, directory), 'host': host, 'unit': unit, 'directory': directory,
                  'mode': mode, 'phase': 'pending', 'restart_issued': False, 'staged': staged,
                  'previous_active': previous_unit['active_state'], 'identity': identity,
                  'changes': [{'name': name, 'previous_sha256': _digest(None if previous[name] is None
                                                                        else previous[name].encode()),
                               'new_sha256': _digest(files[name])} for name in changed]}
        journal = transaction.Journal.create(state_dir, _service(host, directory), _digest(b''.join(
            files[name] for name in changed)), None, [record], None)
        backup = '%s/.previous-%s' % (directory, journal.id)
        record['backup'] = backup
        try:
            held.verify()
            journal.set_phase(0, 'backing-up')
            executor.mkdir(host, directory, 0o700)
            executor.mkdir(host, backup, 0o700)
            for name in changed:
                if previous[name] is not None:
                    executor.write(host, '%s/%s' % (backup, name), previous[name].encode(), 0o600)
                    if executor.sha256(host, '%s/%s' % (backup, name)) != _digest(previous[name].encode()):
                        raise RuntimeError('backup of %s did not verify' % name)
            journal.set_phase(0, 'installing')
            for name in changed:
                held.verify()
                executor.write(host, '%s/%s' % (directory, name), files[name], mode)
            held.verify()
            if not staged:
                record['restart_issued'] = True
                journal.set_phase(0, 'restarting')
                executor.systemctl(host, 'restart', unit)
                journal.set_phase(0, 'verifying')
                if not _wait(executor, host, ready, ready_timeout, sleep, clock):
                    raise RuntimeError('%s did not become ready' % unit)
            elif executor.probe_unit(host, unit)['load_state'] != 'not-found':
                raise RuntimeError('%s was installed while its files were staged' % unit)
            held.verify()
            journal.set_phase(0, 'verified')
            journal.set_status('committed')
        except remote.LockHeld:
            journal.event('lock lost; restoration waits for reconcile')
            raise
        except Exception as error:  # noqa: BLE001 - every failure restores, then reports its kind
            journal.event('failed', error=type(error).__name__)
            _restore(executor, held, journal, ready, restore_ready, ready_timeout, sleep, clock)
            raise RuntimeError('%s did not become ready after the change on %s; previous files restored'
                               % (unit, host)) from None
        if prior is not None and prior.hosts[0].get('backup'):
            # Only the latest backup stays, for an explicit rollback.
            executor.remove_tree(host, prior.hosts[0]['backup'], prior.hosts[0]['directory'])
        return changed


def _restore(executor, held, journal, ready, restore_ready, ready_timeout, sleep, clock):
    """Put the backed-up files back and restart; finishes only with a ready unit."""
    record = journal.hosts[0]
    host, directory = record['host'], record['directory']
    if record['phase'] in ('pending', 'backing-up'):
        journal.set_status('rolled-back')  # no target file was touched
        return
    held.verify()
    journal.set_status('restoring')
    journal.set_phase(0, 'restoring')
    for change in record['changes']:
        held.verify()
        target = '%s/%s' % (directory, change['name'])
        if change['previous_sha256'] is None:
            executor.remove(host, target)
            continue
        saved = executor.read(host, '%s/%s' % (record['backup'], change['name']))
        if saved is None or _digest(saved.encode()) != change['previous_sha256']:
            journal.set_phase(0, 'restore-failed')
            raise RuntimeError('backup of %s on %s is missing or altered' % (change['name'], host))
        if executor.sha256(host, target) != change['previous_sha256']:
            executor.write(host, target, saved.encode(), record['mode'])
    held.verify()
    if record.get('staged') and executor.probe_unit(host, record['unit'])['load_state'] == 'not-found':
        # Staged for a unit that still does not exist: nothing ran with the files.
        journal.set_phase(0, 'restored')
        journal.set_status('rolled-back')
        return
    if record.get('previous_active') != 'active':
        executor.systemctl(host, 'stop', record['unit'])
        held.verify()
        if executor.probe_unit(host, record['unit'])['active_state'] not in ('inactive', 'failed'):
            journal.set_phase(0, 'restore-failed')
            raise RuntimeError('previous stopped unit could not be restored; transaction stays open for reconcile')
        journal.set_phase(0, 'restored')
        journal.set_status('rolled-back')
        return
    executor.systemctl(host, 'restart', record['unit'])
    previous_ready = ready if restore_ready is None else lambda ex, name: restore_ready(ex, name, record)
    if not _wait(executor, host, previous_ready, ready_timeout, sleep, clock):
        journal.set_phase(0, 'restore-failed')
        raise RuntimeError('%s is not ready after restoring previous files on %s; transaction stays open for reconcile'
                           % (record['unit'], host))
    held.verify()
    journal.set_phase(0, 'restored')
    journal.set_status('rolled-back')


def _reconcile(executor, held, host, directory, state_dir, ready, restore_ready, ready_timeout, sleep, clock):
    journal = transaction.Journal.load(state_dir, _service(host, directory))
    if journal is None or journal.status in ('committed', 'rolled-back'):
        return None
    journal.event('reconciling an interrupted transaction')
    _restore(executor, held, journal, ready, restore_ready, ready_timeout, sleep, clock)
    return journal


def reconcile(executor, lock, host, *, state_dir, directory=SIDECAR_CREDENTIALS, ready=None, restore_ready=None,
              ready_timeout=60, sleep=time.sleep, clock=time.monotonic):
    """Finish restoring an interrupted transaction on `host`; returns its id or None."""
    with lock as held:
        journal = _reconcile(executor, held, host, directory, state_dir, ready or sidecar_ready(), restore_ready, ready_timeout,
                             sleep, clock)
        return journal and journal.id


def rollback(executor, lock, host, *, state_dir, directory=SIDECAR_CREDENTIALS, ready=None, restore_ready=None,
             ready_timeout=60, sleep=time.sleep, clock=time.monotonic):
    """Return the latest committed transaction on `host` to its previous files."""
    with lock as held:
        ready = ready or sidecar_ready()
        _reconcile(executor, held, host, directory, state_dir, ready, restore_ready, ready_timeout, sleep, clock)
        journal = transaction.Journal.load(state_dir, _service(host, directory))
        if journal is None or journal.status != 'committed':
            raise RuntimeError('no committed monitoring transaction to roll back on %s' % host)
        _restore(executor, held, journal, ready, restore_ready, ready_timeout, sleep, clock)
        return journal.id
