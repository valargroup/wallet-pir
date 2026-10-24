"""Finish monitoring an activated volume without changing its serving process.

The cache activation receipt stays pending while this installs the observer's
updated configuration through the deploy engine, then updates the central
collector's expected digest. Both operations reuse the caller's production
lock. A retry uses the same release and journals; a failure never undoes the
already serving cache. The collector's installed inventory preserves its mode,
dynamic roster, capacity mappings and unrelated hosts.
"""
import contextlib
import json
from pathlib import Path

from . import durable, monitoring, monitoring_certs, monitoring_roles
from .deploy import descriptors, transaction
from .deploy.engine import Deployer, DeployError
from .paths import ROOT


def adopt_volume(executor, lock, inventory_path, host, volume, state_dir, *, receipt, progress, ready_timeout=60):
    """Return verified monitoring identities, or leave the caller's receipt pending.

    Requires an installed sidecar, its reviewed deployment baseline, and one
    installed central APM. No serving unit or binary is changed. Missing
    prerequisites raise rather than treating a local inventory edit as coverage.
    """
    lock.verify()
    if monitoring.bind_volume(inventory_path, host, volume) == 'no-observer':
        raise DeployError('activated volume has no declared host observer')
    document = json.loads(Path(inventory_path).read_text())
    inventory = descriptors.load_inventory(inventory_path)
    centers = [entry['host'] for role in ('central', 'server')
               for entry in inventory.services.get('apm', {}).get('roles', {}).get(role, [])]
    if len(centers) != 1:
        raise DeployError('volume monitoring needs exactly one central APM')
    center = centers[0]
    services = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
    deployer = Deployer(services[monitoring.SERVICE], inventory, executor, state_dir,
                        lock=lambda: contextlib.nullcontext(lock), only=['observer@' + host])

    def save():
        retained = json.loads(Path(receipt).read_text())
        retained['monitoring'] = progress
        durable.atomic_json(receipt, retained, mode=0o600)

    source = 'cache-volume:' + Path(receipt).stem
    sidecar = transaction.Journal.load(state_dir, monitoring.SERVICE)
    if sidecar is not None and sidecar.data.get('source') == source:
        if sidecar.status in ('failed', 'rolled-back'):
            progress.pop('sidecar_transaction', None)
            save()
        else:
            progress.setdefault('sidecar_transaction', sidecar.id)
    central = transaction.Journal.load(state_dir, monitoring_certs._service(center, '/etc/pir-apm'))
    if central is not None and central.status not in ('committed', 'rolled-back'):
        if (not progress.get('revision')
                or (central.hosts[0].get('identity') or {}).get('revision') != progress['revision']):
            raise DeployError('another collector transaction needs reconciliation before volume adoption')
        if progress.get('sidecar_transaction'):
            deployer.rollback(progress['sidecar_transaction'])
        monitoring_certs.reconcile(executor, contextlib.nullcontext(lock), center, state_dir=state_dir,
                                   directory='/etc/pir-apm', restore_ready=monitoring_certs.collector_restore_ready,
                                   ready_timeout=ready_timeout)
        progress.pop('sidecar_transaction', None)
        save()
    elif sidecar is not None and sidecar.data.get('source') == source and sidecar.status not in transaction.FINAL:
        deployer.rollback(sidecar.id)
        progress.pop('sidecar_transaction', None)
        save()
    # An owned interruption may have left either unit stopped or its files
    # unfinished. Recover it first; unrelated stopped units remain prerequisites.
    if executor.probe_unit(center, monitoring_certs.COLLECTOR_UNIT)['active_state'] != 'active':
        raise DeployError('volume monitoring requires a running central APM')
    installed = executor.read(center, monitoring_roles.APM_OBSERVERS)
    if installed is None:
        raise DeployError('central observer inventory must be installed before volume adoption')
    collector = json.loads(installed)
    entries = [entry for entry in collector['hosts'] if entry['host_id'] == host]
    if len(entries) != 1:
        raise DeployError('activated volume host must already be enrolled exactly once')
    desired = next(entry for entry in monitoring.observers(document, mode=collector['mode'])['hosts']
                   if entry['host_id'] == host)
    state = executor.probe_unit(host, monitoring_certs.SIDECAR_UNIT)
    digest = state.get('exe_sha256')
    if state['active_state'] != 'active' or not digest:
        raise DeployError('volume monitoring requires an installed, running sidecar')
    # Preserve the first sidecar transaction across retries: its backup is the
    # old configuration the old collector can authenticate after a failed update.
    if not progress.get('sidecar_transaction'):
        sidecar = deployer.deploy(digest, source=source, skip_exact_check=True, verify_noop=True)
        progress['sidecar_transaction'] = sidecar.id
        save()
    lock.verify()
    status, body = executor.http_get(host, monitoring_certs.SIDECAR_HEALTH)
    report = json.loads(body)
    if (status != 200 or report.get('ready') is not True or report.get('binary_sha256') != digest
            or report.get('config_digest') != desired['config_digest']):
        raise DeployError('running sidecar has not adopted the volume configuration')
    entries[0].update(config_digest=desired['config_digest'], checks=desired['checks'])
    collector['revision'] = monitoring.observers_revision(collector)
    progress['revision'] = collector['revision']
    save()
    try:
        monitoring_certs.install(
            executor, contextlib.nullcontext(lock), center,
            {'observers.json': (json.dumps(collector, indent=2, sort_keys=True) + '\n').encode()},
            state_dir=state_dir, directory='/etc/pir-apm', unit=monitoring_certs.COLLECTOR_UNIT,
            ready=monitoring_certs.collector_ready(collector['revision']),
            restore_ready=monitoring_certs.collector_restore_ready,
            identity={'revision': collector['revision']}, ready_timeout=ready_timeout, mode=0o644)
    except Exception:
        lock.verify()
        deployer.rollback(progress['sidecar_transaction'])
        monitoring_certs.reconcile(executor, contextlib.nullcontext(lock), center, state_dir=state_dir,
                                   directory='/etc/pir-apm', restore_ready=monitoring_certs.collector_restore_ready,
                                   ready_timeout=ready_timeout)
        progress.pop('sidecar_transaction', None)
        save()
        raise
    lock.verify()
    return {'status': 'complete', 'config_digest': desired['config_digest'], 'revision': collector['revision']}
