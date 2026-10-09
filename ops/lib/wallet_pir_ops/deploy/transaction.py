"""The runner-side transaction journal.

One JSON file per transaction under the state directory (default
`~/.local/state/wallet-pir-deploy`), written durably before every side effect
it describes, plus a `latest-<service>.json` pointer. A host record's `phase`
moves `pending -> backing-up -> installing -> restarting -> verifying ->
verified`; from `installing` on the host is *touched* and a rollback restores
it. Rollback moves a touched host through `restoring -> restored`. A
verification, which restarts no host, records in `rollback_target` the
transaction the pointer named before it (see `Deployer.rollback`).

The journal holds everything a rollback needs (previous unit text, previous
executable digest, readiness checks), so it works from this file alone even if
the inventory has changed since. Each host also keeps its own copy under
`/opt/<svc>/transactions/<id>/`.
"""
import json
import os
from pathlib import Path
import secrets
import time

from .. import durable

TOUCHED = ('installing', 'restarting', 'verifying', 'verified', 'restoring', 'restored', 'restore-failed')
FINAL = ('committed', 'failed', 'rolled-back')


def default_state_dir():
    configured = os.environ.get('WALLET_PIR_DEPLOY_STATE')
    return Path(configured) if configured else Path.home() / '.local/state/wallet-pir-deploy'


def new_id(service, sha):
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    return '%s-%s-%s-%s' % (service, stamp, sha[:12], secrets.token_hex(3))


class Journal:
    def __init__(self, path, data):
        self.path = Path(path)
        self.data = data

    @classmethod
    def create(cls, state_dir, service, sha, source, hosts, baseline, rollback_target=None):
        state_dir = Path(state_dir)
        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        identifier = new_id(service, sha)
        journal = cls(state_dir / (identifier + '.json'), {
            'id': identifier, 'service': service, 'binary_sha256': sha, 'source': source,
            'status': 'staging', 'created_unix': int(time.time()), 'hosts': hosts,
            'baseline_before': baseline, 'rollback_target': rollback_target, 'events': [],
        })
        journal.save()
        durable.atomic_json(state_dir / ('latest-%s.json' % service), {'id': identifier}, mode=0o600)
        return journal

    @classmethod
    def load(cls, state_dir, service, identifier=None):
        state_dir = Path(state_dir)
        if identifier is None:
            pointer = state_dir / ('latest-%s.json' % service)
            if not pointer.exists():
                return None
            identifier = json.loads(pointer.read_text())['id']
        path = state_dir / (identifier + '.json')
        if not path.exists():
            raise FileNotFoundError('no transaction %s in %s' % (identifier, state_dir))
        journal = cls(path, json.loads(path.read_text()))
        if journal.data['service'] != service:
            raise ValueError('transaction %s belongs to service %s' % (identifier, journal.data['service']))
        return journal

    @property
    def id(self):
        return self.data['id']

    @property
    def hosts(self):
        return self.data['hosts']

    @property
    def status(self):
        return self.data['status']

    @property
    def verification_only(self):
        """Whether the transaction restarts no host, so it changes none."""
        return all(record['action'] != 'restart' for record in self.hosts)

    def save(self):
        durable.atomic_json(self.path, self.data, mode=0o600)

    def event(self, message, **fields):
        self.data['events'].append({'unix': round(time.time(), 3), 'message': message, **fields})
        self.save()

    def set_status(self, status, **fields):
        self.data['status'] = status
        self.event('status ' + status, **fields)

    def set_phase(self, index, phase, **fields):
        record = self.hosts[index]
        record['phase'] = phase
        self.event('phase ' + phase, target=record['key'], **fields)

    def touched(self):
        """Indices of hosts a rollback must restore, in rollout order."""
        return [index for index, record in enumerate(self.hosts) if record['phase'] in TOUCHED]
