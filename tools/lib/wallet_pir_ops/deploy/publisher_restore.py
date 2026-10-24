"""Recover History restart evidence captured by older deployment transactions.

New deployments leave activation-generated records to native publisher startup.
The preparation path below is retained for exercising old journals in recovery
tests; ordinary deployment never starts it. Existing journals keep this protocol:

Within an ordinary publisher-cutover/3 transaction, under its production lock:

1. Staging, predecessor serving. Pin the exact active completed History
   publication into a transaction-owned hardlink snapshot
   (``<publication_root>/.cutover-preparation/<id>/transparent_history/publication``,
   with the reviewed snapshot helper), run ``pir-publisher
   prepare-history-restore`` on that pin, writing
   ``/opt/pir-publisher/transactions/<id>/history-restore.json``, require the
   record to name the active publication's map and the active pointer to be
   unchanged. The pin is kept across the stop. Predecessor collection cannot
   alter the pinned inputs; any refusal leaves the predecessor serving.
2. Installing, predecessor stopped and its ownership locks released. Require
   the same active pointer and live map; only these small files are hashed.
   Every pinned file must still be the same inode with the same size in the
   live publication (the helper's ``same``); the retained pin keeps those
   inodes allocated, so a replaced file cannot reuse its number. Then remove
   the pin. Journal the target's prior state (absence, or its bytes saved beside the
   journal as ``history-restore.prior.json``) before atomically installing
   the record beside the publication. Drift of the pointer, map or any pinned
   artifact restores the predecessor through the ordinary transaction rollback
   and refuses with a retryable error.
3. Rollback. Refuse before any effect if the target holds neither the
   installed record nor its journaled prior state; otherwise restore the
   prior bytes or absence exactly and remove a retained pin.

The record only skips the restarted publisher's startup scan; it hashes every
body again in the background after serving starts.
"""
import json
from pathlib import PurePosixPath

from .engine import DeployError
from .publisher import ROOT_DIR, require, sha
from .publisher_warmup import SnapshotPreparation

RECORD = 'history-restore.json'
MAP = 'shards.json'
SECTION = 'transparent_history'
PIN = 'pin history restore snapshot'
PREPARE = 'prepare history restore'
RELEASE = 'remove history restore snapshot'
STAGED = [PIN, PREPARE]
INSTALL = 'install history restore record'
RESTORE = 'restore history restore record'
ROLLBACK_RELEASE = 'restore remove history restore snapshot'


def staged_effects(journal):
    """The exact staging effects a prepared record adds before the predecessor stop."""
    return STAGED if 'history_restore' in journal else []


class HistoryRestore:
    def __init__(self, owner):
        self.owner = owner
        history = owner.request['configuration'][SECTION]
        self.root = history['settings']['publication_root']

    def active(self):
        return self.owner.read(self.root + '/active.json')

    def digest(self, path):
        return self.owner.ex.sha256(self.owner.host, path)

    @property
    def cache(self):
        """The shared snapshot helper's per-transaction preparation directory parent."""
        return self.root + '/.cutover-preparation'

    def prepare(self, journal, binary, staged_config):
        """Verify a pin of the active publication beside the running predecessor."""
        raw = self.active()
        if raw is None:
            # A fresh installation has nothing to restore; it prepares in full.
            return
        active = json.loads(raw)
        directory = active.get('directory')
        require(isinstance(directory, str) and PurePosixPath(directory).parent == PurePosixPath(self.root)
                and PurePosixPath(directory).name not in ('', '.', '..')
                and not PurePosixPath(directory).name.startswith('.'),
                'active History publication is outside its publication root')
        require(isinstance(active.get('map_sha256'), str), 'active History publication names no map')
        raw_map = self.digest(directory + '/' + MAP)
        require(raw_map is not None, 'active History publication map is missing; the predecessor still serves')
        owner = self.owner
        work = self.cache + '/' + journal['id']
        evidence = journal['history_restore'] = {
            'active_sha256': sha(raw), 'directory': directory, 'map_sha256': active['map_sha256'],
            'raw_map_sha256': raw_map, 'work': work, 'pin': work + '/' + SECTION + '/publication',
            'record': ROOT_DIR + '/transactions/' + journal['id'] + '/' + RECORD,
            'target': directory + '/' + RECORD}
        # Ownership of the pin is journaled before it exists.
        owner.save(journal)

        def pin():
            pinned = SnapshotPreparation(owner.serving).pin_snapshot(journal, SECTION, directory)
            require(pinned == evidence['pin'], 'History restore snapshot is outside its transaction')
        owner.effect(journal, PIN, pin)
        require(self.digest(evidence['pin'] + '/' + MAP) == raw_map,
                'pinned History publication differs from the active one; the predecessor still serves')

        def run():
            code, output = owner.ex.run(owner.host, [binary, 'prepare-history-restore', '--config', staged_config,
                                                     '--publication', evidence['pin'],
                                                     '--output', evidence['record']], 7200)
            require(code == 0, 'History restore preparation failed; the predecessor still serves: '
                    + output[-1000:])
        owner.effect(journal, PREPARE, run)
        digest = self.digest(evidence['record'])
        require(digest is not None, 'History restore preparation wrote no record; the predecessor still serves')
        try:
            named = json.loads(owner.read(evidence['record'])).get('map_sha256')
        except (TypeError, ValueError, AttributeError):
            named = None
        require(named == evidence['map_sha256'],
                'History restore record names another publication; the predecessor still serves')
        require(self.unchanged(evidence),
                'History publication changed while its restore record was prepared; '
                'the predecessor still serves, retry the deploy')
        evidence['record_sha256'] = digest
        owner.save(journal)

    def unchanged(self, evidence):
        """The exact prepared publication is still active, comparing only small files."""
        return (sha(self.active()) == evidence['active_sha256']
                and self.digest(evidence['directory'] + '/' + MAP) == evidence['raw_map_sha256'])

    def release(self, journal, name):
        """Remove the transaction-owned pin; repeated removal is harmless."""
        evidence = journal.get('history_restore')
        if evidence is None or RELEASE in journal['completed'] or ROLLBACK_RELEASE in journal['completed']:
            return
        owner, cache = self.owner, self.cache
        require(evidence['work'] == cache + '/' + journal['id'],
                'History restore snapshot is outside its transaction')
        owner.effect(journal, name, lambda: owner.ex.remove_tree(owner.host, evidence['work'], cache))

    def install(self, journal):
        """After the predecessor stopped: install matching evidence or restore the predecessor."""
        evidence = journal.get('history_restore')
        if evidence is None:
            return
        owner = self.owner
        require(all(name in journal['completed'] for name in STAGED) and 'record_sha256' in evidence,
                'History restore preparation did not finish before the stop')
        if not self.unchanged(evidence):
            self.retry(journal, 'History publication advanced after the predecessor stopped')
        require(RELEASE not in journal['completed'], 'History restore snapshot was removed before its comparison')
        if not self.pinned(journal, evidence):
            self.retry(journal, 'History publication artifacts differ from their pin after the predecessor stopped')
        self.release(journal, RELEASE)
        require(self.digest(evidence['record']) == evidence['record_sha256'],
                'prepared History restore record changed outside its transaction')
        text = owner.read(evidence['record'])
        if 'prior' not in evidence:
            evidence['prior'] = self.preserve(journal, evidence['target'])
            owner.save(journal)

        def install():
            owner.write(evidence['target'], text)
            require(self.digest(evidence['target']) == evidence['record_sha256'],
                    'installed History restore record differs')
        owner.effect(journal, INSTALL, install)

    def pinned(self, journal, evidence):
        """Every pinned file is still the live publication's same inode and size; no body is read."""
        owner = self.owner
        script = SnapshotPreparation(owner.serving).prepare_script(journal)
        code, output = owner.ex.run(owner.host, ['python3', script, 'same', evidence['directory'], evidence['pin']],
                                    600)
        require(code == 0, 'could not compare the History publication with its pin: ' + output[-1000:])
        result = json.loads(output)
        require(type(result.get('same')) is bool and type(result.get('compared')) is int,
                'History pin comparison reported no result')
        evidence['pin_comparison'] = result
        owner.save(journal)
        return result['same'] and result['compared'] > 0

    def preserve(self, journal, target):
        """Save the target's prior bytes beside the journal, or record its absence."""
        owner = self.owner
        current = owner.read(target)
        digest = self.digest(target)
        require(sha(current) == digest, 'existing History restore record changed while it was preserved')
        if current is None:
            return {'sha256': None, 'backup': None}
        backup = ROOT_DIR + '/transactions/' + journal['id'] + '/' + RECORD.replace('.json', '.prior.json')
        owner.write(backup, current)
        require(self.digest(backup) == digest, 'preserved History restore record differs')
        return {'sha256': digest, 'backup': backup}

    def retry(self, journal, reason):
        """Restore the predecessor through the ordinary rollback, then refuse retryably."""
        owner = self.owner
        journal['retry'] = reason
        owner.save(journal)
        owner.restore_transaction()
        raise DeployError(reason + '; the predecessor was restored, retry the deploy')

    def guard(self, journal):
        """Before any rollback effect: the target is ours or its prior state."""
        evidence = journal.get('history_restore')
        if evidence is None or 'prior' not in evidence:
            return
        prior = evidence['prior']
        require(self.digest(evidence['target']) in (prior['sha256'], evidence['record_sha256']),
                'History restore record changed outside its transaction: ' + evidence['target'])
        if prior['backup'] is not None:
            require(self.digest(prior['backup']) == prior['sha256'],
                    'preserved History restore record changed outside its transaction')

    def rollback(self, journal):
        """Restore the prior record bytes or absence exactly, then drop a retained pin."""
        evidence = journal.get('history_restore')
        if evidence is None:
            return
        self.guard(journal)
        owner = self.owner
        prior = evidence.get('prior')
        if prior is not None and self.digest(evidence['target']) != prior['sha256']:
            def restore():
                if prior['backup'] is None:
                    owner.ex.remove(owner.host, evidence['target'])
                else:
                    owner.write(evidence['target'], owner.read(prior['backup']))
                require(self.digest(evidence['target']) == prior['sha256'],
                        'restored History restore record differs from its prior state')
            owner.effect(journal, RESTORE, restore)
        self.release(journal, ROLLBACK_RELEASE)
