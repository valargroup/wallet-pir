"""Prepare History restart evidence while the predecessor publisher still serves.

The staged publisher fully verifies the active completed History publication
and writes its restore record into the transaction before any stop. After the
predecessor stops, the record is installed only if the active publication is
still exactly the one verified; otherwise the predecessor is started again and
the deploy refuses with a retryable error. Nothing is hashed while stopped.
"""
import json

from .engine import DeployError
from .publisher import LOAD, ROOT_DIR, UNIT, require, sha

RECORD = 'history-restore.json'
PREPARE = 'prepare history restore'
INSTALL = 'install history restore record'


class HistoryRestore:
    def __init__(self, owner):
        self.owner = owner
        history = owner.request['configuration']['transparent_history']
        self.root = history['settings']['publication_root']
        self.enabled = history.get('enabled', True) and history['settings'].get('distribute_prepared') is True

    def active(self):
        return self.owner.read(self.root + '/active.json')

    def prepare(self, journal, binary, staged_config):
        """Verify the active publication beside the running predecessor."""
        if not self.enabled:
            return
        raw = self.active()
        if raw is None:
            # A fresh installation has nothing to restore; it prepares in full.
            return
        directory = json.loads(raw)['directory']
        require(isinstance(directory, str) and directory.startswith(self.root + '/')
                and '/..' not in directory and '/' not in directory[len(self.root) + 1:],
                'active History publication is outside its publication root')
        record = ROOT_DIR + '/transactions/' + journal['id'] + '/' + RECORD
        journal['history_restore'] = {'active_sha256': sha(raw), 'directory': directory,
                                      'record': record, 'target': directory + '/' + RECORD}
        owner = self.owner

        def run():
            code, output = owner.ex.run(owner.host, [binary, 'prepare-history-restore', '--config', staged_config,
                                                     '--publication', directory, '--output', record], 7200)
            require(code == 0, 'History restore preparation failed; the predecessor still serves: '
                    + output[-1000:])
        owner.effect(journal, PREPARE, run)
        digest = owner.ex.sha256(owner.host, record)
        require(digest is not None, 'History restore preparation wrote no record')
        require(sha(self.active()) == journal['history_restore']['active_sha256'],
                'History publication changed while its restore record was prepared; '
                'the predecessor still serves, retry the deploy')
        journal['history_restore']['record_sha256'] = digest
        owner.save(journal)

    def install(self, journal):
        """After the predecessor stopped: install matching evidence or restore it."""
        evidence = journal.get('history_restore')
        if evidence is None:
            return
        owner = self.owner
        require(PREPARE in journal['completed'] and 'record_sha256' in evidence,
                'History restore preparation did not finish before the stop')
        if sha(self.active()) != evidence['active_sha256']:
            self.restore_predecessor(journal)
        require(owner.ex.sha256(owner.host, evidence['record']) == evidence['record_sha256'],
                'prepared History restore record changed outside its transaction')
        text = owner.read(evidence['record'])

        def install():
            owner.write(evidence['target'], text)
            require(owner.ex.sha256(owner.host, evidence['target']) == evidence['record_sha256'],
                    'installed History restore record differs')
        owner.effect(journal, INSTALL, install)

    def restore_predecessor(self, journal):
        owner = self.owner
        owner.save(journal, 'restoring')
        owner.effect(journal, 'restore start ' + UNIT, lambda: owner.ex.systemctl(owner.host, 'start', UNIT))
        owner.ready(journal['plan']['units'][UNIT]['active_state'] == 'active')
        if 'pause continuous load' in journal['completed']:
            owner.effect(journal, 'restore continuous load', lambda: owner.ex.systemctl(owner.host, 'start', LOAD))
        journal['retry'] = 'History publication advanced after the predecessor stopped'
        owner.save(journal, 'rolled-back')
        raise DeployError('History publication advanced after the predecessor stopped; '
                          'the predecessor was restarted, retry the deploy')

    def rollback(self, journal):
        """Remove evidence this transaction installed; the predecessor ignores it."""
        evidence = journal.get('history_restore')
        if evidence is None or INSTALL not in journal['completed']:
            return
        owner = self.owner
        current = owner.ex.sha256(owner.host, evidence['target'])
        if current is None:
            return
        require(current == evidence['record_sha256'], 'History restore record changed outside its transaction')
        owner.effect(journal, 'restore remove history restore record',
                     lambda: owner.ex.remove(owner.host, evidence['target']))
