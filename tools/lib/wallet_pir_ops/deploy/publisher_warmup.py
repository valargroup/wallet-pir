"""Pin and reuse public snapshots; load runtimes in the real serving processes."""
import json
from pathlib import Path

from .publisher import require, sha

SCRIPT = Path(__file__).with_name('snapshot_warmer.py').read_text()


class SnapshotPreparation:
    def __init__(self, serving):
        self.serving = serving
        self.owner = serving.cutover

    def prepare_script(self, journal):
        require(journal['plan']['serving']['snapshot_warmer_sha256'] == sha(SCRIPT),
                'snapshot validation helper differs from the reviewed plan')
        script = '/opt/pir-publisher/transactions/' + journal['id'] + '/snapshot-warmer.py'
        existing = self.owner.read(script)
        require(existing in (None, SCRIPT), 'snapshot validation helper changed outside its transaction')
        if existing is None:
            self.serving.write(self.owner.host, script, SCRIPT, 0o600)
        return script

    def pin_snapshot(self, journal, section, source):
        owner = self.owner
        settings = owner.request['configuration'][section]['settings']
        root = settings['publication_root' if section == 'transparent_history' else 'root']
        work = root + '/.cutover-preparation/' + journal['id'] + '/' + section
        pin = work + '/publication'
        script = self.prepare_script(journal)
        owner.ex.mkdir(owner.host, work, 0o700)
        code, output = owner.ex.run(owner.host, ['python3', script, 'pin', source, pin], 120)
        require(code == 0, 'could not pin the production snapshot: ' + output[-1000:])
        return pin

    def guard(self, journal):
        owner = self.owner
        for record in journal.get('warm_snapshots', {}).values():
            state = owner.ex.probe_unit(owner.host, record['unit'])
            text = owner.read('/etc/systemd/system/' + record['unit'])
            require(text in (None, record['unit_text']),
                    'snapshot validation unit file changed outside its transaction')
            require(state['load_state'] == 'not-found' or
                    (state['fragment_text'] == record['unit_text'] and not state['drop_ins']
                     and (not state['main_pid'] or not state['need_daemon_reload'])),
                    'snapshot validation unit changed outside its transaction')
            for path, digest in record['inputs'].items():
                content = owner.read(path)
                require((content is None and state['load_state'] == 'not-found') or sha(content) == digest,
                        'snapshot validation input changed outside its transaction')

        for section, record in journal.get('prepared_snapshots', {}).items():
            snapshot = journal['production_snapshots'][section]
            require(record['pin'] == snapshot['pin'], 'prepared snapshot changed outside its transaction')
            for path, digest in record['inputs'].items():
                require(sha(owner.read(path)) == digest, 'snapshot cache helper changed outside its transaction')

    def stop(self, record):
        owner = self.owner
        state = owner.ex.probe_unit(owner.host, record['unit'])
        if state['load_state'] == 'not-found':
            path = '/etc/systemd/system/' + record['unit']
            text = owner.read(path)
            require(text in (None, record['unit_text']), 'snapshot validation unit file changed before cleanup')
            if text is not None:
                owner.ex.remove(owner.host, path)
                owner.ex.systemctl(owner.host, 'daemon-reload')
            return
        require(state['fragment_text'] == record['unit_text'] and not state['drop_ins']
                and (not state['main_pid'] or not state['need_daemon_reload']),
                'snapshot validation unit changed before cleanup')
        owner.ex.systemctl(owner.host, 'stop', record['unit'], '--no-block')
        deadline = owner.clock() + 60
        while True:
            owner.lock.verify()
            state = owner.ex.probe_unit(owner.host, record['unit'])
            if not state['main_pid'] and state['active_state'] in ('inactive', 'failed'):
                break
            require(owner.clock() < deadline, 'snapshot validator did not stop')
            owner.sleep(1)
        owner.ex.remove(owner.host, '/etc/systemd/system/' + record['unit'])
        owner.ex.systemctl(owner.host, 'daemon-reload')

    def cleanup(self, journal):
        self.guard(journal)
        for record in journal.get('warm_snapshots', {}).values():
            self.stop(record)

    def reuse_previous(self, journal, section, root, pin, script):
        source = pin
        previous = journal['plan'].get('previous_transaction')
        if previous is not None:
            path = '/opt/pir-publisher/transactions/' + previous + '/journal.json'
            raw = self.owner.read(path)
            require(raw is not None, 'previous publisher transaction is missing')
            prior = json.loads(raw)
            require(prior['id'] == previous, 'previous publisher transaction identity changed')
            record = (prior.get('prepared_snapshots', {}).get(section)
                      or prior.get('warm_snapshots', {}).get(section))
            if record:
                expected = root + '/.cutover-preparation/' + previous + '/' + section + '/publication'
                require(record['pin'] == expected, 'previous preparation cache is outside its transaction')
                source = expected
        code, output = self.owner.ex.run(self.owner.host, ['python3', script, 'reuse', source, pin], 120)
        require(code == 0, 'could not reuse completed preparation cache: ' + output[-1000:])
        reused = json.loads(output)
        require(type(reused.get('runtime_bytes')) is int and reused['runtime_bytes'] >= 0,
                'preparation cache has no retained-byte accounting')
        return reused['runtime_bytes']

    def stage(self, journal):
        """Reuse cache files; the real services load and validate them once."""
        owner = self.owner
        script = self.prepare_script(journal)
        for section, snapshot in journal['production_snapshots'].items():
            settings = owner.request['configuration'][section]['settings']
            root = settings['publication_root' if section == 'transparent_history' else 'root']
            work = root + '/.cutover-preparation/' + journal['id'] + '/' + section
            pin = work + '/publication'
            require(snapshot['pin'] == pin, 'snapshot pin is outside its transaction')
            map_name = 'shards.json' if section == 'transparent_history' else 'txid-shards.json'
            require(owner.ex.sha256(owner.host, pin + '/' + map_name) == snapshot['raw_map_sha256'],
                    'pinned production snapshot map changed')
            retained_bytes = self.reuse_previous(journal, section, root, pin, script)
            record = {'pin': pin, 'script': script, 'retained_runtime_bytes': retained_bytes,
                      'binary_sha256': self.serving.release['binary_sha256'],
                      'inputs': {script: sha(SCRIPT)}}
            journal.setdefault('prepared_snapshots', {})[section] = record
            owner.save(journal)
            owner.resources()
        self.guard(journal)

    def seed(self, journal):
        self.guard(journal)
        owner = self.owner
        for section, record in {**journal.get('warm_snapshots', {}),
                                **journal.get('prepared_snapshots', {})}.items():
            settings = owner.request['configuration'][section]['settings']
            root = settings['publication_root' if section == 'transparent_history' else 'root']
            code, output = owner.ex.run(owner.host, ['python3', record['script'], 'seed',
                                       record['pin'], root + '/active.json', root], 120)
            require(code == 0, 'could not reuse verified publisher runtimes: ' + output[-1000:])
            record['seeded'] = json.loads(output)
            owner.save(journal)
