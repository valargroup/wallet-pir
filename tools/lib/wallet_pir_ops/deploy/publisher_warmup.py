"""Warm pinned public snapshots before stopping a production publisher."""
import json
from pathlib import Path
import shlex

from .publisher import require, sha

SCRIPT = Path(__file__).with_name('snapshot_warmer.py').read_text()


class SnapshotWarmup:
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
        previous = journal['plan'].get('previous_transaction')
        if previous is None:
            return
        path = '/opt/pir-publisher/transactions/' + previous + '/journal.json'
        raw = self.owner.read(path)
        require(raw is not None, 'previous publisher transaction is missing')
        prior = json.loads(raw)
        require(prior['id'] == previous, 'previous publisher transaction identity changed')
        record = prior.get('warm_snapshots', {}).get(section)
        if not record or record['binary_sha256'] != self.serving.release['binary_sha256']:
            return
        expected = root + '/.cutover-preparation/' + previous + '/' + section + '/publication'
        require(record['pin'] == expected, 'previous preparation cache is outside its transaction')
        code, output = self.owner.ex.run(self.owner.host, ['python3', script, 'reuse', expected, pin], 120)
        require(code == 0, 'could not reuse completed preparation cache: ' + output[-1000:])

    def stage(self, journal):
        owner = self.owner
        script = self.prepare_script(journal)
        try:
            for section, snapshot in journal['production_snapshots'].items():
                settings = owner.request['configuration'][section]['settings']
                root = settings['publication_root' if section == 'transparent_history' else 'root']
                work = root + '/.cutover-preparation/' + journal['id'] + '/' + section
                pin = work + '/publication'
                require(snapshot['pin'] == pin, 'snapshot pin is outside its transaction')
                map_name = 'shards.json' if section == 'transparent_history' else 'txid-shards.json'
                require(owner.ex.sha256(owner.host, pin + '/' + map_name) == snapshot['raw_map_sha256'],
                        'pinned production snapshot map changed')
                self.reuse_previous(journal, section, root, pin, script)
                resources = owner.resources()
                reserve = (resources['memory_total'] + 4) // 5
                memory = resources['memory_available'] - reserve
                require(memory > 0, 'snapshot validation has insufficient spare memory')
                unit = 'pir-snapshot-' + section.replace('_', '-') + '-' + journal['id'] + '.service'
                require(owner.ex.probe_unit(owner.host, unit)['load_state'] == 'not-found'
                        and owner.read('/etc/systemd/system/' + unit) is None,
                        'snapshot validation unit is already owned')
                binary = owner.serving.deployer.service.release_binary(
                    self.serving.release['binary_sha256'], 'pir-query-server')
                specification = {
                    'section': section, 'work': work, 'pin': pin, 'memory_bytes': memory,
                    'disk_bytes': owner.request['https']['publisher_budget_bytes'],
                    'map_sha256': snapshot['map_sha256'], 'binary': binary,
                    'binary_sha256': self.serving.release['binary_sha256'],
                }
                if section == 'transparent_history':
                    identity = snapshot['identity']
                    specification['identity'] = {
                        'shard_schema': identity['schema'], 'map_sha256': snapshot['map_sha256'],
                        **{key: identity[key] for key in ('network', 'genesis_hash', 'shards',
                                                       'start_height', 'covered_through')},
                        'recent_from_shard': identity['shards'],
                    }
                spec_path = work + '/specification.json'
                spec_text = json.dumps(specification, sort_keys=True) + '\n'
                text = ('[Unit]\nDescription=Warm immutable Transparent snapshot before cutover\n'
                        '[Service]\nType=simple\nUser=root\n'
                        'ExecStart=' + shlex.join(['/usr/bin/python3', script, 'warm', spec_path]) + '\n'
                        'MemoryMax=' + str(memory) + '\nMemorySwapMax=0\n'
                        'TimeoutStopSec=30\nKillMode=control-group\nRestart=no\n'
                        'NoNewPrivileges=true\nProtectSystem=strict\nProtectHome=true\nPrivateTmp=true\n'
                        'ReadWritePaths=' + json.dumps(work) + '\n')
                record = {**specification, 'unit': unit, 'unit_text': text, 'script': script,
                          'inputs': {script: sha(SCRIPT), spec_path: sha(spec_text)}}
                journal.setdefault('warm_snapshots', {})[section] = record
                owner.save(journal)
                self.serving.write(owner.host, spec_path, spec_text, 0o600)
                self.serving.write(owner.host, '/etc/systemd/system/' + unit, text, 0o644)
                owner.ex.systemctl(owner.host, 'daemon-reload')
                owner.ex.systemctl(owner.host, 'start', unit)
                deadline = owner.clock() + 960
                while True:
                    owner.lock.verify()
                    raw = owner.read(work + '/report.json')
                    report = json.loads(raw) if raw else {}
                    state = owner.ex.probe_unit(owner.host, unit)
                    progress = len(report.get('checks', []))
                    if progress != record.get('completed_groups', 0):
                        require(report.get('map_sha256') == snapshot['map_sha256']
                                and report.get('binary_sha256') == specification['binary_sha256'],
                                'snapshot warmup progress has another identity')
                        record['completed_groups'] = progress
                        owner.save(journal)
                    if report.get('complete') is True:
                        require(report.get('map_sha256') == snapshot['map_sha256']
                                and report.get('binary_sha256') == specification['binary_sha256'],
                                'snapshot warmup report has another identity')
                        expected = list(range(snapshot['identity']['shards'])) if section == 'transparent_history' else [None]
                        require([check['shard'] for check in report['checks']] == expected,
                                'snapshot warmup did not cover every current shard')
                        record['report'] = report
                        owner.save(journal)
                        break
                    if state['active_state'] == 'failed':
                        _, logs = owner.ex.run(owner.host, ['journalctl', '-u', unit, '-n', '20', '--no-pager'], 10)
                        raise RuntimeError('production snapshot warmer failed: ' + logs[-2000:])
                    require(owner.clock() < deadline, 'production snapshot warmup deadline exceeded')
                    owner.sleep(3)
                self.stop(record)
                owner.resources()
        finally:
            self.cleanup(journal)

    def seed(self, journal):
        owner = self.owner
        for section, record in journal.get('warm_snapshots', {}).items():
            settings = owner.request['configuration'][section]['settings']
            root = settings['publication_root' if section == 'transparent_history' else 'root']
            code, output = owner.ex.run(owner.host, ['python3', record['script'], 'seed',
                                       record['pin'], root + '/active.json', root], 120)
            require(code == 0, 'could not reuse verified publisher runtimes: ' + output[-1000:])
            record['seeded'] = json.loads(output)
            require(record['seeded'].get('linked_runtimes', 0) > 0,
                    'snapshot warmup left no reusable publisher runtimes')
            owner.save(journal)
