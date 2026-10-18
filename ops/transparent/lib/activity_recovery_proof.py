"""Brief real HTTP/SQLite recovery gate, for candidate and coherent v10 rollback.

The frozen reference client performs PIR retrieval and exact sample comparison.
After it exits this independently opens the committed SQLite stores, checks their
encoded events/metadata/provenance and trusted anchor, and retains every native
attempt/report. Extraction correctness is a separate raw-chain oracle gate.
"""
from contextlib import closing
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import time

from wallet_pir_ops import inherited_lock

SPEC = importlib.util.spec_from_file_location('recovery_event_check', Path(__file__).parents[1]/'scripts/activity-reopen-check.py')
E = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(E)
BASELINE_SPEC = importlib.util.spec_from_file_location('recovery_durable', Path(__file__).with_name('activity_schema_baseline.py'))
B = importlib.util.module_from_spec(BASELINE_SPEC)
BASELINE_SPEC.loader.exec_module(B)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def event(raw, schema):
    if schema == 'transparent-shard-v11':
        require(__debug__, 'event checker requires assertions enabled')
        return E.decode(raw)
    require(schema == 'transparent-shard-v10' and len(raw) == 87, 'legacy event must have unavailable metadata')
    kind, coinbase = raw[0] & 1, bool(raw[0] & 2)
    require(raw[0] & ~3 == 0 and not (kind and coinbase), 'invalid legacy event flags')
    return dict(kind=kind, height=int.from_bytes(raw[3:7], 'little'), position=int.from_bytes(raw[1:3], 'little'),
                txid=raw[15:47].hex(), index=int.from_bytes(raw[47:51], 'little'),
                parent=raw[51:83].hex(), parent_index=int.from_bytes(raw[83:87], 'little'),
                fee=None, input_count=None, shielded=None, raw=raw)


def inspect_store(path, sample, schema, *, deadline=None):
    # Retained-evidence reinspection may never create WAL/SHM state. The caller
    # verifies immutable file bytes before and after this read.
    if deadline is not None:
        require(time.monotonic()<deadline, 'retained store inspection deadline exceeded')
        require(all(not Path(str(path)+suffix).exists() for suffix in ('-wal','-shm','-journal')),
                'retained store has SQLite sidecars')
    uri=path.resolve().as_uri()+'?mode=ro'+('&immutable=1' if deadline is not None else '')
    with closing(sqlite3.connect(uri, uri=True, timeout=1 if deadline is not None else 5)) as db:
        if deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic()>=deadline), 1000)
        require(db.execute('PRAGMA integrity_check').fetchone() == ('ok',), 'reopened SQLite integrity failure')
        meta = dict(db.execute('SELECT key,value FROM wallet_meta'))
        require(meta.get('schema_version') == '4', 'recovery reader fence is not the current store version')
        identity = json.loads(meta.get('set_identity', '{}'))
        require(identity.get('shard_schema') == schema and identity.get('genesis_hash') == sample['genesis_hash'],
                'SQLite publication lineage/schema disagrees')
        anchor = json.loads(meta.get('anchor', 'null'))
        require(isinstance(anchor, dict) and anchor.get('height') == sample['anchor_height'] and
                anchor.get('hash') == sample['anchor_hash'], 'wallet has not committed the independently accepted anchor')
        require(db.execute('SELECT COUNT(*) FROM pending_work').fetchone()[0] == 0, 'recovery still has pending pages')
        scripts = list(db.execute('SELECT hex(script),required_from FROM scripts'))
        selection = frozenset(s.lower() for s, _ in scripts)
        candidates = [c for c in sample['clients'] if frozenset(c['scripts']) == selection and
                      all(start == c['required_from'] for _, start in scripts)]
        require(len(candidates) == 1, 'reopened store does not identify one reviewed sample')
        expected = candidates[0]
        events = []
        for table in ('receives', 'spends'):
            for raw, script, height, revision in db.execute('SELECT event,hex(script),height,revision_digest FROM '+table):
                if deadline is not None:
                    require(time.monotonic()<deadline and len(events)<1000000, 'retained store inspection exceeds bound')
                decoded = event(bytes(raw), schema)
                require(script.lower() in selection and decoded['height'] == height and
                        isinstance(revision, str) and re.fullmatch('[0-9a-f]{64}', revision), 'event/source attribution disagrees')
                if expected['required_from'] <= height <= sample['anchor_height']:
                    events.append(decoded)
        events.sort(key=lambda e: (e['height'], e['position'], e['kind'], e['txid'], e['index'], e['parent'], e['parent_index']))
        require(len({e['raw'] for e in events}) == len(events), 'duplicate recovered effects')
        digest = hashlib.sha256(b''.join(e['raw'] for e in events)).hexdigest()
        require(events and len(events) == expected['journal_events'] and digest == expected['expected_digest'],
                'nonempty reopened recovery differs from exact sample')
        metadata = {}
        for e in events:
            facts = (e['height'], e['fee'], e['input_count'], e['shielded'])
            require(e['txid'] not in metadata or metadata[e['txid']] == facts, 'transaction metadata contradiction after restart')
            metadata[e['txid']] = facts
        for script, start in scripts:
            covered = start
            for first, last in db.execute('SELECT start_height,end_height FROM coverage WHERE hex(script)=? ORDER BY start_height', (script,)):
                if first <= covered:
                    covered = max(covered, last+1)
            require(covered > sample['anchor_height'], 'reopened wallet coverage is incomplete')
    return {'database': str(path), 'events': len(events), 'transactions': len(metadata), 'digest': digest,
            'schema': schema, 'trusted_anchor': anchor, 'metadata_available': schema == 'transparent-shard-v11'}


def run(binary, binary_sha256, sample_path, sample_sha256, schema, shard_url, filter_url, root, source_sha):
    require(checksum(binary) == binary_sha256 and checksum(sample_path) == sample_sha256, 'recovery inputs changed')
    sample = json.loads(Path(sample_path).read_text())
    require(sample.get('anchor_hash') and sample['clients'] and
            all(c['journal_events'] > 0 for c in sample['clients']), 'brief recovery sample must be nonempty')
    # A dedicated small reviewed sample avoids using a sample-count shortcut for
    # the required sustained qualification runs. This is a cutover correctness gate.
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    report = root/'native.json'
    command = [str(binary), '--shard-url', shard_url, '--filter-url', filter_url, '--sample', str(sample_path),
               '--steps', '1', '--step-duration', '30s', '--min-completed-per-class', '1',
               '--max-error-rate', '0', '--max-503-rate', '0', '--http-attempts', '1', '--timeout', '60s',
               '--store-dir', str(root/'stores'), '--retain-stores', '--json-out', str(report),
               '--run-id', root.name, '--source-sha', source_sha]
    owner = {'source_sha': source_sha, 'binary_sha256': binary_sha256, 'sample_sha256': sample_sha256,
             'schema': schema, 'pid': os.getpid(), 'started': time.time(), 'command': command}
    B.atomic(root/'owner.json', (json.dumps(owner, indent=2)+'\n').encode())
    result = {'status': 'failed', 'schema': schema}
    try:
        with (root/'native.log').open('xb') as log:
            os.chmod(log.name, 0o600)
            native = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=120,
                                    **inherited_lock.options())
        result['exit_code'] = native.returncode
        require(native.returncode == 0, 'native recovery failed; retain all attempts')
        data = json.loads(report.read_text())
        require(data.get('in_progress') is False and data.get('stop_reason') is None and
                data.get('source_sha') == source_sha and data.get('set', {}).get('schema') == schema and
                data['set'].get('genesis_hash') == sample['genesis_hash'] and
                data['set'].get('target_height') == sample['anchor_height'] and
                data['set'].get('target_hash') == sample['anchor_hash'] and
                data.get('sample', {}).get('sha256') == sample_sha256, 'native recovery report is unfinished or foreign')
        steps = data.get('steps', [])
        require(len(steps) == 1 and steps[0]['syncs'] > 0 and steps[0]['syncs'] == steps[0]['completed'] == steps[0]['exact'] and
                steps[0]['failed'] == 0, 'native recovery has failed/inexact/incomplete attempts')
        require(all(not c['incomplete_by_reason'] for c in steps[0]['classes'].values()),
                'unresolved effects must not be treated as trusted complete recovery')
        require(set(steps[0]['classes']) == {c['class'] for c in sample['clients']} and
                all(c['n'] > 0 and c['n'] == c['completed'] == c['exact'] and c['failed'] == 0
                    for c in steps[0]['classes'].values()), 'a reviewed recovery class was missing or incomplete')
        stores = sorted((root/'stores').rglob('*.sqlite'))
        require(stores, 'native recovery retained no SQLite stores')
        checked = [inspect_store(p, sample, schema) for p in stores]
        result.update(status='passed', observations=checked, native_report_sha256=checksum(report),
                      sample_sha256=sample_sha256, binary_sha256=binary_sha256, source_sha=source_sha)
        return result
    except BaseException as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        result['finished'] = time.time()
        B.atomic(root/'result.json', (json.dumps(result, indent=2)+'\n').encode())
