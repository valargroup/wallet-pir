"""Offline candidate reports derived from immutable native execution evidence.

The trusted observer captures the installed binary identity, terminal child
result, stderr and resource samples. This module checks those retained bytes;
it neither runs a native program nor turns an operator's pass flag into proof.
Production execution and interrupted-owner recovery stay in the locked wrapper.
Certificate coverage comes from the publication manifests, not supplied counts.
"""
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import stat

from wallet_pir_ops import durable


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


C = module('report_candidate', Path(__file__).with_name('activity_candidate.py'))
CERTIFIER_SHA256 = '8fcffe86ce1cc631439026f9cce65cd503440637bea2f4ea7e8fff8d13a8a4f7'
CDF_SHA256 = '4c6e3f5145e3b99f225b9dcaa050be85f6f0ad1fb391acfde080fcd09dfc2266'
MAX_JSON = 16 * 1024 * 1024
CAPTURE_FILES = {'owner', 'result', 'native', 'stderr', 'health'}
ARTIFACT_CHECKS = {'load', 'coverage-contiguous', 'coverage-start', 'coverage-through',
                   'anchor-hash', 'tiers', 'map-sha256'}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate evidence JSON key')
        value[key] = item
    return value


def json_bytes(raw):
    require(len(raw) <= MAX_JSON, 'evidence JSON exceeds bound')
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def blob(reference, maximum=MAX_JSON):
    require(isinstance(reference, dict) and set(reference) == {'path', 'sha256'} and
            isinstance(reference['path'], str) and Path(reference['path']).is_absolute() and
            isinstance(reference['sha256'], str) and C.HEX.fullmatch(reference['sha256']),
            'invalid immutable evidence reference')
    path = Path(reference['path'])
    C.no_links(path)
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= maximum,
            'evidence must be independent bounded regular bytes')
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum and hashlib.sha256(raw).hexdigest() == reference['sha256'],
            'evidence checksum or size changed')
    return raw


def value(reference):
    return json_bytes(blob(reference))


def number(item):
    return type(item) in (int, float) and math.isfinite(item)


def capture(references, executable, publication):
    require(isinstance(references, dict) and set(references) == CAPTURE_FILES,
            'incomplete native capture')
    raw = {name: blob(ref) for name, ref in references.items()}
    owner, result, health = (json_bytes(raw[name]) for name in ('owner', 'result', 'health'))
    require(isinstance(owner, dict) and owner.get('native_source_sha') == C.SOURCE_SHA and
            owner.get('candidate_sha256') == C.identity() and
            owner.get('binary_sha256') == C.ARTIFACTS[executable] and
            owner.get('publication_sha256') == publication and type(owner.get('pid')) is int and
            owner['pid'] > 0 and number(owner.get('started_unix')), 'native owner identity differs')
    require(isinstance(result, dict) and result.get('status') == 'passed' and
            type(result.get('exit_code')) is int and result['exit_code'] == 0 and
            result.get('pid') == owner['pid'] and result.get('started_unix') == owner['started_unix'] and
            number(result.get('ended_unix')) and number(result.get('timeout_seconds')) and
            0 < result['ended_unix'] - owner['started_unix'] <= result['timeout_seconds'] <= 1800,
            'native result is not terminal success within its bound')
    require(isinstance(health, list) and len(health) >= 2, 'native resource coverage is missing')
    times = []
    for sample in health:
        require(isinstance(sample, dict) and number(sample.get('observed_unix')) and
                number(sample.get('memory_available')) and .2 <= sample['memory_available'] <= 1 and
                isinstance(sample.get('disk_available'), dict) and sample['disk_available'] and
                all(number(v) and .2 <= v <= 1 for v in sample['disk_available'].values()),
                'native resource floor or sample differs')
        times.append(sample['observed_unix'])
    require(times == sorted(times) and times[0] <= owner['started_unix'] and
            times[-1] >= result['ended_unix'] and
            all(b-a <= 10 for a, b in zip(times, times[1:])), 'native resource sampling has a gap')
    return json_bytes(raw['native'])


def base(gate, publication, evidence):
    return {'status': 'passed', 'gate': gate, 'native_source_sha': C.SOURCE_SHA,
            'candidate_sha256': C.identity(), 'publication_sha256': publication,
            'binaries': {name: C.ARTIFACTS[name] for name in C.GATES[gate]},
            'raw_evidence': evidence}


def artifact_report(mapping_ref, execution):
    mapping = value(mapping_ref)
    publication = mapping_ref['sha256']
    native = capture(execution, 'shard-verify', publication)
    require(native.get('schema') == 'transparent-shard-verify-v1' and
            native.get('tool_sha') == C.SOURCE_SHA and type(native.get('failures')) is int and
            native['failures'] == 0, 'artifact native output failed or has historical identity')
    checks = native.get('checks')
    require(isinstance(checks, list) and checks and all(isinstance(c, dict) and
            c.get('ok') is True and isinstance(c.get('check'), str) for c in checks),
            'artifact checks failed or missing')
    names = [c['check'] for c in checks]
    require(len(names) == len(set(names)) and ARTIFACT_CHECKS <= set(names),
            'artifact expectations were not all checked')
    rows = mapping.get('shards')
    require(isinstance(rows, list) and rows and mapping.get('start_height') == 0,
            'artifact publication does not cover genesis')
    summary = native.get('set')
    require(isinstance(summary, dict) and summary.get('map_file_sha256') == publication and
            summary.get('shards') == len(rows) and summary.get('start_height') == 0 and
            summary.get('through') == rows[-1]['end_height'] and
            summary.get('terminal_block_hash') == rows[-1]['terminal_block_hash'] and
            set(summary.get('geometries', [])) == {'archive-wide', 'recent-4k-8k'},
            'artifact coverage or publication identity differs')
    result = base('artifact-verification', publication, {'mapping': mapping_ref, 'execution': execution})
    result['verified'] = summary
    return C.verify_gate(result, 'artifact-verification', publication)


def segments(mapping_ref, manifests):
    mapping = value(mapping_ref)
    rows = mapping.get('shards')
    require(isinstance(rows, list) and rows and isinstance(manifests, dict) and
            set(manifests) == {r['manifest_digest'] for r in rows}, 'manifest coverage differs')
    expected = {}
    for row in rows:
        digest = row['manifest_digest']
        require(manifests[digest]['sha256'] == digest, 'manifest is not bound to publication')
        manifest = value(manifests[digest])
        require(manifest.get('schema') == 'transparent-shard-v11' and
                manifest.get('shard_id') == row['shard_id'] and manifest.get('geometry') == row['geometry'] and
                row['geometry'] in ('archive-wide', 'recent-4k-8k'), 'manifest identity differs')
        for table, key in (('directory', 'directory_segments'), ('pages', 'page_segments')):
            entries = manifest.get(key)
            require(isinstance(entries, list) and entries, 'manifest table segments missing')
            for index, item in enumerate(entries):
                identity = (row['shard_id'], table, index)
                require(identity not in expected and type(item.get('rows')) is int and item['rows'] > 0 and
                        type(item.get('row_bytes')) is int and item['row_bytes'] == 4096 and
                        isinstance(item.get('sha256'), str) and C.HEX.fullmatch(item['sha256']),
                        'invalid or duplicate table segment')
                expected[identity] = dict(item, geometry=row['geometry'], manifest_digest=digest)
    require(len(expected) == 180, 'candidate publication does not have all 180 setup bindings')
    return expected


def certifier(path):
    path = Path(path)
    C.no_links(path)
    cdf = path.resolve().parents[2]/'src/native_gaussian_cdf.txt'
    C.no_links(cdf)
    require(C.checksum(path) == CERTIFIER_SHA256 and
            C.checksum(cdf) == CDF_SHA256,
            'certificate evaluator or frozen sampler bytes differ')
    return module('retained_candidate_certifier', path)


def certificate_report(mapping_ref, manifests, executions, certifier_path):
    expected = segments(mapping_ref, manifests)
    publication = mapping_ref['sha256']
    require(isinstance(executions, list) and len(executions) == len(expected),
            'certificate execution coverage differs')
    evaluator = certifier(certifier_path)
    seen, bindings = set(), []
    for item in executions:
        require(isinstance(item, dict) and set(item) == {'shard_id', 'table', 'segment', 'execution'} and
                type(item['shard_id']) is int and type(item['segment']) is int and
                isinstance(item['table'], str), 'invalid certificate execution')
        key = item['shard_id'], item['table'], item['segment']
        require(key in expected and key not in seen, 'duplicate or foreign certificate segment')
        seen.add(key)
        target = expected[key]
        native = capture(item['execution'], 'examples/native_certificate', publication)
        require(native.get('database_sha256') == 'rows_sha256:'+target['sha256'] and
                native.get('product') == 'transparent-segment' and
                native.get('rows') == target['rows'] and
                isinstance(native.get('served_public_sha256'), str) and
                C.HEX.fullmatch(native['served_public_sha256']), 'certificate table or public setup identity differs')
        measured = evaluator.evaluate(native)
        bits = measured['actual_profile']['certified_failure_bits']
        floor = 83 if target['geometry'] == 'archive-wide' and item['table'] == 'pages' else 128
        require(type(bits) is int and bits >= floor, 'native certificate is below the required floor')
        bindings.append({'shard_id': key[0], 'table': key[1], 'segment': key[2],
                         'manifest_digest': target['manifest_digest'], 'geometry': target['geometry'],
                         'table_sha256': target['sha256'], 'public_sha256': native['served_public_sha256'],
                         'certificate': measured, 'required_failure_bits': floor})
    result = base('native-certificates', publication,
                  {'mapping': mapping_ref, 'manifests': manifests, 'executions': executions,
                   'certifier_sha256': CERTIFIER_SHA256, 'sampler_file_sha256': CDF_SHA256})
    result.update(floors=C.FLOORS, segments=len(bindings), setup_bindings=bindings,
                  limitations=['Installed warm worker and canonical setup agreement remains a separate cutover gate.'])
    return C.verify_gate(result, 'native-certificates', publication)


def write_report(path, report):
    """Create one independent immutable local report; never replace an attempt."""
    path = Path(path)
    C.no_links(path)
    with path.open('xb') as stream:
        import os
        os.fchmod(stream.fileno(), 0o400)
        stream.write(durable.canonical(report)+b'\n')
        stream.flush()
        os.fsync(stream.fileno())
