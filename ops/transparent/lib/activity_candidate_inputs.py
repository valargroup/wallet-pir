"""Offline assembly of candidate cutover requests from reviewed immutable inputs.

All candidate gates must already exist. Historical samples, release receipt and
rollback reader keep their provenance. This produces inert local requests only;
remote staging and cutover still require the locked wrapper's own validation.
"""
import copy
import hashlib
import os
from pathlib import Path
import re

from wallet_pir_ops import durable

from activity_candidate_reports import blob, value, json_bytes, require, module

HERE = Path(__file__).parent
C = module('assemble_candidate', HERE/'activity_candidate.py')
I = module('assemble_input_stage', HERE/'activity_input_stage.py')
P = module('assemble_product', HERE/'activity_schema_product.py')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def request(source, receipt, attempt, files):
    result = {'version': 2, 'source_sha': source, 'candidate_sha': C.SOURCE_SHA,
              'release_result_sha256': receipt, 'attempt': attempt, 'files': files}
    require(len(durable.canonical(result)) <= I.MAX_REQUEST, 'assembled request exceeds bound')
    require(all(len(raw.encode()) <= (2 << 20 if name == 'fixture.json' else 256 << 10)
                for name, raw in files.items()), 'assembled file exceeds staging bound')
    return result


def sample(raw):
    item = json_bytes(raw)
    require(isinstance(item, dict) and item.get('schema') == 'transparent-script-sample-v1' and
            item.get('anchor_height') == 3500738 and item.get('tool_sha') == C.HISTORICAL_SHA and
            isinstance(item.get('clients'), list) and item['clients'] and
            all(isinstance(client, dict) and isinstance(client.get('scripts'), list) and client['scripts'] and
                type(client.get('journal_events')) is int and client['journal_events'] > 0 and
                isinstance(client.get('expected_digest'), str) and C.HEX.fullmatch(client['expected_digest'])
                for client in item['clients']), 'historical recovery sample identity/coverage differs')


def assemble(inputs):
    require(isinstance(inputs, dict) and set(inputs) == {'source_sha', 'release_result_sha256', 'attempt',
            'mapping', 'inventory', 'samples', 'gates', 'service_request', 'product_template'},
            'invalid candidate assembly inputs')
    source, receipt, attempt = (inputs[k] for k in ('source_sha', 'release_result_sha256', 'attempt'))
    require(isinstance(source, str) and re.fullmatch('[0-9a-f]{40}', source) and
            isinstance(receipt, str) and C.HEX.fullmatch(receipt) and type(attempt) is int and
            1 <= attempt <= 100, 'invalid assembly identity')
    publication = digest(blob(inputs['mapping']))
    require(isinstance(inputs['samples'], dict) and set(inputs['samples']) == {'v10', 'v11'} and
            isinstance(inputs['gates'], dict) and set(inputs['gates']) == I.CutoverPreparation.GATES,
            'missing recovery samples or candidate gates')
    files = {'inventory.json': blob(inputs['inventory']).decode()}
    inventory = json_bytes(files['inventory.json'].encode())
    require(isinstance(inventory, dict) and set(inventory) == {'hosts', 'ssh', 'lock', 'services'},
            'invalid pinned inventory')
    for kind, reference in inputs['samples'].items():
        raw = blob(reference, 256 << 10); sample(raw)
        files[kind+'-sample.json'] = raw.decode()
    for gate, reference in inputs['gates'].items():
        raw = blob(reference, 256 << 10)
        report = C.verify_gate(json_bytes(raw), gate, publication)
        if gate != 'comprehensive-ci':
            require(isinstance(value(report.get('raw_evidence')), dict), 'candidate raw evidence index is missing')
        if gate == 'native-certificates':
            require(report.get('segments') == 180, 'candidate certificate coverage is incomplete')
        files[gate+'.json'] = raw.decode()
    proof = request(source, receipt, attempt, files)
    proof_root = I.PREPARED/durable.digest(proof)
    def entry(name):
        return {'path': str(proof_root/name), 'sha256': digest(files[name].encode())}
    service = value(inputs['service_request'])
    require(isinstance(service, dict) and set(service) == {'version', 'source_sha', 'candidate_sha',
            'release_result_sha256', 'attempt', 'files'} and type(service['version']) is int and
            service['version'] == 2 and service['source_sha'] == source and service['candidate_sha'] == C.SOURCE_SHA and
            service['release_result_sha256'] == receipt and type(service['attempt']) is int and
            1 <= service['attempt'] <= 100 and isinstance(service['files'], dict) and
            set(service['files']) == I.ServicePreparation.FILES and
            all(isinstance(raw, str) for raw in service['files'].values()), 'service request is not reviewed candidate v2')
    request(source, receipt, service['attempt'], service['files'])
    service_root = I.PREPARED/durable.digest(service)
    spec = copy.deepcopy(value(inputs['product_template']))
    # Require an explicit candidate template. Never upgrade or search/replace a
    # historical product, rollback binary, unit command or arbitrary string.
    require(isinstance(spec, dict) and type(spec.get('version')) is int and spec['version'] == 2 and
            spec.get('candidate_sha') == C.SOURCE_SHA and spec.get('source_sha') == source and
            spec.get('publication_sha256') == publication, 'product template is not the explicit candidate')
    spec['inventory'] = entry('inventory.json')
    spec['gates'] = {gate: entry(gate+'.json') for gate in I.CutoverPreparation.GATES}
    for kind in ('v10', 'v11'):
        spec['routing']['recovery'][kind]['sample'] = entry(kind+'-sample.json')['path']
        spec['routing']['recovery'][kind]['sample_sha256'] = entry(kind+'-sample.json')['sha256']
    for key, name in (('fixture', 'fixture.json'), ('pins', 'pins.json'), ('policy', 'policy.json')):
        require(spec['load'][key] == {'path': str(service_root/name),
                    'sha256': digest(service['files'][name].encode())}, 'load input differs from reviewed service request')
    for host in spec['hosts']:
        if host['plan']['role'] == 'worker':
            for item in host['plan']['installs']:
                if item['target'].startswith('/usr/local/bin/'):
                    name = Path(item['target']).name
                    require(name in ('transparent-shard-server', 'shard-control') and
                            item['source'] == str(I.CANDIDATE_WORKERS/('release-'+C.SOURCE_SHA)/('.input-'+name)) and
                            item['sha256'] == C.ARTIFACTS[name], 'worker install is not staged candidate bytes')
        if host['plan']['role'] != 'coordinator':
            continue
        for item in host['plan']['installs']:
            if item['target'].startswith('/usr/local/bin/'):
                name = Path(item['target']).name
                require(name in C.ARTIFACTS and item['source'] == str(C.path(name)) and
                        item['sha256'] == C.ARTIFACTS[name], 'coordinator install is not candidate bytes')
            else:
                name = Path(item['target']).name
                require(name in service['files'] and item['source'] == str(service_root/name) and
                        item['sha256'] == digest(service['files'][name].encode()),
                        'coordinator configuration differs from reviewed service request')
    P.validate(spec)
    raw = durable.canonical(spec).decode()
    product = request(source, receipt, attempt, {'product.json': raw})
    return {'service': service, 'proof': proof, 'product': product}


def write_requests(out, requests):
    root = Path(out)
    require(root.is_absolute(), 'output must be an absolute new directory')
    C.no_links(root)
    root.mkdir(mode=0o700, exist_ok=False)
    # Retain partial outputs after any error; never overwrite or clean evidence.
    for name in ('service', 'proof', 'product'):
        raw = durable.canonical(requests[name])
        with (root/(name+'-request.json')).open('xb') as stream:
            os.fchmod(stream.fileno(), 0o400)
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        with (root/(name+'-request-sha256')).open('xb') as stream:
            os.fchmod(stream.fileno(), 0o400)
            stream.write(digest(raw).encode()); stream.flush(); os.fsync(stream.fileno())
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
