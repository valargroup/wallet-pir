"""Candidate assembly keeps publication/rollback provenance and gates closed."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_candidate_inputs as M


class Assembly(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(); self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve(); self.counter = 0

    def ref(self, value):
        self.counter += 1; path = self.root/str(self.counter)
        raw = json.dumps(value).encode(); path.write_bytes(raw)
        return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}

    def inputs(self):
        source = 'a'*40; receipt = 'b'*64
        mapping = self.ref({'immutable_publication': True}); publication = mapping['sha256']
        sample = {'schema': 'transparent-script-sample-v1', 'anchor_height': 3500738,
                  'tool_sha': M.C.HISTORICAL_SHA, 'clients': [{'scripts': ['00'],
                  'journal_events': 1, 'expected_digest': 'c'*64}]}
        gates = {}
        for gate in M.C.GATES:
            report = {'status': 'passed', 'gate': gate, 'native_source_sha': M.C.SOURCE_SHA,
                      'candidate_sha256': M.C.identity(), 'publication_sha256': publication,
                      'binaries': {name: M.C.ARTIFACTS[name] for name in M.C.GATES[gate]}}
            if gate == 'comprehensive-ci':
                report.update(ci_run=M.C.CI_RUN, head_sha=M.C.SOURCE_SHA, conclusion='success', jobs=12, jobs_passed=12)
            if gate == 'native-certificates':
                report.update(floors=M.C.FLOORS, setup_bindings=[{}]*180, segments=180)
            if gate != 'comprehensive-ci':
                report['raw_evidence'] = self.ref({'fixture_only': True})
            gates[gate] = self.ref(report)
        service = M.request(source, receipt, 1, {name: '{}' for name in M.I.ServicePreparation.FILES})
        service_root = M.I.PREPARED/M.durable.digest(service)
        spec = {'version': 2, 'source_sha': source, 'candidate_sha': M.C.SOURCE_SHA,
                'publication_sha256': publication, 'hosts': [], 'inventory': {}, 'gates': {},
                'routing': {'recovery': {kind: {'binary': ('historical-reader' if kind == 'v10' else
                    str(M.C.path('transparent-loadtest'))), 'binary_sha256': ('d'*64 if kind == 'v10' else
                    M.C.ARTIFACTS['transparent-loadtest'])} for kind in ('v10', 'v11')}},
                'load': {key: {'path': str(service_root/name), 'sha256': M.digest(service['files'][name].encode())}
                         for key, name in (('fixture', 'fixture.json'), ('pins', 'pins.json'), ('policy', 'policy.json'))}}
        return {'source_sha': source, 'release_result_sha256': receipt, 'attempt': 1, 'mapping': mapping,
                'inventory': self.ref({'hosts': {}, 'ssh': {}, 'lock': {}, 'services': {}}),
                'samples': {kind: self.ref(sample) for kind in ('v10', 'v11')}, 'gates': gates,
                'service_request': self.ref(service), 'product_template': self.ref(spec)}

    def assemble(self, inputs):
        # Native fleet validators have their own tests. This fixture isolates
        # request assembly; it is deliberately not a valid deployment fleet.
        with patch.object(M.P, 'validate', side_effect=lambda spec: spec) as validate:
            output = M.assemble(inputs); validate.assert_called_once()
        return output

    def change(self, inputs, key, mutation):
        item = M.value(inputs[key]); mutation(item); inputs[key] = self.ref(item)

    def test_digests_paths_and_historical_recovery_are_preserved(self):
        inputs = self.inputs(); before = copy.deepcopy(inputs)
        result = self.assemble(inputs)
        self.assertEqual(inputs, before)
        self.assertEqual(result['service'], M.value(inputs['service_request']))
        self.assertTrue(all(v['version'] == 2 for v in result.values()))
        spec = json.loads(result['product']['files']['product.json'])
        original = M.value(inputs['product_template'])
        for kind in ('v10', 'v11'):
            for field in ('binary', 'binary_sha256'):
                self.assertEqual(spec['routing']['recovery'][kind][field], original['routing']['recovery'][kind][field])
        self.assertEqual(spec['inventory']['path'], str(M.I.PREPARED/M.durable.digest(result['proof'])/'inventory.json'))
        self.assertEqual(spec['inventory']['sha256'], inputs['inventory']['sha256'])
        self.assertEqual(result['proof']['files']['v11-sample.json'], M.blob(inputs['samples']['v11']).decode())

    def test_missing_failed_and_historical_gates_refuse_before_output(self):
        for mutation in (lambda v: v.update(status='pending'),
                         lambda v: v.update(native_source_sha=M.C.HISTORICAL_SHA),
                         lambda v: v.update(publication_sha256='0'*64)):
            inputs = self.inputs(); gate = 'independent-chain-oracle'
            report = M.value(inputs['gates'][gate]); mutation(report); inputs['gates'][gate] = self.ref(report)
            with self.assertRaises(ValueError): self.assemble(inputs)
        inputs = self.inputs(); inputs['gates'].pop('artifact-verification')
        with self.assertRaises(ValueError): self.assemble(inputs)

    def test_no_historical_template_upgrade_or_foreign_service_inputs(self):
        for key, mutation in (('product_template', lambda v: v.update(version=1)),
                              ('product_template', lambda v: v.update(source_sha='f'*40)),
                              ('service_request', lambda v: v.update(candidate_sha=M.C.HISTORICAL_SHA)),
                              ('product_template', lambda v: v['load']['pins'].update(sha256='0'*64))):
            inputs = self.inputs(); self.change(inputs, key, mutation)
            with self.assertRaises(ValueError): self.assemble(inputs)

    def test_samples_cannot_be_relabelled_and_tampered_bytes_refuse(self):
        inputs = self.inputs(); sample = M.value(inputs['samples']['v11']); sample['tool_sha'] = M.C.SOURCE_SHA
        inputs['samples']['v11'] = self.ref(sample)
        with self.assertRaises(ValueError): self.assemble(inputs)
        inputs = self.inputs(); Path(inputs['mapping']['path']).write_text('{}')
        with self.assertRaises(ValueError): self.assemble(inputs)

    def test_foreign_worker_and_coordinator_installs_refuse(self):
        for role in ('worker', 'coordinator'):
            inputs = self.inputs()
            self.change(inputs, 'product_template', lambda spec: spec['hosts'].append({'plan': {
                'role': role, 'installs': [{'target': '/usr/local/bin/transparent-shard-server',
                'source': '/old/binary', 'sha256': M.C.ARTIFACTS['transparent-shard-server']}]}}))
            with self.assertRaises(ValueError): self.assemble(inputs)

    def test_complete_five_host_template_passes_real_product_validators(self):
        inputs = self.inputs(); source = inputs['source_sha']; publication = inputs['mapping']['sha256']
        service = M.value(inputs['service_request']); service_root = M.I.PREPARED/M.durable.digest(service)
        spec = M.value(inputs['product_template']); spec['recent_from'] = 3289805
        spec['assignment'] = {'path': '/retained/assignment.json', 'sha256': 'e'*64}
        spec['load']['binary'] = {'path': str(M.C.path('examples/rate-query')),
                                  'sha256': M.C.ARTIFACTS['examples/rate-query']}
        for index, role in enumerate(('coordinator', 'worker', 'worker', 'worker', 'router')):
            h = M.P.H; required = set(h.required_files(role)); optional = set()
            optional.update(p+'.d' for p in h.unit_paths(h.UNITS[role]))
            if role == 'coordinator':
                optional.add('/usr/local/bin/shard-control'); required.add('/opt/transparent-5qps-fixture')
            if role == 'worker':
                optional.add('/opt/transparent-publisher/active.invalid.json')
                optional.update(('/usr/local/lib/transparent-pir/storage-policy.py',
                                 '/usr/local/lib/transparent-pir/headless-console.py'))
            if role == 'router':
                optional.add('/etc/systemd/system/caddy.service')
                required.add('/usr/lib/systemd/system/caddy.service')
                optional.add('/usr/lib/systemd/system/caddy.service.d')
            installs = []
            for target in sorted(h.install_targets(role)):
                if role == 'router': continue
                name = Path(target).name
                if target.startswith('/usr/local/bin/'):
                    path = (M.C.path(name) if role == 'coordinator' else
                            M.I.CANDIDATE_WORKERS/('release-'+M.C.SOURCE_SHA)/('.input-'+name))
                    checksum = M.C.ARTIFACTS[name]
                elif role == 'coordinator':
                    path = service_root/name; checksum = M.digest(service['files'][name].encode())
                else:
                    path = Path('/retained/worker.service'); checksum = 'e'*64
                installs.append({'target': target, 'source': str(path), 'sha256': checksum,
                                 'mode': 0o755 if target.startswith('/usr/local/bin/') else
                                         0o644 if target.endswith('.service') else 0o600})
            worker_root = '/srv/transparent-pir/v11/publications/'+publication
            worker = {'id': 'worker-'+str(index), 'directory': worker_root, 'assignment': worker_root+'/assignment.json',
                      'assignment_sha256': 'e'*64, 'map_sha256': 'f'*64, 'map_file_sha256': publication,
                      'binary_sha256': M.C.ARTIFACTS['transparent-shard-server']} if role == 'worker' else None
            plan = {'version': 1, 'role': role, 'machine_id': format(index+1, '032x'), 'source_sha': source,
                    'transaction': '{transaction}', 'baseline_root': M.P.T.ROOT+'{transaction}',
                    'baseline': {'version': 1, 'files': [{'path': p, 'required': True} for p in sorted(required)] +
                                 [{'path': p, 'required': False} for p in sorted(optional-required)],
                                 'retained': [{'path': '/retained/publication',
                                     'sentinel': '/retained/publication/shards.json', 'sha256': 'e'*64}]},
                    'installs': installs, 'worker': worker}
            spec['hosts'].append({'host': role+'-'+str(index), 'plan': plan})
        routing = spec['routing']; routing.update(version=1, source_sha=source, machine_id=format(1,'032x'),
            transaction='{transaction}', coordinator_baseline=M.P.T.ROOT+'{transaction}',
            old_fleet={'path': '/opt/transparent-publisher/fleet.json', 'sha256': 'e'*64},
            new_fleet={'path': '/opt/transparent-publisher/v11/fleet.json', 'sha256': 'e'*64},
            original_coordinator_sha256='e'*64, private_router='10.142.0.11:8093')
        routing['recovery']['v10']['binary'] = M.P.R.READER
        inputs['product_template'] = self.ref(spec)
        output = M.assemble(inputs)
        self.assertEqual(len(json.loads(output['product']['files']['product.json'])['hosts']), 5)

    def test_actual_fleet_validator_is_required(self):
        with self.assertRaisesRegex(ValueError, 'invalid product specification'):
            M.assemble(self.inputs())

    def test_raw_index_and_all_180_certificates_are_required(self):
        for mutation in (lambda v: v.pop('raw_evidence'), lambda v: v.update(segments=179, setup_bindings=[{}]*179)):
            inputs = self.inputs(); report = M.value(inputs['gates']['native-certificates'])
            mutation(report); inputs['gates']['native-certificates'] = self.ref(report)
            with self.assertRaises(ValueError): self.assemble(inputs)

    def test_bound_and_output_no_overwrite(self):
        inputs = self.inputs(); result = self.assemble(inputs)
        out = self.root/'out'; M.write_requests(str(out), result)
        for name, request in result.items():
            raw = (out/(name+'-request.json')).read_bytes()
            self.assertEqual(M.json_bytes(raw), request)
            self.assertEqual((out/(name+'-request-sha256')).read_text(), M.digest(raw))
            self.assertEqual((out/(name+'-request.json')).stat().st_mode & 0o777, 0o400)
        with self.assertRaises(FileExistsError): M.write_requests(str(out), result)
        with self.assertRaises(ValueError): M.request('a'*40, 'b'*64, 1, {'gate.json': 'x'*(256*1024+1)})


if __name__ == '__main__':
    unittest.main()
