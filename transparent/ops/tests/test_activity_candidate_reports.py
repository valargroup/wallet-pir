"""Reject incomplete, historical and tampered raw candidate qualifications."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]/'ops/lib'))
spec = importlib.util.spec_from_file_location('reports_test', HERE.parent/'lib/activity_candidate_reports.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


class Reports(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve()
        self.counter = 0

    def raw(self, raw):
        self.counter += 1
        path = self.root/str(self.counter)
        path.write_bytes(raw)
        return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}

    def ref(self, value):
        return self.raw(json.dumps(value).encode())

    def capture(self, native, executable, publication):
        return {'native': self.ref(native), 'stderr': self.raw(b'native output\n'),
                'owner': self.ref({'pid': 42, 'native_source_sha': M.C.SOURCE_SHA,
                    'candidate_sha256': M.C.identity(), 'binary_sha256': M.C.ARTIFACTS[executable],
                    'publication_sha256': publication, 'started_unix': 10}),
                'result': self.ref({'pid': 42, 'status': 'passed', 'exit_code': 0,
                    'started_unix': 10, 'ended_unix': 15, 'timeout_seconds': 20}),
                'health': self.ref([{'observed_unix': t, 'memory_available': .3,
                    'disk_available': {'/': .4}} for t in (9, 16)])}

    def replace(self, refs, key, mutation):
        v = M.value(refs[key]); mutation(v); refs[key] = self.ref(v)

    def artifact(self):
        mapping = self.ref({'start_height': 0, 'shards': [
            {'shard_id': 0, 'end_height': 100, 'terminal_block_hash': 'a'*64}]})
        native = {'schema': 'transparent-shard-verify-v1', 'tool_sha': M.C.SOURCE_SHA,
                  'failures': 0, 'checks': [{'check': name, 'ok': True} for name in sorted(M.ARTIFACT_CHECKS)],
                  'set': {'map_file_sha256': mapping['sha256'], 'shards': 1, 'start_height': 0,
                          'through': 100, 'terminal_block_hash': 'a'*64,
                          'geometries': ['archive-wide', 'recent-4k-8k']}}
        execution = self.capture(native, 'shard-verify', mapping['sha256'])
        return mapping, execution

    def certificates(self):
        rows, manifests, executions = [], {}, []
        for shard in range(90):
            geometry = 'archive-wide' if shard < 77 else 'recent-4k-8k'
            manifest = self.ref({'schema': 'transparent-shard-v11', 'shard_id': shard,
                'geometry': geometry, 'directory_segments': [{'rows': 32768, 'row_bytes': 4096, 'sha256': 'a'*64}],
                'page_segments': [{'rows': 65536, 'row_bytes': 4096, 'sha256': 'b'*64}]})
            manifests[manifest['sha256']] = manifest
            rows.append({'shard_id': shard, 'geometry': geometry, 'manifest_digest': manifest['sha256']})
        mapping = self.ref({'shards': rows})
        for row in rows:
            for table, digest, count in (('directory', 'a', 32768), ('pages', 'b', 65536)):
                native = {'database_sha256': 'rows_sha256:'+digest*64, 'rows': count,
                          'product': 'transparent-segment', 'served_public_sha256': 'c'*64, 'fixture_bits': 128}
                executions.append({'shard_id': row['shard_id'], 'table': table, 'segment': 0,
                                   'execution': self.capture(native, 'examples/native_certificate', mapping['sha256'])})
        return mapping, manifests, executions

    def certificate_report(self, mapping, manifests, executions):
        # Fixture evaluator only; production always verifies the retained code
        # and frozen sampler digests before calling its exact-rational evaluator.
        evaluator = SimpleNamespace(evaluate=lambda native: {'actual_profile': {
            'certified_failure_bits': native['fixture_bits']}})
        with patch.object(M, 'certifier', return_value=evaluator):
            return M.certificate_report(mapping, manifests, executions, '/fixture-only')

    def test_artifact_derives_gate_and_keeps_raw_refs_without_relabeling(self):
        mapping, execution = self.artifact()
        report = M.artifact_report(mapping, execution)
        self.assertEqual(report['binaries'], {'shard-verify': M.C.ARTIFACTS['shard-verify']})
        self.assertEqual(report['raw_evidence']['execution'], execution)
        self.assertEqual(report['verified']['map_file_sha256'], mapping['sha256'])
        for field, bad in (('tool_sha', M.C.HISTORICAL_SHA), ('failures', 1)):
            with self.subTest(field=field):
                original = copy.deepcopy(execution)
                self.replace(original, 'native', lambda v: v.update({field: bad}))
                with self.assertRaises(ValueError): M.artifact_report(mapping, original)

    def test_artifact_missing_checks_duplicate_checks_and_wrong_coverage_refuse(self):
        mapping, execution = self.artifact()
        for mutation in (lambda v: v['checks'].pop(), lambda v: v['checks'].append(v['checks'][0]),
                         lambda v: v['set'].update(through=99), lambda v: v['checks'][0].update(ok=False)):
            refs = copy.deepcopy(execution); self.replace(refs, 'native', mutation)
            with self.assertRaises(ValueError): M.artifact_report(mapping, refs)

    def test_foreign_owner_failed_child_pid_drift_and_timeouts_refuse(self):
        mapping, execution = self.artifact()
        for key, mutation in (('owner', lambda v: v.update(native_source_sha=M.C.HISTORICAL_SHA)),
                              ('owner', lambda v: v.update(binary_sha256='0'*64)),
                              ('owner', lambda v: v.update(publication_sha256='0'*64)),
                              ('result', lambda v: v.update(pid=43)),
                              ('result', lambda v: v.update(exit_code=1)),
                              ('result', lambda v: v.update(status='running')),
                              ('result', lambda v: v.update(ended_unix=40)),
                              ('result', lambda v: v.update(started_unix=11))):
            refs = copy.deepcopy(execution); self.replace(refs, key, mutation)
            with self.assertRaises(ValueError): M.artifact_report(mapping, refs)

    def test_resource_floor_coverage_and_nonfinite_samples_refuse(self):
        mapping, execution = self.artifact()
        for samples in ([], [{'observed_unix': 10, 'memory_available': .3, 'disk_available': {'/': .4}}],
                        [{'observed_unix': t, 'memory_available': .19, 'disk_available': {'/': .4}} for t in (9,16)],
                        [{'observed_unix': t, 'memory_available': .3, 'disk_available': {'/': .19}} for t in (9,16)],
                        [{'observed_unix': t, 'memory_available': .3, 'disk_available': {'/': .4}} for t in (9,30)],
                        [{'observed_unix': t, 'memory_available': float('nan'), 'disk_available': {'/': .4}} for t in (9,16)]):
            refs = copy.deepcopy(execution); refs['health'] = self.ref(samples)
            with self.assertRaises(ValueError): M.artifact_report(mapping, refs)

    def test_bytes_symlink_hardlink_duplicate_json_and_missing_stderr_refuse(self):
        mapping, execution = self.artifact()
        Path(execution['stderr']['path']).write_bytes(b'changed')
        with self.assertRaises(ValueError): M.artifact_report(mapping, execution)
        raw = self.raw(b'{"a":1,"a":2}')
        with self.assertRaises(ValueError): M.value(raw)
        raw = self.raw(b'{}'); linked = self.root/'hardlink'; os.link(raw['path'], linked)
        with self.assertRaises(ValueError): M.blob(raw)
        linked = self.root/'symlink'; linked.symlink_to(mapping['path'])
        with self.assertRaises(ValueError): M.blob(dict(mapping, path=str(linked)))
        mapping, execution = self.artifact(); del execution['stderr']
        with self.assertRaises(ValueError): M.artifact_report(mapping, execution)

    def test_certificate_full_coverage_and_both_floors(self):
        mapping, manifests, executions = self.certificates()
        for item in executions:
            if item['shard_id'] < 77 and item['table'] == 'pages':
                self.replace(item['execution'], 'native', lambda v: v.update(fixture_bits=83))
        report = self.certificate_report(mapping, manifests, executions)
        self.assertEqual(report['segments'], 180)
        self.assertEqual(len(report['setup_bindings']), 180)
        for item, bits in ((executions[1], 82), (executions[0], 127), (executions[-1], 127)):
            saved = item['execution']['native']; self.replace(item['execution'], 'native', lambda v: v.update(fixture_bits=bits))
            with self.assertRaises(ValueError): self.certificate_report(mapping, manifests, executions)
            item['execution']['native'] = saved

    def test_missing_duplicate_foreign_and_table_hash_certificate_refuse(self):
        mapping, manifests, executions = self.certificates()
        for bad in (executions[:-1], executions[:-1]+[executions[0]],
                    [dict(executions[0], shard_id=999), *executions[1:]]):
            with self.assertRaises(ValueError): self.certificate_report(mapping, manifests, bad)
        self.replace(executions[0]['execution'], 'native', lambda v: v.update(database_sha256='synthetic:rows'))
        with self.assertRaises(ValueError): self.certificate_report(mapping, manifests, executions)
        with self.assertRaises(ValueError): self.certificate_report(mapping, {}, executions)

    def test_certifier_pin_and_output_replacement_refuse(self):
        raw = self.raw(b'not the retained certifier')
        with self.assertRaises(ValueError): M.certifier(raw['path'])
        mapping, execution = self.artifact(); report = M.artifact_report(mapping, execution)
        out = self.root/'report.json'; M.write_report(out, report)
        self.assertEqual(M.value({'path': str(out), 'sha256': M.C.checksum(out)}), report)
        self.assertEqual(out.stat().st_mode & 0o777, 0o400)
        with self.assertRaises(FileExistsError): M.write_report(out, report)


if __name__ == '__main__':
    unittest.main()
