"""Raw RPC evidence cannot hide missing blocks, prevouts or refused batches."""
import gzip
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_oracle_rpc as M


class RPC(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(); self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve(); self.counter = 0

    def ref(self, value):
        self.counter += 1; path = self.root/str(self.counter)
        raw = json.dumps(value).encode(); path.write_bytes(raw)
        return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}

    def attempt(self, method, params, result, request_id=1):
        return {'status': 200, 'request': self.ref({'id': request_id, 'method': method, 'params': params}),
                'response': self.ref({'id': request_id, 'result': result, 'error': None})}

    def fixtures(self):
        attempts = []; native = {'blocks_compared': 17, 'blocks_disagreeing': 0, 'blocks': []}
        for height in range(17):
            txid = format(height+100, '064x'); block_hash = format(height+1000, '064x')
            tx = {'txid': txid, 'vin': [{'coinbase': '00'}], 'vout': [
                {'n': 0, 'valueZat': 1, 'scriptPubKey': {'hex': '00'}},
                {'n': 1, 'valueZat': 0, 'scriptPubKey': {'hex': '6a00'}}]}
            attempts.append(self.attempt('getblock', [str(height), 1],
                                         {'height': height, 'hash': block_hash, 'tx': [txid]}))
            attempts.append(self.attempt('getrawtransaction', [txid, 1], tx))
            native['blocks'].append({'height': height, 'agrees': True, 'journal_hash': block_hash,
                'node_hash': block_hash, 'missing_count': 0, 'extra_count': 0,
                'missing_from_journal': [], 'extra_in_journal': [], 'node_receives': 1,
                'node_spends': 0, 'node_events': 1, 'journal_events': 1})
        return attempts, native

    def mutate_response(self, attempt, mutation):
        response = M.decode(attempt['response']); mutation(response)
        attempt['response'] = self.ref(response)

    def test_compressed_bodies_bind_both_hashes_and_limit_decompression(self):
        raw = json.dumps({'id': 1, 'result': 'x'*1000}).encode()
        packed = gzip.compress(raw, mtime=0); path = self.root/'body.json.gz'; path.write_bytes(packed)
        ref = {'path': str(path), 'sha256': hashlib.sha256(packed).hexdigest(),
               'decoded_sha256': hashlib.sha256(raw).hexdigest()}
        self.assertEqual(M.decode(ref), json.loads(raw))
        with self.assertRaises(ValueError): M.decode(dict(ref, decoded_sha256='0'*64))
        with patch.object(M, 'MAX_BODY', 128):
            with self.assertRaises(ValueError): M.decode(ref)

    def test_large_proof_fields_are_not_retained_in_index_projection(self):
        tx = {'txid': format(1, '064x'), 'vin': [], 'vout': [], 'proof': 'x'*100000,
              'ironwood': {'actions': [{'proof': 'x'*100000}]}}
        projection = M.project(tx)
        self.assertEqual(projection['_pools'], [False, False, False, True])
        self.assertNotIn('proof', projection)
        self.assertLess(len(json.dumps(projection)), 256)

    def test_large_compressed_body_checks_owned_budget_during_decompression(self):
        raw = json.dumps({'payload': 'x'*(3 << 20)}).encode()
        packed = gzip.compress(raw, mtime=0); path = self.root/'large.json.gz'; path.write_bytes(packed)
        ref = {'path':str(path), 'sha256':hashlib.sha256(packed).hexdigest(),
               'decoded_sha256':hashlib.sha256(raw).hexdigest()}
        calls = []
        self.assertEqual(len(M.decode(ref, check=lambda:calls.append(True))['payload']), 3 << 20)
        self.assertGreaterEqual(len(calls), 8)
        count = 0
        def refuse():
            nonlocal count
            count += 1
            if count == 8:
                raise ValueError('fictional aggregate deadline')
        with self.assertRaisesRegex(ValueError, 'fictional aggregate deadline'):
            M.decode(ref, check=refuse)
        with self.assertRaises(ValueError): M.decode(ref, check=False)

    def test_raw_comparison_refuses_resource_loss_after_transaction_projection(self):
        attempts, native = self.fixtures()
        failed, original = False, M.project
        def project(tx, *, check=None):
            nonlocal failed
            result = original(tx, check=check); failed = True
            return result
        def check():
            if failed: raise ValueError('fictional resource floor')
        with patch.object(M, 'project', side_effect=project), self.assertRaisesRegex(ValueError, 'fictional resource floor'):
            M.compare(attempts, native, check=check)

    def test_all_17_blocks_and_indexing_counts_are_derived_from_raw(self):
        attempts, native = self.fixtures(); report = M.compare(attempts, native)
        self.assertEqual(report['events_compared'], 17)
        self.assertEqual(report['raw_pool_category_coverage']['coinbase'], 17)
        self.assertEqual(report['rpc_method_counts']['getblock'], 17)
        self.assertEqual(report['raw_pool_category_coverage']['sampled_transactions'], 17)

    def test_prevout_scripts_and_pool_categories_require_complete_raw_transactions(self):
        attempts, native = self.fixtures(); previous = format(500, '064x')
        self.mutate_response(attempts[1], lambda r: r['result'].update(
            vin=[{'txid': previous, 'vout': 0}], vjoinsplit=[{}],
            vShieldedSpend=[{}], ironwood={'actions': [{}]}))
        tx = {'txid': previous, 'vin': [{'coinbase': '00'}], 'vout': [
            {'n': 0, 'scriptPubKey': {'hex': '76a9'}, 'valueZat': 2}]}
        attempts.append(self.attempt('getrawtransaction', [previous, 1], tx))
        native['blocks'][0].update(node_spends=1, node_events=2, journal_events=2)
        report = M.compare(attempts, native)
        self.assertEqual(report['events_compared'], 18)
        self.assertEqual(report['raw_pool_category_coverage']['multiple_shielded_pools'], 1)
        self.assertEqual(report['raw_pool_category_coverage']['ironwood_component_transactions'], 1)
        attempts.pop()
        with self.assertRaisesRegex(ValueError, 'previous transaction'): M.compare(attempts, native)

    def test_oversize_refusal_is_retained_and_must_be_resolved_by_exact_retry(self):
        attempts, native = self.fixtures(); txid = format(100, '064x')
        refusal = {'status': 500, 'request': self.ref([{'id': 1, 'method': 'getrawtransaction', 'params': [txid, 1]}]),
                   'response': self.ref({'id': None, 'result': None, 'error': {'code': -32011, 'message': 'large'}})}
        attempts.insert(1, refusal)
        report = M.compare(attempts, native)
        self.assertEqual(report['raw_rpc_application_refusals'][0]['code'], -32011)
        self.assertEqual(report['rpc_attempted_method_counts']['getrawtransaction'], 18)
        self.assertEqual(report['rpc_method_counts']['getrawtransaction'], 17)
        self.assertEqual(report['raw_rpc_application_refusals'][0]['request'], refusal['request'])
        attempts.pop(2)
        with self.assertRaisesRegex(ValueError, 'unresolved'): M.compare(attempts, native)

    def test_missing_block_extra_block_native_disagreement_and_false_counts_refuse(self):
        for mutation in (lambda a,n: a.pop(0), lambda a,n: n.update(blocks_disagreeing=1),
                         lambda a,n: n['blocks'][0].update(node_events=2),
                         lambda a,n: n['blocks'][0].update(journal_hash='f'*64),
                         lambda a,n: n['blocks'][0].update(missing_count=1),
                         lambda a,n: n['blocks'][0].update(node_spends=True)):
            attempts, native = self.fixtures(); mutation(attempts, native)
            with self.assertRaises(ValueError): M.compare(attempts, native)

    def test_response_identity_bad_script_invalid_output_and_transport_failure_refuse(self):
        for mutation in (lambda r: r.update(id=2), lambda r: r['result'].update(txid='f'*64),
                         lambda r: r['result']['vout'][0]['scriptPubKey'].update(hex='0 0'),
                         lambda r: r['result']['vout'][0].update(n=True),
                         lambda r: r.update(error={'code': -1})):
            attempts, native = self.fixtures(); self.mutate_response(attempts[1], mutation)
            with self.assertRaises(ValueError): M.compare(attempts, native)
        attempts, native = self.fixtures(); attempts[1]['status'] = 502
        with self.assertRaises(ValueError): M.compare(attempts, native)

    def test_tampered_bytes_duplicate_ids_and_unsupported_method_refuse(self):
        attempts, native = self.fixtures(); Path(attempts[0]['response']['path']).write_text('{}')
        with self.assertRaises(ValueError): M.compare(attempts, native)
        attempts, native = self.fixtures()
        call = {'id': 1, 'method': 'getrawtransaction', 'params': [format(100,'064x'), 1]}
        attempts[1]['request'] = self.ref([call, call])
        with self.assertRaisesRegex(ValueError, 'duplicate'): M.compare(attempts, native)
        attempts, native = self.fixtures(); call['method'] = 'sendrawtransaction'; attempts[1]['request'] = self.ref(call)
        with self.assertRaises(ValueError): M.compare(attempts, native)


if __name__ == '__main__': unittest.main()
