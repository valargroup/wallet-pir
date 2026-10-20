"""Negative and precedence cases for the independently encoded publication oracle."""
import unittest
import hashlib
from pathlib import Path
import struct
import tempfile
from unittest.mock import patch
from lib import publication_capture
from lib.publication_capture import snapshot, protocol_hash, matches, read_retained_source, change_kind, Oracle, SourceChanged


def block(height, txids):
    return {'height': height, 'hash': ('%064x' % height), 'previousblockhash': ('%064x' % (height - 1)), 'tx': txids}


class PublicationOracleTests(unittest.TestCase):
    def test_canonical_membership_overrides_mempool_and_fork(self):
        txid = '08' * 32
        value = snapshot([block(1, [txid])], [txid], [(txid, 1, '09' * 32)], bytes(32), bytes([1]) * 32)
        canonical = snapshot([block(1, [txid])], [], [], bytes(32), bytes([1]) * 32)
        self.assertEqual(value, canonical)
        self.assertEqual(value['entries'], 1)
        changed = dict(value, entries=2)
        self.assertFalse(matches({'snapshot': value}, changed))

    def test_transaction_order_is_irrelevant_but_mempool_presence_changes_digest(self):
        txids = ['01' * 32, '02' * 32]
        a = snapshot([block(1, txids)], [], [], bytes(32), bytes([1]) * 32)
        b = snapshot([block(1, txids[::-1])], [], [], bytes(32), bytes([1]) * 32)
        self.assertEqual(a, b)
        c = snapshot([block(1, txids)], ['03' * 32], [], bytes(32), bytes([1]) * 32)
        self.assertNotEqual(a['rows_digest'], c['rows_digest'])

    def test_disconnected_blocks_and_duplicate_canonical_transactions_are_refused(self):
        with self.assertRaisesRegex(ValueError, 'noncontiguous'):
            snapshot([block(1, []), block(3, [])], [], [], bytes(32), bytes(32))
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            snapshot([block(1, ['01' * 32]), block(2, ['01' * 32])], [], [], bytes(32), bytes(32))

    def test_unavailable_fork_requires_a_frozen_input_and_must_still_be_noncanonical(self):
        payload = b'STSOBS01' + struct.pack('<I', 4096) + bytes([1]) * 64
        payload += struct.pack('<II', 0, 1) + bytes([2]) * 32 + struct.pack('<I', 1) + bytes([3]) * 32
        class Node:
            def call(self, method, *args):
                if method == 'getblock':
                    raise ValueError('node refused getblock: -5')
                return '04' * 32
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.bin'
            path.write_bytes(payload)
            with self.assertRaisesRegex(ValueError, 'node refused'):
                read_retained_source(path, Node())
            with self.assertRaisesRegex(ValueError, 'frozen'):
                read_retained_source(path, Node(), expected_sha256='0' * 64)
            forks = read_retained_source(path, Node(), expected_sha256=hashlib.sha256(payload).hexdigest())
            self.assertEqual(forks, ([], [('02' * 32, 1, '03' * 32)]))

    def test_coalesced_observation_keeps_the_actual_publication_change(self):
        committed = {'anchor': 'a', 'mempool_digest': 'old'}
        changed = {'anchor': 'a', 'mempool_digest': 'new'}
        latest = dict(changed)
        self.assertEqual(change_kind(changed, latest), 'unchanged')
        self.assertEqual(change_kind(committed, latest), 'mempool')
        self.assertEqual(change_kind(committed, dict(latest, anchor='b')), 'block')

    def test_capacity_relief_does_not_restore_discarded_blocks(self):
        class Node:
            mempool = ['%064x' % i for i in range(3, 6)]
            def call(self, method, *args):
                if method == 'getblockhash':
                    return block(args[0], [])['hash']
                if method == 'getblockcount':
                    return 2
                return self.mempool
            def block(self, height):
                return block(height, ['%064x' % height])
        node = Node()
        with patch.multiple(publication_capture, ROWS=1, SLOTS=6, ROW_BYTES=240):
            oracle = Oracle(node, 4096, bytes(32))
            pressured = oracle.observe()['snapshot']
            self.assertEqual(pressured['coverage_start'], 2)
            self.assertEqual([b['height'] for b in oracle.blocks], [2])
            node.mempool = []
            expected = snapshot([node.block(2)], [], [], oracle.network, bytes(32))
            self.assertEqual(oracle.observe()['snapshot'], expected)
            fresh = Oracle(node, 4096, bytes(32), blocks=oracle.blocks)
            self.assertEqual(fresh.observe()['snapshot'], expected)

    def test_retained_boundary_is_verified_and_used_on_attachment(self):
        retained = block(2, ['02' * 32])
        payload = b'STSOBS01' + struct.pack('<I', 4096) + bytes(64)
        payload += struct.pack('<II', 1, 2) + protocol_hash(retained['hash'])
        payload += protocol_hash(retained['previousblockhash']) + struct.pack('<I', 1)
        payload += protocol_hash(retained['tx'][0]) + struct.pack('<I', 0)
        class Node:
            def call(self, method, *args):
                if method == 'getblock':
                    return retained
                if method == 'getblockhash':
                    return block(args[0], [])['hash']
                return 2 if method == 'getblockcount' else []
            def block(self, height):
                raise AssertionError('retained blocks must not be backfilled')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.bin'
            path.write_bytes(payload)
            node = Node()
            blocks, forks = read_retained_source(path, node)
            oracle = Oracle(node, 4096, bytes(32), forks, blocks)
            self.assertEqual(oracle.observe()['snapshot']['coverage_start'], 2)
            with patch.object(node, 'call', return_value=dict(retained, tx=[])):
                with self.assertRaisesRegex(ValueError, 'retained block'):
                    read_retained_source(path, node)

    def test_hashes_use_protocol_byte_order(self):
        self.assertEqual(protocol_hash('00' * 31 + '01'), b'\x01' + bytes(31))

    def test_block_arriving_during_read_requires_a_complete_new_observation(self):
        class Node:
            height = 1
            reads = 0
            def call(self, method, *args):
                if method == 'getblockhash':
                    return block(args[0], [])['hash']
                if method == 'getblockcount':
                    return self.height
                self.reads += 1
                self.height = 2
                return []
            def block(self, height):
                return block(height, ['%064x' % height])
        node = Node()
        value = Oracle(node, 4096, bytes(32)).observe()
        self.assertEqual(value['anchor'], block(2, [])['hash'])
        self.assertEqual(value['snapshot']['entries'], 2)
        self.assertEqual(node.reads, 2)

        class MovingNode(Node):
            def call(self, method, *args):
                if method == 'getrawmempool':
                    self.height += 1
                    self.reads += 1
                    return []
                return super().call(method, *args)
        moving = MovingNode()
        with self.assertRaisesRegex(SourceChanged, 'three complete reads'):
            Oracle(moving, 4096, bytes(32)).observe()
        self.assertEqual(moving.reads, 3)


if __name__ == '__main__':
    unittest.main()
