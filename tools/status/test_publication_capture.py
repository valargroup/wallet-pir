"""Negative and precedence cases for the independently encoded publication oracle."""
import unittest
import hashlib
from pathlib import Path
import struct
import tempfile
from lib.publication_capture import snapshot, protocol_hash, matches, retained_forks, change_kind


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
                retained_forks(path, Node())
            with self.assertRaisesRegex(ValueError, 'frozen'):
                retained_forks(path, Node(), expected_sha256='0' * 64)
            forks = retained_forks(path, Node(), expected_sha256=hashlib.sha256(payload).hexdigest())
            self.assertEqual(forks, [('02' * 32, 1, '03' * 32)])

    def test_coalesced_observation_keeps_the_actual_publication_change(self):
        committed = {'anchor': 'a', 'mempool_digest': 'old'}
        changed = {'anchor': 'a', 'mempool_digest': 'new'}
        latest = dict(changed)
        self.assertEqual(change_kind(changed, latest), 'unchanged')
        self.assertEqual(change_kind(committed, latest), 'mempool')
        self.assertEqual(change_kind(committed, dict(latest, anchor='b')), 'block')

    def test_hashes_use_protocol_byte_order(self):
        self.assertEqual(protocol_hash('00' * 31 + '01'), b'\x01' + bytes(31))


if __name__ == '__main__':
    unittest.main()
