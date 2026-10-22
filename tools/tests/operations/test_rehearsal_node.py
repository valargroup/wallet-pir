"""The operator rehearsal's fake node, `tools/tests/operations/rehearsal/rehearse.py`.

`serve-publication` activates a publication only once the node confirms each block it
depends on and the Ironwood tree size after it. It reads the size as the receiver's
node client does (`receiver_boundary` in `src/shared/pir-chain/src/receiver.rs`): a
verbose `getblock` by the block's displayed hash, then `trees.ironwood.size`. A fake
that cannot answer leaves activation deferred, so the rehearsal never serves.
"""
import importlib.util
import json
from pathlib import Path
import unittest
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'tools/tests/operations/rehearsal/rehearse.py'
SPEC = importlib.util.spec_from_file_location('rehearse', SCRIPT)
rehearse = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rehearse)


class RehearsalNode(unittest.TestCase):
    def setUp(self):
        self.node = rehearse.Node(rehearse.MAINNET_GENESIS)
        self.addCleanup(self.node.server.shutdown)

    def call(self, method, params):
        """Sends one JSON-RPC request as the node client does; returns the reply."""
        body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode()
        request = urllib.request.Request(self.node.url, body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    def test_a_staged_anchor_answers_its_tree_size(self):
        payments = {'height': 3543406, 'hash': 'a1' * 32, 'size': 8}
        witnesses = {'height': 3543405, 'hash': 'b2' * 32, 'size': 6}
        self.node.stage({'anchors': [payments, witnesses]})
        for anchor in (payments, witnesses):
            self.assertEqual(self.call('getblockhash', [anchor['height']])['result'], anchor['hash'])
            block = self.call('getblock', [anchor['hash'], 1])
            self.assertIsNone(block['error'])
            self.assertEqual(block['result']['trees']['ironwood']['size'], anchor['size'])
            self.assertEqual(block['result']['height'], anchor['height'])
        self.assertGreater(self.call('getblockcount', [])['result'], payments['height'])

    def test_an_unknown_block_is_a_node_error(self):
        self.node.stage({'anchors': [{'height': 3543406, 'hash': 'a1' * 32, 'size': 8}]})
        for hash in ('ee' * 32, 'c3' * 32):
            reply = self.call('getblock', [hash, 1])
            self.assertIsNone(reply['result'])
            self.assertIsNotNone(reply['error'])


if __name__ == '__main__':
    unittest.main()
