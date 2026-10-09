import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('canary_pins', ROOT / 'scripts/transparent-canary-pins.py')
canary_pins = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(canary_pins)


def entry(shard_id, geometry, end, sealed=True):
    return {'shard_id': shard_id, 'geometry': geometry, 'end_height': end, 'sealed': sealed,
            'manifest_digest': f'{shard_id:064x}', 'terminal_block_hash': f'{end:064x}'}


MAP = {'shards': [entry(0, 'archive-wide', 100), entry(1, 'archive-wide', 200),
                  entry(2, 'recent-4k-8k', 300), entry(3, 'recent-4k-8k', 400),
                  entry(4, 'recent-4k-8k', 450, sealed=False)]}


def table(shard_id, geometry, name, nonempty=True):
    return {'shard_id': shard_id, 'revision': f'{shard_id:064x}', 'geometry': geometry, 'table': name,
            'rows': 8, 'row_bytes': 4096, 'segments': 1,
            'samples': [{'row': 0, 'sha256': ['0' * 64], 'nonempty': False},
                        {'row': 3, 'sha256': ['1' * 64], 'nonempty': nonempty}]}


def fixture(tables=None):
    tables = tables if tables is not None else [
        table(0, 'archive-wide', 'directory'), table(1, 'archive-wide', 'pages'),
        table(3, 'recent-4k-8k', 'directory'), table(2, 'recent-4k-8k', 'pages')]
    return {'schema': 'transparent-shard-v11', 'tables': tables}


def encode(value):
    return json.dumps(value).encode()


class PinsTest(unittest.TestCase):
    def test_anchor_is_the_highest_fixture_shard_terminal(self):
        raw = encode(fixture())
        result = canary_pins.pins(raw, encode(MAP))
        self.assertEqual(result['fixture_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual((result['anchor_height'], result['anchor_hash']), (400, f'{400:064x}'))
        self.assertEqual(result['tables'], 4)

    def test_refuses_what_the_canary_cannot_check(self):
        cases = {
            'schema': {**fixture(), 'schema': 'transparent-shard-v10'},
            'provisional': fixture([*fixture()['tables'], table(4, 'recent-4k-8k', 'pages')]),
            'superseded': fixture([*fixture()['tables'], {**table(2, 'recent-4k-8k', 'directory'), 'revision': 'f' * 64}]),
            'absent': fixture([*fixture()['tables'], table(9, 'recent-4k-8k', 'pages')]),
            'group': fixture(fixture()['tables'][:3]),
            'unoccupied': fixture([*fixture()['tables'], table(1, 'archive-wide', 'directory', nonempty=False)]),
            'segments': fixture([*fixture()['tables'], {**table(1, 'archive-wide', 'directory'), 'segments': 2}]),
        }
        for name, value in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                canary_pins.pins(encode(value), encode(MAP))


class MainTest(unittest.TestCase):
    def run_main(self, hash_at_node):
        with tempfile.TemporaryDirectory() as directory:
            paths = {name: Path(directory) / name for name in ('fixture.json', 'shards.json', 'cookie')}
            paths['fixture.json'].write_bytes(encode(fixture()))
            paths['shards.json'].write_bytes(encode(MAP))
            paths['cookie'].write_text('user:password\n')
            out = io.StringIO()
            with mock.patch.object(canary_pins, 'node_hash', return_value=hash_at_node) as node, \
                    contextlib.redirect_stdout(out):
                code = canary_pins.main(['--fixture', str(paths['fixture.json']), '--shard-map', str(paths['shards.json']),
                                         '--rpc-url', 'http://127.0.0.1:8232', '--cookie', str(paths['cookie'])])
            node.assert_called_once_with('http://127.0.0.1:8232', paths['cookie'], 400)
            return code, json.loads(out.getvalue())

    def test_canonical_anchor_passes(self):
        code, result = self.run_main(f'{400:064x}')
        self.assertEqual(code, 0)
        self.assertEqual(result['anchor_checked'], 'node getblockhash')

    def test_noncanonical_anchor_fails(self):
        code, result = self.run_main('e' * 64)
        self.assertEqual(code, 1)
        self.assertIn('error', result)


if __name__ == '__main__':
    unittest.main()
