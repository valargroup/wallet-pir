"""Fictional block records; deterministic selection is not oracle qualification."""
import hashlib
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
spec = importlib.util.spec_from_file_location('oracle_sample_test', HERE.parents[1]/'lib/activity_oracle_sample.py')
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)


class SampleTests(unittest.TestCase):
    def test_native_identity_recipe_full_snapshot_tip_and_agreement_are_bound(self):
        # Only values, never a real snapshot receipt or qualification report.
        heights = sorted(M.fixed_heights() | {1,2,3})
        self.assertEqual(len(heights),17)
        sample = {'recipe':{'explicit':list(M.EXPLICIT), 'top':3, 'random':5, 'seed':20261001,
                           'last':3, 'through':M.THROUGH, 'heights':heights},
                  'canonical_hashes':{str(h):format(h+1,'064x') for h in heights}}
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory).resolve())
            native = {'schema':'transparent-event-spotcheck-v2', 'tool_sha':M.C.SOURCE_SHA,
                      'data_dir':path, 'genesis_hash':sample['canonical_hashes']['0'],
                      'journal':{'start_height':0, 'covered_through':M.THROUGH+1},
                      'sample':copy.deepcopy(sample['recipe']), 'blocks_compared':17, 'blocks_disagreeing':0,
                      'blocks':[{'height':h, 'journal_hash':sample['canonical_hashes'][str(h)],
                                 'node_hash':sample['canonical_hashes'][str(h)], 'agrees':True,
                                 'missing_count':0,'extra_count':0,'missing_from_journal':[],'extra_in_journal':[]}
                                for h in heights]}
            kwargs = dict(journal_dir=path, genesis_hash=sample['canonical_hashes']['0'], covered_through=M.THROUGH+1)
            self.assertIs(M.bind_native(native,sample,**kwargs),native)
            mutations = [lambda n:n.update(tool_sha=M.C.HISTORICAL_SHA),
                         lambda n:n.update(data_dir='/srv/transparent-activity/full-v3/journal'),
                         lambda n:n['journal'].update(covered_through=M.THROUGH),
                         lambda n:n['journal'].update(start_height=False),
                         lambda n:n['sample'].update(seed=1),
                         lambda n:n['sample'].update(explicit=[False,*M.EXPLICIT[1:]]),
                         lambda n:n['blocks'].reverse(),
                         lambda n:n['blocks'][0].update(node_hash='f'*64),
                         lambda n:n['blocks'][0].update(missing_count=1),
                         lambda n:n.update(blocks_disagreeing=True)]
            for mutate in mutations:
                changed=copy.deepcopy(native); mutate(changed)
                with self.assertRaises(ValueError): M.bind_native(changed,sample,**kwargs)

    def test_actual_closed_seed_has_expected_distinct_random_heights(self):
        expected = set(M.EXPLICIT) | {1210880,1470438,2185090,2664258,2721514,3500736,3500737}
        self.assertEqual(M.fixed_heights(), expected)

    def test_equal_later_density_does_not_replace_initial_tuple_ties(self):
        values = M.Densest()
        for height in range(1,5): values.add(9, height, str(height))
        self.assertEqual([x[1] for x in values.entries], [3,2,1])
        values.add(10, 5, '5')
        self.assertEqual([x[1] for x in values.entries], [5,3,2])

    def test_streaming_digest_hash_endianness_extra_bytes_and_budget_refusal(self):
        # Shrink only the fictional fixture recipe, not the production constants.
        with tempfile.TemporaryDirectory() as directory, patch.multiple(M, THROUGH=30, EXPLICIT=(0,), RANDOM=0, LAST=1), patch.object(M, 'fixed_heights', return_value={0,30}), patch.object(M, 'TOP', 15):
            path = Path(directory).resolve()/'blocks.bin'
            raw = b''.join(M.RECORD.pack(height.to_bytes(32,'little'),height,100 if 1 <= height <= 15 else 1)
                           for height in range(31))
            path.write_bytes(raw); path.chmod(0o400)
            sha = hashlib.sha256(raw).hexdigest()
            result = M.derive(path,len(raw),sha,30,lambda:None)
            self.assertEqual(result['recipe']['heights'], [0,*range(1,16),30])
            self.assertEqual(result['canonical_hashes']['30'], format(30,'064x'))
            with self.assertRaises(ValueError): M.derive(path,len(raw),'0'*64,30,lambda:None)
            with self.assertRaisesRegex(ValueError,'fictional budget'):
                M.derive(path,len(raw),sha,30,lambda:(_ for _ in ()).throw(ValueError('fictional budget')))
            with self.assertRaises(ValueError): M.derive(path,len(raw)+1,sha,30,lambda:None)


if __name__ == '__main__': unittest.main()
