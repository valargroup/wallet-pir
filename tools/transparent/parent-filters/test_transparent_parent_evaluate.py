"""Selection must not trade recent regressions for archive savings or tune on holdout."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('parent_eval', Path(__file__).with_name('transparent_parent_evaluate.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class SelectionTests(unittest.TestCase):
    def test_incomplete_http_run_cannot_recommend_a_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m.write(root/'variants.json', [{'id': 'baseline', 'manifests': {}},
                    {'id': 'candidate', 'eligible_held_out': True}])
            m.write(root/'order.json', [[1, 0], [1, 1]])
            result = m.http_summary(root, root)
            self.assertEqual(result['status'], 'incomplete')
            self.assertIsNone(result['recommended'])
            self.assertFalse(result['results'][1]['correctness_gate'])

    def test_recent_guardrail_is_per_profile(self):
        base = {'profiles': {p: 100 for p in m.RECENT}}
        candidate = {'profiles': dict(zip(m.RECENT, [101, 1, 1]))}
        self.assertFalse(m.eligible(candidate, base))

    def test_primary_band_precedes_mixed_cost(self):
        def candidate(name, recent, mixed):
            return {'id': name, 'tuning': {'recent_mean': recent, 'mixed_mean': mixed}, 'requests': 1}
        result = m.ranked([candidate('best-primary', 100, 1000),
                           candidate('within-band', 100.5, 900),
                           candidate('bad-primary', 110, 1)])
        self.assertEqual([r['id'] for r in result], ['within-band', 'best-primary'])

    def test_holdout_cannot_replace_tuning_finalists(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = []
            for profile in m.WEIGHTS:
                for split in ('tuning', 'held-out'):
                    rows.append({'index': len(rows), 'profile': profile, 'split': split,
                                 'child_bytes': 1000, 'parent_bytes': 0, 'metadata_bytes': 0, 'requests': 2})
            m.write(root/'baseline-wallets.json', rows)
            candidates = []
            for n in range(4):
                name=f'archive-{n}'
                candidate_rows=[dict(r) for r in rows]
                for r in candidate_rows:
                    if r['profile']=='restore-old':
                        r['child_bytes'] = (100+n*10) if r['split']=='tuning' else (1 if n==3 else 900)
                m.write(root/f'{name}-wallets.json',candidate_rows)
                candidates.append({'id':name,'tier':'archive-wide'})
            m.write(root/'report.json', {'candidates':candidates})
            result=m.select(root)
            self.assertEqual([c['id'] for c in result['finalists']],['archive-0','archive-1','archive-2'])
            self.assertEqual(result['status'],'provisional_requires_http')


if __name__ == '__main__':
    unittest.main()
