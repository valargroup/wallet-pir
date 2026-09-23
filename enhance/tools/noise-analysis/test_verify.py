import copy
import json
import unittest
from pathlib import Path
from verify import analyze, sampler, exp_upper, Fraction


class VerifierTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from verify import Q, ceildiv, PIN
        cls.x = dict(format=1, implementation=PIN, n=2048, q=Q, p=65536, ell=3,
                     cdf_max_val=65, cdf_table=json.loads((Path(__file__).parent/'sampler-cdf.json').read_text()),
                     rows=4096, params=dict(db_rows=4096, db_cols=12288, instances=6, p=65536, query_bits=46, q_prime_1=1<<20),
                     threshold=(Q//65536)//2, column_sums=[0]*12288,
                     deterministic_bounds=[ceildiv(Q,1<<21)+1+Q%65536]*12288,
                     partitioned_hint_matches=True, partitioned_intermediates_match=True,
                     queries=[dict(exact_answer=True,max_expected_phase_error=0)],
                     failures=0,max_expected_phase_error=0,
                     **{key:'00'*32 for key in ('database_sha256','public_c1_sha256','binary_sha256','setup_seed')})
        cls.x['blocks'] = [dict(block=i, exact_digits=True, ntt_action_crosschecks=2,
                               **{key:[0]*2048 for key in ('sum_squares','sum_absolute','sum_signed','maximum_weight')}) for i in range(6)]

    def test_valid_complete(self):
        r = analyze(self.x)
        self.assertTrue(r['analytical_pass'])
        self.assertEqual(r['status'], 'not certified')  # Review cannot be inferred from mathematics.

    def test_corruption(self):
        for mutate in [
            lambda x: x['blocks'].pop(),
            lambda x: x['blocks'][0]['sum_squares'].pop(),
            lambda x: x['blocks'][1].update(block=0),
            lambda x: x['column_sums'].pop(),
            lambda x: x.update(blocks=None),
            lambda x: x.update(implementation='old'),
            lambda x: x['params'].update(query_bits=45),
            lambda x: x['blocks'][0].update(exact_digits=False),
            lambda x: x['blocks'][0]['maximum_weight'].__setitem__(0, 100),
            lambda x: x['deterministic_bounds'].__setitem__(0, 0),
            lambda x: x.update(failures=1),
        ]:
            with self.subTest(mutate=mutate):
                x = copy.deepcopy(self.x)
                mutate(x)
                with self.assertRaises((ValueError, KeyError)):
                    analyze(x)

    def test_negative_deterministic_budget_is_not_a_decoding_failure(self):
        from verify import Q, ceildiv
        x = copy.deepcopy(self.x)
        x['rows'] = x['params']['db_rows'] = 32768
        x['column_sums'] = [32768 * 65535] * 12288
        x['deterministic_bounds'] = [(65 + ceildiv(Q, 1 << 47) + 1) * v + ceildiv(Q, 1 << 21) + 1 + Q % 65536 for v in x['column_sums']]
        x['blocks'] = None
        r = analyze(x)
        self.assertFalse(r['analytical_pass'])
        self.assertEqual(r['status'], 'not certified')
        self.assertLess(r['min_deterministic_allowance'], 0)

    def test_failure_not_hidden_by_good_bound(self):
        x = copy.deepcopy(self.x)
        x['queries'][0]['exact_answer'] = False
        x['failures'] = 1
        self.assertEqual(analyze(x)['status'], 'decoding failure')

    def test_exact_lambda_recovers_margin_lost_by_fixed_dyadic_choice(self):
        from verify import Q, ceildiv
        x = copy.deepcopy(self.x)
        x['rows'] = x['params']['db_rows'] = 8192
        x['column_sums'] = [8192 * 65535] * 12288
        x['deterministic_bounds'] = [(65 + ceildiv(Q, 1 << 47) + 1) * v + ceildiv(Q, 1 << 21) + 1 + Q % 65536 for v in x['column_sums']]
        for b in x['blocks']:
            b['maximum_weight'] = [270596803] * 2048
            b['sum_squares'] = [286651564758432568] * 2048
            b['sum_absolute'] = [10**10] * 2048
        r = analyze(x)
        self.assertTrue(r['analytical_pass'])
        self.assertLessEqual(r['union_log2_upper'], -128)
        self.assertEqual(r['limiting_coefficient']['lambda_denominator'], 4 * 270596803)

    def test_wide_profile_requires_correct_precision_and_current_weights(self):
        from verify import WIDE_PIN, Q, ceildiv
        x = copy.deepcopy(self.x)
        x['implementation'] = WIDE_PIN
        x['params']['query_bits'] = 49
        x['rows'] = x['params']['db_rows'] = 32768
        x['column_sums'] = [32768 * 65535] * 12288
        x['deterministic_bounds'] = [(65 + ceildiv(Q, 1 << 50) + 1) * v + ceildiv(Q, 1 << 21) + 1 + Q % 65536 for v in x['column_sums']]
        self.assertGreater(analyze(x)['min_deterministic_allowance'], 0)
        for bits in [46, 47, 48, 50]:
            wrong = copy.deepcopy(x)
            wrong['params']['query_bits'] = bits
            with self.assertRaises(ValueError):
                analyze(wrong)
        x['blocks'] = None
        with self.assertRaises(ValueError):
            analyze(x)

    def test_q48_has_explicit_78_bit_target_without_weakening_other_profiles(self):
        from verify import Q48_PIN, WIDE_PIN, Q, ceildiv
        x = copy.deepcopy(self.x)
        x['implementation'] = Q48_PIN
        x['params']['query_bits'] = 48
        x['rows'] = x['params']['db_rows'] = 32768
        x['column_sums'] = [32768 * 65535] * 12288
        x['deterministic_bounds'] = [(65 + ceildiv(Q, 1 << 49) + 1) * v + ceildiv(Q, 1 << 21) + 1 + Q % 65536 for v in x['column_sums']]
        for block in x['blocks']:
            block['maximum_weight'] = [270596803] * 2048
            block['sum_squares'] = [286651564758432568] * 2048
            block['sum_absolute'] = [10**10] * 2048
        result = analyze(x)
        self.assertEqual(result['target_failure_bits'], 78)
        self.assertTrue(result['analytical_pass'])
        self.assertLessEqual(result['union_log2_upper'], -78)
        self.assertGreater(result['union_log2_upper'], -128)
        self.assertEqual(analyze(self.x)['target_failure_bits'], 128)
        x['implementation'] = WIDE_PIN
        with self.assertRaises(ValueError):
            analyze(x)

    def test_rc2_q48_identity_keeps_exact_transport_and_target(self):
        from verify import Q48_PIN, Q48_RC2_PIN
        x = copy.deepcopy(self.x)
        x['params']['query_bits'] = 48
        x['implementation'] = Q48_PIN
        before = analyze(x)
        x['implementation'] = Q48_RC2_PIN
        self.assertEqual(analyze(x), before)
        for bits in (46, 47, 49, 50):
            x['params']['query_bits'] = bits
            with self.assertRaises(ValueError):
                analyze(x)

    def test_finite_cdf_validation(self):
        with self.assertRaises(ValueError):
            sampler((1 << 64,) * 131)
        with self.assertRaises(ValueError):
            sampler((1, 0) + (0,) * 129)
        self.assertEqual(exp_upper(Fraction(0)), 1)


if __name__ == '__main__':
    unittest.main()
