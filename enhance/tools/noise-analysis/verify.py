#!/usr/bin/env python3
"""Conservative integer/rational fixed-snapshot sufficient bound; not an audit.

Uses the finite CDF actually exported by the pinned backend. Automorphic reuse
is grouped in Rust before moments are formed. Results remain conditional on that
extractor and on independent sampler draws; ChaCha20 replacement is separate.
"""
import argparse
from fractions import Fraction
from functools import lru_cache
import hashlib
import gzip
import json
from pathlib import Path

PIN = 'dac5b050cfa00770405f9d6b464b8adb2b17a3c0'
Q = 72057594037641217
WIDE_PIN = 'b1c540f90f62e112c834a0f57f025e3c605e55d1'
PROFILE_BITS = {PIN: 46, WIDE_PIN: 49}
N = 2048
COLS = 12288


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(x):
    return type(x) is int


def ceildiv(x, y):
    return (x + y - 1) // y


@lru_cache(None)
def exp_upper(x):
    term = total = Fraction(1)
    for k in range(1, 129):
        term *= x / k
        total += term
    require(0 <= x < 130, 'Taylor bound domain')
    return total + term * x / 129 / (1 - x / 130)


@lru_cache(None)
def sampler(cdf):
    require(len(cdf) == 131, 'CDF size')
    last = -1
    mean = 0
    mass = 0
    moment = Fraction(0)
    for i, threshold in enumerate(cdf):
        require(integer(threshold) and last <= threshold < 1 << 64, 'CDF monotonicity/range')
        count = threshold - last
        last = threshold
        mass += count
        mean += count * (i - 65)
        u = Fraction(abs(i - 65), 4) + Fraction(1, 4096)
        moment += Fraction(count, 1 << 64) * (exp_upper(u) - 1 - u)
    u = Fraction(1, 4096)
    moment += Fraction((1 << 64) - mass, 1 << 64) * (exp_upper(u) - 1 - u)
    require(abs(mean) * 1024 < 1 << 64, 'sampler mean bound')
    require(16 * moment <= 100, 'local MGF constant')
    return mean


def validate(x):
    require(x['format'] == 1 and x['implementation'] in PROFILE_BITS, 'wrong implementation')
    require((x['n'], x['q'], x['p'], x['ell'], x['cdf_max_val']) == (N, Q, 65536, 3, 65), 'wrong profile')
    p = x['params']
    require(x['rows'] in (4096, 8192, 16384, 32768), 'unsupported domain')
    require(p['db_rows'] == x['rows'] and p['db_cols'] == COLS and p['instances'] == 6, 'wrong shape')
    require(p['p'] == 65536 and p['query_bits'] == PROFILE_BITS[x['implementation']] and p['q_prime_1'] == 1 << 20, 'transport profile changed')
    require(x['threshold'] == (Q // 65536) // 2, 'threshold mismatch')
    require(len(x['column_sums']) == COLS and all(integer(v) and 0 <= v <= x['rows'] * 65535 for v in x['column_sums']), 'column coverage/range')
    require(x['partitioned_hint_matches'] is True and x['partitioned_intermediates_match'] is True, 'composition mismatch')
    for key in ('database_sha256', 'public_c1_sha256', 'binary_sha256', 'setup_seed'):
        require(isinstance(x[key], str) and len(x[key]) == 64 and len(bytes.fromhex(x[key])) == 32, 'invalid digest')
    require(x['queries'] and all(type(q['exact_answer']) is bool and integer(q['max_expected_phase_error']) and 0 <= q['max_expected_phase_error'] <= Q//2 for q in x['queries']), 'query evidence')
    require(x['failures'] == sum(not q['exact_answer'] for q in x['queries']), 'failure count mismatch')
    require(x['max_expected_phase_error'] == max(q['max_expected_phase_error'] for q in x['queries']), 'maximum mismatch')
    require(hashlib.sha256(b''.join(v.to_bytes(8, 'little') for v in x['cdf_table'])).hexdigest() == 'bc68011d7224eb5dcb649a206c70697d4e6bce32af77966f4ebd60c0683cac2e', 'pinned sampler CDF mismatch')
    mean = sampler(tuple(x['cdf_table']))
    deterministic = [(65 + ceildiv(Q, 1 << (p['query_bits'] + 1)) + 1) * v + ceildiv(Q, 1 << 21) + 1 + Q % 65536 for v in x['column_sums']]
    require(x['deterministic_bounds'] == deterministic, 'deterministic bounds mismatch')
    return mean, deterministic


def analyze(x):
    mean, deterministic = validate(x)
    threshold = x['threshold']
    result = dict(query_bits=x['params']['query_bits'], status='not certified', analytical_pass=False, independent_review='pending',
                  failures=x['failures'], queries=len(x['queries']),
                  min_deterministic_allowance=threshold-max(deterministic),
                  max_expected_phase_error=x['max_expected_phase_error'],
                  threshold_utilization=x['max_expected_phase_error']/threshold,
                  independent_sampler_model=True, prg_replacement_required=True)
    if x['failures']:
        result.update(status='decoding failure', reason='wrong plaintext observed')
    blocks = x['blocks']
    if blocks is None:
        require(max(deterministic) >= threshold, 'weights absent despite positive deterministic allowance')
        result['reason'] = 'deterministic sufficient budget exhausted; no decoding failure implied'
        return result
    require(len(blocks) == 6 and [b['block'] for b in blocks] == list(range(6)), 'incomplete/duplicate blocks')
    worst = None
    for b in blocks:
        require(b['exact_digits'] is True and b['ntt_action_crosschecks'] == 2, 'missing extraction checks')
        for key in ('sum_squares', 'sum_absolute', 'sum_signed', 'maximum_weight'):
            require(len(b[key]) == N and all(integer(v) for v in b[key]), 'weight coefficient coverage')
        for k, (sq, ab, si, mx) in enumerate(zip(b['sum_squares'], b['sum_absolute'], b['sum_signed'], b['maximum_weight'])):
            require(0 <= mx <= ab and abs(si) <= ab and mx*mx <= sq <= ab*ab and sq <= mx*ab, 'inconsistent weight statistics')
            allowance = threshold - deterministic[b['block'] * N + k] - ceildiv(abs(mean * si), 1 << 64)
            # Optimize the quadratic MGF exponent exactly, subject to
            # lambda max|W| <= 1/4. Coarse power-of-two lambda choices can
            # incorrectly leave an otherwise adequate sufficient bound weak.
            if allowance <= 0:
                exponent, lam = -1, Fraction(0)
            elif sq == 0:
                # All weights vanish, hence this output has no random key noise.
                exponent, lam = 10**9, Fraction(0)
            else:
                lam = min(Fraction(allowance, 200 * sq), Fraction(1, 4 * mx))
                natural = lam * allowance - 100 * lam * lam * sq
                # ln(2) < 694/1000; conversion is conservative, without floats.
                bits = natural * Fraction(1000, 694)
                exponent = bits.numerator // bits.denominator
            item = dict(exponent_bits_floor=exponent, block=b['block'], coefficient=k,
                        lambda_numerator=lam.numerator, lambda_denominator=lam.denominator,
                        key_allowance=allowance, maximum_weight=mx, sum_squares=sq)
            if worst is None or exponent < worst['exponent_bits_floor']:
                worst = item
    bound = (2 * COLS - 1).bit_length() - worst['exponent_bits_floor']
    passed = bound <= -128 and worst['key_allowance'] > 0
    result.update(analytical_pass=passed, union_log2_upper=bound, limiting_coefficient=worst)
    if x['failures'] == 0:
        result['reason'] = ('analytical target met; independent review pending' if passed
                            else 'current sufficient bound does not meet 2^-128 per full query')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('files', nargs='+', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    rows = []
    for path in args.files:
        data = path.read_bytes()
        x = json.loads(gzip.decompress(data) if path.suffix == ".gz" else data)
        rows.append(dict(file=path.name, sha256=hashlib.sha256(data).hexdigest(),
                         rows=x['rows'], used_rows=x['used_rows'], pattern=x['pattern'], shard=x['shard'],
                         **analyze(x)))
    text = json.dumps(rows, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end='')


if __name__ == '__main__':
    main()
