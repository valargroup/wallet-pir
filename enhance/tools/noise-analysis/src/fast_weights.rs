//! Exact public error-weight extraction by two applications of the backend NTT.
//!
//! For a base error monomial X^t, W_t(X) = sum_a D_a(X) X^(a t).
//! At beta_v = psi^e_v (e_v odd), write a=2m+1:
//! W_t(beta_v) = beta_v^t sum_m D_(2m+1)(beta_v) (psi²)^(m e_v t).
//! Thus a cyclic NTT across m gives every t for one v. Inverse negacyclic
//! NTT across v then recovers W_t's integer coefficients. Twisting by psi^-m
//! lets the existing negacyclic backend perform that cyclic NTT. We infer its
//! root ordering from NTT(X), rather than assuming a platform-specific layout.
//! All values are public, and the checked integer bound makes centered lifting
//! exact (no ambiguity modulo q). No floating-point FFT or new crypto primitive.
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use rayon::prelude::*;
use spiral_rs::poly::{from_ntt_alloc, PolyMatrix, PolyMatrixNTT, PolyMatrixRaw};
use std::collections::HashMap;

fn mul(a: u64, b: u64, q: u64) -> u64 {
    ((a as u128 * b as u128) % q as u128) as u64
}

/// Result[j][t*n+k] is the signed integer weight of base g-error e[j,t]
/// in output coefficient k, already summed over all its automorphic reuse.
pub fn grouped(
    r: &RlweParams,
    block: &QueryPackPreprocessed<'_>,
    top: &TopKeyImages<'_>,
) -> Vec<Vec<i64>> {
    let n = r.d;
    let q = r.q;
    assert_eq!(r.spiral.crt_count, 1);
    assert!(2 * (n as u64 - 2) * (1_u64 << r.gadget.bits_per) < q);
    let exps: Vec<_> = top
        .kg_body_left_tables
        .iter()
        .rev()
        .chain(top.kg_body_right_tables.iter().rev())
        .map(|t| t.exponent() as usize % (2 * n))
        .collect();
    assert_eq!(exps.len(), n - 2);
    let mut x = PolyMatrixRaw::zero(&r.spiral, 1, 1);
    x.get_poly_mut(0, 0)[1] = 1;
    let roots: Vec<_> = x.ntt().as_slice().iter().map(|v| v % q).collect();
    assert_eq!(roots.len(), n);
    let psi = roots[0];
    let mut powers = vec![1; 2 * n];
    for i in 1..2 * n {
        powers[i] = mul(powers[i - 1], psi, q);
    }
    assert_eq!(powers[n], q - 1);
    assert_eq!(mul(powers[2 * n - 1], psi, q), 1);
    let index: HashMap<_, _> = (0..n).map(|i| (powers[2 * i + 1], 2 * i + 1)).collect();
    assert_eq!(index.len(), n);
    let exponents: Vec<_> = roots.iter().map(|root| *index.get(root).unwrap()).collect();
    let mut lane = vec![usize::MAX; n];
    for (v, &e) in exponents.iter().enumerate() {
        assert_eq!(lane[(e - 1) / 2], usize::MAX);
        lane[(e - 1) / 2] = v;
    }
    (0..r.gadget.ell)
        .into_par_iter()
        .map(|j| {
            let mut f = PolyMatrixRaw::zero(&r.spiral, n, 1);
            for v in 0..n {
                let row = f.get_poly_mut(v, 0);
                for (step, &a) in exps.iter().enumerate() {
                    assert_eq!(a % 2, 1);
                    let m = (a - 1) / 2;
                    row[m] = (row[m] + block.digits_ntt[step].get_poly(j, 0)[v] % q) % q;
                }
                for (m, value) in row.iter_mut().enumerate() {
                    *value = mul(*value, powers[(2 * n - m) % (2 * n)], q);
                }
            }
            let transformed = f.ntt();
            drop(f);
            let mut w = PolyMatrixNTT::zero(&r.spiral, n, 1);
            for v in 0..n {
                let mut phase = 1;
                for t in 0..n {
                    w.get_poly_mut(t, 0)[v] = mul(
                        transformed.get_poly(v, 0)[lane[exponents[v] * t % n]] % q,
                        phase,
                        q,
                    );
                    phase = mul(phase, roots[v], q);
                }
            }
            drop(transformed);
            let raw = from_ntt_alloc(&w);
            raw.as_slice()
                .iter()
                .map(|&v| {
                    let value = if v > q / 2 {
                        v as i64 - q as i64
                    } else {
                        v as i64
                    };
                    assert!(value.unsigned_abs() <= (n as u64 - 2) * (1_u64 << r.gadget.bits_per));
                    value
                })
                .collect()
        })
        .collect()
}
