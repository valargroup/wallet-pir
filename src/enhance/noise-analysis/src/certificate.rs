//! Research-only grouped error weights. Adapted from ipir-sp 0beedf6.
//! Each original Gaussian error is accumulated across its automorphic reuse
//! BEFORE squaring. No historical carry correction: digits come from the pinned
//! exact decomposition, including its full top digit.
use inspiring::{PackingKeys, QueryPackPreprocessed, RlweParams, TopKeyImages};
use rayon::prelude::*;
use serde_json::{json, Value};
use spiral_rs::poly::{from_ntt_alloc, PolyMatrix, PolyMatrixRaw};

fn public_error(family: usize, j: usize, t: usize, challenge: usize) -> i64 {
    let mut x = (family * 71 + j * 13 + t * 17 + challenge * 101) as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((x ^ (x >> 31)) % 131) as i64 - 65
}

pub fn weights(
    r: &RlweParams,
    pre: &[QueryPackPreprocessed<'_>],
    top: &TopKeyImages<'_>,
) -> Vec<Value> {
    weights_using(r, pre, top, true)
}

fn weights_using(
    r: &RlweParams,
    pre: &[QueryPackPreprocessed<'_>],
    top: &TopKeyImages<'_>,
    fast: bool,
) -> Vec<Value> {
    let n = r.d;
    let ell = r.gadget.ell;
    let exps: Vec<usize> = top
        .kg_body_left_tables
        .iter()
        .rev()
        .chain(top.kg_body_right_tables.iter().rev())
        .map(|t| t.exponent() as usize)
        .collect();
    assert_eq!(exps.len(), n - 2);
    let keys: Vec<_> = (0..2)
        .map(|challenge| {
            let make = |family| {
                let mut mat = PolyMatrixRaw::zero(&r.spiral, 1, ell);
                for j in 0..ell {
                    for t in 0..n {
                        mat.get_poly_mut(0, j)[t] =
                            i128::from(public_error(family, j, t, challenge))
                                .rem_euclid(i128::from(r.q)) as u64;
                    }
                }
                mat.ntt()
            };
            PackingKeys {
                kg_body: make(0),
                kh_body: make(1),
            }
        })
        .collect();
    let oracle: Vec<_> = keys
        .iter()
        .map(|key| {
            ipir_sp::server::pack_intermediate_blocks(&vec![0; pre.len() * n], key, top, pre)
                .unwrap()
        })
        .collect();
    // Identical blocks (e.g. all-maximum fixtures) have identical weights.
    let mut cache = std::collections::HashMap::<Vec<u8>, Value>::new();
    pre.iter().enumerate().map(|(block_idx, block)| {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        for digit in &block.digits_ntt { for &v in digit.as_slice() { digest.update(v.to_le_bytes()); }}
        let hash = digest.finalize().to_vec();
        if let Some(v) = cache.get(&hash) {
            let mut v = v.clone(); v["block"] = json!(block_idx); return v;
        }
        let digits: Vec<Vec<i64>> = block.digits_ntt.iter().map(|d| {
            from_ntt_alloc(d).as_slice().iter().map(|&v| {
                if v > r.q/2 { (i128::from(v)-i128::from(r.q)) as i64 } else { v as i64 }
            }).collect()
        }).collect();
        assert_eq!(digits.len(), n-1);
        let z = 1_i64 << r.gadget.bits_per;
        for d in &digits {
            assert_eq!(d.len(), ell*n);
            assert!(d[..(ell-1)*n].iter().all(|v| (-z/2..z/2).contains(v)));
            assert!(d[(ell-1)*n..].iter().all(|v| (0..=z).contains(v)));
        }
        let fast_g = fast.then(|| crate::fast_weights::grouped(r, block, top));
        let (sq, absolute, signed, action, maximum) = (0..n).into_par_iter().map(|t| {
            let mut w = vec![0_i64; ell*n];
            if let Some(g) = &fast_g {
                for j in 0..ell { w[j*n..(j+1)*n].copy_from_slice(&g[j][t*n..(t+1)*n]); }
            } else {
            for (step, &exp) in exps.iter().enumerate() {
                let pos = t*exp % (2*n); let shift=pos%n; let sign=if pos<n {1} else {-1};
                for j in 0..ell {
                    let src=&digits[step][j*n..(j+1)*n]; let dst=&mut w[j*n..(j+1)*n];
                    for k in shift..n { dst[k] += sign*src[k-shift]; }
                    for k in 0..shift { dst[k] -= sign*src[n+k-shift]; }
                }
            }
            }
            let mut sq=vec![0_u128;n]; let mut ab=vec![0_u128;n]; let mut si=vec![0_i128;n];
            let mut ac=vec![[0_i128;2];n]; let mut mx=vec![0_u128;n];
            for family in 0..2 { for j in 0..ell { for k in 0..n {
                let v=if family==0 {w[j*n+k]} else if k>=t {digits[n-2][j*n+k-t]} else {-digits[n-2][j*n+n+k-t]};
                let v=i128::from(v);
                sq[k]+=(v*v) as u128; ab[k]+=v.unsigned_abs(); si[k]+=v; mx[k]=mx[k].max(v.unsigned_abs());
                for (challenge, a) in ac[k].iter_mut().enumerate() { *a+=v*i128::from(public_error(family,j,t,challenge)); }
            }}}
            (sq,ab,si,ac,mx)
        }).reduce(||(vec![0;n],vec![0;n],vec![0;n],vec![[0;2];n],vec![0;n]), |mut a,b| {
            for k in 0..n {a.0[k]+=b.0[k];a.1[k]+=b.1[k];a.2[k]+=b.2[k]; for c in 0..2 {a.3[k][c]+=b.3[k][c];}a.4[k]=a.4[k].max(b.4[k]);} a
        });
        for challenge in 0..2 {
            let actual=from_ntt_alloc(&oracle[challenge][block_idx].inner);
            for (k,ac) in action.iter().enumerate() {
                assert_eq!(ac[challenge].rem_euclid(i128::from(r.q)) as u64, actual.get_poly(1,0)[k], "grouped/NTT mismatch at {block_idx}/{k}");
            }
        }
        let value=json!({"block":block_idx,"sum_squares":sq,"sum_absolute":absolute,"sum_signed":signed,
            "maximum_weight":maximum,"ntt_action_crosschecks":2,"exact_digits":true,"digits_sha256":hex::encode(&hash)});
        cache.insert(hash,value.clone()); value
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use inspiring::GadgetParams;

    #[test]
    #[ignore = "explicit full-degree reference/fast equivalence check"]
    fn fast_matches_full_degree_reference() {
        let profile = ipir_sp::ProductionSimplePirParams::new(
            4096,
            2048 * 16,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .unwrap();
        let r = profile.rlwe();
        let mut crs = PolyMatrixRaw::zero(&r.spiral, r.d, 1);
        for (i, v) in crs.as_mut_slice().iter_mut().enumerate() {
            *v = (i as u64).wrapping_mul(0x9e3779b97f4a7c15).rotate_left(13) % r.q;
        }
        let pre = vec![QueryPackPreprocessed::build(r, &crs.ntt()).unwrap()];
        let top = TopKeyImages::build(r);
        let at = std::time::Instant::now();
        let reference = weights_using(r, &pre, &top, false);
        let reference_seconds = at.elapsed().as_secs_f64();
        let at = std::time::Instant::now();
        let fast = weights_using(r, &pre, &top, true);
        assert_eq!(fast, reference);
        eprintln!(
            "full-degree exact reference equality: reference={reference_seconds:.3}s fast={:.3}s",
            at.elapsed().as_secs_f64()
        );
    }

    #[test]
    fn grouped_weights_match_every_tiny_basis_vector() {
        let r = RlweParams::new(
            8,
            12289,
            4,
            3.2,
            GadgetParams {
                bits_per: 3,
                ell: 5,
            },
        )
        .unwrap();
        let mut crs = PolyMatrixRaw::zero(&r.spiral, r.d, 1);
        for (i, v) in crs.as_mut_slice().iter_mut().enumerate() {
            *v = (i as u64 * 197 + 31) % r.q;
        }
        let pre = vec![QueryPackPreprocessed::build(&r, &crs.ntt()).unwrap()];
        let top = TopKeyImages::build(&r);
        let grouped = weights(&r, &pre, &top);
        let fast = crate::fast_weights::grouped(&r, &pre[0], &top);
        let mut sq = vec![0u128; r.d];
        let mut ab = vec![0u128; r.d];
        let mut si = vec![0i128; r.d];
        let mut mx = vec![0u128; r.d];
        // A dense independent oracle: inject every original error basis vector
        // through the backend and center the output. Tiny weights cannot wrap q/2.
        for family in 0..2 {
            for (j, fast_digit) in fast.iter().enumerate() {
                for t in 0..r.d {
                    let mut g = PolyMatrixRaw::zero(&r.spiral, 1, r.gadget.ell);
                    let mut h = PolyMatrixRaw::zero(&r.spiral, 1, r.gadget.ell);
                    if family == 0 {
                        g.get_poly_mut(0, j)[t] = 1;
                    } else {
                        h.get_poly_mut(0, j)[t] = 1;
                    }
                    let key = PackingKeys {
                        kg_body: g.ntt(),
                        kh_body: h.ntt(),
                    };
                    let packed = pre[0].pack_b(&vec![0; r.d], &key, &top).unwrap();
                    let raw = from_ntt_alloc(&packed.inner);
                    for (k, &v) in raw.get_poly(1, 0).iter().enumerate() {
                        let w = if v > r.q / 2 {
                            v as i128 - r.q as i128
                        } else {
                            v as i128
                        };
                        if family == 0 {
                            assert_eq!(fast_digit[t * r.d + k] as i128, w);
                        }
                        assert!(w.abs() < 1000);
                        sq[k] += (w * w) as u128;
                        ab[k] += w.unsigned_abs();
                        si[k] += w;
                        mx[k] = mx[k].max(w.unsigned_abs());
                    }
                }
            }
        }
        assert_eq!(grouped[0]["sum_squares"], json!(sq));
        assert_eq!(grouped[0]["sum_absolute"], json!(ab));
        assert_eq!(grouped[0]["sum_signed"], json!(si));
        assert_eq!(grouped[0]["maximum_weight"], json!(mx));
    }
}
