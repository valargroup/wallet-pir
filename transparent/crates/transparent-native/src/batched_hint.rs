//! The public hint, computed exactly but batched across columns.
//!
//! [`pir_native::hint`] is the definition: `H = A * D`, one exact lifted
//! negacyclic product per mask block and column, reduced modulo `q`. It runs
//! each column alone through Reinspiring's lifted products, whose constant-time
//! scalar NTT over 42-bit primes is built for secret operands. A recent
//! worker rebuilds its tail hints at every publication, and nothing in a hint
//! is secret: the masks are published seeds and the plaintext is the server's
//! own table.
//!
//! [`hint`] computes the same integers through three 30-bit NTT primes. One
//! transform runs over a tile of columns at once, so every butterfly applies a
//! single twiddle across contiguous lanes the compiler vectorises. The integer
//! sum is reconstructed whole by CRT and only then reduced modulo `q`. The
//! result is therefore exactly the reference's whenever the primes' product
//! exceeds twice the sum's bound. That bound is checked from the masks
//! actually given, as the reference checks it; a shape beyond it is handed to
//! the reference rather than approximated.

use crate::{D, Q};
use rayon::prelude::*;
use std::sync::OnceLock;

const LOG_D: u32 = D.trailing_zeros();
/// NTT-friendly primes below 2^30, each 1 modulo 2D. Their product is just
/// under 2^90.
const PRIMES: [u32; 3] = [1_073_692_673, 1_073_668_097, 1_073_655_809];
/// Columns transformed together.
const LANES: usize = 32;
/// The largest plaintext coefficient: a u16.
const DATA_BOUND: u128 = u16::MAX as u128;

const _: () = assert!(D.is_power_of_two());

/// One prime's transform tables: Shoup pairs of the bit-reversed powers of a
/// primitive `2D`-th root and of its inverse.
struct Prime {
    p: u32,
    forward: Vec<(u32, u32)>,
    inverse: Vec<(u32, u32)>,
    /// `D^-1` modulo `p`.
    d_inverse: u32,
}

fn pow_mod(mut base: u64, mut exp: u64, p: u64) -> u64 {
    let mut out = 1u64;
    base %= p;
    while exp > 0 {
        if exp & 1 == 1 {
            out = out * base % p;
        }
        base = base * base % p;
        exp >>= 1;
    }
    out
}

fn shoup(w: u32, p: u32) -> (u32, u32) {
    (w, (((w as u64) << 32) / p as u64) as u32)
}

impl Prime {
    fn new(p: u32) -> Self {
        let p64 = p as u64;
        let order = 2 * D as u64;
        assert_eq!((p64 - 1) % order, 0, "prime is 1 mod 2D");
        // A primitive 2D-th root: psi^D = -1.
        let psi = (2..p64)
            .map(|g| pow_mod(g, (p64 - 1) / order, p64))
            .find(|&psi| pow_mod(psi, D as u64, p64) == p64 - 1)
            .expect("a primitive 2D-th root exists");
        let psi_inverse = pow_mod(psi, p64 - 2, p64);
        let reverse = |k: usize| (k.reverse_bits() >> (usize::BITS - LOG_D)) as u64;
        let table = |root: u64| -> Vec<(u32, u32)> {
            (0..D)
                .map(|k| shoup(pow_mod(root, reverse(k), p64) as u32, p))
                .collect()
        };
        Self {
            p,
            forward: table(psi),
            inverse: table(psi_inverse),
            d_inverse: pow_mod(D as u64, p64 - 2, p64) as u32,
        }
    }
}

fn primes() -> &'static [Prime; 3] {
    static TABLES: OnceLock<[Prime; 3]> = OnceLock::new();
    TABLES.get_or_init(|| PRIMES.map(Prime::new))
}

/// `x * w mod p` for `x < 2^32`, `w < p < 2^30`, with `ws` = floor(w 2^32 / p).
#[inline(always)]
fn mul(x: u32, (w, ws): (u32, u32), p: u32) -> u32 {
    let estimate = ((x as u64 * ws as u64) >> 32) as u32;
    let r = x.wrapping_mul(w).wrapping_sub(estimate.wrapping_mul(p));
    r.min(r.wrapping_sub(p))
}

#[inline(always)]
fn add(a: u32, b: u32, p: u32) -> u32 {
    let s = a + b;
    s.min(s.wrapping_sub(p))
}

#[inline(always)]
fn sub(a: u32, b: u32, p: u32) -> u32 {
    let s = a + p - b;
    s.min(s.wrapping_sub(p))
}

/// In-place negacyclic forward transform of `L` interleaved polynomials,
/// coefficients in `[0, p)`, bit-reversed output.
fn forward<const L: usize>(a: &mut [[u32; L]], prime: &Prime) {
    let p = prime.p;
    let mut t = D;
    let mut m = 1;
    while m < D {
        t >>= 1;
        for i in 0..m {
            let w = prime.forward[m + i];
            let (low, high) = a[2 * i * t..2 * (i + 1) * t].split_at_mut(t);
            for (x, y) in low.iter_mut().zip(high.iter_mut()) {
                for l in 0..L {
                    let u = x[l];
                    let v = mul(y[l], w, p);
                    x[l] = add(u, v, p);
                    y[l] = sub(u, v, p);
                }
            }
        }
        m <<= 1;
    }
}

/// The inverse of [`forward`] without the final `D^-1` scale, which callers
/// fold into an operand.
fn inverse<const L: usize>(a: &mut [[u32; L]], prime: &Prime) {
    let p = prime.p;
    let mut t = 1;
    let mut m = D;
    while m > 1 {
        let h = m >> 1;
        for i in 0..h {
            let w = prime.inverse[h + i];
            let (low, high) = a[2 * i * t..2 * (i + 1) * t].split_at_mut(t);
            for (x, y) in low.iter_mut().zip(high.iter_mut()) {
                for l in 0..L {
                    let (u, v) = (x[l], y[l]);
                    x[l] = add(u, v, p);
                    y[l] = mul(sub(u, v, p), w, p);
                }
            }
        }
        t <<= 1;
        m = h;
    }
}

/// Signed representative of a canonical residue modulo `q`.
fn centered(x: u64) -> i128 {
    if x > Q / 2 {
        x as i128 - Q as i128
    } else {
        x as i128
    }
}

/// Garner reconstruction of the signed integer with these residues, reduced
/// modulo `q`. Exact while its magnitude is below half the primes' product.
struct Crt {
    p0_inverse_mod_p1: u64,
    p01_inverse_mod_p2: u64,
    p01: u128,
    product: u128,
}

impl Crt {
    fn new() -> Self {
        let [p0, p1, p2] = PRIMES.map(|p| p as u64);
        let p01 = p0 as u128 * p1 as u128;
        Self {
            p0_inverse_mod_p1: pow_mod(p0 % p1, p1 - 2, p1),
            p01_inverse_mod_p2: pow_mod((p01 % p2 as u128) as u64, p2 - 2, p2),
            p01,
            product: p01 * p2 as u128,
        }
    }

    #[inline(always)]
    fn reduce(&self, r: [u32; 3]) -> u64 {
        let [p0, p1, p2] = PRIMES.map(|p| p as u64);
        let [r0, r1, r2] = r.map(|r| r as u64);
        let v1 = (r1 + p1 - r0 % p1) % p1 * self.p0_inverse_mod_p1 % p1;
        let low = r0 as u128 + p0 as u128 * v1 as u128;
        let v2 = (r2 + p2 - (low % p2 as u128) as u64) % p2 * self.p01_inverse_mod_p2 % p2;
        let x = low + self.p01 * v2 as u128;
        let signed = if x > self.product / 2 {
            x as i128 - self.product as i128
        } else {
            x as i128
        };
        signed as u64 & (Q - 1)
    }
}

/// Whether reconstruction is exact for masks whose largest centered
/// coefficients sum to `maxima`.
fn fits(maxima: u128, crt: &Crt) -> bool {
    maxima
        .checked_mul(D as u128 * DATA_BOUND)
        .and_then(|b| b.checked_mul(2))
        .is_some_and(|b| b < crt.product)
}

/// The public hint `H = A * D` for one segment, byte for byte
/// [`pir_native::hint`]'s, with its shape checks and arguments.
pub fn hint<'a>(
    masks: &[Vec<u64>],
    rows: usize,
    cols: usize,
    column: impl Fn(usize) -> &'a [u16] + Sync,
) -> Result<Vec<Vec<Vec<u64>>>, String> {
    if rows == 0 || !rows.is_multiple_of(D) || !cols.is_multiple_of(D) || masks.len() != rows / D {
        return Err("native hint shape".into());
    }
    let mut maxima = 0u128;
    for mask in masks {
        if mask.len() != D || mask.iter().any(|&x| x >= Q) {
            return Err("noncanonical lifted polynomial or wrong degree".into());
        }
        maxima += mask
            .iter()
            .map(|&x| centered(x).unsigned_abs())
            .max()
            .unwrap();
    }
    let crt = Crt::new();
    // Every output coefficient is a sum of D * blocks products, each at most
    // a mask coefficient times a u16. Reconstruction is exact below half the
    // product; beyond it the reference decides, including its own refusal.
    if !fits(maxima, &crt) {
        return crate::pir_hint(masks, rows, cols, column);
    }
    let primes = primes();
    // Mask transforms with D^-1 folded in, as Shoup pairs: [prime][block][row].
    let masks: Vec<Vec<Vec<(u32, u32)>>> = primes
        .iter()
        .map(|prime| {
            let p = prime.p as i128;
            masks
                .iter()
                .map(|mask| {
                    let mut poly: Vec<[u32; 1]> = mask
                        .iter()
                        .map(|&x| [centered(x).rem_euclid(p) as u32])
                        .collect();
                    forward(&mut poly, prime);
                    poly.iter()
                        .map(|[x]| {
                            let scaled =
                                (*x as u64 * prime.d_inverse as u64 % prime.p as u64) as u32;
                            shoup(scaled, prime.p)
                        })
                        .collect()
                })
                .collect()
        })
        .collect();
    let blocks = rows / D;
    let tiles: Vec<Vec<Vec<u64>>> = (0..cols / LANES)
        .into_par_iter()
        .map(|tile| -> Result<Vec<Vec<u64>>, String> {
            let first = tile * LANES;
            let data: Vec<&[u16]> = (first..first + LANES).map(&column).collect();
            if data.iter().any(|col| col.len() != rows) {
                return Err("native hint column length".into());
            }
            let mut plain = vec![[0u32; LANES]; D];
            let mut work = vec![[0u32; LANES]; D];
            let mut sums = vec![vec![[0u32; LANES]; D]; primes.len()];
            for block in 0..blocks {
                for (r, row) in plain.iter_mut().enumerate() {
                    for (lane, col) in data.iter().enumerate() {
                        row[lane] = col[block * D + r] as u32;
                    }
                }
                for ((prime, mask), sum) in primes.iter().zip(&masks).zip(&mut sums) {
                    let p = prime.p;
                    work.copy_from_slice(&plain);
                    forward(&mut work, prime);
                    for ((acc, x), &w) in sum.iter_mut().zip(&work).zip(&mask[block]) {
                        for l in 0..LANES {
                            acc[l] = add(acc[l], mul(x[l], w, p), p);
                        }
                    }
                }
            }
            for (prime, sum) in primes.iter().zip(&mut sums) {
                inverse(sum, prime);
            }
            Ok((0..LANES)
                .map(|lane| {
                    (0..D)
                        .map(|r| crt.reduce([sums[0][r][lane], sums[1][r][lane], sums[2][r][lane]]))
                        .collect()
                })
                .collect())
        })
        .collect::<Result<_, _>>()?;
    let mut columns = tiles.into_iter().flatten();
    Ok((0..cols / D)
        .map(|_| columns.by_ref().take(D).collect())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;

    /// Schoolbook negacyclic product modulo `q`.
    fn negacyclic(a: &[u64], b: &[u16]) -> Vec<u64> {
        let mut out = vec![0u64; D];
        for (i, &x) in a.iter().enumerate() {
            for (j, &y) in b.iter().enumerate() {
                let term = x.wrapping_mul(y as u64);
                let k = i + j;
                if k < D {
                    out[k] = out[k].wrapping_add(term);
                } else {
                    out[k - D] = out[k - D].wrapping_sub(term);
                }
            }
        }
        out.iter().map(|x| x & (Q - 1)).collect()
    }

    #[test]
    fn transforms_invert_exactly() {
        let mut rng = ChaCha20Rng::from_seed([3; 32]);
        for prime in primes() {
            let p = prime.p;
            let a: Vec<u32> = (0..D).map(|_| rng.gen_range(0..p)).collect();
            let mut t: Vec<[u32; 1]> = a.iter().map(|&x| [x]).collect();
            forward(&mut t, prime);
            inverse(&mut t, prime);
            let back: Vec<u32> = t
                .iter()
                .map(|[x]| (*x as u64 * prime.d_inverse as u64 % p as u64) as u32)
                .collect();
            assert_eq!(back, a, "prime {p}");
        }
    }

    #[test]
    fn crt_reconstructs_signed_values_at_the_capacity_edges() {
        let crt = Crt::new();
        let half = (crt.product / 2) as i128;
        for value in [
            0i128,
            1,
            -1,
            half,
            -half + 1,
            1 << 83,
            -(1 << 83),
            12_345_678_901_234_567,
        ] {
            let residues = PRIMES.map(|p| value.rem_euclid(p as i128) as u32);
            assert_eq!(crt.reduce(residues), value as u64 & (Q - 1), "{value}");
        }
    }

    /// Masks at both extremes of the centered range and data at both u16
    /// extremes, against an independent schoolbook product.
    #[test]
    fn extreme_operands_match_schoolbook_products() {
        let cols = D;
        let rows = 2 * D;
        let masks = vec![vec![Q / 2; D], vec![Q / 2 + 1; D]];
        let mut rng = ChaCha20Rng::from_seed([5; 32]);
        let data: Vec<Vec<u16>> = (0..cols)
            .map(|c| match c % 3 {
                0 => vec![u16::MAX; rows],
                1 => (0..rows).map(|_| rng.gen()).collect(),
                _ => vec![0; rows],
            })
            .collect();
        let fast = hint(&masks, rows, cols, |c| &data[c]).unwrap();
        for c in [0, 1, 2, 1_000, 2_047] {
            let mut expected = vec![0u64; D];
            for (block, mask) in masks.iter().enumerate() {
                let product = negacyclic(mask, &data[c][block * D..(block + 1) * D]);
                for (dst, x) in expected.iter_mut().zip(product) {
                    *dst = dst.wrapping_add(x) & (Q - 1);
                }
            }
            assert_eq!(fast[0][c], expected, "column {c}");
        }
    }

    #[test]
    fn matches_the_reference_hint_and_its_shape_refusals() {
        let mut rng = ChaCha20Rng::from_seed([9; 32]);
        let (rows, cols) = (4 * D, 2 * D);
        let masks: Vec<Vec<u64>> = (0..rows / D)
            .map(|_| (0..D).map(|_| rng.gen_range(0..Q)).collect())
            .collect();
        let data: Vec<Vec<u16>> = (0..cols)
            .map(|_| (0..rows).map(|_| rng.gen()).collect())
            .collect();
        assert_eq!(
            hint(&masks, rows, cols, |c| &data[c]).unwrap(),
            crate::pir_hint(&masks, rows, cols, |c| &data[c]).unwrap()
        );
        assert!(hint(&masks, rows + D, cols, |c| &data[c]).is_err());
        assert!(hint(&masks, rows, cols + 1, |c| &data[c]).is_err());
        assert!(hint(&masks, rows, cols, |c| &data[c][1..]).is_err());
        let mut noncanonical = masks.clone();
        noncanonical[1][7] = Q;
        assert!(hint(&noncanonical, rows, cols, |c| &data[c]).is_err());
    }

    /// Every registry geometry fits with room to spare; 512 blocks of
    /// extreme masks would not, and would be handed to the reference.
    #[test]
    fn capacity_covers_deployed_shapes_and_refuses_beyond_it() {
        let crt = Crt::new();
        let extreme = |blocks: u128| blocks * (Q / 2) as u128;
        assert!(fits(extreme(64), &crt));
        assert!(fits(extreme(511), &crt));
        assert!(!fits(extreme(512), &crt));
        assert!(!fits(u128::MAX / 2, &crt));
    }
}
