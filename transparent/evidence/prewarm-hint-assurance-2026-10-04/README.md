# Prewarm hint assurance, 2026-10-04

This follows [the prewarm hint record](../prewarm-hint-2026-10-03/README.md),
which review rejected on assurance, not on a confirmed wrong output. It is a
source and test change on the development hub. No timing was re-measured:
recent-worker numbers are the earlier record's, and nothing here is a
production, fleet or freshness qualification. Nothing was deployed. Its
projection of 23–29 s burst visibility remains arithmetic only, and every
production freshness, load, capacity and fault gate stays open.
[manifest.json](manifest.json) records sources, commands and hashes.

## Review points

**1. Provenance and derivation.** The module documentation of
`transparent/crates/transparent-native/src/batched_hint.rs` now states:
- Provenance. The transforms follow Longa–Naehrig (CANS 2016,
  ePrint 2016/504) Algorithms 1 and 2. Twiddle multiplication is Shoup's
  precomputed quotient, as analysed by Harvey (JSC 2014). Reconstruction is
  Garner's CRT. Nothing has been externally audited, and the record does not
  claim otherwise. Spiral's NTT, the reference's, is not reused because it
  transforms one polynomial at a time, and lane batching is the whole saving.
  The reference stays the definition, the fallback and a test oracle.
- A derivation of each step: that the root has exact order `2D`; that output
  index `k` is the evaluation at `psi^(2 brv(k)+1)`, the roots of `X^D + 1`;
  the residue range of every operation, including a proof of Shoup's
  `[0, 2p)` bound for `x < 2^32`; the signed bound
  `|S| <= D * 65535 * sum max|c_b|`, the one `prepare_public_dot` checks; why
  Garner and the `X > P/2` mapping recover `S` exactly when `2B < P`; and why
  masking the two's-complement value by `q - 1` equals the reference's
  canonical result modulo `q = 2^54`.
- Public operands. The masks expand from published seeds and the data is the
  server's own table, so variable timing reveals nothing a client lacks.
  Outputs are the reference's integers, so published masks, epochs and every
  binding over them are unchanged. No protocol, seed, geometry, wire,
  reservation or slot changed.

The new tests check the derivation's premises:
- `primes_and_roots_satisfy_the_derivation`: each prime is prime, below 2^30
  and 1 mod 2D. `psi^D = -1`, the inverse table uses `psi^-1`, `D^-1` is
  correct, and every Shoup quotient matches.
- `forward_evaluates_at_odd_powers_in_bit_reversed_order`: checked by Horner
  evaluation.
- `shoup_products_are_canonical`: extreme and 100,000 random operands per prime.

**Known-answer vectors.**
[`testdata/batched_hint_kat.py`](../../crates/transparent-native/testdata/batched_hint_kat.py)
computes the expected hints with Python integers. It uses Kronecker
substitution, one exact big-integer product per block and column, with
negacyclic folding, and no NTT or CRT. A direct schoolbook loop cross-checks
two columns per case. It wrote
[`batched_hint_kat.json`](../../crates/transparent-native/testdata/batched_hint_kat.json).
That file holds a SHA-256 over all 2,048 output columns plus ten explicit
coefficients per case. The cases are:
- `basis`: monomial masks `x^(D-1)` and `-x^5`, with monomial data. This
  covers negacyclic wrap in every column, at every lane position.
- `mixed`: three blocks of full-width masks. They include the forced boundary
  coefficients `0, 1, q/2-1, q/2, q/2+1, q-2, q-1`, plus all-`0xffff`,
  one-hot, random and `{0, 0xffff}` columns.
- `extreme`: recent-8k's four blocks with masks at both centered extremes
  (`q/2`, `q/2+1`). This is the largest sum a recent table can form.

Inputs come from splitmix64 formulas, mirrored in the Rust test.
`known_answer_vectors` requires both the batched path and the forced reference
path to reproduce every digest and sample. Generation took 3 min 23 s.

**2. Recent-only dispatch.** `TableRuntime::build` now uses the batched hint,
and the trailing-zero-block trim, only for geometries in
`BATCHED_HINT_GEOMETRIES` (`recent-8k`, `recent-4k`, `recent-4k-8k`). Any
other geometry runs the base code unchanged, `native::hint` over every block.
That includes `archive-32k`, `archive-wide` and any geometry added later. So
no archive-equivalence or archive-memory claim is needed.
`the_batched_hint_is_dispatched_for_recent_geometries_only` asserts the split
over the whole registry. It also checks that both tables' deployed masks at
every recent geometry select `Path::Batched`, so recent tails never fall back
silently.

**3. Over-capacity fallback.** `hint` now calls `hint_within(..., capacity)`
with the primes' product. Only tests pass a smaller capacity. Reaching the real
edge needs 512 blocks of extreme masks: 2^20 rows × 2,048 columns, about 4 GiB.
`capacity_edge_selects_the_path` checks that edge from actual masks (511
batched, 512 reference) and the exact integer edge of `fits`.
`over_capacity_falls_back_to_the_reference` forces the fallback and checks:
- `Path::Reference`, with output equal to `pir_native::hint` and the batched
  path;
- the switch at exactly the masks' own bound (`need + 1` batched, `need`
  reference);
- column-length refusal returned as the reference's own error;
- shape and noncanonical-mask refusals.

The reference's own capacity refusal (about 2^126) cannot be reached by any
constructible input, since it would need about 2^45 blocks, so it is not
tested. The KATs above also run through the forced fallback. Existing
malformed, shape, CRT-edge and mutation-sensitive tests are kept. So is the
encrypted-query equivalence test at recent-8k
(`the_batched_hint_runtime_is_the_reference_runtime`).

## Mutations

[mutations.log](mutations.log) records each mutation applied singly, its tests
run, then restored with `git checkout`. These ran before two clippy-only edits:
a `Hint` type alias and `is_multiple_of` in tests. Neither changes semantics.

| Mutation | Failing tests |
|---|---|
| Forward twiddles in natural order (`psi^k`) | 6, including Horner order and KATs |
| `D^-1` not folded into mask transforms | 4, including KATs |
| Shoup product without the final subtraction | 7, including Shoup and KATs |
| CRT never maps to negative | 5, including CRT edges and KATs |
| Capacity seam ignored (always batched) | `known_answer_vectors`, `over_capacity_falls_back_to_the_reference` |
| Runtime dispatches batched for every geometry | `the_batched_hint_is_dispatched_for_recent_geometries_only` |

## Validation

Results of the focused suite, clippy and `make check-fast` are in [results.md](results.md). [SHA256SUMS](SHA256SUMS) covers the retained logs and the manifest.

**First failure.** The first focused run failed two disk-cache integration
tests with `CacheError::Overloaded`:
- `restart_coalesces_restore_and_corruption_falls_back_without_losing_budget`
- `blocked_snapshot_does_not_delay_serving_or_release_owned_memory`

The cache admits builds and restores against live cgroup usage. The task
cgroup held 11.1 of 12 GiB, mostly page cache from the preceding builds. After
`memory.reclaim` on the task's own cgroup, both passed at the same source,
along with the other five `runtime::disk` tests. The failure is recorded in
[focused-first-run.log](focused-first-run.log).

## Limits

- No timing was re-measured, and no production or fleet run was made.
- The derivation is written and tested, not externally reviewed or formally
  verified.
- KAT inputs are synthetic. Real tails were compared against the reference in
  the earlier record.
- Archive geometries are unchanged and use the reference. They gain no speed.
