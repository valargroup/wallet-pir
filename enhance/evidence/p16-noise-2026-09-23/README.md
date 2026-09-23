# p16 noise qualification — 2026-09-23

**Decision: not cleared for an unrestricted production rollout.** Current-data
numerical results meet the requested `2^-128` full-query target, but dense larger
databases exhaust the current sufficient budget. The independent code-and-mathematics review accepted the conditional bound;
see `independent-review.md`. This is not an external cryptographic audit. No decoding failure has been observed in the
completed cases. These are distinct claims.

The assessed wallet source is `8e6b6c5` (schema 11), with IPIR+SP pinned to
`dac5b050cfa00770405f9d6b464b8adb2b17a3c0`. Historical `2^-143` results were for
the previous gadget decomposition and are not reused here.

## Priority results

All entries below use six instances / 12,288 output coefficients and 128 fresh
secrets and evaluation-key sets. Bounds union over the entire query, assuming
independent finite-CDF sampler draws; PRG replacement and OS entropy assumptions
remain separate. A numerical pass is not an independent review.

| Case | Occupied / domain rows | Exact answers | Maximum threshold use | Analytical result |
|---|---:|---:|---:|---|
| All-maximum reference | 8,192 / 8,192 | 128 / 128 | 8.96% | `2^-216`, target met |
| All-maximum largest domain | 32,768 / 32,768 | 128 / 128 | 9.83% | Not certified: deterministic budget exhausted |
| Canonical generation 69 data projected to schema 11 | 17,419 / 32,768 | 128 / 128 | 9.09% | `2^-185`, target met |
| Generation 71 projection | 17,421 / 32,768 | 128 / 128 | 9.11% | `2^-186`, target met |
| Generation 72 projection | 17,422 / 32,768 | 128 / 128 | 8.99% | `2^-186`, target met |
| Generation 73 projection | 17,423 / 32,768 | 128 / 128 | 9.32% | `2^-185`, target met |

`priority-results.json` contains exact numbers. Compressed source evidence is
included beside this report and can be checked without running Rust:

```sh
python3 enhance/tools/noise-analysis/verify.py enhance/evidence/p16-noise-2026-09-23/*.json.gz
```

The reference 8K result used the portable worker kernel. The campaign and other
priority results use runtime-selected worker kernels compared against the
portable monolithic reference. Source-identity files and compressed source archives
distinguish the original reference, v2 campaign, and accelerated extractors.
Machine-readable verifier output retains its generic independent-review-pending
label; the separate review report records the completed conditional review.

## Why the wide dense cases are not certified

With p16/q46, the deterministic allowance alone, before key-error tails, is:

| All-maximum occupied rows | Decoding threshold minus deterministic budget |
|---:|---:|
| 4,096 | 360,242,708,475 |
| 8,192 | 205,089,382,395 |
| 16,384 | -105,217,269,765 |
| 32,768 | -725,830,574,085 |

A negative allowance is a limitation of this sufficient bound, not a measured
wrong answer. More empirical queries cannot repair the proof gap. Increasing
query precision could restore allowance, but would require a separately
qualified profile and wallet/server compatibility work. At 32K maximum density,
48 query bits are needed just to make this deterministic allowance positive;
that alone does not establish the complete `2^-128` bound. Alternatively, a
stronger reviewed correctness argument is needed. No protocol parameters have
been changed by this work.

The verifier optimizes the Chernoff parameter as an exact rational, constrained
by the local MGF domain. Restricting it to the historical dyadic choices gave
only `2^-113` on the reference fixture; optimization gives `2^-216` for the same
weights. This is an analytical improvement, not a change in the sampler or keys.

## Retained deployment captures

The canonical coordinator was stopped during capture while a separate schema-10
exercise endpoint ran. Retained canonical generations 69–73 contain four
unique database/setup pairs. All 15 padded-unit content hashes matched the
read-only captured records; identities are in `retained-snapshot-capture.json`.
Generations 69 and 70 share a database/setup pair.

Those publications predate the exact top-digit change: the recorded release
`b1863b1` pins IPIR+SP `2259726`. Reconstructing the original schema-10 rows with
today's dependency fails the published-material hash check, as expected for the
changed decomposition. The tool rejects that mismatch. **These deployed pairs
are not certified by this campaign.** No service or deployment was changed.

The projected-data case drops the first 84 bytes from each captured 737-byte
record (32-byte ephemeral key plus 52-byte ciphertext prefix), retaining the
653 bytes specified by schema 11. It preserves the shard ID and row geometry,
but rebuilds public packing material with the current dependency. It is
candidate evidence, not a certificate for the old published setup.

## Full campaign and verification

The resumable campaign covers both occupancy edges of all 12 production-derived
unit layouts, six fixture patterns, three public shard setups, and 128 fresh
queries per case: 432 cases / 55,296 queries per platform. The macOS ARM64 campaign completed all 432 cases / 55,296 queries with zero
wrong answers. 300 cases meet the original `2^-128` analytical target and 132 do
not. Compressed complete summaries and the execution manifest are retained here.
The Linux baseline was stopped after partial coverage to prioritize the selected
q48 profile; incomplete Linux coverage is not treated as passed. The user selected
a separate `2^-78` correctness target for q48, without changing the target attached
to this historical q46 evidence.

Completed harness checks:

- Exhaustive tiny-ring basis-vector comparison of grouped weight moments with
  the packing backend, plus two full-size public error-action crosschecks.
- Full-degree exact equality of reference and accelerated extractors on ARM and
  native AVX512 Linux, plus end-to-end equality on 8K maximum and generation 69.
- Six Python verifier tests, including malformed/incomplete evidence,
  profile mismatch, exhausted budgets, decoding failures, and the optimized
  rational Chernoff parameter.
- Release build, formatting, and all-target Clippy with warnings denied.
- Partitioned hints and each query intermediate compared with monolithic
  reference evaluation in every completed case.

The existing production persistence/reload benchmark completed successfully: all
2K/4K/8K units persisted and reloaded, all four query domains reused and reloaded
without canonical rereads, and 16 exact-answer queries passed. See
`persistence.jsonl`. This is functional evidence, not hardware qualification.
All four unique captured-data projections are recorded above.

For production acceptance, finish the required coverage, resolve the wide-dense
proof gap, retain the reviewed extractor/bound and certify the
actual release's snapshot/setup pair. Continued publication needs fresh
certificates or a reviewed theorem covering future snapshots. This PR adds
analysis tooling and evidence only; it does not enforce a publication gate.

Completed ARM numerical results by query domain:

| Domain | Cases | Meet original bound | Wrong answers |
|---:|---:|---:|---:|
| 4,096 | 72 | 72 | 0 |
| 8,192 | 36 | 36 | 0 |
| 16,384 | 108 | 93 | 0 |
| 32,768 | 216 | 99 | 0 |
