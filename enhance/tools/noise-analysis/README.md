# p16 noise qualification

This standalone research tool evaluates the pinned IPIR+SP implementation using
wallet geometry and production shard-composition helpers. It never contacts or
modifies a service. Its analytical results require independent review; numerical
success is intentionally not labeled certified.

## Build and test

```sh
cargo build --locked --release --manifest-path enhance/tools/noise-analysis/Cargo.toml
cargo test --locked --release --manifest-path enhance/tools/noise-analysis/Cargo.toml
python3 -m unittest discover -s enhance/tools/noise-analysis -v
```

The standalone lockfile pins research dependencies. The current candidate uses
P16Q48 (`611a2928`, ipir-sp rc.2) with an explicitly selected `2^-78` full-query correctness
target and production protocol v6. Historical q46 and q49 inputs keep their
original `2^-128` target; the verifier binds each target to its exact dependency
revision and query precision. See the [candidate evidence](../../evidence/p16-q48-2026-09-23/README.md).
On Linux use `RUSTFLAGS="-C target-cpu=native"` to exercise
the accelerated runtime-selected worker kernel against the portable monolithic
reference. On ARM the portable worker path is selected.

## Campaign

```sh
RAYON_NUM_THREADS=2 python3 enhance/tools/noise-analysis/run.py \
  --binary enhance/tools/noise-analysis/target/release/enhance-noise-analysis \
  --output /new/campaign-directory --jobs 4
```

The default campaign is 432 cases: both occupancy edges of 12 valid layouts,
six patterns, and public shard setups 0, 1, 2; 128 fresh-secret/fresh-key queries
per case. Each query covers all six output instances. Targets include both ends
of the occupied database and every unit boundary, followed by reproducible
pseudorandom targets. Raw stress fixtures fill complete coefficients; valid
schema-11 record fixtures leave one record absent to exercise partial rows and
zero padding. Fixtures are public, deterministic data, never private RNG seeds.

Run `--matrix` on the binary to enumerate production-derived shapes. One case:

```sh
RAYON_NUM_THREADS=2 enhance/tools/noise-analysis/target/release/enhance-noise-analysis \
  --used-rows 32768 --pattern max --shard 0 --queries 128 --output /new/case.json
python3 enhance/tools/noise-analysis/verify.py /new/case.json
```

Each result includes grouped weight moments for all 12,288 coefficients, CDF,
column sums, content/setup/binary hashes, individual empirical query results,
and partitioned-versus-monolithic comparisons. If the deterministic allowance
is already exhausted, weight extraction is skipped and the case is explicitly
not certified; `--force-weights` overrides this optimization. Zero empirical
failures never substitutes for the analytical bound.

Resume with the same executable/configuration and output directory. A manifest
mismatch is an error. Interrupted case files must be inspected before rerunning;
no partial case is silently counted as successful. A process error without a
complete result stops the campaign. A decoding failure is saved and remains a
failure while other cases are evaluated.

The accelerated extractor uses exact backend NTT transforms and centered lifting
to compute the same grouped weights faster. The original method remains as a
reference. Run the full-degree equivalence regression explicitly:

```sh
cargo test --locked --release --manifest-path enhance/tools/noise-analysis/Cargo.toml \
  --bin accelerated -- --include-ignored
```

When switching executables, use a new output directory and `--reuse-results DIR`
only for explicitly trusted prior results whose extractor equivalence has been
checked. The manifest records prior manifest hashes and summaries retain each
case executable hash. `compare.py LEFT RIGHT --expected-cases 432` requires full coverage and checks deterministic evidence
across platforms; fresh-query empirical samples naturally differ.

## Captured snapshots

Supply a read-only captured contiguous record file with `--pattern records
--records FILE`. Schema 11 is the default. Historical schema-10 captures require
`--record-width 737` and remain labeled schema 10. Set `--shard` to the captured
shard identity and `--expected-public-sha256` to its published session digest;
any reconstruction mismatch aborts. Validate the captured unit content hashes
against its manifest before interpreting results. Future generations require
new evidence. The tool does not certify production activation or enforce a
publication policy.

## Analytical method and limits

The extraction is adapted from ipir-sp commit `0beedf6`, recomputed from the
exact top-digit decomposition introduced at `dac5b050` and retained by the q48
candidate. No discarded-carry allowance
is used. For each original Gaussian key-error coefficient, all signed
negacyclic/automorphic contributions are combined before squaring. Two public
error vectors cross-check the action against the NTT backend. An exhaustive tiny
basis-vector test independently checks every grouped moment against backend
outputs; this does not replace independent cryptographic review.

The verifier recomputes the finite-CDF mean and proves the local centered MGF
bound `log E exp(u(X-mean)) <= 100u²` for `|u| <= 1/4`, with rational Taylor and
geometric remainder bounds. It includes query error and rounding, response
rounding, and encoding residuals in a deterministic allowance. It optimizes the
Chernoff parameter using exact fractions under the MGF-domain constraint, then
uses `ln(2) < 694/1000` and unions over all output coefficients. Sampler and
transport/profile changes are rejected. Claims assume independent sampler draws;
ChaCha20 replacement and OS entropy remain separate assumptions.

The checker validates structure and consistency, not a proof of honest extraction
from a digest alone. Source/binary hashes and independently checked captures are
part of the evidence trust boundary. The all-maximum fixture can exhaust this
sufficient budget without demonstrating a decoding failure. Improving a bound
requires a reviewed mathematical argument; do not silently change parameters.

For persistence and reload, use the repository's existing production integration
benchmark in addition to this arithmetic campaign:

```sh
cargo run --locked --profile release-fast -p enhance-pir-server \
  --example v4-preprocess -- --output /new/persistence-directory --repetitions 1
```

Historical `6f74a2d7` matrices retain their original implementation identity.
The rc.2 extractor uses the same 78-bit target under its own exact pin; rerun
qualification for this executable and compare deterministic evidence. Do not
rewrite historical case identities or treat a verifier pass as deployment approval.
