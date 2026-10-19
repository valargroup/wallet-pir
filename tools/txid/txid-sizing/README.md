# Offline txid sizing and evidence verification

This directory retains offline analysis and verification for historical
display-v1 sizing studies, plus the maintained Rust journal census. Historical
display-v1 models are not the current display codec or a production PIR client.
Read the [findings](../../../docs/txid/transparent-txid-sizing-and-anonymity-findings.md)
before interpreting their results. `UNQUALIFIED` is intentional.

## Coordinator journal census

The [read-only census instructions](JOURNAL-CENSUS.md) describe the Rust exporter's
journal reader, including display-v2x journals, output definitions, source identity
and required ingest receipt. This command does not require the removed Python
acquisition pipeline. Retain original journals, sidecars and ingest receipts.
The reader's compiled identity cannot establish the original ingest's identity.

## Retained offline checks

```sh
python3 -m unittest discover -s tools/txid/txid-sizing -p 'test_*.py'
cargo test --locked --profile release-fast \
  --manifest-path tools/txid/txid-sizing/export/Cargo.toml
python3 tools/txid/txid-sizing/verify_evidence.py --evidence /path/to/restored-run
```

The verifier requires an explicit restored run directory. It checks hashes,
source pins and deterministic report regeneration; retained source pins are
verified at their original Git revisions rather than against evolving code.
Keep immutable raw inputs and provenance outside Git. Historical report bundles
remain at `archive/pre-hygiene-2026-10-16`.

`analyze.py` independently models display-v1 bytes and packing. The Rust exporter
tests invoke it to cross-check candidate rows, directory bytes and overflow pages;
the frozen input in `fixtures/` remains an independent conformance oracle.
Other thresholds, compact codecs, routing choices and geometry sweeps are
counterfactuals. Distinct txids, fragments, occupied rows and encrypted requests
are separate measures. A scan-byte proxy is not measured CPU time or latency.

`survey.py` and `day_analysis.py` regenerate probability-sample reports from
retained inputs. Their finite-population corrections and confidence intervals
use blocks as the sampling units. Sparse or unobserved tails do not establish
absence; sampled minima cannot qualify the true minimum population. Frozen
segments, refresh/timing cohorts and uniform openings remain assumptions.
`geometry.py` budgets layouts from illustrative populations or survey estimates;
it does not establish full-chain packing, native memory use or latency.

The Python `census.py` remains a dependency of the retained analysis modules;
its historical RPC acquisition interface is distinct from the maintained Rust
journal census. Offline verification does not invoke that acquisition interface.

## Historical acquisition and campaign reproduction

The vector fetcher, one-day sampler, resumable full-chain acquisition pipeline,
aggregate runner and acquisition-specific evidence/provenance helpers are no
longer maintained here. Reproduce those campaigns using the
[complete historical tools and instructions](https://github.com/valargroup/wallet-pir/tree/113a7d9533cdea6f79119620128cba4f8d761b58/tools/txid/txid-sizing)
with their original inputs and source pins. The historical checkout preserves
the removed sampling-design and acquisition tests alongside their consumers.
