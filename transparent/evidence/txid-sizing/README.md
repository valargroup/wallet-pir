# Txid sizing evidence — sampled sizing, unqualified anonymity minima

[Findings](../../../docs/transparent-txid-sizing-and-anonymity-findings.md) distinguish
measured sample bytes, synthetic routing controls, and geometry projections.
[Reproduction](../../tools/txid-sizing/README.md) regenerates the compact reports offline.
Fresh acquisition requires the explicitly sanctioned read-only gateway credential.

## Completed one-day probability study

- [Fixed plan](day-plan.json), [source gate](day-source-gate.json) and [canonical recheck receipt](day-receipt.json.gz): 9,216 SRSWOR blocks across the full anchored range; no parent requests.
- [Compact sufficient statistics](day-statistics.json.gz): complete selected-block eligibility, histograms, paired fee-size bounds and public route hashes; block clusters/design weights retained.
- [Analysis](day-analysis.json) and [bootstrap](day-bootstrap.json): all requested cutoffs/frontiers, per-era/coinbase/noncoinbase inventories, uncertainty and conservative threshold selection. Recommend current-codec 128 bytes; no true anonymity minimum qualified.
- [Geometry](day-geometry.json): additive packing, reservations and query/response body models, not measured native capacity.
- [Source pins](day-sources.json), [parser build pins](day-parser-build-pins.json), [node era fields](day-node-info.json), [canonical extraction inventory](day-extraction-receipt.json.gz), [final summary inventory](day-summary-receipt.json.gz) and [vector conformance](day-vector-validation.json): exact provenance and retained raw/record checksums.

Public raw source frames and canonical records are retained, unchanged, in
`/home/ai-dev/.cache/wallet-pir-day-t-c3f424facb15494f/`; the compact artifacts
are independently report-regenerable offline. Source membership is the node’s
rechecked canonical view, not fresh full-chain consensus validation. No live node
executable SHA is available. The prior prefix and convenience vectors are never
pooled into probability estimates. All prior data artifacts below remain retained.

## Retained PR #124 convenience oracles

- [Discovery](discovery.json): examined sources, selectors/access outcomes and dataset decision.
- [Pins](sources.json): source/dependency revisions, raw/text checksums, all block hashes and sparse selection.
- [Canonical sample](canonical-sample.json): 60 complete display records, 74 explicit missing-prevout exclusions, 41 block inventories and Rust implemented-packing oracle. Scripts/values/txids are public chain facts.
- [Analysis](analysis.json): every codec/threshold, era/coinbase distributions, missingness bounds, routing intersections, policy counts and five-candidate negative controls.
- [Geometry projections](geometry-projections.json): explicit assumed workload and independent table-placement sweeps. No measured minimum or latency.
- [Checksums](SHA256SUMS): immutable compact data inputs/outputs. Regenerate with the offline verifier.

The 354,315 decoded raw block bytes remain in the pinned public upstream commit
and the existing hub source cache; `fetch_vectors.py` verifies all 41 originals.
No selected demo conclusion, private wallet data, client keys, production secrets,
or encrypted query bodies are retained. The sample is biased, covers sparse
historical heights through 1,687,121, and establishes no present full-chain census,
production threshold, complete raw-script/no-output coverage or privacy minimum.

## Retained incomplete full-chain continuation (inactive)

The [resume receipt](resume-receipt.json) and [checkpoint](full-chain-checkpoint.json)
cover only genesis through height 30,811, targeting anchor 3,502,662. The
[partial aggregates](full-chain-aggregates.json) retain inventories, prefix
frontiers and thresholds; full histograms and both SQLite databases stay in
`/home/ai-dev/.cache/wallet-pir-census/`, pinned by the receipt. These prefix
frontiers are not chain recommendations or stratified estimates.

[Resume throughput](resume-throughput.json) measured 3,000 new blocks in 228.26
seconds (13.14/s), projecting 73.38h remaining; the prior task's 72h gate stopped
acquisition. That procedure is superseded here; no continuation ran in this study. [Inherited pipeline throughput](pipeline-throughput.json) is a
raw-only, height-string preflight and is not the resumed end-to-end rate.
The live node executable SHA is unavailable through the allowed RPC methods;
the parser pin does not attest it. No full-chain or native privacy/cost
qualification is claimed. The original vector exporter lock is verified at
PR #124's exact head; the resume receipt pins the current exporter lock.
