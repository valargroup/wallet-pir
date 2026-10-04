# Txid sizing evidence — UNQUALIFIED population

[Findings](../../../docs/transparent-txid-sizing-and-anonymity-findings.md) distinguish
measured sample bytes, synthetic routing controls, and geometry projections.
[Reproduction](../../tools/txid-sizing/README.md) uses no production credentials.

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

## Incomplete full-chain continuation

The [resume receipt](resume-receipt.json) and [checkpoint](full-chain-checkpoint.json)
cover only genesis through height 30,811, targeting anchor 3,502,662. The
[partial aggregates](full-chain-aggregates.json) retain inventories, prefix
frontiers and thresholds; full histograms and both SQLite databases stay in
`/home/ai-dev/.cache/wallet-pir-census/`, pinned by the receipt. These prefix
frontiers are not chain recommendations or stratified estimates.

[Resume throughput](resume-throughput.json) measured 3,000 new blocks in 228.26
seconds (13.14/s), projecting 73.38h remaining; the required 72h gate stopped
acquisition. [Inherited pipeline throughput](pipeline-throughput.json) is a
raw-only, height-string preflight and is not the resumed end-to-end rate.
The live node executable SHA is unavailable through the allowed RPC methods;
the parser pin does not attest it. No full-chain or native privacy/cost
qualification is claimed. The original vector exporter lock is verified at
PR #124's exact head; the resume receipt pins the current exporter lock.
