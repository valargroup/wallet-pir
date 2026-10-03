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
