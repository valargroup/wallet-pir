# Evidence

- [Transparent recovery](../transparent/evidence/README.md): accepted milestones, correctness, fleet and filter measurements.
- [Enhance](../enhance/evidence/README.md): reported performance and preflight results, with provenance limits.

## Retention and historical paths

Retain acceptance results, relevant failed attempts, unresolved incidents and the
raw inputs, configuration, source identities and logs needed to interpret them.
Superseded diagnostics can be deleted after dependency review; document the reason
and source revision in the [cleanup ledger](../docs/cleanup-2026-09-14.md).

Retained raw files are immutable. A correction is a new note or run, not a rewritten
measurement. Dated failures remain failures even if a later attempt succeeds.
Navigation in Markdown may be updated when files move.

[Historical path mappings](historical-paths.json) map old repository locations to
this layout. Paths and commands inside captured JSON, logs, scripts, source manifests
and archives describe the original checkout or capture host; do not rewrite them.
Use the recorded source revision to reproduce a historical run. Links to removed
investigations point to immutable Git history and provide context, not current
instructions. The top-level [SHA256SUMS](SHA256SUMS) records the retained raw bytes
at cleanup; run `shasum -a 256 -c evidence/SHA256SUMS` from the repository root.
Per-run checksums use their run directory as the base. Where navigation edits
changed a checksummed Markdown note, `SHA256SUMS` now hashes that note's current
bytes and the untouched capture manifest is kept as `SHA256SUMS.original`.
The M3 September 10 README already disagreed with its capture checksum before
cleanup; this pre-existing documentation mismatch is not a raw-data change.
The other six changed document digests reflect navigation edits. All 632 original
non-Markdown artifacts, including those five capture manifests, retain their bytes.
A source manifest identifies historical source, not the current tree.

## Required run metadata

Every new run directory must include a machine-readable manifest and short results note:

- Run id, UTC time, source/tool commit, exact command/flags and dependency/build profile.
- Backend/crates, schema, named geometry and row dimensions, seal policy, packing/key-reuse mode.
- Network/genesis, journal identity, inclusive height range, anchor hash/time, cutoff height/algorithm, event and script counts.
- Actual host SKU, region, CPU model/sharing, RAM, storage, OS; client device and network.
- Workload definition, script-to-wallet grouping, concurrency and duration, repetitions, filter/setup/runtime cache state and revision churn.
- Raw outputs, failures/timeouts/incomplete counts, query and completed-sync rates, latency distribution, memory and disk peaks.
- Separate upload, response, setup, filters and transport bytes; decimal GB versus binary GiB; measured versus projected values and formulas.
- Comparison baseline with identical coverage/workload, limitations and acceptance result.


Distinguish measurements, derived arithmetic, projections and deployment decisions.
Never infer wallet capacity from isolated scan bandwidth, add tier percentiles,
or omit failed/incomplete work from a rate or latency denominator.
