# Evidence retention

- [Enhance and Status](../enhance/evidence/README.md)
- [Transparent history and txid display](../transparent/evidence/README.md)
- [Receiver directory](../receiver/evidence/README.md)

## Current evidence

Keep concise decision and acceptance notes, essential certificates and independent
oracle provenance. Retain the relevant failed attempts and limitations with every
claim. A result is bound to its source revision, configuration, workload and
measurement conditions; source code and historical captures do not establish
present deployment or qualification.

Essential captured files are immutable. Corrections are new notes or captures,
never edited measurements. Verify the current raw proofs from the repository root:

```sh
shasum -a 256 -c evidence/SHA256SUMS
```

## Historical runs

The complete pre-cleanup tree is pinned by the published tag
`archive/pre-hygiene-2026-10-16`, commit
`c00c2101e85fb9518ef8509f4478ed097ce49814`.
[Browse the original evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/README.md), including
original checksum manifests, logs, inputs, failed runs and experiment sources.
A retained note's historical link uses this full commit and original path.
Retrieve individual files without restoring the whole tree:

```sh
git show c00c2101e85fb9518ef8509f4478ed097ce49814:<original-path>
```

All raw files retained in the current tree match their pre-cleanup bytes. Raw
captures and paths inside them were not rewritten. The current `SHA256SUMS`
lists only current proofs; original full-run checksums remain in the archive.
See the [historical cleanup ledger](https://github.com/valargroup/wallet-pir/blob/3765536ee363af7646cc70d8c11ea4da27818ef5/docs/cleanup-2026-09-14.md) for the removal boundary.

## New development runs

Write exploratory scripts and generated output under ignored `.local-pir/`, or
retain temporary CI diagnostics as CI artifacts. They do not belong in commits.
Promote a script into a maintained tool with tests only when it has a reusable
consumer. Consumed fixtures belong beside their tests or tools, with independent
provenance and checksums where required.

Promote evidence only when it supports an acceptance decision, a correctness or
security argument, an unresolved incident, or a design decision still governing
code. Keep one concise note with exact source/build identity, command and flags,
relevant configuration and host/workload conditions, expected and actual results,
failures, limitations, and links or hashes for the proof needed to check the claim.
Preserve immutable raw inputs needed to interpret the claim; do not rely only on
an expiring CI artifact. Add bounded allowances with a reason in
`tools/ci/hygiene.toml` for essential data and include its digest in `SHA256SUMS`.
Do not add complete inventories, copied source trees, task reports or repeated
terminal captures when they add no necessary evidence. See the
[development workflow](../docs/development.md#keep-changes-focused).
