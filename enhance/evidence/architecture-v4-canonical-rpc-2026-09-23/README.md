# Canonical-mode RPC ingestion integration — September 23, 2026

The actual `enhance-pir-v4 coordinator` executable runs against a local JSON-RPC
test double, using its canonical mode and journal. Two real worker HTTP listeners
serve encrypted PIR queries. No live service, developer cookie, or infrastructure
is used; `test:test` is a public fixture credential accepted only by the test.

The RPC returns the existing public Ironwood transaction inside synthetic block
envelopes. Those envelopes are parsable, not consensus-valid or evidence of chain
inclusion. The test checks authenticated RPC access, block and tree metadata
methods, canonical-source mode, and exact records against the frozen transaction
byte oracles. Replacing the block hash at the same height requires journal rewind
and replay; record coverage stays two rather than growing to four. An old client
still queries its retained generation. Restarting the coordinator reopens the
journal, resumes RPC polling and preserves the same published generation without
duplicating records; current queries match both oracle records.

`v4_rpc` passed in 29.67 seconds. Clippy for the test passed with warnings denied.
The full-CI test selection now includes `v4_http`, `v4_canonical_record`,
`v4_note_recovery`, and `v4_rpc`. The two full-size HTTP campaigns remain explicitly
ignored by default. All four selected integration executables compile locally;
workflow actionlint passes. Remote GitHub Actions has not been dispatched or
verified, and local test evidence does not establish CI runner memory capacity.

This closes the bounded local RPC-to-journal-to-publication integration gap. It
is not independent snapshot-wide extraction, consensus validation, live archive
compatibility, downstream wallet conformance, or production qualification.
