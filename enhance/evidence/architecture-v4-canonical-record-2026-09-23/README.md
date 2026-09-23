# Canonical transaction byte oracle through PIR — September 23, 2026

Two records from the checked-in public Ironwood transaction now have frozen
raw-byte oracles. The integration test checks transaction/record hashes, exact
raw offsets, Zakura-parsed action fields, expiry 3483371 and fee 10000. It then
publishes parser-produced records and retrieves both via real v4 HTTP PIR at
synthetic positions 32 and 33, across a row boundary, using both retained and
current sessions after a second publication. All returned bytes match.

[Integration test](tests.log): passed in 7.65 seconds.
[Clippy](clippy.log): passed with warnings denied. Formatting and diff checks passed.
See [fixture provenance](../../services/enhance-pir-server/tests/fixtures/ironwood-record-oracle.md).

The oracle uses fixed slices of one public transaction; offsets were initially
located with the pinned parser. It is not an independent transaction parser,
chain inclusion proof, wallet scan or decryption of these public notes. Positions
and journal block hashes are synthetic. Canonical snapshot-wide conformance,
wallet recovery, hardware qualification and live deployment remain incomplete.
