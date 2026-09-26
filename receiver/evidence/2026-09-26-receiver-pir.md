# Receiver to Enhance integration

September 26, 2026. The Studio's complete receiver publication was served over
loopback using the new encrypted receiver protocol. The returned position then
selected an encrypted Enhance v7 query against the isolated integration service,
through an SSH tunnel. No wallet state or production service was accessed.

The known refund returned one payment at height 3,496,114, transaction index 16,
Action index 0, position 610503. Its compact fields matched the public chain
fixture. Enhance returned the matching ciphertext suffix, value commitment, and
outgoing ciphertext. `Action::from_payment` joined the two records, and library
authenticated zero-OVK recovery returned the expected receiver. The full Action
bytes also matched the independent fixture.

The receiver terminal block at 3,497,109 and Enhance generation 446's anchor at
3,497,346 were independently checked against canonical RPC. Exact hashes, revision,
wire sizes, and the single-query timing are in [the evidence record](2026-09-26-receiver-pir.json).

This validates private retrieval and public-output authentication. It does not
validate wallet ownership, received-note nullifier tracking, witness construction,
wallet insertion, or restored spending. Those remain the next integration work.

Run the public fixture test with the verified receiver manifest and an isolated
Enhance origin reachable locally:

```sh
RECEIVER_MAINNET_MANIFEST=/path/to/publications/current.json \
ENHANCE_PIR_ORIGIN=http://127.0.0.1:18381 \
cargo test -p receiver-pir-server --test http \
  known_mainnet_refund_through_receiver_and_enhance_pir -- --ignored --nocapture
```

Seven receiver-directory tests and the focused receiver client/service tests
cover authentication, corrupted setup, mismatched requests/revisions/anchors,
continuation errors, budget exhaustion, absence, and oversized requests. The
live integration test is opt-in because it needs a large local publication and
an isolated service. Hardware integration is deferred.
