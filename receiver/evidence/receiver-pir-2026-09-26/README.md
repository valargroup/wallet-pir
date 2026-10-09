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
wire sizes, and the single-query timing are in [the evidence record](results.json),
and the run metadata in [`manifest.json`](manifest.json).

This record is historical. It measured the `ironwood-receiver-pir-v1-q48` protocol,
since replaced by `ironwood-receiver-pir-v1-two-mask-m29`, whose request at 8192
rows is 77,876 bytes rather than 135,220. The opt-in test below existed at the
recorded commits but is not part of the current tree.

This validates private retrieval and public-output authentication. It does not
validate wallet ownership, received-note nullifier tracking, witness construction,
wallet insertion, or restored spending. Those remain the next integration work.

At those commits, the public fixture test ran with the verified receiver manifest
and an isolated Enhance origin reachable locally:

```sh
RECEIVER_MAINNET_MANIFEST=/path/to/publications/current.json \
ENHANCE_PIR_ORIGIN=http://127.0.0.1:18381 \
cargo test -p receiver-pir-server --test http \
  known_mainnet_refund_through_receiver_and_enhance_pir -- --ignored --nocapture
```

The Rust toolchain and Cargo build profile were not recorded, and
[`manifest.json`](manifest.json) declares both unavailable. As shown, the command
passes no `--profile`, so Cargo would build the test with its default `test`
profile, but nothing records that it ran exactly as shown or how the isolated
Enhance service was built. The `profile` in `results.json` is the q48 protocol
profile, not a Cargo profile.

Seven receiver-directory tests and the focused receiver client/service tests
cover authentication, corrupted setup, mismatched requests/revisions/anchors,
continuation errors, budget exhaustion, absence, and oversized requests. The
live integration test is opt-in because it needs a large local publication and
an isolated service. Hardware integration is deferred.
