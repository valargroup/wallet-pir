# Enhance PIR changelog

## 0.0.1 — Unreleased

- Introduce the `enhance-pir` client and protocol crate for schema 11,
  `ironwood-enhance-pir-v7`, with 653-byte Ironwood ciphertext-suffix records
  and the `simplepir-p16-q48-v1` query profile.
- Replace storage loans with fixed shard ownership and composed successor-tail
  domains; seal after 1,000 canonical successor blocks.
- Separate content-addressed session identity from routing and placement; persist
  deep-recovery epochs and revocations and fence admitted responses.
- Add explicit wallet routing refresh and reusable sessions, with optional
  birthday-based cover rounds. v6 framing and clients are incompatible.
- Add coordinator packing admission and retain worker resource lifetime pins.
- Provide the optional `enhance-pir-cli` diagnostic client and the coordinated
  `enhance-pir-server` and `enhance-pir-load-test` binaries.
- Include manifest and session validation, private row queries, and
  routing-, session-, request- and anchor-bound responses. Wallets remain responsible for accepting the
  chain anchor and authenticating reconstructed notes.

This entry describes the source prepared for the first release. Production
promotion requires separate hardware and operational qualification; the
repository's dated evidence does not qualify this source tree automatically.
