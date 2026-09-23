# Enhance PIR changelog

## 0.0.1 — Unreleased

- Introduce the `enhance-pir` client and protocol crate for schema 11,
  `ironwood-enhance-pir-v6`, with 653-byte Ironwood ciphertext-suffix records
  and the `simplepir-p16-q48-v1` query profile.
- Provide the optional `enhance-pir-cli` diagnostic client and the coordinated
  `enhance-pir-server` and `enhance-pir-load-test` binaries.
- Include manifest and session validation, private row queries, and
  generation-bound responses. Wallets remain responsible for accepting the
  chain anchor and authenticating reconstructed notes.

This entry describes the source prepared for the first release. Production
promotion requires separate hardware and operational qualification; the
repository's dated evidence does not qualify this source tree automatically.
