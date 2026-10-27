# Enhance PIR changelog

## 0.0.1 — Unreleased

- A restarted publisher serves Enhance again sooner. Verification hashes a
  publication's objects on up to four threads instead of one, and preparation
  hashes each unit's rows once instead of twice. On the production host, where
  hashing is about 17 s of one core per publication, this shortens the gap after
  every publisher start. Each committed publication logs how long it took to build
  and verify and how much it hashed.

- A publisher stop ends Enhance publication building and verification at the
  next shard, unit or object, including between the units, hint and packing of a
  changed shard, so a stop mid-preparation no longer waits up to 26 s.

- Wallet `init` and Status `init` requests wait up to 15 s for a restarting
  publisher's authority instead of answering 503 (or, for Status, 410) at once,
  re-checking authority on every attempt. Enhance, Status and Receiver query roles
  treat a publisher that does not answer, or whose proxy answers 502–504, as
  restarting and re-poll it every second, for up to 30 tries, instead of backing
  off for 10–40 s.

- Load autonomous ingress router assignments from host JSON and enforce publisher
  compatibility before opening state or listeners. A confirmed major-version
  mismatch during synchronization stops the host with the discovery error;
  transient failures still retry.

- Read canonical blocks, fees and Status chain data through wallet-libraries'
  server-only chain adapters (`zakura-server-chain`); the server no longer
  depends on the node's crates. Fee rules, spent-output caching and batching, and
  failure behaviour are unchanged.
- Introduce the `enhance-pir` client and protocol crate for schema 11,
  `ironwood-enhance-pir-v7`, with 653-byte Ironwood ciphertext-suffix records
  and the `simplepir-p16-q48-v1` query profile.
- Replace storage loans with fixed shard ownership and composed successor-tail
  domains; seal after 1,000 canonical successor blocks.
- Separate content-addressed session identity from routing and placement; persist
  deep-recovery epochs and revocations and fence admitted responses.
- Add explicit wallet routing refresh and reusable sessions, with optional
  birthday-based cover rounds. v6 framing and clients are incompatible.
- Add packing admission and retain worker resource lifetime pins.
- Serve the manifest, session documents and queries only from query ingress
  backed by packing routers; the coordinator keeps private operator routes.
- Provide the optional `enhance-pir-cli` diagnostic client and the coordinated
  `enhance-pir-server` and `enhance-pir-load-test` binaries.
- Include manifest and session validation, private row queries, and
  routing-, session-, request- and anchor-bound responses. Wallets remain responsible for accepting the
  chain anchor and authenticating reconstructed notes.

- Publish exact whole-transaction fees for every non-coinbase transaction with
  Ironwood actions, including transparent, Sapling and Orchard parts. Spent
  outputs are resolved from the block, a bounded cache and batched
  `getrawtransaction`; an unresolvable output fails the block. Add
  `rebuild-journal` and coordinator startup adoption of a verified staged journal
  to repair historical fee-absent records. The record layout is unchanged.
- Move to ipir-sp rc.6. The experimental `native-reinspiring` build uses
  two-mask output with 29-bit rounded public masks
  (`ironwood-enhance-pir-v9-native-two-mask-m29`, control version 4). This
  halves the native packing-key upload. The same feature adds a native Status
  profile, `status-pir-v3-native-two-mask-m29`. Default q48 protocols are unchanged.
- Production hardening. The query ingress buffers the complete bounded body
  under a separate `--uploads` limit (default 256, four per client) before
  taking a forwarding permit; the packing router and legacy coordinator read
  the body before admission. Coordinator readiness recovers after a successful
  control refresh. Public 5xx/429 messages no longer echo upstream transport
  text. `ENHANCE_INTERNAL_TOKEN` optionally requires a shared bearer token on
  worker, router and ingress control routes. Public manifest and session reads
  are bounded to 64 concurrent requests. Under `native-reinspiring`, parameter
  identities bind the native params encoding, mask/query/response widths and
  column count; default v7 identities are unchanged. The interim
  `ironwood-enhance-pir-v8-native-poc` revision is retired.
- A second external worker on one host gets its own configuration file and
  loopback management port, instead of sharing the first worker's.

This entry describes the source prepared for the first release. Production
promotion requires separate hardware and operational qualification; the
repository's dated evidence does not qualify this source tree automatically.
