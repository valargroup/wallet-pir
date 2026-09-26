# Enhance PIR changelog

## 0.0.1 — Unreleased

- Allow an explicitly selected canonical RPC without authentication through
  `--zakura-no-auth`, mutually exclusive with cookie and fixture modes.

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

This entry describes the source prepared for the first release. Production
promotion requires separate hardware and operational qualification; the
repository's dated evidence does not qualify this source tree automatically.
