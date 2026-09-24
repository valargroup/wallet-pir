# Historical v6 wallet/server q48 interoperability

This standalone harness pins wallet-libraries commit
`9b190657d129d08e964623d0ecc1d8e4ffb31b1d`, which implements protocol v6.
It is preserved historical tooling, outside the active workspace. Its historical packing API calls
and v6 SDK pin are incompatible with the current v7 server; do not
use this directory on current main to qualify wallet interoperability.

For the matching v6 server and harness, use repository revision
`f26595aca5684e2e1dc6c941a194b64905bff1f5` in a separate checkout:

```sh
git worktree add --detach ../wallet-pir-q48-v6 f26595aca5684e2e1dc6c941a194b64905bff1f5
cd ../wallet-pir-q48-v6
RAYON_NUM_THREADS=2 cargo run --locked --release \
  --manifest-path enhance/tools/q48-interop/Cargo.toml > interop.jsonl
```

That harness builds server hints, serializes manifests and sessions through JSON
into the independent wallet SDK, and round-trips SDK encrypted query bodies
through the matching server engine. The wallet compares each decoded record byte
for byte. Its twelve cases cover unit allocations across 4K, 8K, 16K and 32K
query domains, partial final rows, and both sides of unit boundaries.

Current server HTTP integration coverage lives in
[`packing_http.rs`](../../services/enhance-pir-server/tests/packing_http.rs).
That test uses the repository's native client; it does not replace independent
wallet SDK interoperability or hardware qualification. See the current
[qualification record](../../docs/qualification.md) for dated wallet evidence.
