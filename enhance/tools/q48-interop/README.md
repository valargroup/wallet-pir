# Actual wallet/server q48 interoperability

This standalone executable pins wallet-libraries commit
`9b190657d129d08e964623d0ecc1d8e4ffb31b1d`. It builds production server hints,
serializes manifests and sessions through JSON into the independent wallet SDK,
and round-trips SDK encrypted query bodies through the production server engine.
The wallet decodes each response and compares the requested record byte for byte.

```sh
RAYON_NUM_THREADS=2 cargo run --locked --release \
  --manifest-path enhance/tools/q48-interop/Cargo.toml > interop.jsonl
```

The twelve cases cover all supported unit allocations across 4K, 8K, 16K and
32K query domains, including partial final rows and both sides of unit
boundaries. Output reports actual encoded upload and response lengths. This
checks application wire compatibility; it does not exercise HTTP/TLS, wallet
transaction recovery, or replace the noise qualification matrix.
