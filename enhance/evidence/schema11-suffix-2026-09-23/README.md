# Schema-11 suffix records — local validation

The server matches wallet-libraries PR #28 at
`de3ec78f31b6fd184596fc952fe4f78d3a63cd0a`: 653-byte records, 33 records per row,
schema 11 and `ironwood-enhance-pir-v5`. The server uses ipir-sp
`dac5b050cfa00770405f9d6b464b8adb2b17a3c0`; the pinned wallet uses
`225972648cc2982abfac66ba5b7a3930b223051a`. Real loopback tests verify their
interoperability without modifying wallet-libraries.

## Width comparison

Apple M4 Max, 128 GiB RAM, macOS; `release-fast` build. Each case runs in a
separate process, at the same 4,096-row geometry and p16/q46 profile. Three warmup
queries precede ten measured queries. All thirteen answers in each case were
byte-exact. This is a small local sample, not a production capacity measurement.

| Metric | 737-byte records | 653-byte records |
| --- | ---: | ---: |
| Raw row bytes | 24,321 | 21,549 |
| Raw bytes for 4,096 rows | 99,618,816 | 88,264,704 |
| PIR instances / database columns | 6 / 12,288 | 6 / 12,288 |
| Expanded u16 database bytes | 100,663,296 | 100,663,296 |
| Persisted artifact bytes | 302,039,528 | 302,039,528 |
| Published setup bytes | 86,016 | 86,016 |
| Query bytes, including header | 109,596 | 109,596 |
| Response bytes, including header | 30,748 | 30,748 |
| Process peak RSS bytes | 2,964,504,576 | 2,964,668,416 |
| Median server evaluation + packing | 5.225 ms | 5.346 ms |

The raw storage reduction is 84/737 = **11.4%**. It does not cross a PIR
instance boundary, so expanded storage, artifacts and network sizes remain the
same. The latency sample does not establish a speed improvement. Peak RSS
includes preprocessing, client setup and packing, not just resident worker data.
Keep existing capacity reservations until separately qualified.

Reproduce from this checkout:

```sh
cargo build -p enhance-pir-server --example suffix_width --profile release-fast --locked
/usr/bin/time -l target/release-fast/examples/suffix_width 737
/usr/bin/time -l target/release-fast/examples/suffix_width 653
```

Raw outputs are `width737.json`, `width653.json` and their `.time` files. The
example uses the current production PIR implementation for both widths; it
isolates width changes rather than comparing separate historical binaries.

## Correctness and compatibility

- Protocol/client unit tests: 19 passed. Server unit tests: 75 passed.
- Real HTTP lifecycle suite: 7 passed; 2 existing full-size hardware tests ignored.
  Coverage includes generation retention, failover, restart, row repair and placement.
- Canonical transaction oracle: independently sliced historical transaction bytes
  agree with the suffix codec and records retrieved across a row boundary.
- Incoming and outgoing recovery: authenticates reconstructed ciphertext and
  rejects corrupt suffixes, mismatched compact fields, commitments and keys.
- Canonical RPC ingestion, reorg rejection, rewind and restart pass with the
  suffix representation; the historical operator payloads remain rejection fixtures.
- Cutover tests reject 737-byte journals and old controller/worker state without
  changing durable bytes; legacy serving commands exit before creating data.
- Preprocessing regression rejects schema-10 artifacts despite equal PIR dimensions.
- Pinned-wallet harness: 2 passed. It checks byte-exact row-boundary queries,
  generation expiry and fresh acceptance, plus two real compact-scanned SQLite
  wallet actions queried in one row. Duplicate identities retain their outcomes;
  a corrupt live response rejects the entire batch; valid suffixes apply atomically.

Formatting and strict Clippy pass for client, server and load-test targets.
Operational Python tests pass (121 passed, 3 skipped); tooling tests pass (17).

Reproduce the cross-repository tests with:

```sh
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries
```

See [deployment](../../docs/deployment.md) for the clean cutover contract.
No remote deployment, production load test, or full-size fleet qualification
was performed for this change.
