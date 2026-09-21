# SimplePIR baseline for the Enhance database — September 20, 2026

Fresh reference-implementation measurement on the same logical database size as the earlier Enhance comparison: **466,986 records, 737 bytes each, 344,168,682 raw bytes (328.225 MiB)**. This deliberately fixes the earlier record count rather than mixing in a later live count. Synthetic data, no production mutation.

| Metric | Reference SimplePIR | Enhance, 29 records/row |
|---|---:|---:|
| Query upload | 66,720 B / 65.156 KiB | 169,992 B / 166.008 KiB |
| Response | 69,888 B / 68.250 KiB | 30,736 B / 30.016 KiB |
| Recurring total | 136,608 B / **133.406 KiB** | 200,728 B / **196.023 KiB** |
| Initial download | **71,565,312 B / 68.250 MiB hint**, plus 16-byte public seed and metadata | **115,992 B / 113.273 KiB JSON envelope** |
| Server computation p50 / p95 | **14.39 / 14.95 ms** | **75.26 / 82.12 ms** |
| Client query preparation p50 | 7.93 ms, including serialization | 11.43 ms, including serialization |
| Client decode / validation p50 | 1.06 ms | 1.22 ms |
| Correctness | 103/103 full record coefficient comparisons | 103/103 full row comparisons |

Timing comparison uses **the same Apple M4 Max machine, one execution thread per implementation, sequential runs**, 100 timed queries plus three warmups each. SimplePIR sets GOMAXPROCS=1; its upstream C kernels are single-threaded. Enhance sets RAYON_NUM_THREADS=1. These are local computation timings, not WAN latency, production Intel performance, AMD performance or maximum throughput. The earlier 42-ms Enhance result used four threads on a different AMD machine and must not be substituted into this table.

SimplePIR server timing covers Answer (including its output allocation); explicit little-endian response serialization is outside that timer. Enhance timing also includes request parsing and response serialization. Client decode scopes include full coefficient validation for SimplePIR; Enhance validates after its decoder timer. The timing figures are an implementation baseline, not a rigorously identical instruction-boundary microbenchmark or a security-normalized protocol comparison.

## Interpretation

SimplePIR uses a smaller upload because it does not upload the response-packing keys. It has a larger answer and a much larger database-dependent client hint. Its measured recurring total is 31.9% below the current Enhance transport; its initial hint is about 617 times the Enhance init envelope. The measured local server path is also substantially faster, without online cryptographic response packing and using different coefficient/kernel parameters.

For Q queries sharing an unchanged database setup, excluding tiny SimplePIR metadata:

- SimplePIR: 71,565,312 + 16 + Q × 136,608 bytes.
- Current Enhance: 115,992 + Q × 200,728 bytes.

SimplePIR amortizes its initial download relative to this Enhance transport at approximately **1,115 queries per setup**. This is calculated from measured lengths, not a session/network benchmark. Hint updates for changing databases could alter that result; incremental hint refresh was not implemented or measured here. The initial hint must match the queried database. Fetching a full new hint at every update is not asserted to be the only possible update protocol.

One SimplePIR query targets one **737-byte record**. One Enhance query returns the **29-record row** containing the requested record. This is a same-database single-query comparison, not an equal-output adjacent-record batching comparison. Fetching several useful adjacent records may favor Enhance; the table does not normalize for that workload.

## Reference parameters and actual byte counts

Pinned upstream: `https://github.com/ahenzinger/simplepir`, revision `e9020b03bf2872c75b8954e749e32408b5db87ed`. The unmodified source is archived in `reference-source.tar.gz`. This is plain SimplePIR, not DoublePIR or YPIR+SP.

Called the reference selector `PickParams(466986, 5896, 1024, 32)`:

- LWE dimension n=1024, ciphertext modulus 2^32, Gaussian sigma=6.4, plaintext modulus p=701.
- Database matrix L=17,472 and M=16,679, 624 base-p coefficients per record, 28 vertical record groups. This is upstream's near-square default; no claim of an exhaustive setup-inclusive layout optimum.
- Query has M coefficients, padded to 16,680 by the reference packed-matrix alignment: 16,680 × 4 = 66,720 bytes.
- Answer has L coefficients: 17,472 × 4 = 69,888 bytes.
- Hint is L × n coefficients: 17,472 × 1,024 × 4 = 71,565,312 bytes.
- Public seed is 16 bytes. The expanded public matrix is 68,317,184 bytes, generated from that seed; it is not another network download.
- Packed server database buffer: 388,577,280 bytes (370.576 MiB). The reference packs three 10-bit slots into a 32-bit word. Do not compare this one buffer to a full retained Enhance service's RSS.

The benchmark really serialized query, response and hint coefficients as little-endian 32-bit words and measured byte lengths. No HTTP/TLS, JSON or base64 wrapper was imposed on SimplePIR. Enhance retains its existing eight-byte request header, sixteen-byte response header and JSON/base64 init envelope. Reference SimplePIR does not provide the production generation/epoch API used by Enhance; additional integration framing would need specifying.

## Correctness and preprocessing

Used the actual reference InitCompressed and Setup, **not FakeSetup**. Public-matrix generation took 0.773 seconds; Setup including hint construction and database squishing took 21.398 seconds (hint wire serialization outside that setup timer). All 103 queries verified all 624 coefficients of the selected record. Targets include first/last records, adjacent records crossing a matrix column boundary, midpoint and an interior position.

The fixture uses deterministic base-701 digits with its highest digit zero, ensuring every logical record fits within 737 bytes. Padding records are zero. It is not a canonical wallet-record fixture. Query secrets and noise use the reference implementation's randomness.

The reference convenience recovery API returns uint64, which cannot represent a 737-byte record. The harness therefore follows its recovery equations and compares every decoded base-p digit with the original record's digits instead of validating only a truncated uint64. This avoids claiming full-record correctness from the upstream convenience return value.

Parameters are the upstream defaults; no independent security audit/equal-security proof was performed. The benchmark is not a production replacement proposal.

## Reproduction

Unpack `reference-source.tar.gz` into an isolated directory, place `main.go` at `cmd/baseline/main.go`, and run:

```
go build -o baseline ./cmd/baseline
/usr/bin/time -l ./baseline > run.jsonl 2> run.stderr
```

On this machine Go was `go1.26.1 darwin/arm64`. Upstream C flags `-O3 -march=native` compiled without modification. See `manifest.json` for hardware, versions and executable hashes.

Enhance was rebuilt from the earlier immutable harness and archived source in `enhance/evidence/layout-benchmark-2026-09-20`, rather than importing the concurrently changing production source. Those sources correspond to wallet-pir revision `48c910c72f7349e69fe5d6d8631caad90e834221` and ipir-sp revision `accc424e879d8da425fa620aad80f0f2c4e0defd`. Ran the identical benchmark arguments `29 466986 100 0 8192` with `RAYON_NUM_THREADS=1` and the earlier captured init template.

Raw results are `run.jsonl` and `enhance-one-thread.jsonl`; `.stderr` files include `/usr/bin/time -l` process memory and exit evidence. `summary.json` contains medians and nearest-rank p95. The earlier AMD host was busy with a separate qualification job and was not used or interrupted.
