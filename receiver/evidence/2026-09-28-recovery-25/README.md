# Controlled 25-key recovery test

September 28, 2026. All six runs passed. Ordinary scanning was faster overall on
this short synthetic history. PIR reduced scan time but added lookup and import work.

## Results

Medians of three alternating trials per mode on an Apple M3 Ultra:

| Phase | Ordinary scan | Private recovery |
| --- | ---: | ---: |
| Total including wallet setup | 436.7 ms | 701.1 ms |
| Wallet/cache setup | 134.9 ms | 137.2 ms |
| Compact scanning | 169.0 ms | 44.1 ms |
| Receiver lookups | 0.0 ms | 328.6 ms |
| Enhance fetches | 116.6 ms | 116.5 ms |
| Authentication and private note import | 5.0 ms | 63.7 ms |

Each run used a fresh file-backed wallet with the same account, birthday and 25 keys.
The 257-block fixture has 4097 Ironwood Actions, five 100000-zatoshi payments and one
later spend. Both modes ended with exactly the same five notes and spent state.
The four unspent notes had valid witnesses read back from their wallet databases.
Total unspent value was 400000 zatoshis. No transaction was proved or broadcast.

## Traffic

Private discovery made 25 encrypted receiver queries, plus manifest, public-parameter
and common-witness downloads. This used 3380500 uploaded and 146681 downloaded body
bytes. Each receiver query uploaded 135220 bytes and downloaded 5172 bytes.

Both paths made five Enhance queries and two setup downloads. Each used 264260
uploaded and 290085 downloaded body bytes. Including both services, private recovery
used 4081526 body bytes, versus 554345 for the baseline. HTTP/TLS overhead is excluded.
The fixture has 8192 receiver rows and a 2224-byte witness file. These are not mainnet
publication sizes.

## Scope

This tests the real scan, SQLite recovery and encrypted HTTP code with a deterministic
synthetic chain. Compact blocks were already available in a local cache. Servers ran
on the same host. Fixture construction and server startup were outside timing.
Each trial used fresh clients. Trial order was scan/PIR, PIR/scan, scan/PIR.
No artificial network delay was added. Per-phase medians need not sum to total median.

The check covers note state and inclusion witnesses, not a complete Vizor seed restore.
UI transaction enrichment, outgoing recovery, lightwalletd downloads, WAN/Tor latency,
publication lag and transaction proving are excluded. Both paths authenticate memos.
The baseline relies on the scan's stored notes and witnesses. Private recovery also
inserts its notes and witnesses. Post-run correctness checks are outside timing for
both paths. The production 50-address policy is unchanged.

## Reproduction

See [the harness instructions](../../tools/recovery-benchmark/README.md).
`versions.json` pins source commits, build profiles, binary hashes and lockfile hashes.
`fixture.json` includes the compact-chain digest and exact expected note set.
`results.json` contains all measurements and HTTP body counts. `summary.json` contains
the calculated medians. Locked builds, dependency metadata/tree checks and Rust format
checks passed. This evidence uses the committed harness source.

The handwritten tooling and instructions form one cohesive change of about 1000 lines.
The standalone generated Cargo.lock adds 4300 lines because of the wallet dependency
graph. Evidence is kept in a separate commit. No production service or existing wallet
was changed.
