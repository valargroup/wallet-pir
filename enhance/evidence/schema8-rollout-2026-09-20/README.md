# Schema-8 rollout, September 20, 2026

Raw state and isolated qualification for the 29-record (schema 8) layout at
source revision `c26c8f038536171aa70cae37d608b19c35b282cb`.

Read [status](../../docs/status.md) for what was and was not deployed. This
directory holds the bytes; it draws no conclusion the files do not support.

## Artifacts

Built on `wallet-pir-ci-01` (`s-8vcpu-16gb-amd`, DO-Premium-AMD) with
`RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"` and
`cargo build --locked --release`. `x86-64-v3` rather than `native` because the
build host is AMD and the workers are Intel `c-4`.

| Binary | SHA-256 |
|---|---|
| `enhance-pir-server` | `d33e4159179b5888cc946d37b415857dcd5a7949a4a704923a72c5d0c15f7584` |
| `enhance-pir-worker` | `d2b59f30a35fb2e3a823de96d452ae088bf5bd8ed28244381c6e218d9d5f406b` |
| `enhance-pir-cli` | `003615f774c63fa3374831c65b9d1301b20d6dfc8c2f464cc9e1c314ba586bff` |
| `enhance-pir-qualify` | `150a91ac649d9f22ed2c0d4b4714995fa24ceb2ed5c977a3f75d9a8e21aee279` |

Verified by digest on the coordinator and on both worker replicas.

## Isolated memory qualification

`enhance-pir-qualify` against a worker started by `systemd-run` under the
production cgroup settings, read back from the unit rather than assumed:
`MemoryHigh=6G`, `MemoryMax=7G`, `MemorySwapMax=2G`, `CPUQuota=400%`. Ten
publications, synthetic records, no production worker involved.

| Shards | Publications | Queries | Mismatches | Peak | `memory.events` | Swap |
|---:|---:|---:|---:|---:|---|---:|
| 3 | 10 | 4,114 | 0 | 6,040,612,864 B (5,760 MiB) | all zero | 0 |

Cold preparation 22.69 s, longest publication 11.41 s, paired-client p99
0.219 s, retained-session queries answered. `passed: true` in
`qualification-3shards-result.json`, whose own `scope` field is the honest
limit: a remote exact-answer fixture. It is not the six-hour, 300-publication,
full-capacity, failover qualification, and nothing here should be read as one.

The peak climbs with the retained window and most of the growth is late:
3,748 MiB at two publications, 4,327 at four, 5,125 at six, 5,318 at seven,
5,760 at ten. A soak shorter than the retention window understates it.

**The host is AMD and production is Intel.** Resident bytes are data-structure
sizes and should carry across; the timings should not be read as production
latency.

## Journal preparation

`enhance-pir-server --prepare-only` into `/srv/zakura/enhance-data-r29` on the
coordinator, replaying canonical blocks from Ironwood activation while
production continued serving schema 7 from the untouched
`/srv/zakura/enhance-data-v7`.

The prepared journal was checked against the live one by content, not by
trusting the re-ingest: the schema-8 `records.bin` prefix covering the first
109,849 positions hashes to
`e68877574abc14ed2919f70b8b551c081c9175da10bebbbfdfcc9010bad3bd98`, equal to the
same prefix of the live schema-7 file. The 737-byte record encoding is unchanged
between the two schemas -- only the number packed into a PIR row changed -- so
the journals are expected to be byte-identical, and this confirms it rather than
assuming it.

## Stage captures

`collect.sh <stage>` writes `<stage>-init.json`, `<stage>-health.json`,
`<stage>-coordinator-unit.txt` and a per-worker cgroup/disk capture. It is
read-only. `before-cutover-*` is the schema-7 fleet immediately before any
change: 467,912 positions, seven shards, 65,536 logical rows.
