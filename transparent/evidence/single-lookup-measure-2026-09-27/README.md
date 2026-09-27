# Single-lookup directory measurement, 2026-09-27

Phase C local measurement of directory choice tables (see the
[offline census](../single-lookup-census-2026-09-27/README.md) and
[architecture update §2](../../docs/architecture_update.md#2-single-lookup-directories-for-sealed-shards)).
The same journal slice was published twice:

- **off:** no choice tables, two directory queries per matched script;
- **all:** a choice table in every manifest, the tail's included.

Each set was served in turn by a local shard server. Wallets from the pinned sample
synced against it over loopback HTTP with real PIR. Every recovered ledger was
checked against the sample's expected digest.

This is a paired local measurement on one host. It is not a fleet, WAN or mobile
measurement, and it changes no deployment setting.

| Field | Value |
|---|---|
| Source | `72c9031d` (branch `single-lookup-directory`), release profile, built on the host. Binary sha256: server `a59526e2…1d2b`, loadtest `21b50742…72b4`, shard-publish `b3c0e960…cd24e` |
| Host | `roman-ipir-bench-8vcpu`, 8 vCPU Xeon Platinum 8358, 31 GiB, Ubuntu 24.04.4 (see the census run's `host.txt`). The server and the load client share the host. |
| Journal | The recent slice from the [census run](../single-lookup-census-2026-09-27/README.md): heights 3,262,749–3,473,686, 4,185,700 events, anchor `0000000000755137…0be1d`. The parent hash `0000000000b5d4b7…176a` (height 3,262,748) was read from the coordinator's journal index. |
| Publication | `shard-publish --recent-geometry recent-8k --through 3473686 --directory-choice {off,all}`, schema `transparent-shard-v7`. 14 shards (13 sealed, 1 tail) with identical boundaries and byte-identical `directory.0.bin` in both sets. Ids start at 0, so placement salts differ from production ids 160–173. Maps: `shards-off.json`, `shards-all.json`; records: `publication-*.json`. |
| Workload | `derive-sample.py` keeps the 716 [sample](../workload-sample-2026-09-08/README.md) clients whose required range lies inside the slice (sample sha256 `be4eb6c2…98e6`). `--classes restore-6m,multi-script,catch-up-1d,catch-up-7d,catch-up-30d`, seed 1. Both variants therefore sync the same wallets in the same order. |
| Procedure | `run.sh`: for each variant a fresh server (`--cache-bytes 12 GiB --pilot-cold`), an unrecorded warm-up pass, then steps 1, 8 and 32. Run order off, all, all, off. |
| Output | `report-*.json` and `loadtest-*.log` (raw), `server-*.log`, `summary.tsv` / `summary.json` from `analyze.py` |

## Results

**Correctness:** 939 of 939 syncs completed and were exact, with 0 failed, 0
mismatched and no 503s. The two repetitions of each variant produced identical
byte counts, and their latencies differ by a few percent.

**Payload per sync** (measured, means over all syncs in the step, both
repetitions pooled; latency is repetition 1):

| Class | Conc. | n per variant | off bytes | all bytes | Change | Directory queries off → all | p50 sync s off → all |
|---|---:|---:|---:|---:|---:|---:|---:|
| restore-6m | 1 | 10 | 3,223,256 | 2,411,238 | −25.2% | 12.8 → 6.4 | 0.40 → 0.22 |
| restore-6m | 8 | 20 | 3,013,560 | 2,279,429 | −24.4% | 11.6 → 5.8 | 1.01 → 0.56 |
| restore-6m | 32 | 64 | 3,050,049 | 2,331,086 | −23.6% | 11.4 → 5.7 | 4.14 → 2.27 |
| multi-script | 1 | 10 | 18,929,153 | 11,519,504 | −39.1% | 112.8 → 56.4 | 3.94 → 2.17 |
| multi-script | 8 | 19 / 20 | 17,911,890 | 10,870,080 | −39.3% | 106.9 → 53.4 | 9.96 → 5.52 |
| multi-script | 32 | 64 | 25,492,194 | 18,294,378 | −28.2% | 109.6 → 54.8 | 41.3 → 23.3 |
| catch-up-30d | 32 | 64 | 1,119,609 | 806,229 | −28.0% | 4.9 → 2.5 | 1.50 → 0.77 |
| catch-up-7d | 32 | 66 / 64 | 856,044 | 571,468 | −33.2% | 4.5 → 2.25 | 1.54 → 0.78 |
| catch-up-1d | 32 | 64 | 1,072,243 | 720,548 | −32.8% | 5.4 → 2.7 | 2.26 → 1.16 |

`summary.tsv` has every step. Directory queries per sync are exactly halved in
every row. Page queries per sync are identical between variants. Manifest bytes
rise from about 6.2 KB to 44.5 KB per restore-6m sync: the base64 tables, including
the tail's, since every manifest carries one. The step-32 multi-script mix is
heavier (68.7 page queries per sync against 14.5 at step 8), so its relative saving
is lower. The same wallets appear in both variants.

**Throughput** (completed syncs/s, repetition 1 → 2):

| Step | off | all |
|---|---:|---:|
| 1 | 0.98, 0.98 | 1.71, 1.72 |
| 8 | 2.61, 2.79 | 4.66, 4.75 |
| 32 | 1.53, 1.53 | 1.96, 1.97 |

Both variants saturate the shared 8 vCPU host at step 32. In both, throughput
falls below step 8, and the load tool stops stepping.

**Against the census projection:** restore-6m measured −23.6% to −25.2%,
against a projected −23.9% (8K, sealed only; −24.1% with the tail). Multi-script
measured −39% at steps 1 and 8, against a projected −35%.

## Limits

- Loopback on one host. Latency here reflects server and client CPU, not network
  round trips. Removing a sequential directory round trip per matched pair should
  save more wall time on a WAN, but that is not measured here.
- Sample sizes per step are small: 5–33 syncs per class. The two repetitions
  agree, but these are not population estimates.
- Client CPU and memory for table evaluation are not measured separately. The
  table costs one SHA-256 per matched script and shard.
- Wallets here run this branch's `transparent-wallet`. The production wallet
  (wallet-libraries) is not updated.
- The publication covers the recent slice only, with shard ids from 0.

## Outcome

This supports promoting single-lookup directories at `recent-8k`:
- the directory saving the projection claimed is measured;
- ledgers are exact;
- the manifest overhead is small next to the saving.

Remaining before any production use:
- port the change to wallet-libraries;
- run a WAN/fleet comparison (fleet series r4 against r3) after a candidate
  publication with `--directory-choice sealed` or `all`;
- decide whether tails carry tables (the `all` variant was measured here).
