# Streaming artifact review fixes — September 22, 2026

This supplement records fixes to the [initial streaming implementation](../streaming-artifacts-2026-09-22/README.md). The original evidence is unchanged. Schema remains 8 and artifact format remains 7; no migration is required.

## Review outcomes

- **Corrupt pinned CRS recovery:** publication readers share terminal failure state. Preparation validates cached CRS before returning success, including malformed headers whose decoder may stop before checksum verification. Failed artifacts can be reloaded or rebuilt for the same row digest. A failed repair preserves the old query runtime. Regression coverage exercises truncation, early decoder cancellation, persistence failure, atomic operator replacement, and exact query answers through recovery.
- **Preparation slot lifetime:** blocking work returns its owned permit to the async installer, which holds it through cache insertion. Regression tests block insertion and cancel preparation to verify that another load cannot overlap the protected work.
- **Validation-only read overhead:** the scanner checks framing and consumes coefficient payloads in chunks bounded to 64 KiB. A counting reader checks reduced call counts and the maximum read size, including a row crossing the buffer boundary. Partial reads and interrupted reads do not incorrectly poison a publication.

Disk I/O remains streamed with bounded buffers. The final query database and coordinator's decoded CRS still occupy memory as required by the algorithms. Preparing an already cached shard now reads and verifies its CRS; the cost of that extra validation and corruption recovery is not measured by the load-only experiment below.

## Validation

`make check` passed: formatting, Clippy, operations/tool/report checks, documentation links, and the full release workspace suite (617 Rust tests passed, 2 existing tests ignored). All 46 server unit tests passed, including five new regression tests.

## Non-blocking performance investigation

Five cached loads before the review fixes and five after, each in a fresh process using the same schema-8 fixture and a warm OS file cache:

| Median | Before review fixes | After review fixes |
|---|---:|---:|
| Cached load | 1.044375 s | 1.044879 s |
| Peak process RSS | 208,601,088 bytes | 208,633,856 bytes |

There is **no demonstrated end-to-end speedup**. The regression test establishes fewer scanner read calls, but this small sequential sample on a shared developer machine cannot isolate their timing contribution. Both versions remain around 199 MiB peak RSS. This is neither production qualification nor an HTTP transport benchmark.

## Evidence and reproduction

[Manifest](manifest.json) records source hashes, geometry, host, exact commands, artifact hashes, binary hashes, and limitations. [Summary](summary.json) contains every timing and RSS sample; the numbered `before-*` and `after-*` files preserve harness output and macOS `time -l` output. [Build log](build.log) and [release check log](make-check.log.gz) retain validation output. [Checksums](SHA256SUMS) cover this supplement.

The two source snapshots are the parent record's [candidate patch](../streaming-artifacts-2026-09-22/candidate.patch) and this record's [candidate patch](candidate.patch), each applied independently to base commit `93fa9da7ba274e4d7f6f14e4b1b514b16bdca8be`. Use separate fresh checkouts and output directories. Build the parent's [harness](../streaming-artifacts-2026-09-22/harness.rs) with its [Cargo template](../streaming-artifacts-2026-09-22/Cargo.toml.template), substituting each checkout path, and this record's [harness lockfile](harness.Cargo.lock). The parent's [reproduction script](../streaming-artifacts-2026-09-22/reproduce.py) shows harness setup and extraction of baseline modules from the base commit.

Create one fixture with `wallet-pir-stream-bench candidate cold <fresh-artifact-root>`, then run `/usr/bin/time -l <binary> candidate load <artifact-root>` five times per source snapshot, preserving JSON stdout and time stderr separately. Keep builds and tests outside the timed runs. Paths in the manifest identify the captured run; use fresh local paths for reproduction. Never overwrite retained evidence.
