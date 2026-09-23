# P16Q48 qualification — 2026-09-23

Status: selected candidate; full qualification in progress. No production
deployment or full-matrix clearance is claimed by this report.

The operator selected 48-bit query transport and accepted a correctness target
of at most `2^-78` per complete six-instance query (12,288 decoded coefficients).
This is a decoding-error target, not a privacy-security estimate. Q48 transport
is deterministic post-processing of the same full-modulus RLWE query; the
sampler, modulus, dimension, plaintext layout, packing decomposition and
20-bit response precision are unchanged.

The source candidate is `2a06d5f29cb5409c7369dc3fc0d12fc7a9137570`, with IPIR
`6f74a2d754b58934f925fc8a347f46620148ad47`. It uses protocol v6, schema 11,
and profile `simplepir-p16-q48-v1`. The independent wallet counterpart is
`9b190657d129d08e964623d0ecc1d8e4ffb31b1d` in wallet-libraries PR #29.

## Recorded results

The actual dense 32K maximum-value fixture passes the `2^-78` sufficient bound
under each of three public setups. All 384 fresh queries decoded correctly.
The remaining deterministic noise allowance is 98,790,563,835, against a
decoding threshold of 549,755,813,886. This is an upper bound under the stated
sampler model, not an estimate of observed failure frequency.

`priority-results.json` contains verifier summaries. Compressed raw results
bind the database, public setup, sampler, extracted moments, and executable.
`source-sha256.json` records extractor and verifier source hashes. Generic
verifier output deliberately does not infer review or production clearance.

The full ARM64 campaign and native AVX512 Linux campaign each require 432 cases
and 55,296 fresh queries. Linux work is partitioned by public setup across two
hosts. Their final summaries and cross-platform comparison are still pending.
Exact production release/snapshot/setup acceptance and actual wallet/server
interoperability also remain required before deployment.

## Communication

These are cached-setup application request-plus-response sizes, including
56 bytes of framing and excluding HTTP/TLS overhead.

| Domain rows | Upload bytes | Download bytes | Total bytes | Change from q46 |
|---:|---:|---:|---:|---:|
| 4,096 | 110,620 | 30,748 | 141,368 | +1,024 |
| 8,192 | 135,196 | 30,748 | 165,944 | +2,048 |
| 16,384 | 184,348 | 30,748 | 215,096 | +4,096 |
| 32,768 | 282,652 | 30,748 | 313,400 | +8,192 |

Public setup remains 84 KiB raw / 112 KiB base64 per cached shard session,
plus metadata. At 32K, q48 saves 4 KiB per query compared with q49.

## Independent review

The explicitly authorized independent review approved the q48 profile,
acceptance-policy mapping, and compatibility changes with no blocking issue.
It independently passed eight verifier tests, six dependency parameter tests,
both state-preservation rejection tests, and the encrypted runtime round-trip
and cache-restart test with compatibility negatives.

The verifier applies the 78-bit target only to the exact q48 dependency and
precision. Historical q46 and q49 evidence retain their 128-bit target.
Old controller and worker state are rejected before modification or recovery.
This review approves the code changes; it does not replace the outstanding
matrix, interoperability, or exact production-pair qualification. The extractor
is trusted research tooling, not an authenticated publication gate.
