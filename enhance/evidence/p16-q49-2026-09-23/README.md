# P16Q49 qualification — 2026-09-23

**Status: superseded comparison profile, not production clearance.**
The user selected q48 with a `2^-78` correctness target after reviewing this
comparison. The q49 campaigns were stopped after partial coverage; the recorded
q49 results retain their original `2^-128` target.
The actual q49 six-instance campaign has passed the dense 32K maximum-value
fixture under all three public setups. Each gives an ideal independent-sampler
full-query failure upper bound of `2^-255`, against the requested `2^-128` target,
and each decoded all 128 fresh queries correctly. The planned full q49 campaigns on ARM64 and native AVX512 Linux were not
completed; no full-matrix q49 qualification is claimed.

The IPIR prerequisite is [ipir-sp PR #20](https://github.com/valargroup/ipir-sp/pull/20),
commit `b1c540f90f62e112c834a0f57f025e3c605e55d1`. The paired wallet is
[wallet-libraries PR #29](https://github.com/zakura-core/wallet-libraries/pull/29),
stacked on the schema-11 wallet work. This source uses protocol v6, schema 11,
and profile `simplepir-p16-q49-v1`. The sampler, RLWE parameters, plaintext
layout, packing decomposition, and 20-bit response precision are unchanged.
Only query transport precision changes cryptographically.

`priority-results.json` and the compressed source results bind the three actual
runs to their binary, database, public setup, and extracted moments. The candidate
extractor source is recorded in `source-sha256.json`; later integration changes
add rejection of old persistent state and compatibility tests. Generic verifier
output does not infer independent review or production certification.

## Why 49 bits

For the same dense 32K maximum-value database and shard-0 packing weights:

| Query bits | Remaining deterministic allowance | Full-query sufficient bound |
|---:|---:|---:|
| 46 | -725,830,574,085 | No useful bound |
| 47 | -176,083,148,805 | No useful bound |
| 48 | 98,790,563,835 | `2^-78` |
| 49 | 236,227,420,155 | `2^-255` |
| 50 | 304,945,848,315 | `2^-344` |

The 47/48/50 rows are analytical precision studies, not qualified transport
profiles. The q49 row was subsequently reproduced by actual q49 queries under
all three setups. These are sufficient upper bounds, not estimates of actual
failure frequency. The decoding threshold is 549,755,813,886; it does not change
when query precision increases. The fixed-snapshot trust and PRG/OS-entropy
assumptions from the q46 analysis still apply.

## Bandwidth

Packing keys remain 84 KiB, response ciphertext remains 30 KiB, and total request
and response framing remains 56 bytes. Cached-setup request-plus-response sizes:

| Domain rows | q46 | q49 | Increase |
|---:|---:|---:|---:|
| 4,096 | 140,344 bytes | 141,880 bytes | 1,536 bytes / 1.09% |
| 8,192 | 163,896 bytes | 166,968 bytes | 3,072 bytes / 1.87% |
| 16,384 | 211,000 bytes | 217,144 bytes | 6,144 bytes / 2.91% |
| 32,768 | 305,208 bytes | 317,496 bytes | 12,288 bytes / 4.03% |

Public setup stays 84 KiB raw / 112 KiB base64 per shard session, plus metadata.
HTTP/TLS overhead is additional. The record layout and 33-record row capacity
are unchanged.

## Review and remaining work

An independent review agent approved the profile prerequisite, privacy
post-processing argument, and verifier mathematics. This is not an external
cryptographic audit or a new lattice-security estimate. The review required
updating old positive q46 assertions; those now pass with q49. Old protocol,
session precision, cache identity, query-body length, controller state, and
worker-state rejection checks have been added.

Finish both platform matrices, the pinned actual-wallet interoperability test,
CI, and certification of the exact release/snapshot/setup pair before deployment.
Fixed-fixture success does not certify arbitrary future snapshots or public
setups. No serving service has been changed by this qualification work.
