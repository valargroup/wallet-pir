# Dithered 44-bit query screen for Enhance and Status, 2026-10-09

Correctness reports for the native two-mask m29 profile at both query widths
the Enhance and Status servers now accept. These are 49 bits rounded to
nearest, which the in-repo clients keep sending, and 44 bits with dithered
rounding, told apart by the body's exact length. The protocols, envelopes,
parameter identities and served documents are unchanged. **These use synthetic
databases under the products' own masks and setups; none certifies a served
snapshot.**

## Method

- **What dithering changes.** The client rounds each selection coefficient up
  with probability equal to the fraction it drops, using fresh coins. The
  checker then budgets rounding as a variance term from each block's
  `query_l2_squared`, not as a worst case (16 times the column L1 at 49 bits).
  Masks and packing weights are the same at both widths.
- **Report tool.** The `native_certificate` example in
  `enhance/services/enhance-pir-server/examples/`, built with
  `native-reinspiring`, has two additions:
  - `--query-rounding dithered` emits `native-noise-two-mask-rounded-dithered-v1`.
  - `--fill random|max` builds a synthetic database. It hints that database
    under the product's masks and packing setup, uses the worst-case query term
    and records `served_public_checked: false`.

  Without these flags the report is byte-for-byte what the tool produced
  before. That was checked against `2d1b9abc` in `status` mode on an
  all-0xffff `rows.bin`, with measured and with worst-case query terms.
- **Shapes.**
  - Enhance: shard 0, every query-domain size from 4,096 to 32,768 rows, by
    12,288 columns.
  - Status: 8,192 × 6,144, under the network and salt of the generation the
    [2026-09-26 certificates](../native-certificate-2026-09-26/README.md)
    recorded.
- **Checker.** `certify_native.py` from ipir-sp `d76e61a` (PR #30), SHA-256
  `9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7`, run with
  the default `--require-bits 128`. Its certificate is conditional on the
  exported weights and idealised sampler draws.
- **Command.** `run.sh <certify_native.py>`, `release-fast` profile, on an
  Apple M4 Max with 128 GiB under macOS. It took 91 s, build included.
  `manifest.json` has the provenance and `results.json` one row per report.
  `SHA256SUMS` covers the retained files.

## Results

Certified failure bits; 128 required.

| Shape | Rows | Fill | 49 nearest | 44 dithered |
|---|---:|---|---:|---:|
| Enhance shard 0 | 4,096 | random / max | 274 / 274 | 263 / 263 |
| Enhance shard 0 | 8,192 | random / max | 258 / 265 | 240 / 246 |
| Enhance shard 0 | 16,384 | random / max | 223 / 226 | 211 / 214 |
| Enhance shard 0 | 32,768 | random / max | 158 / 163 | 166 / 172 |
| Status generation 203 setup | 8,192 | random / max | 259 / 270 | 241 / 250 |

Every shape meets 128 bits at both widths. Each dithered certificate's screen
of 49-bit nearest on the same weights equals the separate nearest report. The
32,768-row random figure at 49 bits, 158, equals the bound recorded for the
live generation-15 snapshot on 2026-09-26, which used that snapshot's hint and
the same worst-case query term.

A dithered selection is `rows * 5 / 8` bytes shorter: 20,480 B per Enhance
query at 32,768 rows and 5,120 B per Status query.

## Limits and deployment

- Servers built from this source accept both widths. The in-repo Enhance and
  Status clients, the monitor and the load test still send 49 bits, so a
  dithered query comes only from another client.
- Before such a client is used against a snapshot, that snapshot needs a
  44-bit dithered certificate as well as the 49-bit one
  (`native_certificate ... --query-rounding dithered`). The checker must be the
  `d76e61a` script above, because older checkers refuse the dithered format.
- Synthetic packing weights are representative, not bounds over every
  database. Nothing here counts decryption failures.
