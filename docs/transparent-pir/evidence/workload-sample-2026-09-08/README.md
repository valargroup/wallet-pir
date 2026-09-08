# Load-test workload sample, 2026-09-08

The input every transparent load run cites: 1,080 synthetic wallets, 120 per
class, drawn from the pinned journal by `script-sample` with seed 1, each
carrying the SHA-256 of the events an exact recovery over its range must
hold and the count of those events. A load run reports a sync as exact only
when its store digests to this value.

| Field | Value |
|---|---|
| Run | [Sample transparent load workload, run 34186533065](https://github.com/valargroup/enhance-pir/actions/runs/34186533065) |
| Tool commit | `6d9ac786b88172c96b0f75df09fe2ba80b819f6f` |
| Set | genesis `00040fe8…dce08`, heights 0–3,473,686, cutoff 3,262,749, anchor `0000000000755137…0be1d`; matches the [publication](../publication-2026-09-08/README.md) |
| Cost | 3 min 56 s wall, peak RSS 933,008 KiB on the coordinator |

| Class | Scripts per wallet (p50) | Events (p50 / max) | Starts from |
|---|---:|---:|---|
| unused | 4 | 0 / 0 | genesis; filters only, no private work |
| small-active | 1 | 2 / 4 | its own first event |
| catch-up-1d, -7d, -30d | 4 | 3–4 / 158–797 | one, seven, thirty days below the anchor |
| restore-6m | 10 | 11 / 225 | the cutoff |
| restore-old | 3 | 9 / 4,452 | genesis |
| multi-script | 40 | 169 / 56,703 | the cutoff |
| reused-tail | 1 | 3,434 / 1,491,291 | its own first event; the heaviest public scripts |

The scripts are public on-chain scripts grouped synthetically; no wallet is
described and no user population is represented. Class weights in a load
run are equal by construction; a product mix is a separate assumption to be
stated with the run. `expected_digest` is SHA-256 over the canonical 96-byte
event records in `[required_from, anchor]`, canonical order, deduplicated.
