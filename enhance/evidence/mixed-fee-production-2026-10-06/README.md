# Mixed-transaction fee repair in production — October 6, 2026

The production Enhance coordinator now publishes exact whole-transaction fees for
Ironwood transactions that also have transparent, Sapling or Orchard parts. Every
historical record it had published without such a fee was regenerated from chain
and is served again. Source `2474cdbb` (full CI green), CI artifact
`enhance-pir-native-2474cdbb…` (`release`, `x86-64-v3`, `native-reinspiring`,
archive SHA-256 `add6035a9a46…eddc8`, binary `cfeaa3baa40e…6cc60`). The code and its
local tests are in [mixed-fee-publication-2026-10-06](../mixed-fee-publication-2026-10-06/README.md).
Metadata is in [manifest.json](manifest.json), checksums in [SHA256SUMS](SHA256SUMS).

This repairs published data only. Wallets that already stored a transaction without
a fee need their own retry and backfill in the wallet library; this deployment does
not change any existing wallet database.

## Before

The live journal (one unsealed shard, 695,249 records through 3508462) had 381,333
records without a fee, in 47,428 of 51,056 non-empty blocks. An independent oracle
([tools/record_oracle.py](tools/record_oracle.py)) rebuilds each 653-byte record only
from the node's verbose JSON RPC (Ironwood ciphertexts, `cv`, `outCiphertext`,
`vin` spent-output values via `getrawtransaction`, `vout`, and every pool's
`valueBalanceZat`), without zakura-chain or Enhance code. On 500 live blocks
(3506524–3506724 and 300 random) it agreed with every non-fee byte of 4,164 records;
2,466 differed only in the fee fields
([baseline-live-compare.json](raw/baseline-live-compare.json)).

## Rebuild (no downtime)

`preflight --stage --only coordinator` installed the binary beside the running one
([preflight](raw/preflight-stage.txt)). `rebuild-journal` then ran on the
coordinator against its local node while production kept serving: 80,480 blocks
in 1,550 s, 24,209 spent-output transactions fetched in 5,550 batches, 108,178
cache and 1,918 same-block hits ([log](raw/rebuild.log.gz)). Its
[receipt](raw/rebuild-receipt.json) `3508622-ddbb96b832e22e2f` covers heights
through 3508622, tree size 696,096: 374,853 records gained a fee and 7,027 kept none.

The oracle then compared the staged journal over all of 3506524–3506724, 2,000
random non-empty blocks and every block still holding a fee-absent record: 60,570
records in 5,028 blocks were byte-identical, and all 7,027 fee-absent records belong
to coinbase transactions ([staged-compare.json](raw/staged-compare.json)).

## Cutover

Two operator checks pin exact record bytes for 28 positions (anchor 3494753): the
deploy exact-check oracle and the external monitor's canary oracle. 17 of those
records gained a fee. Both oracles were regenerated from the staged journal and
cross-checked against the independent oracle (only fee bytes changed). APM and the
monitor were put in shadow alert mode for the window.

`wallet-pir-deploy.py deploy enhance --archive … --sha 2474cdbb… --kind
enhance-pir-native --only coordinator --skip-exact-check` committed transaction
`enhance-20261006T194503Z-cfeaa3baa40e-219a4e` ([output](raw/deploy.txt),
[journal](raw/deploy-transaction.json)). Only the coordinator restarted; workers,
packing router, query ingress and the GPU worker kept `397d8ba9…`.

- 19:45:33Z restart; 19:45:35Z the coordinator adopted the staged journal and kept
  the previous one as `enhance.before-rebuild-3508622-ddbb96b832e22e2f`
  ([log](raw/coordinator-journal-1945-1948.log.gz)). It caught up to 3508665 with
  the fixed code.
- Public `/v1/enhance/init` answered 502 for four seconds (19:45:33–37); Status was
  unaffected ([probe](raw/public-probe-1944-1950.txt)). Queries go through the
  query ingress, which did not restart.
- Generation 9839, the first with repaired content, was published by 19:47:25Z.
  The coordinator logged no warning or error after the restart.

## Acceptance

| Check | Result |
| --- | --- |
| Running coordinator executable | `cfeaa3baa40e…`, ready ([status](raw/status-after.txt)) |
| Public exact answers, regenerated 28-record oracle | 120/120 correct, p99 133 ms ([json](raw/exact-new.json)) |
| Same check with the previous oracle | 77/120 "incorrect": served bytes now carry the fees ([json](raw/exact-old.json)) |
| Live journal vs independent oracle, 3506524–3506724 and every block ingested after the deploy (3508623–3508670) | 1,024/1,024 records identical; 988 with fees (559 mixed), 36 coinbase without ([json](raw/live-after-compare.json)) |
| Public PIR queries over those 1,024 records | 1,194/1,200 exact, 0 incorrect; 6 `coverage` errors for positions in blocks not yet published ([json](raw/exact-window.json)) |
| Publication | generations 9839→9846 within nine minutes, APM lag 0–2 blocks |
| Workers and router | ready; CPU workers 1.2–1.6 GiB (unchanged); router eligible worker is the preferred GPU worker |
| Alerting | monitor canary correct on the new oracle; APM and monitor back to `active`; no APM incident firing |

The monitor's two Transparent service canaries (`oracle_invalid` against their own
anchor) were already firing before this window, in shadow, and are unrelated.

## Rollback

- Binary: `wallet-pir-deploy.py rollback enhance --transaction
  enhance-20261006T194503Z-cfeaa3baa40e-219a4e` restores `397d8ba9…`. It reads the
  repaired journal unchanged; new mixed blocks would again lack fees.
- Data: with the coordinator stopped under the production lock, move `enhance` aside
  and rename `enhance.before-rebuild-3508622-ddbb96b832e22e2f` back to `enhance`.
  On start the coordinator catches up from that journal's tip with the running binary.
- Oracles: the previous files are kept beside the new ones with the suffix
  `.before-fee-repair-20261006` (and the monitor's environment file likewise).

## Limitations

- The receipt's `binary_source_revision` is `unrecorded`; the binary is identified
  by its SHA-256 and the CI artifact for `2474cdbb`.
- Exact-answer queries sample positions at random; the full window was compared
  against the journal, and public queries covered it by sampling.
- No load test was run beyond the exact-answer checks.
