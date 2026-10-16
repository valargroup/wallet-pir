# Transparent evidence

These records support only their recorded source, workload and acceptance limits.
No live verification was performed during repository cleanup. Current contracts,
operating targets and open gates belong in the product documentation.

| Retained decision record | Scope |
|---|---|
| [Wallet API v1 retired, 2026-10-13](v1-sunset-2026-10-13/README.md) | Dated retirement of the old wallet API, load result and metadata outage. |
| [Transparent monitoring moved to `/v2`, 2026-10-13](observability-v2-2026-10-13/README.md) | Monitoring coverage and shadow-alert activation limits. |
| [Txid display on `txid-4k8` tables, 2026-10-13](txid-display-4k8-2026-10-13/README.md) | Current linked display layout, table certificates and bounded correctness checks. |
| [Txid display tables at the 8-bit profile, 2026-10-13](txid-display-p8-2026-10-13/README.md) | 8-bit display cutover and its measured limits. |
| [Wallet API v2 with the 8-bit history profile, 2026-10-12](api-v2-p8-2026-10-12/README.md) | 8-bit history profile and wallet API deployment observations. |
| [v12 offline end to end on the hub, 2026-10-12](v12-offline-e2e-2026-10-12/README.md) | Bounded-journal publication and exact HTTP recovery; no whole-chain load claim. |
| [v12 whole-chain census, 2026-10-12](v12-census-2026-10-12/README.md) | Whole-journal table census and projected worker memory. |
| [v12 native correctness screen, 2026-10-12](v12-native-certificates-2026-10-12/README.md) | Synthetic native correctness certificates; no served-snapshot certification. |
| [8-bit native profile on every v12 table, 2026-10-12](p8-trial-2026-10-12/README.md) | 8-bit table screening, certificate reports and bandwidth/memory tradeoffs. |
| [Served-segment certificates at both query widths, 2026-10-09](served-segment-certificates-2026-10-09/README.md) | Snapshot-bound segment certificates, including failed attempts and reruns. |
| [Prewarm hint assurance, 2026-10-04](prewarm-hint-assurance-2026-10-04/README.md) | Independent known-answer oracle and immutable source hashes. |
| [Sealed tier boundary on the live journal, 2026-10-08](tier-boundary-sealed-2026-10-08/README.md) | Sealed tier-boundary rule and its evidence limits. |
| [Changed-native activity candidate `c3c66b9b`, 2026-10-04](activity-metadata-2026-10-04/README.md) | Candidate and recovery evidence; failed attempts do not become acceptance. |
| [M1 six-hour fleet acceptance](productionize-m1-six-hour-acceptance-2026-09-13/README.md) | Accepted historical operational baseline and bounded workload. |
| [M3 deployed regression attempt — September 13, 2026](productionize-m3-live-2026-09-13/README.md) | Historical public recovery regression and remaining wallet/application limits. |

Essential certificate outputs and oracle provenance remain in the tree, unchanged.
Their digests are in the shared [SHA256SUMS](../../evidence/SHA256SUMS).
Supporting raw captures, earlier runs and experiment scripts remain available
in the [complete pre-cleanup evidence index](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/README.md).
Follow the [retention policy](../../evidence/README.md); an old deployment record
does not establish current service health or qualification.
