# Enhance production deploy of `eae5641a`, 2026-10-11

Every Enhance role, including the GPU worker, moved to the full-CI release of
`eae5641a` (run 38156379697). The release adds memory reporting to workers and the
packing router and a new `capacity` alert family in the APM. The CPU roles went
from `0bc560dd…` to `538137e7…`; the GPU worker went from `d562b420…`, a
September 30 build that the October 6, 9 and 10 deploys had all skipped, to the
CUDA build `bc9f91cf…`. Status was not changed.

| Transaction | Targets | Exact answers |
|---|---|---|
| `enhance-20261011T185451Z-538137e74744-e144a8` | worker-01 | 120/120, p50 90 ms, p99 149 ms ([json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/enhance/evidence/production-deploy-eae5641a-2026-10-11/raw/exact-worker-01.json)) |
| `enhance-20261011T190032Z-538137e74744-0b4388` | worker-02, packing router, query ingress, coordinator | 120/120, p50 89 ms, p99 129 ms ([json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/enhance/evidence/production-deploy-eae5641a-2026-10-11/raw/exact-final.json)) |
| `enhance-20261011T191808Z-bc9f91cfcb38-7fe75d` | GPU worker (`enhance-pir-native-cuda`) | 120/120, p50 120 ms, p99 148 ms ([json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/enhance/evidence/production-deploy-eae5641a-2026-10-11/raw/exact-gpu.json)) |

The GPU worker reports the CUDA backend on device 0. After its restart the
publication of generation 14531 went out without it, so the packing router had no
preferred worker for domain 0 from 19:20:49 until generation 14532 at 19:24:49.
The CPU workers answered every query in that window (145 outcomes, 0 failures),
and `domain_0_unavailable` fired only in shadow.

## Alerting

The APM moved to `pir-apm` `9db95685…` under
`/opt/pir-quality/releases/apm-eae5641a` with one later-sorting ExecStart drop-in
that also sets `PIR_APM_CAPACITY_ALERT_MODE=shadow`. The main family was in shadow
from 18:51 until every main incident had recovered, then re-armed (the temporary
drop-in is kept in `/root/apm-eae5641a/`). Quality and scaling families stay in
shadow; the capacity family starts its 24-hour shadow review here.

After the rollout all three workers and the router reported memory: CPU workers
about 79% host memory available, router 92%, GPU host 92%, no swap, no
`memory.high` events, no OOM kills, and every worker's assignment modelled at 38%
of its resident limit. The coordinator reported 141,723 placement rows and 10,651
rows before shard 0 fills.

## Deploy path

The GPU worker previously had its own CI workflow (`deploy-enhance-cuda.yml`),
which now refuses on its runner because `/var/lib/wallet-pir-deploy/cuda` is not a
private root-owned directory. It was deployed with `wallet-pir-deploy.py` instead,
from an inventory that lists it with `artifact_kind = enhance-pir-native-cuda`.
The same change makes every regular Enhance deploy require that CUDA bundle.

The tool's lock session from the second transaction outlived the tool and was
ended by PID, as recorded for earlier deploys from this workstation.

Rollback: `rollback enhance --transaction` with each id above, newest first. For the
APM, remove `zzzzzzzzzzzzzzzzzzzz-apm-eae5641a.conf` and restart `pir-apm`.

Not established: no load beyond the exact checks; the capacity family is not yet
active.
