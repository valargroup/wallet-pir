# Active c-4 no-swap campaign interrupted by host upgrades

The second isolated active campaign on temporary group `g01` started at
04:21:29 UTC on 2026-09-24 and failed at 06:53:55 UTC after 129 publications.
This attempt does not satisfy the six-hour, 300-publication hardware gate.
The production serving pair and public load remained active.

The coordinator recorded `error sending request for url
(http://10.142.0.2:8291/internal/activate)` at 06:53:54 UTC. The first
worker's system journal shows that `apt-daily-upgrade.service` requested a
systemd re-execution at 06:53:49, stopped `enhance-pir-worker.service` at
06:53:53 and started it again at 06:53:54. The request therefore crossed a
package-upgrade service restart. Ubuntu unattended upgrades had begun at
06:51:12 on that worker and 06:52:38 on its peer. The exercise process exited
with status 1; this was not an OOM kill or a worker swap event.

Both direct samplers were stopped after the exercise failed, so their manifests
are marked `interrupted`. The first worker recorded 9,118 samples with zero
sample errors, zero sampled cgroup swap, and no OOM events. The second recorded
9,239 samples with one error during the upgrade, zero sampled cgroup swap and
no OOM events. These partial traces cannot be joined to a future run.

The complete failed exercise, publication trace, worker traces and filtered
system journals are preserved outside the repository under
`/tmp/wallet-pir-readiness/active-noswap-failed/`. Their SHA-256 digests are:

| File | SHA-256 |
| --- | --- |
| `coordinator/exercise.json` | `107118a692791d89199c53ce748045972ae68def5da80606e5dcdf48c4127993` |
| `coordinator/publications.jsonl` | `ae4dc2d07a070e6b03ac35012606699ce81a0f19461114779c327c783454a4c9` |
| `coordinator/failure-journal.txt` | `b1da09e2d9ceb7c3e95c9882976bf3c9c7f2b641ccb2f1feec69cba77663b57d` |
| `g01-r1/manifest.json` | `0e05b9f6cadf589813527cf8297a8128d9783d07ca543c33129d98d328804971` |
| `g01-r1/samples.jsonl` | `89b7d4d2f8bc83da1aeb0eac6a8b81d4d3ddcfc2d99e24c30d6f5589938e3d4b` |
| `g01-r1/restart-journal.txt` | `3edb9199f77ba69fe8e4f8932b565f41b95d17e9e16d9ba2b890627ab5d663f0` |
| `g01-r2/manifest.json` | `8d49fa0a79b09a8414f223fa86a71a53a8f4d176b90e7c5d19053b1482f7bf28` |
| `g01-r2/samples.jsonl` | `2b6cbc5ec3237dc6310cea55f77f86e9a331aa7cc30fd06ab2c66048a4127fe7` |

The `apt-daily.timer` and `apt-daily-upgrade.timer` units were masked on all
four temporary c-4 workers at approximately 06:56 UTC to prevent another
scheduled upgrade during qualification. The upgrades already running on `g01`
were allowed to finish with exit status zero. A third attempt started with
fresh worker and exercise data directories, new direct policies and samplers
at 07:01:29 UTC. It needs its own uninterrupted six-hour result.
The temporary timer masks must be removed if any of these hosts are retained
after qualification; the intended disposition is reviewed Terraform cleanup.
