# Corrected release qualification progress

Status at 2026-09-24 04:22 UTC: **in progress, not a release sign-off**.
The corrected release source is
`216b9993cc3ae4e0f1820d60c6b8f03d104b5e46`; the deployed server SHA-256
is `7be19ca82108c2046d571ce6b901dd348d2e43435510a74257d530a49b309fe1`.
The coordinator and both original production workers serve this release with
two published replicas, advancing anchor, no ingestion error and zero service
restarts. The previous release and canonical data remain for rollback. CI is
green on PR #97 head `c92eee649a1a2df3761f54fc470c6751b268af29`.

The new one-second worker samplers and ten-second coordinator freshness
observer began around 03:28:53 UTC, before the external full public run. The
chain-derived oracle was extracted from a published anchor and its JSON SHA-256
is `8a28ce362d64e82eb17934660763bf79c2be05f326ecf2a361159397e799474f`.
The corrected load-driver SHA-256 is
`328626ba2ed4ce1fdd787e32c79eeddcf60ac4ec2e6b11619be7143c33d87925`.
The 10-second public smoke passed 10/10 exact answers, zero errors.

The full public run began at about 03:33:54 UTC on the external
`roman-ipir-bench-8vcpu` host. Its first 30-minute 1 QPS stage passed:
1,800/1,800 exact answers, no errors or unstarted arrivals, successful
scheduled p99 390.911 ms. Immutable snapshots of both worker traces cover
that window with zero sample errors, no findings or swap, stable process and
release identities, and maximum gaps 1.003/1.004 seconds. The freshness
assessment after a five-minute tail passed: 21/21 sampled tip advances bounded,
maximum conservative lag 60.001 seconds, no findings. The 2 QPS stage is
running; the 4 QPS stage, six-hour soak and burst remain.

The first active isolated campaign started at 03:51:12 UTC on temporary c-4
group `g01`, after both one-second direct samplers started. It was
[stopped after 12 publications](active-c4-swap-stop-2026-09-24.md): both
worker cgroups began swapping during the measured phase. Its complete partial
traces are retained and this attempt does not pass the hardware gate. A fresh
retry began at 04:21:29 UTC with separate worker and exercise data directories.
Both physical workers have the same 7 GiB soft and 7.609 GB hard memory limits,
but `MemorySwapMax=0`; new one-second traces began before this exercise. This
is an experiment with stricter limits than the serving workers. A passing
result would still require production configuration alignment and affected
requalification before pilot admission. The sealed campaign has not started.

The likely consuming app branch is chainapsis/vizor-wallet PR #601 at
`edbe5663f698e10a9d21f81ff52b7d2da17701dc`. Its current Cargo manifest
pins the required wallet-libraries q48/v6 commit
`9b190657d129d08e964623d0ecc1d8e4ffb31b1d`. An isolated checkout passed
22 targeted Rust recovery tests, 70 targeted desktop Flutter tests, 43 targeted
mobile Flutter tests, and `fvm flutter analyze` with no issues. Its PR body
still describes an older dependency and must be corrected before release
review. The actual app restore/resume/reorg check over public HTTPS is open.

The fresh rc.2 ARM64/Linux matrices and deterministic comparison are complete.
A current published snapshot, independent numerical/cryptographic reviewer,
fault/alert rehearsal, sealed hardware campaign, full public assessments and
24-hour opt-in observation remain open.
