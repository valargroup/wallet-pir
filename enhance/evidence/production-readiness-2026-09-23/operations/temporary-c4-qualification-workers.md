# Temporary c-4 qualification workers

Status at 2026-09-24 06:20 UTC: four isolated workers provisioned; the active
six-hour campaign is running on group `g01` and the sealed group `g02` is idle.
Neither hardware campaign has passed. The existing production coordinator and
serving pair continue to serve the public origin.

The workers were created from `ops/infra/digitalocean/enhance-v4` in the
Valargroup wallet-pir project `85639967-fecb-4c8d-88be-c0e3dee3f86c`, AMS3,
under the separate remote Terraform state key
`qualification/2026-09-24/terraform.tfstate`. The VPC is
`c5bd6679-aa32-48fc-9d5d-51d422fb3468`. The operator retained a private
state backup before applying. The reviewed final plan created exactly four
`c-4` Droplets and four project memberships; the isolated tag and firewall
were already in that state after reconciliation. Apply reported eight adds,
zero changes and zero destroys. A full post-apply plan reported no changes.
The production Terraform state, DNS, autoscaler target and worker inventory
were not modified.

| Profile | Replica | Droplet ID | Private IPv4 | Public IPv4 |
| --- | --- | ---: | --- | --- |
| active | `enhance-pir-v4-g01-r1` | 603202997 | 10.142.0.2 | 165.232.84.36 |
| active | `enhance-pir-v4-g01-r2` | 603202996 | 10.142.0.5 | 146.190.31.49 |
| sealed | `enhance-pir-v4-g02-r1` | 603202995 | 10.142.0.4 | 188.166.30.65 |
| sealed | `enhance-pir-v4-g02-r2` | 603202998 | 10.142.0.13 | 146.190.237.236 |

Provider inventory confirmed 4 vCPU, 8,192 MiB, 50 GB disk, the requested
VPC and project membership for each. Roman's registered public SSH key ID
`56343657` was supplied to Terraform; no private key was copied. SSH through
the coordinator succeeded after cloud-init finished. Each host reported 4
logical CPUs, about 7,941 MiB usable RAM and an unused 2 GiB swap file.
The isolated firewall admits worker RPC port 8291 only from the coordinator's
private address; SSH is limited to that address and the existing operator
CIDRs in the production infrastructure configuration.

Each worker now runs the pinned corrected server revision
`216b9993cc3ae4e0f1820d60c6b8f03d104b5e46` from an extracted archive
whose SHA-256 is
`4c24c30c0adbd19779671ee731adc09bad3ea77b941ca51fb01e235bcd26a3ac`.
Every release file passed `SHA256SUMS` verification. The server binary SHA-256
is `7be19ca82108c2046d571ce6b901dd348d2e43435510a74257d530a49b309fe1`.
At provisioning, the worker units used a dedicated service account, separate profile data
directories under `/srv/enhance-pir-v6/qualification/`, exact private IPs on
port 8291, six sealed shard slots, and cgroup limits of 7 GiB high,
7,609,516,032 bytes max and 2,147,479,552 bytes swap max. All four workers
reported idle protocol-v6 health with epoch/revision zero and no published
data. Three one-second direct samples on each host recorded zero errors and
bound the running binary, exact data directory, private listener, manifest and
cgroup limits.

## Current campaign staging

The first active campaign was stopped after both workers swapped; its raw
traces and exercise record were retained. A fresh active retry started on
`g01` at 04:21:29 UTC with new data directories and `MemorySwapMax=0`, while
keeping the same high and hard memory limits. Direct one-second samplers began
before the exercise. This stricter swap limit differs from the production
serving units and cannot alone qualify them. The first attempt's inactive data
directories were removed after the raw evidence digests were verified, leaving
about 31 GB free on each active worker.

The idle `g02` pair was moved to fresh sealed-profile data directories and
`MemorySwapMax=0`; both workers passed pinned-release idle health and effective
limit checks. A separate root-only inventory at
`/etc/enhance-pir-v6/qualification-sealed-four-physical-workers-216b999.json`
on the coordinator names `g02` as the sealed shard group and `g01` as the
physical active support group. Its SHA-256 is
`cf0512c2200c3718bf1751e8c908092bba47be3ea9eea7638c3e9df4846182d9`.
It has not been used. After the active run completes, `g01` needs fresh support
data directories and new samplers before the sealed exercise starts; the
ongoing active data and traces must first be finalized and preserved.

Each campaign needs at least six measured hours, 300 publications, complete
physical replica traces, an assessor report and operator review. The Terraform
root has `prevent_destroy` guards; retire these temporary hosts by a separate
reviewed cleanup plan after their evidence is secured.
