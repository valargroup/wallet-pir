# Isolated live v4 expansion, 2026-09-23

This test used a fresh synthetic coordinator and dedicated DigitalOcean `c-4`
worker replicas in the Valargroup wallet-pir project. It did not send requests
to the production PIR service. The candidate revision was
`9718a6dcf9801385f69f31bb71f02efd261914f0`, with verified worker binary
SHA-256 `258fc7d0070b821d34710d5c25edbf9176472c6e39defca020f8944a045c1def`.
The Terraform root used a separate Spaces state key and a pinned
coordinator-host lock.

The fixture appended 540,672 records per generation, starting with five shards
on group 1. Each group had two replicas. The tested capacity rule permits at
most five shards on the active group and six on a fully sealed group. The
coordinator issued `successor-5-pair-2`; the live provisioning adapter created
both group 2 Droplets, the bootstrap driver installed both services, and the
isolated test driver registered the pair in inventory. The registration receipt
remained `qualification: unqualified`.

| UTC time | Generation | Records | Placement | Replica health |
| --- | ---: | ---: | --- | --- |
| 13:16:32 | 4 | 4,460,514 | shards 0–4 on group 1 | 2 published on group 1 |
| 13:21:12 | 6 | 5,677,026 | shards 0–4 on group 1; shard 5 on group 2 | 2 published on each group |
| 13:26:27 | 8 | 6,758,370 | shards 0–5 sealed on group 1; growing shard 6 on group 2 | 2 published on each group |

The shard 5 move followed rollover of shard 6. The placement trace recorded
no blocked reason through these transitions. It ended at generation 17 and
11,624,418 records, when both registered groups were full. The last three of
360 placement samples reported `fleet capacity exhausted; preserve published
generation`; earlier samples had no block and none had an observation error.

## Thirty-minute query and RAM result

The load client ran for 1,800.145 seconds at concurrency 2, with a fixture
answer oracle. It completed 17,976 queries: 17,974 correct answers, zero
incorrect answers, and two HTTP 429 responses. Median, p95, and p99 latency
were 195.583, 257.151, and 362.239 ms. The configured zero-error gate failed
because of the two 429s. These errors were not attributed to a specific
placement transition. After the load, a direct query at position 5,406,720 in
the moved shard returned the position encoded in the fixture record; a query at
6,488,064 on group 2 did too.

Each worker was sampled every second for about 1,799 seconds. All four traces
had zero sampling errors, one boot ID, one worker process, zero OOM kills, and
zero cgroup swap use. Their peak cgroup charges stayed near the 7 GiB
`memory.high` setting, below the page-aligned 7.08 GiB `memory.max` setting.
Reclaim did occur (`memory.events high` rose by 6,265 and 6,200 on group 1;
11,769 and 12,514 on group 2). This shows the configured RAM held the tested
workload for 30 minutes, with pressure at the soft limit. It is not a six-hour
memory qualification or a zero-error query result. Exact counters, transitions,
and SHA-256 trace digests are in [summary.json](summary.json).

Two live provider assumptions failed during the run and were corrected in this
branch. The wallet-pir project is team-owned, so its `owner_uuid` differs from
the authenticated user's `/v2/account` UUID. DigitalOcean also refreshes the
firewall's computed `droplet_ids` during expansion, even when the firewall's
planned action is `no-op`. The adapter now pins the project owner separately and
accepts only that narrow computed-field refresh.

A later forecast request for pair 3 exposed a test-driver safety gap: an
unqualified rerun provisioned two extra test Droplets before its bootstrap policy
rejected them. The driver now requires an expected operation ID and target group
count, and a completed pinned request returns read-only even if later demand
exists. Pair 3 was never bootstrapped or registered. After evidence collection,
the coordinator, all six test worker Droplets, the dedicated firewall and tag,
and the isolated Terraform state key were removed. Provider identities were
checked before deletion and absence was checked afterward.

This is an end-to-end test of isolated demand, provisioning, bootstrap,
registration, placement and query behavior. It does not satisfy the six-hour
worker hardware qualification, canonical ingestion, failure recovery, or a
production promotion gate.
