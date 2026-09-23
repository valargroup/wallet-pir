# Enhance query admission production measurement — September 23, 2026

The deployed coordinator binary was built from merged commit
`682ee124621fb155033200c6a1565a8cae75c863` and had SHA-256
`eab1a8dc35e8934f92192044988948096b050e1f983caf01c9ee2da9b0053982`.
The public Caddy query route and bounded admission came from merged PR #94;
four active slots came from PR #95; the two-second queue deadline came from
PR #96. Two existing v6 worker replicas remained in place. The coordinator
had no automatic restarts during the final run; sampled cgroup memory peak
was 8,067,477,504 bytes. Rollback copies of the prior service unit and Caddy
configuration remain under `/srv/enhance-pir-v6/validation/` on the coordinator.

The [manifest](manifest.json) captured generation 59 at anchor 3,493,703,
covering 585,542 records in one 32,768-row logical shard. At 653 bytes per
record, raw record storage was 382,358,926 bytes (364.6 MiB), with 17,744
populated rows. The nine-position exact-answer oracle came from the current
canonical journal, including row boundaries and the last covered record; its
[provenance](oracle-provenance.json) is retained without copying record bytes
into the repository. This checks retrieval against ingestion output, not an
independent chain extractor.

The [30-second public check](public-c2.json) used two clients: 596 correct
measured answers, zero errors, 19.82/s, and p99 125.887 ms. The
[five-minute public test](public-c8.json) used eight clients after five seconds
of warmup: 9,809 correct measured answers, zero incorrect answers or request
errors, 32.679/s, and p50/p95/p99 242.559/263.167/348.927 ms. All 173 warmup
answers were correct with zero warmup errors. The exact commands are saved as
[two-client](public-c2-command.json) and [eight-client](public-c8-command.json)
JSON arrays. Both drivers ran on the production coordinator through the public
HTTPS origin, so remote client network latency is not represented.

Earlier same-day [two-slot](initial-two-slot-c8.json) and
[four-slot, one-second](four-slot-one-second-c8.json) reports show the tuning
sequence: 8,216 correct answers with 84 HTTP 429s, then 9,498 correct answers
with four HTTP 429s. Both had zero incorrect answers. The final run met the
zero-error and sub-second p99 targets. This short run is not a sustained
hardware qualification or a claim about peak fleet capacity.
