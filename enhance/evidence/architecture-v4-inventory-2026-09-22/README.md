# V4 online inventory validation — September 22, 2026

The coordinator now reloads its inventory file and durably registers additional
replica pairs without restarting. This is an infrastructure-controller integration
point; automatic provisioning, forecasting, and qualification receipts remain
unfinished. Existing group identities, ordering, and endpoints are immutable.
Both new replicas must respond as distinct idle v4 processes with fresh state.
Failed registration leaves durable inventory unchanged; exact replay is a no-op.

## Evidence

- [Registration HTTP test](registration-http.log): passed partial readiness,
  invalid identities, duplicate normalized origins, endpoint mutation, removal,
  replay with a subsequently unavailable replica, and restart persistence.
- [V4 library tests](v4-unit-tests.log): all nine selected runtime, fencing,
  placement, and durability tests passed. The final registration test also covers
  the subsequent empty-replica-name validation.
- [Serving HTTP regression](serving-http.log): passed encrypted answers, retained
  sessions, cleanup, failover, commit recovery, and restart with published state.
- [Clippy](clippy.log): server targets/features passed with warnings denied.
- [Final process manifest](final/manifest.json): four independent worker processes
  and one coordinator on the same shared macOS host. After the first publication,
  the harness atomically replaced the inventory file, observed two registered
  groups and placement revision two, then ran exact-answer traffic.
- [Final load report](final/load-c2.json): 1,500 correct answers, zero incorrect
  answers and zero errors at concurrency two; p99 58.207 ms. The measured interval
  was about 15.5 seconds. The fixture began with 67 records and continued appending.

The second group stays idle in this small fixture: this proves online registration
and continued serving, not placement at a full group's boundary, consolidation,
or 8 GiB hardware capacity. The load driver uses a permissive error threshold for
characterization; this run happened to have zero errors. It is not release
qualification. The earlier run in this directory's root predates the final empty
name check; `final/` contains the final binary run. Binary digests are recorded in
each run manifest; [source hashes](source-sha256.json) identify the changed files.

Reproduce with optimized binaries:

```sh
python3 enhance/ops/scripts/test-v4-local.py --out /tmp/v4-inventory-evidence \
  --seconds 15 --concurrency 2 --expand-inventory
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http inventory_expansion
```

No cloud resources or production services changed. See the remaining
[implementation and deployment gates](../../docs/architecture_2-implementation.md).
