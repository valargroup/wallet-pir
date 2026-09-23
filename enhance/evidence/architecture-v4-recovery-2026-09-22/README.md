# V4 retention and reorg validation — September 22, 2026

This follow-up extends the [core validation](../architecture-v4-core-2026-09-22/README.md).
It does not qualify deployment, worker memory limits, or production load. Source
identities for the changed implementation and tests are in [source hashes](source-sha256.json).

## Changes and results

Coordinator artifact cleanup now runs under publication exclusion after operation
recovery. It validates the full set of retained snapshot references before deleting
owned, unreferenced snapshot/hint files. Unresolved candidates postpone cleanup;
foreign files are preserved. Query runtimes remain owned by their in-memory views.

The [retention HTTP test](retention-http.log) passed. It verifies five-generation
artifact retention, expiry/refresh, exact encrypted answers, failover, commit
notification recovery, and restart after cleanup. An unreadable retained snapshot
prevents deletion; restoring it permits reclamation of an injected orphan while
preserving an unrelated operator file.

The [full-size reorg HTTP test](reorg-http.log) passed. With a real 32K shard and
two worker HTTP listeners, it tests loan and return, then rejects a candidate at
the canonical-anchor validation step, recovers that operation, undoes return,
undoes split, and reapplies split with changed ciphertext bytes. Retained sessions
continue to return their original branch's records; the new session returns the
changed bytes under the same stable shard ID. These are synthetic record fixtures,
not authenticated wallet recovery or an independent canonical-chain oracle.

The initial alternate-branch fixture changed a fee byte without the corresponding
flag and was correctly rejected by record validation. The final test changes a
ciphertext byte, preserving valid encoding.

Commands:

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http distributed_round_trip
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http full_shard_loan -- --ignored
cargo clippy --locked -p enhance-pir-server --all-targets --all-features -- -D warnings
```

[Clippy](clippy.log), formatting, and documentation link checks passed. No cloud
resources, production services, or release configuration changed. The remaining
[implementation and deployment gates](../../docs/architecture_2-implementation.md)
still apply; earlier load results describe their recorded binaries, not this
subsequent cleanup change.
