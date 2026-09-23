# Enhance PIR

Enhance PIR retrieves the encrypted output data a wallet needs after scanning
Ironwood compact actions. The wallet queries an output's position privately,
then decrypts and validates the returned data using its existing transaction
context. The service does not supply transaction history, witnesses, or a
complete wallet sync.

The supported server implements schema 11 of `ironwood-enhance-pir-v6`, using
653-byte suffix records and 33 records per 21,549-byte row. It requires the
wallet to retain compact encryption fields. The architecture-2 binary and
modules retain their `v4` names. Earlier clients and persisted state are
incompatible; use the deployment guide for a clean cutover. Historical evidence
and status pages describe their recorded revisions, not this build.

| Guide | Contents |
|---|---|
| [Integration](integration.md) | Rust client, CLI, wallet responsibilities, sessions and failures |
| [Protocol](protocol.md) | Records, initialization, binary messages and validation |
| [Architecture](architecture.md) | Repository layout, ingestion, storage, queries and replicas |
| [Architecture 2 implementation](architecture_2-implementation.md) | Opt-in v4 runtime, local validation, and outstanding qualification gates |
| [Deployment](deployment.md) | Local operation, release preparation, cutover, rollback and monitoring |
| [Capacity expansion](capacity-expansion.md) | Infrastructure migration, qualification and controller operation |
| [16-bit expansion](plaintext16-expansion.md) | Rationale, exact capacity benefit, correctness evidence and migration contract |
| [Performance](performance.md) | Measured latency and throughput, benchmark evidence and reproduction |
| [Status](status.md) | Dated public observations and unresolved deployment facts |
| [Remaining work](remaining-work.md) | Evidence and acceptance still required |
| [Evidence](../evidence/README.md) | Raw results, provenance and historical reports |

[Transparent script-history recovery](../../transparent/docs/README.md) is a
separate product. The retained `transparent-spend-pir` crate is also distinct:
it is compatibility code and its tables are not served by Enhance.
