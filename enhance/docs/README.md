# Enhance PIR

Enhance PIR retrieves the encrypted Ironwood output data a wallet needs after
compact scanning. The current implementation serves schema 11 of
`ironwood-enhance-pir-v7`: 653-byte suffix records, 33 records per row, and
private position queries. A wallet retains the compact encryption prefix and
validates the answer against its own chain context.

- [Protocol](protocol.md): record bytes, HTTP messages, and wallet trust boundary.
- [Wallet integration](integration.md): client flow, batching, and interoperability.
- [Architecture](architecture.md): publication, placement, retention, and recovery.
- [Deployment](deployment.md): fresh-state release and coordinated cutover.
- [Qualification](qualification.md): dated results and open production gates.
- [Changelog](../CHANGELOG.md): Enhance PIR release history.
- [Evidence](../evidence/README.md): retained raw runs and provenance.

The experimental Status PIR service has a separate [architecture](architecture_status.md),
[synthetic backend runbook](status_backend.md), and [APM rollout](status_apm_rollout.md).
It is not integrated with wallets or live chain ingestion.

The transparent script-history product has its own [documentation](../../transparent/docs/README.md).
