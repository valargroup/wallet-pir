# Enhance PIR

Enhance PIR retrieves the encrypted Ironwood output data a wallet needs after
compact scanning. The current implementation serves schema 11 of
`ironwood-enhance-pir-v6`: 653-byte suffix records, 33 records per row, and
private position queries. A wallet retains the compact encryption prefix and
validates the answer against its own chain context.

- [Protocol](protocol.md): record bytes, HTTP messages, and wallet trust boundary.
- [Wallet integration](integration.md): client flow, batching, and interoperability.
- [Architecture](architecture.md): publication, placement, retention, and recovery.
- [Deployment](deployment.md): fresh-state release and coordinated cutover.
- [Qualification](qualification.md): dated results and open production gates.
- [Changelog](../CHANGELOG.md): Enhance PIR release history.
- [Evidence](../evidence/README.md): retained raw runs and provenance.

The transparent script-history product has its own [documentation](../../transparent/docs/README.md).
