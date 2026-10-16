# Enhance PIR

Enhance PIR retrieves the encrypted Ironwood output data a wallet needs after
compact scanning. Source supports schema 11 with profile-specific framing; the latest linked
[production record](../evidence/production-deploy-eae5641a-2026-10-11/README.md)
uses the native `ironwood-enhance-pir-v9-native-two-mask-m29` profile: 653-byte suffix records, 33 records per row, and
private position queries. A wallet retains the compact encryption prefix and
validates the answer against its own chain context.

- [Protocol](protocol.md): record bytes, HTTP messages, and wallet trust boundary.
- [Wallet integration](integration.md): client flow, batching, and interoperability.
- [Architecture](architecture.md): publication, placement, retention, and recovery.
- [Deployment](deployment.md): fresh-state release and coordinated cutover.
- [Qualification](qualification.md): dated results and open production gates.
- [Changelog](../CHANGELOG.md): Enhance PIR release history.
- [Evidence](../evidence/README.md): key decision records, proofs and historical provenance.

Status PIR has its own controller, router and worker roles in production deployment
records; its serving library is `enhance/crates/status-serving`, composed by
the Enhance application package ([package boundaries](architecture.md#package-boundaries)). Its [architecture](architecture_status.md),
[distributed runbook](status_distributed.md), [qualification gate](status_qualification.md)
and [APM rollout](status_apm_rollout.md) separate implementation, observations and
remaining qualification. The [synthetic backend](status_backend.md) is a test mode.
Start shared operations with the [fleet guide](../../docs/operations.md).

The transparent script-history product has its own [documentation](../../transparent/docs/README.md).

The `status-pir` release artifact selects `status-pir-v3-native-two-mask-m29`.
The default-feature `status-pir-v2-q48` implementation and its v1/v2 measurements
remain profile-specific test and historical material. Status replies are
observations, not inclusion proofs; source block hashes remain internal.
