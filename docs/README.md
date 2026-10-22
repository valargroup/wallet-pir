# Documentation

- [PR management and labels](pr-management.md): automatic component labels and author/agent guidance for breaking changes, security, privacy and PIR schemes.
- [Unified publisher](publisher.md): source layout, configuration, managed services and gated cutover.
- [Query server host](query-server.md): one serving process for any compiled subset of Enhance, Status, and Transparent.
- [Service discovery releases](service-discovery-release.md): the publisher-query server contract crate and its crates.io publication.

- [Fleet operator guide](operations.md): architecture, configuration ownership, packaged operations, lifecycle commands and recovery.

- [Enhance PIR](enhance/README.md): Ironwood integration, architecture, deployment and measured performance.
- [Transparent PIR](transparent/README.md): recovery contract, filters, wallet adapter, deployment and acceptance.
- [Status PIR](status/README.md): serving architecture, distributed runbook, qualification and APM rollout.
- [Transaction-detail (txid) display](txid/README.md): txid display protocol, shipped runtimes plan and sizing findings.
- [Receiver directory](receiver/README.md): zero-OVK receiver records, publications, filters and witnesses.
- [Transparent activity metadata plan](transparent-pir-activity-metadata-plan.md): proposed compact fee/shape metadata, accepted activity limitations, and separate future txid PIR.
- [Transparent txid sizing and anonymity findings](txid/transparent-txid-sizing-and-anonymity-findings.md): whole-range probability sizing estimates, independent routing and remaining full-population gates.
- [Transparent txid display](txid/txid-display.md): server protocol, native demo, tiered proof of concept and next wallet integration stage.
- [Evidence](../evidence/README.md): retained measurements and their provenance.
- [Serving contract](serving-contract.md): process identity, serving authority and refusal codes across Enhance, Status and Transparent.
- [Decentralized query serving](decentralized-query-serving.md): phased plan for independent providers, prepared publications, embedded router/worker deployments and service discovery.
- [Publisher ingest model specification](spec/spec-publisher-ingest-model.md): immutable objects, publication references, verification reuse, atomic advancement and safe collection.
- [Shard promotion specification](spec/spec-shard-promotion.md): normative hierarchy generation advancement and continuous query-server handover rules.
- [Multi-server wallet specification](spec/spec-multi-server-wallet.md): tier-unaware wallet endpoints, provider routing and the initial fixed Valargroup archive upstream.
- [Control sessions](control-sessions.md): the shared supervised SSH control transport and the restricted forwarding account (Status observed on the shared supervisor in dated captures; the restricted account and other products not adopted).

`roman_notes.md` is preserved personal working material, including local edits.
It is not a protocol specification or a source of deployment parameters; its
historical paths may refer to the pre-cleanup revision. Follow the product
indexes when implementing or operating a service.

- [CI and deployment latency](ci-performance.md): fast/full gates, warm runners, prepared artifacts and timing evidence.

- [Development workflow](development.md): affected local checks, setup, durable validation and measurements.
