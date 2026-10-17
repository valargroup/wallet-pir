# Multi-server wallet specification

Status: draft. This document specifies wallets using interchangeable PIR provider
endpoints without selecting recent or archive workers. It does not establish
implemented wallet failover, provider qualification or live deployment.

The [decentralized query-serving architecture](../decentralized-query-serving.md)
explains the implementation phases. The [shard promotion specification](spec-shard-promotion.md)
governs ownership changes and generation handover. Keep these documents aligned.
Existing product contracts and the [serving contract](../serving-contract.md)
continue to govern validation, authority, refusals and retries.

## Scope and terminology

The keywords MUST and MUST NOT express requirements. MAY expresses a permitted
choice.

- **Provider endpoint:** the complete wallet-facing API for a supported product,
  backed by a router and local or remote workers.
- **Recent provider:** a provider whose local worker holds recent data. Its router
  obtains required historical coverage from the archive upstream.
- **Archive upstream:** the service to which a provider routes requests for
  archive holdings. Initially this is a fixed Valargroup-operated service.
- **Provider list:** wallet configuration identifying eligible provider endpoints
  for a particular product, network and protocol version. Entries need no local
  storage-tier classification.

The initial deployment is an embedded router and recent worker at each external
provider, with one fixed Valargroup archive upstream. Separate routers and workers
MAY compose the same interfaces. A wallet can use one configured provider; a list
MAY contain several. Supporting a list does not require parallel queries or
automatic failover in the first implementation.

## Wallet-facing provider contract

1. Each provider endpoint MUST expose initialization, metadata and manifests,
   public setup, and encrypted queries as required by its supported product.
   Wallets MUST NOT need a separate Valargroup ingress to complete those steps.
2. The endpoint MUST provide access to the complete required coverage for that
   product through local holdings or forwarding. A bare recent worker without
   archive access MUST NOT be presented as an equivalent complete provider.
3. The wallet MUST NOT need to identify a provider's local holdings as recent,
   archive or combined, or choose a worker based on that classification.
4. A provider MUST NOT advertise products, protocols or complete coverage that it
   cannot support. Local coverage and coverage accessible through forwarding
   MUST remain distinguishable in operator status and service discovery.
5. Completeness is product-specific. Transparent history and display keep their
   respective coverage rules; Enhance retains required query domains; Status
   retains rolling observations and freshness; Receiver retains complete required
   recovery history. A recent storage policy MUST NOT silently truncate any of
   these contracts.

Providing access to complete coverage is a routing and service requirement, not
an unconditional availability guarantee. An outage MUST produce the existing
product failure behavior, rather than a successful answer that omits history.

## Wallet configuration and provider selection

A wallet MUST select endpoints compatible with the requested product, network
and protocol version. The list MAY be bundled configuration; no discovery
protocol, signed provider-list format or new wallet wire fields are introduced
here. Supporting one product MUST NOT imply support for every PIR product.

For a product operation, the wallet selects a provider and uses its existing API.
The wallet MUST retain independent answer validation, chain acceptance,
completeness checks and existing session and revision bindings. Not knowing the
storage tier does not authorize trusting unvalidated answers.

The wallet MAY select another configured provider for a later operation or after
a failure, subject to the existing product rules. Switching providers MUST NOT
assume that they share active publications, setup or session state. Initialization
and setup MUST be obtained or verified for the selected provider as required by
the existing protocol. Cached material MAY be reused only when its existing
identity and validity checks permit it.

Automatic provider ranking, rotation and failover policy are outside the initial
scope. Any later failover MUST preserve the product's retry restrictions,
including uncertain evaluation acceptance. Membership in the list alone MUST NOT
authorize replaying an encrypted request at another provider. This specification
does not introduce query fan-out or require a wallet to contact several providers
to answer one request.

## Provider routing

1. The router MUST use a local worker when that worker holds the exact requested
   data and qualifies to serve it. Requests for archive holdings MUST be routed
   to the fixed archive upstream when they cannot be served locally.
2. Routing MUST use existing public request identities, such as shard, revision
   and session bindings. Selected private records MUST NOT become plaintext
   routing inputs. Encrypted framing and response validation MUST be preserved.
3. An archive destination MUST support the requested product, protocol, profile
   and exact data or session binding. Its configured address alone MUST NOT
   establish readiness, serving authority or correctness.
4. The provider MUST NOT reinterpret an encrypted request against another
   revision, setup or session to make forwarding succeed. Existing stale-session
   refusal and refresh behavior continues to apply.
5. Forwarding MUST have bounded waits and preserve existing admission,
   cancellation, response ownership and retry rules. A request MUST NOT circulate
   between providers. The initial archive upstream MUST terminate archive routes
   at qualified owners rather than route them back to recent providers.

Some products' routers perform preparation or response packing as well as HTTP
forwarding. Those responsibilities remain inside the provider boundary and MUST
preserve the existing product semantics.

## Initial Valargroup archive upstream

The provider installation MUST supply a fixed Valargroup archive service address
through its trusted deployment configuration. This is a server-side default,
not a wallet-visible role or protocol constant. The address MUST NOT be learned
from an untrusted wallet request. No new public hostname or IP address is assigned
by this specification.

The initial provider MUST NOT require archive discovery, operator selection among
archive providers, or automatic archive failover. The archive service MAY route
internally to multiple workers; it remains one configured upstream from the
external provider's perspective.

Recent queries MUST be answerable locally without passing through the Valargroup
archive service, provided local data, setup and serving authority remain valid.
If the archive is unavailable, those independent local requests MUST remain
serviceable. Archive-dependent operations MUST fail explicitly within existing
product timeouts and refusal semantics. A wallet operation requiring both kinds
of coverage may therefore fail even while recent queries still succeed.

Missing archive coverage MUST NOT become an empty result, a claim of completed
recovery or an automatic public lookup. Report archive reachability separately
from local worker readiness; neither signal alone proves complete service.

This initial choice leaves archive availability dependent on Valargroup. It does
not require Valargroup to receive every recent query or act as the wallet's
mandatory entrypoint. Additional archive upstreams can be specified later without
requiring wallets to learn storage tiers.

## Promotion and independent updates

Providers MAY update independently. A publisher/query-server hierarchy generation
MUST NOT become a new wallet routing requirement.

Before retiring a local promoted shard, a recent provider MUST establish an
eligible archive route for the unchanged shard under the
[promotion and wallet compatibility rules](spec-shard-promotion.md#promotion-and-wallet-compatibility).
It MUST keep its old serving view available while preparing the replacement,
switch routing and worker state atomically when ready, and preserve admitted
requests under the existing lifetime rules. Reorg invalidation and expired
authority still require their existing refusals.

## Conformance scenarios

A conforming implementation demonstrates:

- An unchanged wallet client obtains metadata and setup and performs encrypted
  recent and historical queries through one provider endpoint, with equivalent
  answers and refusals to the existing product API.
- Two independently operated provider endpoints can each serve that sequence
  without the wallet classifying either by storage tier. Test each endpoint with
  a fresh client session; this does not establish cross-provider session reuse.
- Recent queries execute locally and historical queries reach the fixed
  Valargroup archive upstream, preserving exact request and response bindings.
- A fresh wallet restore or rescan obtains all required historical coverage;
  recent-only success cannot be mistaken for complete recovery.
- Archive failure leaves independent valid recent queries serviceable, while
  archive-dependent queries fail explicitly. Recovery restores archive routing
  without changing wallet configuration.
- Incompatible, stale or unready archive holdings cannot produce a successful
  response for a different requested publication or session. Overload, timeout
  and cancellation retain the existing product behavior.
- Promotion and independently updating providers preserve service according to
  the shard promotion specification, including when an archive destination is
  not yet ready. Query-server generations never require wallet interpretation.
- A provider switch uses the existing initialization and validation rules;
  incompatible session state or cached setup is not silently reused.

## Exclusions

This version does not define wallet-side automatic discovery, provider ranking or
automatic failover; archive discovery or multiple archive-upstream selection;
payments, reputation or incentives; a new PIR primitive, manifest schema or wire
protocol; or shard rewriting. It does not claim the existing wallet applications
already implement provider lists. No production deployment is authorized here.
