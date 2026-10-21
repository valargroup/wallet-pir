# pir-service-discovery

The compatibility contract between Wallet PIR publishers and query servers.

A publisher serves one small JSON document:

```http
GET /.well-known/pir-service-discovery

{"service":"pir-publisher","version":"1.0.0"}
```

A query server fetches it from its configured publisher origin before it starts
and before every synchronization pass, and compares the publisher's version
with its own. Publishers are authoritative: the query server adapts or stops.

## What the version covers

The version is this crate's package version. One number covers the discovery
document and the whole Publisher-Query Server HTTPS contract that the query
server consumes. It is not a host software release and not a per-product
version. There is no configuration override.

## Compatibility policy

- **Patch** (`1.0.x`): fully compatible fixes.
- **Minor** (`1.x.0`): additive changes. New fields are optional or ignored.
  A newer query server never requires an addition that an older publisher of
  the same major version lacks.
- **Major** (`x.0.0`): breaking. Any breaking change to a covered product
  contract, or to this crate's public Rust API, bumps the major version.

The same major version is compatible in both directions, whatever the minor
and patch versions. Only stable versions with major version 1 or later are
accepted: malformed, `0.x` and prerelease versions are refused. Build metadata
is ignored. Unknown document fields are ignored. The path and the two-field
document stay the same across majors, so a mismatch can always be named.

The document is compatibility metadata. It is not a health check and does not
authorize a publication; product chain, schema, canonicality and revocation
checks still apply. It is authenticated only by TLS to the configured
publisher, not signed end to end.

## API

- `PATH`, `PUBLISHER_SERVICE`, `VERSION`, `MAX_DOCUMENT_BYTES`: the endpoint,
  identity, this build's version and the read limit.
- `Document::publisher()` and `Document::to_json()`: the publisher's body.
- `check_publisher(body)`: parse a body and check identity and compatibility.
- `parse_version`, `compatible`, `version`: the policy on its own.
- `Error` and `Incompatible`: typed refusals. `Error::incompatible()` returns
  the confirmed mismatch, the only permanent condition.

The crate has no HTTP, runtime or workspace dependencies.

## Operator actions

When a query server reports an incompatible publisher:

- If the query server's major version is lower, upgrade the query server.
- If it is higher, the publisher is older: upgrade the publisher, or run a query
  server release for the publisher's major version.

If the query server reports HTTP 404, the publisher does not serve discovery or
its HTTPS edge does not forward the exact path to the publisher's management
listener. Forward only that path; never expose other management routes.

## Releases

Changes are recorded in [CHANGELOG.md](CHANGELOG.md) under `Unreleased`.
A release moves those entries under the new version, sets the same version in
`Cargo.toml`, and is published by the manually dispatched
`publish-pir-service-discovery` workflow, which tags the published commit
`pir-service-discovery-vX.Y.Z`. See
[docs/service-discovery-release.md](../../../docs/service-discovery-release.md).
