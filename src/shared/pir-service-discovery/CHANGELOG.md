# Changelog

All notable changes to `pir-service-discovery` are recorded here. Versions
follow the compatibility policy in [README.md](README.md): the same major
version is compatible in both directions.

## Unreleased

## 2.0.0

- Prepared artifact distribution uses explicitly tagged checksums and BLAKE3
  across Enhance, Status, Transparent History and Transparent Display.
- Artifact inventory schemas and prepared runtime files change together;
  version 1 peers are incompatible. Wallet-protocol commitments are unchanged.

## 1.0.0

Initial stable contract.

- Publisher discovery document at `/.well-known/pir-service-discovery`:
  `{"service":"pir-publisher","version":"<contract version>"}`, served
  uncached. Unknown fields are ignored.
- SemVer policy: same major version is compatible regardless of minor and
  patch; malformed, `0.x` and prerelease versions are refused; build metadata
  is ignored.
- Typed `Error` and `Incompatible` refusals with operator guidance for both
  mismatch directions.
- Covers the Transparent history and transaction-detail HTTPS publication
  contracts consumed by the unified query server.
