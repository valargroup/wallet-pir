# Releasing pir-service-discovery

[`pir-service-discovery`](../src/shared/pir-service-discovery/README.md) is the
one versioned contract between publishers and query servers. It is released to
crates.io on its own; host releases do not change its version.

## Choosing the version

- Patch: fixes with no contract change.
- Minor: additive, optional or ignorable fields only. A newer query server must
  still work against an older publisher of the same major version.
- Major: any breaking change to a covered product contract or to the crate's
  public Rust API.

## Steps

1. On a branch, move the `Unreleased` entries in
   `src/shared/pir-service-discovery/CHANGELOG.md` under a `## X.Y.Z` heading,
   set `version = "X.Y.Z"` in its `Cargo.toml`, update the `version` of every
   local `pir-service-discovery` dependency to match and refresh `Cargo.lock`.
   Merge to main through a pull request.
2. Dispatch **Publish pir-service-discovery** with the version and the full main
   commit. The workflow refuses a commit that is not on main, a version that
   differs from `Cargo.toml` or has no changelog heading, or an existing tag. It
   runs the crate tests and `cargo publish --dry-run --locked`, so the packaged
   sources build alone.
3. A reviewer of the protected `crates-io` environment approves the publish
   job. It publishes that exact commit and pushes the tag
   `pir-service-discovery-vX.Y.Z`.

## Configuration prerequisite

Not yet provisioned. Before the first release:

- Create the `crates-io` GitHub environment with required reviewers and a
  deployment rule restricted to `main`.
- Store a crates.io API token scoped to publishing `pir-service-discovery` in
  Infisical as `CARGO_REGISTRY_TOKEN`.
- Create an Infisical machine identity with OIDC authentication bound to this
  repository's `crates-io` environment, with read access to that secret path
  only, and set the environment variables `INFISICAL_CRATES_IO_IDENTITY_ID`,
  `INFISICAL_CRATES_IO_PROJECT_SLUG`, `INFISICAL_CRATES_IO_ENV_SLUG` and
  `INFISICAL_CRATES_IO_SECRET_PATH`.
- Confirm the crate name is still unregistered on crates.io (it was at the
  time this workflow was written), and add the organization's expected
  co-owners after the first publication.
