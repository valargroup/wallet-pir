# Repository rename consumer follow-up

The `valargroup/enhance-pir` GitHub repository is renamed to
`valargroup/wallet-pir`. GitHub's redirect keeps existing Git fetches working,
but consumers should adopt the canonical URL and the reorganized documentation
paths.

The active `zakura-core/wallet-libraries` work contains the known source
references. Update its `zakura/wallet-transparent/Cargo.toml` entries for
`transparent-wallet`, `transparent-filter`, `transparent-shard`,
`transparent-events`, and `transparent-shard-server`, including the
feature-bearing dev dependency, then regenerate its Cargo lockfile. Preserve
the pinned revision unless that consumer is deliberately upgrading behavior.

Update `docs/zakura_transparent_pir.md` to use
`https://github.com/valargroup/wallet-pir` and the new paths under
`transparent/docs/` and `transparent/crates/`. Verify the canonical repository
and any active release branches rather than editing local experimental
worktrees independently.
