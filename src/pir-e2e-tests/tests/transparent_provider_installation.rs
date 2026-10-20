//! Checkout-free provider installation: the packaged `pir-query-server`,
//! `shard-assign` and `shard-prune` from an extracted release, configured by the
//! extracted role renderer, against real HTTPS publisher and archive origins.
#[path = "../../transparent/transparent-query-server/tests/support/history_set.rs"]
mod history_set;
#[path = "../../transparent/transparent-query-server/tests/support/revision.rs"]
mod revision;
use history_set::*;

#[path = "../../transparent/transparent-query-server/tests/support/provider_fixture.rs"]
mod provider_fixture;
use provider_fixture::*;

#[path = "../../transparent/transparent-query-server/tests/support/provider_install.rs"]
mod install;
