//! Checkout-free provider installation: the packaged `pir-query-server`,
//! `shard-assign` and `shard-prune` from an extracted release, configured by the
//! extracted role renderer, against real HTTPS publisher and archive origins.
#[path = "../../../src/pir-query-server/tests/transparent/support/history_set.rs"]
mod history_set;
#[path = "../../../src/pir-query-server/tests/transparent/support/revision.rs"]
mod revision;
use history_set::*;

#[path = "../../../src/pir-query-server/tests/transparent/support/provider_fixture.rs"]
mod provider_fixture;
use provider_fixture::*;

#[path = "../../../src/pir-query-server/tests/transparent/support/provider_install.rs"]
mod install;
