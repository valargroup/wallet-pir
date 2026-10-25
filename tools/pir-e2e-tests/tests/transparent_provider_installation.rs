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

#[allow(dead_code)] // Only the wallet transport is used here.
#[path = "../../../src/pir-query-server/tests/support/status_http.rs"]
mod status_http;
// Both publisher fixtures use this terminator. One declaration keeps the test
// from loading the same file as two modules.
#[path = "support/status_publisher.rs"]
mod status_publisher;
#[path = "../../../src/pir-query-server/tests/support/status_tls.rs"]
mod tls;
// Transaction-detail publication and wallet behind the shared public listener case.
#[path = "support/display_publisher.rs"]
mod display_publisher;
#[path = "../../../src/pir-query-server/tests/transparent/support/display_world.rs"]
mod display_world;
// The combined Enhance product of the all-products shared listener case.
#[cfg(all(feature = "enhance-tests", feature = "native-reinspiring"))]
#[path = "support/all_products.rs"]
mod all_products;
#[cfg(all(feature = "enhance-tests", feature = "native-reinspiring"))]
#[path = "support/enhance_wallet.rs"]
mod enhance_wallet;

#[cfg(feature = "receiver-tests")]
#[path = "support/query_host.rs"]
mod query_host;
#[cfg(feature = "receiver-tests")]
#[path = "support/receiver.rs"]
mod receiver_publisher;
