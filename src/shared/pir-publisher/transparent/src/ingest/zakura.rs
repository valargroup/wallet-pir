//! The Zakura JSON-RPC client this service reads the node through.
//!
//! The client is wallet-libraries' `zakura_server_chain::NodeClient`, which also
//! reads and checks blocks and transactions. Blocks and transactions are fetched
//! raw (verbosity 0) and parsed there. The verbose JSON form renders scripts
//! through the node's own notion of what is standard; raw parsing yields every
//! script exactly as it appears on chain, which is what a filter must cover.

use std::path::Path;

pub use zakura_server_chain::{NodeClient, NodeError};

/// A client for the node at `rpc_url`, authenticating with the cookie file at
/// `cookie_path` as it is now, that names this service in the node's logs.
pub fn connect(
    rpc_url: impl Into<String>,
    cookie_path: impl AsRef<Path>,
) -> Result<NodeClient, NodeError> {
    Ok(NodeClient::from_cookie_file(rpc_url, cookie_path)?.with_request_id("transparent-filter"))
}
