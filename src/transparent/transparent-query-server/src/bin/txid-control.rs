//! Send one JSON command on stdin to a txid display worker's control socket
//! and print its reply line; exit 1 unless the reply says `"ok": true`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    transparent_query_server::control::run_client::<
        transparent_query_server::tx_detail::live::DisplayCommand,
    >("txid-control")
}
