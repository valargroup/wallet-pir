//! Send one JSON command on stdin to a txid display worker's control socket
//! and print its reply line; exit 1 unless the reply says `"ok": true`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    transparent_shard_server::control::run_client::<
        transparent_shard_server::display::live::DisplayCommand,
    >("txid-control")
}
