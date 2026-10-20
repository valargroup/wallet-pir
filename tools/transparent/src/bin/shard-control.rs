//! Send one JSON command on stdin to the worker's root-only control socket.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    transparent_query_server::control::run_client::<transparent_query_server::history::live::Command>(
        "shard-control",
    )
}
