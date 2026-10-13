//! Send one JSON command on stdin to the worker's root-only control socket.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    transparent_shard_server::control::run_client::<transparent_shard_server::live::Command>(
        "shard-control",
    )
}
