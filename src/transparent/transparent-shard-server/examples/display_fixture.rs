//! Operator-rehearsal fixture for an external txid display worker; never a production tool.
//!
//! `stage ROOT LABEL REVISION` writes the display test world's archive shard and
//! a recent shard (revision 0, or revision 1 superseding it and reaching further)
//! under `ROOT`, then a candidate publication `ROOT/candidate-LABEL`, and prints its
//! directory and map digest. `lookup URL REVISION` looks the fixture's records up
//! with the wallet's own display client and exits zero only if every entry is
//! exactly the publisher's: the oracle is the construction, not the server.
#[path = "../tests/support/display_world.rs"]
mod display_world;
use display_world::*;
use std::path::Path;
use transparent_shard_server::display::synth;

fn shards(root: &Path, revision: u32) -> Vec<synth::Published> {
    let a0 = write_shard(root, &spec(0, 100, 199, true, ""), &records(10, &[]));
    let mut recent = spec(1, 200, 260, false, &a0.digest);
    let r0 = write_shard(root, &recent, &records(20, &[last_record(20)]));
    if revision == 0 {
        return vec![a0, r0];
    }
    recent.revision = 1;
    recent.supersedes = r0.digest.clone();
    recent.end_height = 270;
    vec![
        a0.clone(),
        write_shard(root, &recent, &records(20, &[last_record(20)])),
    ]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("stage") => {
            let root = Path::new(&args[2]);
            let shards = shards(root, args[4].parse().expect("REVISION"));
            let (directory, map_sha256) =
                synth::write_candidate(root, &params(1), &shards, &args[3]).unwrap();
            println!(
                "{}",
                serde_json::json!({"directory": directory, "map_sha256": map_sha256})
            );
        }
        Some("lookup") => {
            let revision: u32 = args[3].parse().expect("REVISION");
            let mut wallet = Wallet::at(&args[2]);
            let archive = records(10, &[]);
            let recent = records(20, &[last_record(20)]);
            let mut checked = 0;
            for (set, height) in [(&archive, 150), (&recent, 230)] {
                for record in &set[CLASSES..CLASSES + 6] {
                    wallet.found(record, height);
                    checked += 1;
                }
            }
            let tip = if revision == 0 { 255 } else { 265 };
            wallet.found(recent.last().unwrap(), tip);
            checked += 1;
            println!("{}", serde_json::json!({"exact": true, "lookups": checked}));
        }
        _ => {
            eprintln!("usage: display_fixture stage ROOT LABEL REVISION | lookup URL REVISION");
            std::process::exit(2);
        }
    }
}
