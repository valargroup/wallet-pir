//! Batch driver for the research harness.
//!
//! Reads one JSON request on stdin and writes one JSON result on stdout, so the
//! Python harness can drive real HTTP retrieval without a process per query.
//! Command-line framing costs are not part of the reported byte counts: those
//! come from the client's own accounting of what it sent and received.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use transparent_history_pir::types::Table;
use transparent_history_pir::TransparentHistoryClient;

#[derive(Deserialize)]
struct Request {
    base_url: String,
    table: String,
    rows: Vec<usize>,
    pad_to: usize,
}

#[derive(Serialize)]
struct Response {
    rows: BTreeMap<String, String>,
    queries: u64,
    upload_bytes: u64,
    download_bytes: u64,
    setup_download_bytes: u64,
    /// Published sets this table's plan downloaded, and what one costs. The
    /// harness charges a table's sets once per sync, so it needs both the
    /// decision and its unit price rather than only the product.
    public_sets: u64,
    public_params_set_bytes: u64,
    /// Batches sent, and the bytes of packing keys within the upload. Keys
    /// travel once per batch, so these two separate the reuse saving from the
    /// total rather than leaving it to be inferred.
    batches: u64,
    key_upload_bytes: u64,
    generation_id: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let request: Request = serde_json::from_str(&input)?;
    let table = match request.table.as_str() {
        "directory" => Table::Directory,
        "pages" => Table::Pages,
        other => return Err(format!("unknown table {other}").into()),
    };

    let client = TransparentHistoryClient::connect(&request.base_url).await?;
    let session = client.session();
    let generation_id = session.generation_id.clone();
    // Re-derived from the session this process fetched, so the reported charge
    // can be checked against it rather than trusted. The published parameters
    // are the cost key reuse is traded against, and a count reported where
    // bytes were meant still looks like a number.
    let set_bytes = match table {
        Table::Directory => session.directory.public_params_set_bytes,
        Table::Pages => session.pages.public_params_set_bytes,
    };

    // Measurement override. Unset is the policy a wallet would run; a value
    // forces every table onto that many sets, which is how the fresh and
    // always-share baselines are produced under this build's accounting.
    let forced = match std::env::var("PIR_KEY_SETS") {
        Ok(value) => Some(value.parse::<usize>()?),
        Err(_) => None,
    };
    let (decoded, charges) = client
        .fetch_rows_with(table, &request.rows, request.pad_to, forced)
        .await?;
    // This process starts cold, so it downloaded every set the plan asked for.
    // A wallet holds one session and would download a set once; the harness
    // charges each table's sets once per sync for that reason.
    if charges.setup_download_bytes != charges.public_sets * set_bytes {
        return Err(format!(
            "setup charge {} does not match {} sets of {set_bytes} bytes",
            charges.setup_download_bytes, charges.public_sets
        )
        .into());
    }

    use base64::Engine as _;
    let rows = request
        .rows
        .iter()
        .zip(decoded.iter())
        .map(|(row, bytes)| {
            (
                row.to_string(),
                base64::engine::general_purpose::STANDARD.encode(bytes),
            )
        })
        .collect();

    println!(
        "{}",
        serde_json::to_string(&Response {
            rows,
            queries: charges.queries,
            upload_bytes: charges.upload_bytes,
            download_bytes: charges.download_bytes,
            setup_download_bytes: charges.setup_download_bytes,
            public_sets: charges.public_sets,
            public_params_set_bytes: set_bytes,
            batches: charges.batches,
            key_upload_bytes: charges.key_upload_bytes,
            generation_id,
        })?
    );
    Ok(())
}
