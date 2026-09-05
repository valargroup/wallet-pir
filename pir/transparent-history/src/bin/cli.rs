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
    // The published parameters are downloaded once and are public. Charging
    // them to the session rather than to each call keeps a batch of queries
    // from being credited with setup it did not repeat.
    let setup_download_bytes =
        (session.directory.public_params.len() + session.pages.public_params.len()) as u64;
    let generation_id = session.generation_id.clone();

    let (decoded, charges) = client
        .fetch_rows(table, &request.rows, request.pad_to)
        .await?;

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
            setup_download_bytes,
            generation_id,
        })?
    );
    Ok(())
}
