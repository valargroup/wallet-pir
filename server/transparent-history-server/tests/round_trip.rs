//! End-to-end: publish a generation, serve it, retrieve rows privately, and
//! check each decoded row against the raw table.
//!
//! This is the property the measurement rests on. A byte comparison against
//! ordinary retrieval means nothing if the rows coming back are not the rows
//! that were stored, so the retrieval path is checked against the plaintext
//! table rather than against itself.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use sha2::{Digest, Sha256};
use std::path::Path;
use tower::ServiceExt;
use transparent_history_pir::types::{HistorySession, Table};
use transparent_history_pir::TableClient;
use transparent_history_server::generation::LoadedGeneration;
use transparent_history_server::service::{router, ServiceState};

/// The smallest geometry the harness can emit, so the test stays quick: the
/// scheme's cost is driven by rows and row bytes.
const DIRECTORY_ROWS: u64 = 2048;
const DIRECTORY_ROW_BYTES: u32 = 3584;
const PAGE_ROWS: u64 = 2048;
const PAGE_ROW_BYTES: u32 = 3584;

/// Deterministic filler that differs per row and per offset, so a row returned
/// from the wrong index or a row that is silently zero both fail.
fn table_bytes(rows: u64, row_bytes: u32, salt: u8) -> Vec<u8> {
    let mut data = vec![0u8; (rows as usize) * (row_bytes as usize)];
    for row in 0..rows as usize {
        for offset in 0..row_bytes as usize {
            data[row * row_bytes as usize + offset] =
                (row as u8) ^ (offset as u8).rotate_left(3) ^ salt;
        }
    }
    data
}

fn publish(dir: &Path) -> (Vec<u8>, Vec<u8>) {
    let directory = table_bytes(DIRECTORY_ROWS, DIRECTORY_ROW_BYTES, 0x11);
    let pages = table_bytes(PAGE_ROWS, PAGE_ROW_BYTES, 0x77);
    let manifest = serde_json::json!({
        "schema": transparent_history_server::generation::SCHEMA,
        "generation_schema": transparent_history_server::generation::GENERATION_SCHEMA,
        "network": "main",
        "start": 3_470_268u64,
        "end": 3_471_419u64,
        "anchor": "00".repeat(32),
        "directory": {
            "rows": DIRECTORY_ROWS,
            "row_bytes": DIRECTORY_ROW_BYTES,
            "sha256": hex::encode(Sha256::digest(&directory)),
        },
        "pages": {
            "rows": PAGE_ROWS,
            "row_bytes": PAGE_ROW_BYTES,
            "sha256": hex::encode(Sha256::digest(&pages)),
        },
    });
    // The harness names a generation directory by the digest of its manifest
    // bytes, and the loader re-derives that, so the bytes written here are the
    // bytes hashed.
    let encoded = serde_json::to_vec(&manifest).unwrap();
    let id = hex::encode(Sha256::digest(&encoded));
    let out = dir.join(&id);
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("manifest.json"), &encoded).unwrap();
    std::fs::write(out.join("directory.bin"), &directory).unwrap();
    std::fs::write(out.join("pages.bin"), &pages).unwrap();
    (directory, pages)
}

fn generation_dir(root: &Path) -> std::path::PathBuf {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .expect("published generation")
}

async fn get(state: &ServiceState, path: &str) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body.to_vec())
}

async fn post(state: &ServiceState, path: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body.to_vec())
}

struct Fixture {
    state: ServiceState,
    session: HistorySession,
    directory: Vec<u8>,
    pages: Vec<u8>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let (directory, pages) = publish(dir.path());
    let loaded = LoadedGeneration::open(&generation_dir(dir.path())).unwrap();
    let state = ServiceState::build(&loaded).unwrap();
    let (status, body) = get(&state, "/v1/transparent-history/init").await;
    assert_eq!(status, StatusCode::OK);
    let session: HistorySession = serde_json::from_slice(&body).unwrap();
    Fixture {
        state,
        session,
        directory,
        pages,
        _dir: dir,
    }
}

fn raw_row(table: &[u8], row_bytes: u32, row: usize) -> &[u8] {
    &table[row * row_bytes as usize..(row + 1) * row_bytes as usize]
}

#[tokio::test]
async fn retrieved_rows_equal_the_published_table() {
    let f = fixture().await;

    for (table, session, raw, row_bytes, path) in [
        (
            Table::Directory,
            f.session.directory.clone(),
            &f.directory,
            DIRECTORY_ROW_BYTES,
            "/v1/transparent-history/directory/query",
        ),
        (
            Table::Pages,
            f.session.pages.clone(),
            &f.pages,
            PAGE_ROW_BYTES,
            "/v1/transparent-history/pages/query",
        ),
    ] {
        let client = TableClient::new(session, table).unwrap();
        // First, last and an interior row: an off-by-one in the row mapping
        // survives a single-row test.
        for row in [0usize, 1, 1023, client.rows() - 1] {
            let query = client.prepare(row).unwrap();
            let (status, response) = post(&f.state, path, query.body.clone()).await;
            assert_eq!(status, StatusCode::OK, "{table:?} row {row}");
            let decoded = client.decode(query, &response).unwrap();
            assert_eq!(
                decoded,
                raw_row(raw, row_bytes, row),
                "{table:?} row {row} decoded to the wrong bytes"
            );
        }
    }
}

/// Every query is the same size regardless of which row it selects, and so is
/// every response. A difference here would leak the selection through the
/// transport even though the contents are encrypted.
#[tokio::test]
async fn queries_and_responses_have_a_fixed_size() {
    let f = fixture().await;
    let client = TableClient::new(f.session.pages.clone(), Table::Pages).unwrap();

    let mut sizes = std::collections::BTreeSet::new();
    let mut response_sizes = std::collections::BTreeSet::new();
    for row in [0usize, 7, 1500, client.rows() - 1] {
        let query = client.prepare(row).unwrap();
        sizes.insert(query.body.len());
        let (status, response) = post(
            &f.state,
            "/v1/transparent-history/pages/query",
            query.body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        response_sizes.insert(response.len());
        client.decode(query, &response).unwrap();
    }
    assert_eq!(sizes.len(), 1, "query size varies with the selected row");
    assert_eq!(
        response_sizes.len(),
        1,
        "response size varies with the selected row"
    );
}

#[tokio::test]
async fn a_query_naming_another_generation_is_refused() {
    let f = fixture().await;
    let client = TableClient::new(f.session.directory.clone(), Table::Directory).unwrap();
    let mut query = client.prepare(3).unwrap();
    query.body[..8].copy_from_slice(&0xdead_beefu64.to_le_bytes());

    let (status, _) = post(
        &f.state,
        "/v1/transparent-history/directory/query",
        query.body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_truncated_query_is_refused_rather_than_padded() {
    let f = fixture().await;
    let client = TableClient::new(f.session.directory.clone(), Table::Directory).unwrap();
    let query = client.prepare(3).unwrap();
    let truncated = query.body[..query.body.len() - 1].to_vec();

    let (status, _) = post(
        &f.state,
        "/v1/transparent-history/directory/query",
        truncated,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The two tables have different published parameters, so a session for one
/// must not validate as the other.
#[tokio::test]
async fn a_table_session_does_not_validate_as_the_other_table() {
    let f = fixture().await;
    assert!(TableClient::new(f.session.pages.clone(), Table::Directory).is_err());
    assert!(TableClient::new(f.session.directory.clone(), Table::Pages).is_err());
}
