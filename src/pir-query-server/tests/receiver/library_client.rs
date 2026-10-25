//! Wallets reach this service through wallet-libraries' receiver client
//! (`zakura-pir-client`'s `receiver` module). These tests drive that
//! client against the production router over real HTTP, so its manifest, filter, PIR,
//! witness and revision handling interoperate with these routes byte for byte.
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/history.rs"]
mod history;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
mod prepared;
#[path = "../../../receiver/receiver-pir-server/tests/common/router.rs"]
mod router;
use common::{manifest, receiver, record};
use history::{history, Accepted, Session};
use receiver_pir_server::{Publication, Publications};
use router::router_with_publications;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use zakura_pir_client::receiver::transport::Transport;
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::session::MIN_ROWS;
use zakura_pir_query_server::receiver::snapshot::Snapshot;

/// The chain anchor that [`manifest`] ends at.
fn accepted() -> Accepted {
    Accepted {
        genesis: [1; 32],
        required_start: 100,
        height: 101,
        hash: [3; 32],
    }
}

/// A publication with `count` pages for the fixture receiver.
fn snapshot(count: u32) -> Snapshot {
    let records: Vec<_> = (0..count).map(|page| record(page, count)).collect();
    let mut manifest = manifest(MIN_ROWS);
    manifest.end_position = manifest.end_position.max(200 + u64::from(count));
    Snapshot::build(manifest, &records).unwrap()
}

struct Running {
    origin: String,
    publications: Publications,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serves `publication` through the production router on a local socket.
async fn serve(publication: Publication) -> Running {
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let app = router_with_publications(publications.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    Running {
        origin,
        publications,
        task,
    }
}

/// A wallet's transport over reqwest: no redirects, 410 as [`Error::Revision`], and
/// a body refused once it crosses its limit. It counts encrypted queries.
struct Http {
    client: reqwest::Client,
    queries: AtomicUsize,
}

impl Http {
    fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            queries: AtomicUsize::new(0),
        }
    }
}

impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.client.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        read(self.client.post(url).body(body), limit).await
    }
}

async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
    let failed = |e: reqwest::Error| Error::Transport(e.to_string());
    let mut response = request.send().await.map_err(failed)?;
    match response.status() {
        reqwest::StatusCode::GONE => return Err(Error::Revision),
        status if !status.is_success() => return Err(Error::Transport(status.to_string())),
        _ => {}
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failed)? {
        if body.len() + chunk.len() > limit {
            return Err(Error::Malformed);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Connects over PIR to the publication `origin` advertises.
async fn connect<'a>(origin: &str, http: &'a Http) -> Result<Session<&'a Http>, Error> {
    history::connect(origin, http, accepted()).await
}

/// The library client reads a two-page history over this service's PIR, one encrypted
/// query per page.
#[tokio::test]
async fn the_library_client_looks_up_over_pir() {
    let server = serve(prepared::prepare_payments(snapshot(2)).unwrap()).await;
    let http = Http::new();
    let client = connect(&server.origin, &http).await.unwrap();
    let payments = history(&client, receiver()).await.unwrap();
    let expected: Vec<_> = (0..2).map(|page| record(page, 2).payment).collect();
    assert_eq!(payments.len(), 2);
    for (payment, expected) in payments.iter().zip(&expected) {
        assert_eq!(
            (
                payment.position,
                payment.txid,
                payment.tx_index,
                payment.cmx
            ),
            (
                expected.position,
                expected.txid,
                expected.tx_index,
                expected.cmx
            ),
        );
    }
    assert_eq!(http.queries.load(Ordering::Relaxed), 2);
    // A receiver without payments reads as empty, not as an error.
    let mut other = receiver();
    other[0] ^= 1;
    assert!(history(&client, other).await.unwrap().is_empty());
}

/// The library client reads a payment's companion payload over this service's PIR by
/// the ordinal its record carries, one encrypted query per payload.
#[tokio::test]
async fn the_library_client_reads_companion_payloads_over_pir() {
    let snapshot = snapshot(2);
    let payloads = [[3; 512], [4; 512]];
    let table = prepared::companion_table(&snapshot, &payloads);
    let server = serve(prepared::prepare(snapshot, table).unwrap()).await;
    let http = Http::new();
    let client = connect(&server.origin, &http).await.unwrap();
    for page in 0..2 {
        let record = client.page(receiver(), page).await.unwrap().unwrap();
        assert_eq!(
            client.witness(record.ordinal).await.unwrap(),
            payloads[record.ordinal as usize]
        );
    }
    assert_eq!(http.queries.load(Ordering::Relaxed), 4);
}

/// A revoked publication answers the library client's next query 410, which it
/// reports as a revision change rather than a result.
#[tokio::test]
async fn a_revoked_publication_is_a_revision_to_the_library_client() {
    let server = serve(prepared::prepare_payments(snapshot(1)).unwrap()).await;
    let http = Http::new();
    let client = connect(&server.origin, &http).await.unwrap();
    server.publications.revoke();
    assert!(matches!(
        history(&client, receiver()).await,
        Err(Error::Revision)
    ));
    assert!(matches!(client.witness(0).await, Err(Error::Revision)));
}
