//! Bounded wallet retrieval and refusal checks through a real HTTP listener.
#[path = "../../receiver-store/tests/common/mod.rs"]
mod common;
#[path = "common/history.rs"]
mod history;
#[path = "common/router.rs"]
mod router;
use common::{manifest, receiver, record};
use history::history;
use receiver_directory::snapshot::Snapshot;
use receiver_pir::{
    server::Server,
    transport::{DirectoryClient, Transport},
    AcceptedCoverage, Error, MIN_ROWS,
};
use receiver_pir_server::{Publication, Publications};
use router::router_with_publications;
use std::time::Duration;

/// The chain anchor that [`manifest`] ends at.
fn accepted() -> AcceptedCoverage {
    AcceptedCoverage {
        genesis: [1; 32],
        required_start: 100,
        height: 101,
        hash: [3; 32],
    }
}
fn snapshot(count: u32) -> Snapshot {
    snapshot_rows(count, MIN_ROWS)
}
/// A publication of `rows` rows with `count` pages for the fixture receiver.
fn snapshot_rows(count: u32, rows: u32) -> Snapshot {
    let records: Vec<_> = (0..count).map(|page| record(page, count)).collect();
    let mut manifest = manifest(rows);
    // Positions start at 200; a long history needs room after them.
    manifest.end_position = manifest.end_position.max(200 + u64::from(count));
    Snapshot::build(manifest, &records).unwrap()
}
struct Running {
    origin: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(snapshot: Snapshot) -> Running {
    serve_publication(Publication::verified(Server::new(snapshot).unwrap(), None)).await
}
/// Serve one prepared publication through the production router.
async fn serve_publication(publication: Publication) -> Running {
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    serve_router(router_with_publications(publications)).await
}
async fn serve_router(app: axum::Router) -> Running {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    Running { origin, task }
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// A test transport over reqwest; wallets bring their own. It maps 410 to [`Error::Revision`].
struct Http(reqwest::Client);

impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}

/// Send `request` and return its successful body, failing as soon as it exceeds `limit`
/// bytes.
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

/// Connects over PIR to the publication `origin` advertises, accepted at `accepted`.
async fn connect<T: Transport>(
    origin: &str,
    http: T,
    accepted: AcceptedCoverage,
) -> Result<DirectoryClient<T>, Error> {
    let manifest = DirectoryClient::fetch_manifest(origin, &http).await?;
    DirectoryClient::connect_manifest(origin, http, accepted, manifest).await
}

#[tokio::test]
async fn encrypted_http_recovers_exact_payments_and_refuses_wrong_anchors() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = serve(snapshot(2)).await;
        let client = connect(&server.origin, Http(http()), accepted())
            .await
            .unwrap();
        let payments = history(&client, receiver()).await.unwrap();
        assert_eq!(
            payments,
            (0..2)
                .map(|page| record(page, 2).payment)
                .collect::<Vec<_>>()
        );
        let mut wrong = accepted();
        wrong.hash[0] ^= 1;
        assert!(connect(&server.origin, Http(http()), wrong).await.is_err());
        let malformed = http()
            .post(format!("{}/v1/receiver/query", server.origin))
            .body(vec![0; 8])
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    })
    .await
    .expect("receiver HTTP smoke exceeded its deadline");
}
