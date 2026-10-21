//! Exercise withdrawal through the serving router, including after reopening.
use axum::{body::Body, http::StatusCode, Router};
use std::io::{Read, Seek, Write};
use std::time::Duration;
use tower::ServiceExt;
use transparent_shared::{history::shardset::SegmentSource, publication};

pub fn damage_received(source: &SegmentSource) {
    let path = source.path();
    let artifact = publication::Artifact {
        path: String::new(),
        sha256: source.sha256().into(),
        bytes: source.bytes() as u64,
        kind: publication::Kind::RecentTable,
    };
    publication::record_received_file(path, &artifact, path).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.rewind().unwrap();
    file.write_all(&byte).unwrap();
}

async fn status(app: &Router, path: &str) -> StatusCode {
    app.clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

pub async fn assert_withdrawn(app: Router, failed: &str, healthy: &str, pending: bool) {
    if pending {
        assert_eq!(status(&app, failed).await, StatusCode::OK);
    } else {
        assert_eq!(
            status(&app, failed).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "a saved failure must be refused on the first request after reopening"
        );
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while status(&app, failed).await != StatusCode::SERVICE_UNAVAILABLE {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the retained revision is checked and withdrawn");
    assert_eq!(status(&app, healthy).await, StatusCode::OK);
}
