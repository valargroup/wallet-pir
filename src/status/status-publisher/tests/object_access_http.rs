//! A retained name authorizes access, while the shared opener checks its file.
use pir_publication::PublishedObjectStore;
use reqwest::StatusCode;
use status_publisher::publisher::{Config, Publisher};

#[tokio::test]
async fn retained_objects_serve_after_restart_but_unpublished_or_redirected_files_do_not() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("distribution/objects");
    let store = PublishedObjectStore::open(directory.clone()).unwrap();
    let retained = store.store(b"retained bytes").unwrap();
    store
        .retain_publication([retained.clone()], 100, 0)
        .unwrap();
    let unpublished = store.store(b"unpublished bytes").unwrap();
    drop(store);
    let publisher = Publisher::new(Config {
        state_dir: root.path().into(),
        salt_hex: "01".repeat(32),
        window_blocks: 1,
        private_listen: "127.0.0.1:0".parse().unwrap(),
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!(
        "http://{}/provider/status/v1/objects",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, publisher.private_routes())
            .await
            .unwrap()
    });
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{origin}/{}", retained.sha256))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(&response.bytes().await.unwrap()[..], b"retained bytes");
    assert_eq!(
        client
            .get(format!("{origin}/{}", unpublished.sha256))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .get(format!("{origin}/retained-publications.json"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Retention alone cannot authorize a replacement redirect on disk.
    let retained_path = directory.join(&retained.sha256);
    std::fs::remove_file(&retained_path).unwrap();
    std::os::unix::fs::symlink(directory.join(&unpublished.sha256), &retained_path).unwrap();
    assert_eq!(
        client
            .get(format!("{origin}/{}", retained.sha256))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    server.abort();
}
