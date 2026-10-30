//! Bounded wallet retrieval and refusal checks through a real HTTP listener.
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/lookup.rs"]
mod lookup;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
mod prepared;
#[path = "../../../receiver/receiver-pir-server/tests/common/router.rs"]
mod router;
use std::time::Duration;

/// Two pages recover exactly over encrypted queries, and a malformed query is 400.
#[tokio::test]
async fn encrypted_http_recovers_exact_payments_and_refuses_malformed_queries() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let paid: Vec<_> = (0..2).map(common::paid).collect();
        let tables = prepared::tables(prepared::coverage(3), &paid, 2048);
        let publications = receiver_pir_server::Publications::default();
        assert!(publications.publish(prepared::prepare(tables).unwrap(), 0));
        let app = router::router_with_publications(publications);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let wallet = lookup::Wallet::at(&origin, "/v1/receiver");
        let found = wallet.history(&common::receiver()).await.unwrap();
        assert_eq!(
            found.into_iter().map(|r| r.payment).collect::<Vec<_>>(),
            paid.into_iter().map(|p| p.1).collect::<Vec<_>>()
        );
        assert_eq!(
            wallet.post(vec![0; 8]).await,
            Err(reqwest::StatusCode::BAD_REQUEST)
        );
        server.abort();
    })
    .await
    .expect("receiver HTTP smoke exceeded its deadline");
}
