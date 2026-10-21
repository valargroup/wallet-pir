//! A real wallet body cannot select a different publication, setup or anchor.
use enhance_query_server::{
    client::QuerySession,
    protocol::{QueryBinding, ShardSession, HEADER_BYTES},
};
use pir_e2e_tests::enhance_fixture::{record, System};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wallet_binding_mismatches_are_refused_before_forwarding() {
    let system = System::start((0..67).map(record).collect()).await;
    let http = reqwest::Client::new();
    let session: ShardSession = http
        .get(format!(
            "{}/v1/enhance/sessions/{}/0",
            system.origin, system.manifest.generation
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session = QuerySession::new(&system.manifest, session).unwrap();
    let (query, _) = session.prepare_position(66).unwrap();
    let initial = QueryBinding::decode(query.body()).unwrap();
    for fault in ["generation", "recovery", "anchor", "session"] {
        let mut binding = initial;
        match fault {
            "generation" => binding.generation += 1,
            "recovery" => binding.recovery_epoch += 1,
            "anchor" => binding.anchor_hash[0] ^= 1,
            _ => binding.session_id[0] ^= 1,
        }
        let mut body = binding.encode();
        body.extend_from_slice(&query.body()[HEADER_BYTES..]);
        let response = http
            .post(format!("{}/v1/enhance/query", system.origin))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if matches!(fault, "generation" | "recovery") {
                reqwest::StatusCode::CONFLICT
            } else {
                reqwest::StatusCode::GONE
            },
            "{fault}"
        );
    }
}
