use enhance_pir::{EnhanceSession, client::QuerySession};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read(std::env::args().nth(1).ok_or("expected captured init path")?)?;
    let session: EnhanceSession = serde_json::from_slice(&raw)?;
    let query = QuerySession::from_session(session)?.prepare_position(0)?.0;
    println!("query_body_bytes={}", query.body().len());
    Ok(())
}
