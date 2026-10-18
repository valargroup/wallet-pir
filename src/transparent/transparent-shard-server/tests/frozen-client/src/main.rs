//! Test-only process using the original released 391 adapter and c085 engine.
use std::{io::Read, time::Duration};
use zakura_pir_transparent::{
    FilterSource, HttpExchange, HttpFailure, HttpReply, HttpRequest, ShardTransport,
    TransparentPirHttp,
};
struct Exchange {
    origin: String,
    client: reqwest::blocking::Client,
}
impl HttpExchange for Exchange {
    fn send(&self, request: &HttpRequest) -> Result<HttpReply, HttpFailure> {
        let method = reqwest::Method::from_bytes(request.method.as_str().as_bytes()).unwrap();
        let mut response = self
            .client
            .request(method, format!("{}{}", self.origin, request.path))
            .header("content-type", "application/octet-stream")
            .body(request.body.clone())
            .send()
            .map_err(|_| HttpFailure::Unreachable)?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|s| s.to_str().ok())
            .map(str::to_owned);
        let success = (200..300).contains(&status);
        let limit = if success {
            request.limit
        } else {
            request.error_limit
        };
        let mut body = Vec::new();
        response
            .by_ref()
            .take(limit as u64 + u64::from(success))
            .read_to_end(&mut body)
            .map_err(|_| HttpFailure::Unreachable)?;
        if success && body.len() > limit {
            return Err(HttpFailure::TooLarge);
        }
        Ok(HttpReply {
            status,
            retry_after,
            map_sha256: None,
            body,
        })
    }
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: frozen-provider-client ORIGIN SCRIPT_HEX BIRTHDAY".into());
    }
    let mut http = TransparentPirHttp::new(
        Exchange {
            origin: args[1].clone(),
            client: reqwest::blocking::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(15))
                .build()?,
        },
        16 << 20,
    );
    let (mut filters, mut shards) = http.split();
    let (map, map_bytes) = filters.shard_map()?;
    let map: transparent_filter::ShardMap = serde_json::from_slice(&map)?;
    map.check_shape()?;
    let (init, _) = shards.init()?;
    let geometry = transparent_wallet::parse_init(&init)?;
    let birthday = args[3].parse()?;
    let script = transparent_filter::ScriptBytes::new(hex::decode(&args[2])?);
    let last = map.shards.last().ok_or("empty map")?;
    let outcome = transparent_wallet::sync(
        &map,
        map_bytes,
        &geometry,
        &mut filters,
        &mut shards,
        &[script],
        birthday,
        &transparent_wallet::StaticChain::from_map(&map),
        &transparent_wallet::Anchor {
            height: last.end_height,
            hash: last.terminal_block_hash.clone(),
        },
    )?;
    let utxos: Vec<_> = outcome.ledger.utxos().map(|u| serde_json::json!({"txid":u.txid.to_display_hex(), "index":u.output_index,
        "value":u.value, "script":hex::encode(&u.script), "height":u.creation_height, "coinbase":u.coinbase})).collect();
    let spends: Vec<_> = outcome.ledger.spends().iter().map(|s| serde_json::json!({"spent_txid":s.spent_txid.to_display_hex(),
        "spent_index":s.spent_output_index, "spending_txid":s.spending_txid.to_display_hex(), "input_index":s.input_index,
        "height":s.height,"value":s.value,"script":hex::encode(&s.script)})).collect();
    let history: Vec<_> = outcome
        .ledger
        .history()
        .iter()
        .map(|h| {
            serde_json::json!({"txid":h.txid.to_display_hex(),"height":h.height,
        "received":h.received,"spent":h.spent})
        })
        .collect();
    println!(
        "{}",
        serde_json::json!({"covered_through":outcome.covered_through,"matched_shards":outcome.matched_shards,
        "utxos":utxos,"spends":spends,"history":history,"unresolved":outcome.ledger.unresolved().len()})
    );
    Ok(())
}
