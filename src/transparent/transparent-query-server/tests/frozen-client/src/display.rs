//! The original released transaction-detail client over the original HTTP adapter.
use super::Exchange;
use zakura_pir_transparent::{TxidDisplayClient, TxidHttp, TxidLookup};

pub fn lookup(
    origin: &str,
    txid: &str,
    height: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let txid: [u8; 32] = hex::decode(txid)?
        .try_into()
        .map_err(|_| "txid must be 32 bytes")?;
    let mut transport = TxidHttp::new(Exchange {
        origin: origin.into(),
        client: reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(std::time::Duration::from_secs(15))
            .build()?,
    });
    let result = TxidDisplayClient::new().with_min_recent_floor(1).lookup(
        &mut transport,
        txid,
        height.parse()?,
        &|| false,
    )?;
    match result {
        TxidLookup::Found { entry, .. } => {
            println!(
                "{}",
                serde_json::json!({"entry_hex": hex::encode(entry.encode()?)})
            );
            Ok(())
        }
        other => Err(format!("released display client did not find entry: {other:?}").into()),
    }
}
