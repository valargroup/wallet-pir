//! A wallet's walk of one receiver's history, one encrypted PIR query per page.
#![allow(dead_code)]
use receiver_directory::{snapshot::check_next, Payment, RawRecord, ReceiverBytes};
use receiver_pir::{
    transport::{DirectoryClient, Transport},
    Error,
};

/// Reads `receiver`'s pages from zero until the last one its records count, each
/// continuing the one before ([`check_next`]). An absent first page is an empty
/// history. Any failure is an error, never a partial history.
pub async fn history<T: Transport>(
    client: &DirectoryClient<T>,
    receiver: ReceiverBytes,
) -> Result<Vec<Payment>, Error> {
    let mut records: Vec<RawRecord> = Vec::new();
    loop {
        let Some(record) = client.page(receiver, records.len() as u32).await? else {
            return Ok(Vec::new());
        };
        if let Some(previous) = records.last() {
            check_next(previous, &record).map_err(|_| Error::Malformed)?;
        }
        let complete = record.page + 1 == record.total;
        records.push(record);
        if complete {
            return Ok(records.into_iter().map(|r| r.payment).collect());
        }
    }
}
