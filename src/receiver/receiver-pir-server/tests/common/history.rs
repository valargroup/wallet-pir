//! A wallet's walk of one receiver's history, one encrypted PIR query per page, over a
//! captured inventory.
#![allow(dead_code)]
use std::sync::Arc;
use zakura_pir_client::receiver::transport::{Inventories, InventoryClient, Segment, Transport};
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::session::Manifest;
use zakura_pir_query_server::receiver::{
    snapshot::check_next, witness::PAYLOAD_BYTES, Hash, Payment, RawRecord, ReceiverBytes,
};

/// The chain anchor and required history start a wallet accepts an inventory at.
#[derive(Clone, Copy, Debug)]
pub struct Accepted {
    pub genesis: Hash,
    pub required_start: u32,
    pub height: u32,
    pub hash: Hash,
}

/// A captured inventory, accepted at a wallet's anchor, and its first shard's first
/// segment, which single-table fixtures hold everything in.
pub struct Session<T> {
    pub client: InventoryClient<T>,
    segment: Arc<Segment>,
}

/// Fetches the inventory `origin` advertises, captures and accepts it, and connects to
/// its first segment's sessions.
pub async fn connect<T: Transport>(
    origin: &str,
    http: T,
    accepted: Accepted,
) -> Result<Session<T>, Error> {
    let head = Inventories::head(origin, &http, &accepted.genesis).await?;
    let inventory = Inventories::capture(origin, &http, head).await?;
    inventory.accept(
        &accepted.genesis,
        accepted.required_start,
        accepted.height,
        &accepted.hash,
    )?;
    let client = InventoryClient::new(origin, http, inventory, false);
    let segment = client.segment(0, 0).await?;
    Ok(Session { client, segment })
}

impl<T: Transport> Session<T> {
    /// The first segment's payment session manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.segment.publication().payments
    }

    /// Page `page` of `receiver`'s history in the first segment.
    pub async fn page(
        &self,
        receiver: ReceiverBytes,
        page: u32,
    ) -> Result<Option<RawRecord>, Error> {
        self.client.page(&self.segment, receiver, page).await
    }

    /// The first segment's companion payload for `ordinal`.
    pub async fn witness(&self, ordinal: u32) -> Result<[u8; PAYLOAD_BYTES], Error> {
        self.client.witness(&self.segment, ordinal).await
    }
}

/// Reads `receiver`'s pages in the first segment from zero until the last one its
/// records count, each continuing the one before ([`check_next`]). An absent first page
/// is an empty history. Any failure is an error, never a partial history.
pub async fn history<T: Transport>(
    session: &Session<T>,
    receiver: ReceiverBytes,
) -> Result<Vec<Payment>, Error> {
    segment_history(&session.client, &session.segment, receiver).await
}

/// Reads `receiver`'s history in every shard and segment of `client`'s inventory,
/// blindly, in chain order.
pub async fn inventory_history<T: Transport>(
    client: &InventoryClient<T>,
    receiver: ReceiverBytes,
) -> Result<Vec<Payment>, Error> {
    let mut payments = Vec::new();
    for shard in client.inventory().shards() {
        for index in 0..shard.segments.len() {
            let segment = client.segment(shard.index, index as u32).await?;
            payments.extend(segment_history(client, &segment, receiver).await?);
        }
    }
    payments.sort_by_key(|p| p.position);
    Ok(payments)
}

/// [`history`] in one segment.
async fn segment_history<T: Transport>(
    client: &InventoryClient<T>,
    segment: &Segment,
    receiver: ReceiverBytes,
) -> Result<Vec<Payment>, Error> {
    let mut records: Vec<RawRecord> = Vec::new();
    loop {
        let Some(record) = client.page(segment, receiver, records.len() as u32).await? else {
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
