//! A wallet's lookups over HTTP, for tests until wallet-libraries' client reads the
//! directory and companion tables:
//! capture the inventory, then in each segment one directory query on the row the
//! choice object names and, on a hit, one companion query per record.
#![allow(dead_code)]
use reqwest::StatusCode;
use zakura_pir_client::native::{ProfileClientExt as _, D, P8};
use zakura_pir_query_server::receiver::inventory::{Chunk, Head, Shard};
use zakura_pir_query_server::receiver::{
    choose_row, decode_choice, lookup_entry, lookup_slot,
    native::{profile, COLS, MAGIC},
    slot_row, tag, Publication, Session, Table,
};
use zakura_pir_query_server::receiver::{
    session::HEADER_BYTES, wire::PAYLOAD_BYTES, RawRecord, ReceiverBytes,
};

/// One record a lookup read, with its witness payload.
pub type Read = (RawRecord, [u8; PAYLOAD_BYTES]);

/// The wallet routes below `base`: an origin and `/receiver/v1` or `/v1/receiver`.
#[derive(Clone)]
pub struct Wallet {
    pub http: reqwest::Client,
    pub base: String,
    /// Encrypted queries sent.
    pub queries: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Wallet {
    /// The canonical routes of `origin`.
    pub fn new(origin: &str) -> Self {
        Self::at(origin, "/receiver/v1")
    }

    /// The routes of `origin` below `prefix`.
    pub fn at(origin: &str, prefix: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("{origin}{prefix}"),
            queries: Default::default(),
        }
    }

    /// The body of `path`, or the refusal's status.
    pub async fn get(&self, path: &str) -> Result<Vec<u8>, StatusCode> {
        let response = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap();
        match response.status() {
            StatusCode::OK => Ok(response.bytes().await.unwrap().to_vec()),
            status => Err(status),
        }
    }

    /// Posts one query body, returning the response or the refusal's status.
    pub async fn post(&self, body: Vec<u8>) -> Result<Vec<u8>, StatusCode> {
        self.queries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let response = self
            .http
            .post(format!("{}/query", self.base))
            .body(body)
            .send()
            .await
            .unwrap();
        match response.status() {
            StatusCode::OK => Ok(response.bytes().await.unwrap().to_vec()),
            status => Err(status),
        }
    }

    /// The current inventory's head and every shard, fetched chunk by chunk.
    pub async fn inventory(&self) -> Result<(Head, Vec<Shard>), StatusCode> {
        let head: Head = serde_json::from_slice(&self.get("/inventory").await?).unwrap();
        let mut shards = Vec::new();
        for index in 0..head.shards.div_ceil(16) {
            let path = format!("/inventory/{}/{index}", hex::encode(head.inventory));
            let chunk: Chunk = serde_json::from_slice(&self.get(&path).await?).unwrap();
            assert_eq!((chunk.inventory, chunk.index), (head.inventory, index));
            shards.extend(chunk.shards);
        }
        Ok((head, shards))
    }

    /// Reads row `row` of `session`'s table through PIR with a query over its first
    /// `sent` rows, checking the public setup's digest and the echoed header.
    pub async fn read<M: Table>(
        &self,
        session: &Session<M>,
        row: u32,
        sent: u32,
    ) -> Result<Vec<u8>, StatusCode> {
        use sha2::Digest;
        let id = session.id().unwrap();
        let public = self.get(&format!("/public/{}", hex::encode(id))).await?;
        assert_eq!(sha2::Sha256::digest(&public)[..], session.public_digest[..]);
        let (secret, body) = query(session, row, sent);
        let response = self.post(body.clone()).await?;
        if response[..HEADER_BYTES] != body[..HEADER_BYTES] {
            // A response for another request or session is never decoded.
            return Err(StatusCode::BAD_GATEWAY);
        }
        Ok(P8
            .decode_cols(&secret, &public, &response[HEADER_BYTES..], COLS)
            .unwrap())
    }

    /// `receiver`'s records in `segment`: none on a miss, else every page in order.
    pub async fn lookup(
        &self,
        segment: &Publication,
        receiver: &ReceiverBytes,
    ) -> Result<Vec<Read>, StatusCode> {
        let (d, c) = (&segment.directory, &segment.companion);
        let choice = self
            .get(&format!(
                "/choice/{}",
                hex::encode(d.manifest.choice.digest)
            ))
            .await?;
        let choice = decode_choice(&d.manifest, &choice).unwrap();
        let t = tag(&d.manifest.salt, receiver);
        let row = choose_row(&d.manifest.salt, &choice, &t);
        let bytes = self.read(d, row, d.manifest.rows()).await?;
        let Some(entry) = lookup_entry(&d.manifest, &bytes, &t).unwrap() else {
            return Ok(Vec::new());
        };
        let mut read = Vec::new();
        for slot in entry.slots() {
            let bytes = self.read(c, slot_row(slot), c.manifest.query_rows).await?;
            let found = lookup_slot(&d.manifest, &c.manifest, &entry, slot, &bytes).unwrap();
            assert_eq!(
                &found.0.receiver, receiver,
                "a tag hit for another receiver"
            );
            read.push(found);
        }
        Ok(read)
    }

    /// `receiver`'s records in every segment of the current inventory, in chain order.
    pub async fn history(&self, receiver: &ReceiverBytes) -> Result<Vec<RawRecord>, StatusCode> {
        let (_, shards) = self.inventory().await?;
        let mut records = Vec::new();
        for segment in shards.iter().flat_map(|s| &s.segments) {
            records.extend(
                self.lookup(segment, receiver)
                    .await?
                    .into_iter()
                    .map(|r| r.0),
            );
        }
        Ok(records)
    }
}

/// A fresh query for row `row` of `session`'s table over its first `sent` rows, with its
/// secret: the request header, then the P8 upload.
pub fn query<M: Table>(
    session: &Session<M>,
    row: u32,
    sent: u32,
) -> (zakura_pir_client::native::NativeSecret, Vec<u8>) {
    let profile = profile(M::KIND, session.manifest.rows()).unwrap();
    let (secret, upload) = P8
        .prepare(
            &profile.setup,
            &profile.masks[..sent as usize / D],
            sent as usize,
            row as usize,
        )
        .unwrap();
    let id = session.id().unwrap();
    (secret, [&MAGIC[..], &id, &[7; 16], &upload].concat())
}
