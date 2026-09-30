//! Reference/demo transport only: no wallet scheduling, persistence or authority.
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use transparent_events::Txid;
use transparent_shard::{
    manifest::query_binding,
    txid::{self, TransparentDisplayRecord},
    Geometry, ShardManifest,
};
use transparent_shard_server::shardset::Table;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug)]
pub enum LookupResult {
    Found(TransparentDisplayRecord),
    Absent,
    Unsupported,
    PlacementUnknown,
}
pub struct PrivateClient {
    pub http: reqwest::Client,
    pub url: String,
    pub paths: Vec<String>,
    pub uploaded: u64,
    pub downloaded: u64,
    pub queries: u64,
    setup: HashMap<(String, Table, u32), (Vec<u8>, [u8; 8])>,
}
impl PrivateClient {
    /// Placement is supplied by the caller's accepted chain; never discover it
    /// through a public txid URL. These two early results dispatch no requests.
    pub async fn lookup_mined(
        &mut self,
        map: &transparent_filter::ShardMap,
        txid: Txid,
        accepted_height: Option<u64>,
    ) -> Result<LookupResult, Error> {
        let Some(height) = accepted_height else {
            return Ok(LookupResult::PlacementUnknown);
        };
        let Some(entry) = map
            .shards
            .iter()
            .find(|s| (s.start_height..=s.end_height).contains(&height))
        else {
            return Ok(LookupResult::PlacementUnknown);
        };
        if entry.txid_segments.is_none() {
            return Ok(LookupResult::Unsupported);
        }
        let manifest = self.manifest(entry).await?;
        let geometry =
            transparent_shard::layout::by_name(&manifest.geometry).ok_or("unsupported geometry")?;
        Ok(
            match self.lookup(&manifest, geometry, txid, height).await? {
                Some(record) => LookupResult::Found(record),
                None => LookupResult::Absent,
            },
        )
    }
    pub fn new(url: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .unwrap(),
            url,
            paths: Vec::new(),
            uploaded: 0,
            downloaded: 0,
            queries: 0,
            setup: HashMap::new(),
        }
    }
    async fn get(&mut self, path: String) -> Result<Vec<u8>, Error> {
        self.paths.push(path.clone());
        let bytes = self
            .http
            .get(format!("{}{path}", self.url))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        self.downloaded += bytes.len() as u64;
        Ok(bytes.to_vec())
    }
    pub async fn manifest(
        &mut self,
        entry: &transparent_filter::ShardMapEntry,
    ) -> Result<ShardManifest, Error> {
        let b = self
            .get(format!(
                "/v1/shards/{}/revisions/{}/manifest",
                entry.shard_id, entry.manifest_digest
            ))
            .await?;
        let m: ShardManifest = serde_json::from_slice(&b)?;
        if m.digest() != entry.manifest_digest
            || m.shard_id != entry.shard_id
            || m.start_height != entry.start_height
            || m.end_height != entry.end_height
            || m.terminal_block_hash != entry.terminal_block_hash
            || m.geometry != entry.geometry
        {
            return Err("manifest identity/placement mismatch".into());
        }
        Ok(m)
    }
    pub async fn row(
        &mut self,
        m: &ShardManifest,
        g: &Geometry,
        table: Table,
        row: u64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let digest = m.digest();
        let profile = transparent_native::TableProfile::new(
            &m.schema,
            g.name,
            table.as_str(),
            table.rows(g),
            table.row_bytes(g),
        )?;
        let segments = match table {
            Table::Directory => m.directory_segments.len(),
            Table::Pages => m.page_segments.len(),
            Table::TxDirectory => m
                .txid_display
                .as_ref()
                .ok_or("display unsupported")?
                .directory_segments
                .len(),
            Table::TxPages => m
                .txid_display
                .as_ref()
                .ok_or("display unsupported")?
                .page_segments
                .len(),
        };
        let mut params = Vec::new();
        for segment in 0..segments {
            let key = (digest.clone(), table, segment as u32);
            if !self.setup.contains_key(&key) {
                let path = format!(
                    "/v1/shards/{}/revisions/{digest}/setup/{}/{segment}",
                    m.shard_id,
                    table.as_str()
                );
                let s: serde_json::Value = serde_json::from_slice(&self.get(path).await?)?;
                if s["manifest_digest"] != digest
                    || s["table"] != table.as_str()
                    || s["segment"] != segment
                    || s["segments"] != segments
                {
                    return Err("setup identity mismatch".into());
                }
                let bytes = B64.decode(s["public_params"].as_str().ok_or("setup params")?)?;
                if bytes.len() != profile.scheme.public_bytes
                    || s["public_params_sha256"] != hex::encode(Sha256::digest(&bytes))
                {
                    return Err("setup digest/length".into());
                }
                let epoch: [u8; 8] =
                    hex::decode(s["public_params_epoch"].as_str().ok_or("setup epoch")?)?
                        .try_into()
                        .map_err(|_| "epoch length")?;
                self.setup.insert(key.clone(), (bytes, epoch));
            }
            params.push(self.setup[&key].clone());
        }
        let (secret, upload) = profile.prepare(usize::try_from(row)?)?;
        let binding = query_binding(&digest, table.as_str());
        let mut body = binding.to_vec();
        body.extend(upload);
        self.uploaded += body.len() as u64;
        self.queries += 1;
        let path = format!(
            "/v1/shards/{}/revisions/{digest}/query/{}",
            m.shard_id,
            table.as_str()
        );
        self.paths.push(path.clone());
        let response = self
            .http
            .post(format!("{}{path}", self.url))
            .body(body)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        self.downloaded += response.len() as u64;
        let stride = 16 + profile.scheme.response_bytes;
        if response.len() != segments * stride {
            return Err("response length".into());
        }
        (0..segments)
            .map(|i| {
                let frame = &response[i * stride..(i + 1) * stride];
                if frame[..8] != binding || frame[8..16] != params[i].1 {
                    return Err("response binding/epoch".into());
                }
                profile
                    .decode(&secret, &params[i].0, &frame[16..])
                    .map_err(Into::into)
            })
            .collect()
    }
    pub async fn lookup(
        &mut self,
        m: &ShardManifest,
        g: &Geometry,
        txid: Txid,
        accepted_height: u64,
    ) -> Result<Option<TransparentDisplayRecord>, Error> {
        if !(m.start_height..=m.end_height).contains(&accepted_height) {
            return Err("unknown accepted placement".into());
        }
        let d = m.txid_display.as_ref().ok_or("display unsupported")?;
        d.validate()?;
        let mut rows = Vec::new();
        for row in txid::candidate_rows(txid, m.shard_id, g.directory_rows)
            .into_iter()
            .collect::<BTreeSet<_>>()
        {
            rows.extend(self.row(m, g, Table::TxDirectory, row).await?);
        }
        let Some(entry) = txid::find_directory(&rows, txid)? else {
            return Ok(None);
        };
        let mut pages = Vec::new();
        if entry.pages > 0 {
            let first = entry.first_page as u64 - 1;
            if first + entry.pages as u64 > d.page_segments.len() as u64 * g.page_rows {
                return Err("page extent".into());
            }
            for row in (first..first + entry.pages as u64)
                .map(|r| r % g.page_rows)
                .collect::<BTreeSet<_>>()
            {
                pages.extend(self.row(m, g, Table::TxPages, row).await?);
            }
        }
        Ok(Some(txid::assemble(&entry, &pages)?))
    }
}
