//! Private retrieval of directory rows and pages from a shard set.
//!
//! One `TableClient` per geometry per table, reused across every shard naming
//! that geometry. That reuse is the point of naming geometries from a closed
//! registry rather than letting each shard choose: the expensive part of a
//! session — the parameter re-derivation and the public query setup — is done
//! once per geometry rather than once per shard. Both are the native
//! ReinspiRING two-mask profile of `transparent-native`. A sync spanning an archive
//! tier and a recent tier pays it twice, not once per shard in either.
//!
//! What *is* per segment is the published pair of masks, because it is derived
//! from that segment's own database. So a shard *revision* is "opened" by fetching the
//! setup of each of its segments, and those bytes are charged once per segment
//! per sync. A shard normally has one segment; one whose content did not fit
//! its geometry has more, and every one of them answers each query, so the
//! segment holding a script is never named in a request.
//!
//! Setup is cached against the **manifest digest**, not the shard id. A growing
//! tail is republished as a new revision with different bytes, so setup cached
//! under the id would be reused against a database it was not derived from —
//! which does not error, it decodes to plausible nonsense.
//!
//! Everything the server sends is re-derived rather than trusted. A client that
//! adopted the server's parameters would decode against whatever geometry the
//! server chose, including one that leaks the selection.
//!
//! The query is sent at 44 dithered bits when the service advertises the
//! dithered scheme and it reproduces exactly; otherwise at the 49 bits every
//! service accepts. The fallback is what keeps a newer wallet working against
//! a service that predates dithering, and it needs no trust: the 49-bit scheme
//! is still checked whole.

use crate::transport::{BoxError, ByteCharges, Overloaded, ShardTransport, StaleRevision};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use transparent_native::{NativeScheme, NativeSecret, TableProfile};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("session: {0}")]
    Session(String),
    #[error("response: {0}")]
    Response(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("pir: {0}")]
    Pir(String),
    /// The service no longer serves the revision this query named.
    ///
    /// Kept as a type rather than folded into `Transport`: it is recoverable by
    /// refreshing the map and re-deriving the shard, and a string cannot be
    /// matched on. Every query goes through here, so collapsing it here would
    /// lose the refusal for all of them.
    #[error(transparent)]
    Stale(#[from] StaleRevision),
    /// The service had no cache capacity for this query. Retryable as it
    /// stands, without refreshing anything.
    #[error(transparent)]
    Overloaded(#[from] Overloaded),
}

/// Which table a query addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Table {
    Directory,
    Pages,
}

impl Table {
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::Pages => "pages",
        }
    }
}

/// A prepared query and the secret needed to decode its response.
pub struct PreparedQuery {
    pub body: Vec<u8>,
    secret: NativeSecret,
}

/// One segment's published setup for one table: its two rounded masks, as
/// downloaded, and the epoch they imply.
struct SegmentSetup {
    public: Vec<u8>,
    epoch: [u8; 8],
}

/// A client for one table geometry, across every shard.
pub struct TableClient {
    schema: String,
    table: Table,
    profile: TableProfile,
    /// Whether queries go at 44 dithered bits, which the service advertised
    /// and this client reproduced, rather than 49.
    dithered: bool,
    /// Per segment, because the published masks come from each segment's own
    /// database. Keyed by manifest digest and segment index, so a superseded
    /// revision's setup is never reused for the revision that replaced it.
    segments: HashMap<(String, u32), SegmentSetup>,
    /// Queries a batch already sent, per revision, in the order the walk will
    /// ask for their rows. See [`TableClient::stash`].
    stashed: HashMap<String, VecDeque<Stashed>>,
}

/// A query sent ahead of the walk, with its outcome.
struct Stashed {
    row: usize,
    query: PreparedQuery,
    reply: Result<Vec<u8>, BoxError>,
}

impl TableClient {
    /// Builds a client for one table of `geometry`, re-deriving the native
    /// parameters — bit widths, query masks and packing setup — rather than
    /// adopting what the service published.
    ///
    /// `served` is the 49-bit scheme, which must reproduce. `served_dithered`
    /// is the 44-bit dithered scheme, if the service advertised one: queries
    /// use it only when it reproduces too, and otherwise fall back to 49 bits.
    pub fn new(
        table: Table,
        geometry: &str,
        rows: u64,
        row_bytes: u32,
        served: &NativeScheme,
        served_dithered: Option<&NativeScheme>,
    ) -> Result<Self, ClientError> {
        Self::new_with_schema(
            transparent_shard::SCHEMA,
            table,
            geometry,
            rows,
            row_bytes,
            served,
            served_dithered,
        )
    }

    pub fn new_with_schema(
        schema: &str,
        table: Table,
        geometry: &str,
        rows: u64,
        row_bytes: u32,
        served: &NativeScheme,
        served_dithered: Option<&NativeScheme>,
    ) -> Result<Self, ClientError> {
        if !transparent_shard::manifest::supported_schema(schema) {
            return Err(ClientError::Session("unsupported schema".into()));
        }
        let profile = TableProfile::new(schema, geometry, table.as_str(), rows, row_bytes)
            .map_err(ClientError::Pir)?;
        if served != &profile.scheme {
            return Err(ClientError::Session(format!(
                "{} scheme does not match the pinned geometry",
                table.as_str()
            )));
        }
        // Compared whole, like the 49-bit scheme. One that does not reproduce
        // is not adopted; the query falls back to the 49 bits checked above.
        let dithered = served_dithered == Some(&profile.dithered_scheme);
        Ok(Self {
            schema: schema.to_string(),
            table,
            profile,
            dithered,
            segments: HashMap::new(),
            stashed: HashMap::new(),
        })
    }

    pub fn rows(&self) -> usize {
        self.profile.rows
    }

    /// Bits each selection coefficient is sent at: 44 when the service
    /// advertised the dithered scheme and it reproduced, otherwise 49.
    pub fn query_bits(&self) -> usize {
        if self.dithered {
            self.profile.dithered_scheme.query_bits
        } else {
            self.profile.scheme.query_bits
        }
    }

    /// Records one segment's published setup after checking it against its
    /// digest.
    pub fn open_segment(
        &mut self,
        revision: &str,
        segment: u32,
        public_params_base64: &str,
        public_params_sha256: &str,
    ) -> Result<(), ClientError> {
        let public_params = BASE64_STANDARD
            .decode(public_params_base64.as_bytes())
            .map_err(|error| ClientError::Session(error.to_string()))?;
        let digest = Sha256::digest(&public_params);
        if hex::encode(digest) != public_params_sha256 {
            return Err(ClientError::Session(format!(
                "revision {revision} published parameter digest mismatch"
            )));
        }
        if public_params.len() != self.profile.scheme.public_bytes {
            return Err(ClientError::Session(format!(
                "revision {revision} published parameter length mismatch"
            )));
        }
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        self.segments.insert(
            (revision.to_string(), segment),
            SegmentSetup {
                public: public_params,
                epoch,
            },
        );
        Ok(())
    }

    pub fn is_open(&self, revision: &str, segment: u32) -> bool {
        self.segments.contains_key(&(revision.to_string(), segment))
    }

    /// Prepares one query against a named revision.
    ///
    /// The body opens with the revision-and-table binding, which the service
    /// re-derives and checks and the response repeats. It is a fixed-width
    /// public value — the revision is already in the request path and in the
    /// published map — so it discloses nothing while making a query answered by
    /// the wrong runtime fail rather than decode. A fresh secret is sampled for
    /// every query and never leaves the returned value.
    pub fn prepare(&self, revision: &str, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.profile.rows {
            return Err(ClientError::Session("row outside table".into()));
        }
        let (secret, upload) = if self.dithered {
            self.profile.prepare_dithered(row)
        } else {
            self.profile.prepare(row)
        }
        .map_err(ClientError::Pir)?;
        let mut body = transparent_shard::manifest::query_binding_for_schema(
            &self.schema,
            revision,
            self.table.as_str(),
        )
        .to_vec();
        body.extend(upload);
        Ok(PreparedQuery { body, secret })
    }

    /// Decodes one answer: one row per segment, in segment order.
    ///
    /// Every segment answered the same query, so the response carries as many
    /// bodies as the shard has segments and the caller decides which row it was
    /// looking for — by the exact script bytes, never by position.
    pub fn decode(
        &self,
        revision: &str,
        segments: u32,
        query: PreparedQuery,
        response: &[u8],
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let binding = transparent_shard::manifest::query_binding_for_schema(
            &self.schema,
            revision,
            self.table.as_str(),
        );
        let each = 16 + self.profile.scheme.response_bytes;
        // A fixed length per segment, and a fixed segment count from the
        // published map: a response of any other size is not this shard's.
        if response.len() != each * segments as usize {
            return Err(ClientError::Response("response length mismatch".into()));
        }

        let mut rows = Vec::with_capacity(segments as usize);
        for segment in 0..segments {
            let part = &response[segment as usize * each..(segment as usize + 1) * each];
            let setup = self
                .segments
                .get(&(revision.to_string(), segment))
                .ok_or_else(|| {
                    ClientError::Session(format!(
                        "revision {revision} segment {segment} is not open"
                    ))
                })?;
            // The prefix binds the response to the revision and table the query
            // named, so an answer produced from another revision's bytes fails
            // here rather than decoding to plausible rows for a range the
            // wallet did not ask about. The epoch does the same for the
            // segment, since each segment publishes its own masks.
            if part.get(..8) != Some(binding.as_slice()) {
                return Err(ClientError::Response("revision or table mismatch".into()));
            }
            if part.get(8..16) != Some(setup.epoch.as_slice()) {
                return Err(ClientError::Response("parameter epoch mismatch".into()));
            }
            let row = self
                .profile
                .decode(&query.secret, &setup.public, &part[16..])
                .map_err(ClientError::Response)?;
            if row.len() != self.profile.row_bytes {
                return Err(ClientError::Response("decoded row is too short".into()));
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// Keeps a query that was sent ahead of the walk, with its outcome, for
    /// the walk's next [`fetch_row`](Self::fetch_row) of that revision.
    ///
    /// Stashed queries are handed out once, in order, and only for the row
    /// they were prepared for: the first request that does not match drops
    /// the revision's stash and goes to the network. A stashed refusal or
    /// failure is returned as if the query had just been sent, and drops the
    /// rest of the revision's stash, so whatever the walk does next — a
    /// retry, a refresh — is a fresh request.
    pub(crate) fn stash(
        &mut self,
        revision: &str,
        row: usize,
        query: PreparedQuery,
        reply: Result<Vec<u8>, BoxError>,
    ) {
        self.stashed
            .entry(revision.to_string())
            .or_default()
            .push_back(Stashed { row, query, reply });
    }

    /// Drops every stashed query.
    pub(crate) fn clear_stash(&mut self) {
        self.stashed.clear();
    }

    /// Fetches one row from every segment of a shard, charging what it cost.
    ///
    /// Uses a query a batch already sent for this row when one is stashed;
    /// the charge is the same either way.
    pub fn fetch_row(
        &mut self,
        transport: &mut impl ShardTransport,
        shard_id: u64,
        revision: &str,
        segments: u32,
        row: usize,
        charges: &mut ByteCharges,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        if let Some(queue) = self.stashed.get_mut(revision) {
            let next = queue.pop_front();
            if queue.is_empty() {
                self.stashed.remove(revision);
            }
            match next {
                Some(stashed) if stashed.row == row => {
                    let response = match stashed.reply {
                        Ok(response) => response,
                        Err(error) => {
                            self.stashed.remove(revision);
                            return Err(classify_transport(error));
                        }
                    };
                    charges.add_query(
                        self.table,
                        stashed.query.body.len() as u64,
                        response.len() as u64,
                    );
                    return self.decode(revision, segments, stashed.query, &response);
                }
                // Out of step with the walk: nothing further is trusted to
                // line up, so the rest of this revision goes to the network.
                _ => {
                    self.stashed.remove(revision);
                }
            }
        }
        let query = self.prepare(revision, row)?;
        let uploaded = query.body.len() as u64;
        let response = transport
            .query(shard_id, revision, self.table, &query.body)
            .map_err(classify_transport)?;
        charges.add_query(self.table, uploaded, response.len() as u64);
        self.decode(revision, segments, query, &response)
    }
}

/// Sorts a boxed transport error into the refusals a caller can act on.
///
/// The refusals are recovered before the fallback, because both are boxed into
/// the same `BoxError` as an ordinary failure and stringifying first would
/// throw away the only thing that distinguishes them.
pub(crate) fn classify_transport(error: crate::transport::BoxError) -> ClientError {
    if let Some(stale) = StaleRevision::found_in(&error) {
        return ClientError::Stale(stale.clone());
    }
    if let Some(overloaded) = Overloaded::found_in(&error) {
        return ClientError::Overloaded(overloaded.clone());
    }
    ClientError::Transport(crate::transport::describe_error(error.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(table: Table) -> TableProfile {
        TableProfile::new(
            transparent_shard::SCHEMA,
            "recent-4k",
            table.as_str(),
            4_096,
            4_096,
        )
        .unwrap()
    }

    fn client(table: Table, dithered: Option<&NativeScheme>) -> TableClient {
        let profile = profile(table);
        TableClient::new(table, "recent-4k", 4_096, 4_096, &profile.scheme, dithered).unwrap()
    }

    fn body_len(client: &TableClient) -> usize {
        client.prepare("revision", 17).unwrap().body.len()
    }

    /// A service advertising the dithered scheme gets 44-bit queries.
    #[test]
    fn a_reproduced_dithered_scheme_is_used() {
        let profile = profile(Table::Directory);
        let client = client(Table::Directory, Some(&profile.dithered_scheme));
        assert_eq!(client.query_bits(), 44);
        assert_eq!(body_len(&client), 8 + profile.dithered_scheme.request_bytes);
        assert_eq!(body_len(&client), 8 + 27_648 + 22_528);
    }

    /// A service that predates dithering, or advertises a dithered scheme this
    /// build does not derive, gets the 49-bit query it has always accepted.
    #[test]
    fn anything_else_falls_back_to_the_49_bit_query() {
        let profile = profile(Table::Directory);
        let legacy = 8 + profile.scheme.request_bytes;
        assert_eq!(legacy, 8 + 27_648 + 25_088);

        let absent = client(Table::Directory, None);
        assert_eq!((absent.query_bits(), body_len(&absent)), (49, legacy));

        let mut moved = profile.dithered_scheme.clone();
        moved.query_mask_seed = profile.scheme.packing_setup_id.clone();
        let unknown = client(Table::Directory, Some(&moved));
        assert_eq!((unknown.query_bits(), body_len(&unknown)), (49, legacy));

        // Another table's dithered scheme is not this one's.
        let other = self::profile(Table::Pages);
        let crossed = client(Table::Directory, Some(&other.dithered_scheme));
        assert_eq!(crossed.query_bits(), 49);
    }

    /// The 49-bit scheme is checked whatever the dithered one says: a service
    /// cannot get a wallet past a moved scheme by advertising a good dithered
    /// one beside it.
    #[test]
    fn the_49_bit_scheme_must_still_reproduce() {
        let profile = profile(Table::Directory);
        let mut moved = profile.scheme.clone();
        moved.rows = 8_192;
        let refused = TableClient::new(
            Table::Directory,
            "recent-4k",
            4_096,
            4_096,
            &moved,
            Some(&profile.dithered_scheme),
        );
        assert!(matches!(refused, Err(ClientError::Session(_))));
    }
}
