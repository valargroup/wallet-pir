//! Immutable CPU evaluator. HTTP admission and concurrency limits belong to the service.
use crate::{profile, query_bytes, setup_seed, Error, Manifest, HEADER_BYTES, MAGIC, PROTOCOL};
use inspiring::{QueryPackPreprocessed, TopKeyImages};
use ipir_sp::{
    server::{IPIRServer, MatvecBackend},
    IPIRClient,
};
use receiver_directory::{
    snapshot::{Snapshot, ROW_BYTES},
    Hash,
};
use sha2::{Digest, Sha256};

/// A publication prepared for PIR evaluation.
pub struct Server {
    manifest: Manifest,
    id: Hash,
    public: Vec<u8>,
    rows: std::sync::Arc<[u8]>,
    filters: std::sync::Arc<[u8]>,
    server: IPIRServer<u16>,
    top: TopKeyImages<'static>,
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
}

impl Server {
    /// Verify the entire publication before preparing its PIR data. No chain trust is implied.
    pub fn new(snapshot: Snapshot) -> Result<Self, Error> {
        snapshot.manifest.validate()?;
        let p = profile(snapshot.manifest.rows)?;
        if snapshot.data.len() != snapshot.manifest.rows as usize * ROW_BYTES
            || Hash::from(Sha256::digest(&snapshot.data)) != snapshot.manifest.data_sha256
            || Hash::from(Sha256::digest(&snapshot.filters)) != snapshot.manifest.filters_sha256
        {
            return Err(Error::Malformed);
        }
        receiver_directory::filter::Filters::decode(&snapshot.filters)?;
        let client = IPIRClient::new(p);
        let setup =
            client.generate_public_query_setup_simplepir_from_seed(setup_seed(&snapshot.manifest)?);
        let values = snapshot
            .data
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u16::from_le_bytes([b[0], b[1]]));
        let server = IPIRServer::<u16>::try_from_profile_with_backend(
            p,
            values,
            false,
            true,
            MatvecBackend::Cpu,
        )
        .map_err(|_| Error::Pir)?;
        let offline = server.perform_offline_precomputation_simplepir(p.rlwe(), setup.polys());
        let top = TopKeyImages::build(p.rlwe());
        let preprocessed = ipir_sp::server::build_pack_preprocessed_blocks_with_top(
            p.rlwe(),
            &offline.crs_blocks,
            &top,
        )
        .map_err(|_| Error::Pir)?;
        let public = ipir_sp::server::published_c1_rows(&preprocessed, p.rlwe().q);
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            directory: snapshot.manifest,
            public_digest: Sha256::digest(&public).into(),
        };
        let id = manifest.id()?;
        Ok(Self {
            manifest,
            id,
            public,
            rows: snapshot.data.into(),
            filters: snapshot.filters.into(),
            server,
            top,
            preprocessed,
        })
    }

    /// The session manifest that clients fetch.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The public setup that clients download once per session.
    pub fn public(&self) -> &[u8] {
        &self.public
    }

    /// Identical immutable directory bytes for every caller, already digest-checked.
    pub fn rows(&self) -> std::sync::Arc<[u8]> {
        self.rows.clone()
    }

    /// The publication's filter file, identical for every caller and already checked.
    pub fn filters(&self) -> std::sync::Arc<[u8]> {
        self.filters.clone()
    }

    /// Answer one encrypted query for this session, rejecting any other length or session.
    pub fn respond(&self, body: &[u8]) -> Result<Vec<u8>, Error> {
        if body.len() != query_bytes(self.manifest.directory.rows)? || &body[..4] != MAGIC {
            return Err(Error::Malformed);
        }
        if body[4..36] != self.id {
            return Err(Error::Revision);
        }
        let p = profile(self.manifest.directory.rows)?;
        let key_end = HEADER_BYTES + ipir_sp::serialize::serialized_packing_keys_len(p.rlwe());
        let keys =
            ipir_sp::serialize::deserialize_packing_keys(p.rlwe(), &body[HEADER_BYTES..key_end])
                .map_err(|_| Error::Malformed)?;
        let (result, _) = self
            .server
            .perform_full_online_computation_simplepir_measured(
                p.rlwe(),
                &body[key_end..],
                &keys,
                &self.top,
                &self.preprocessed,
            )
            .map_err(|_| Error::Pir)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        response.extend(result);
        Ok(response)
    }
}
