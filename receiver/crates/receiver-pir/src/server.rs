//! Immutable CPU evaluator. HTTP admission and concurrency limits belong to the service.
use crate::{
    profile, query_bytes, setup_seed, Error, Manifest, HEADER_BYTES, MAGIC, PROTOCOL, ROWS,
};
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

pub struct Server {
    manifest: Manifest,
    id: Hash,
    public: Vec<u8>,
    server: IPIRServer<u16>,
    top: TopKeyImages<'static>,
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
}

impl Server {
    /// Verify the entire publication before preparing its PIR data. No chain trust is implied.
    pub fn new(snapshot: Snapshot) -> Result<Self, Error> {
        snapshot.manifest.validate()?;
        if snapshot.manifest.rows != ROWS as u32 {
            return Err(Error::Unsupported);
        }
        if snapshot.data.len() != ROWS * ROW_BYTES
            || Hash::from(Sha256::digest(&snapshot.data)) != snapshot.manifest.data_sha256
        {
            return Err(Error::Malformed);
        }
        let p = profile();
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
            server,
            top,
            preprocessed,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn public(&self) -> &[u8] {
        &self.public
    }

    pub fn respond(&self, body: &[u8]) -> Result<Vec<u8>, Error> {
        if body.len() != query_bytes() || &body[..4] != MAGIC {
            return Err(Error::Malformed);
        }
        if body[4..36] != self.id {
            return Err(Error::Revision);
        }
        let key_end =
            HEADER_BYTES + ipir_sp::serialize::serialized_packing_keys_len(profile().rlwe());
        let keys = ipir_sp::serialize::deserialize_packing_keys(
            profile().rlwe(),
            &body[HEADER_BYTES..key_end],
        )
        .map_err(|_| Error::Malformed)?;
        let (result, _) = self
            .server
            .perform_full_online_computation_simplepir_measured(
                profile().rlwe(),
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
