use crate::{
    profile, public_bytes, response_bytes, setup_seed, AcceptedCoverage, Error, Manifest,
    HEADER_BYTES, MAGIC,
};
use ipir_sp::{IPIRClient, IPIRSeed, PublicQuerySetup};
use rand::{rngs::OsRng, Rng};
use receiver_directory::{snapshot, Hash, Receiver, Record};
use sha2::{Digest, Sha256};

pub struct Client {
    manifest: Manifest,
    id: Hash,
    client: IPIRClient,
    setup: PublicQuerySetup,
    public: Vec<Vec<u64>>,
}

/// One fresh encryption, consumed when its response is decoded. Contains ephemeral secrets.
pub struct Query {
    body: Vec<u8>,
    seed: IPIRSeed,
    receiver: Receiver,
    page: u32,
}

impl Query {
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl Client {
    pub fn new(
        manifest: Manifest,
        public: &[u8],
        accepted: AcceptedCoverage,
    ) -> Result<Self, Error> {
        manifest.validate()?;
        accepted.check(&manifest.directory)?;
        if public.len() != public_bytes(manifest.directory.rows)?
            || Hash::from(Sha256::digest(public)) != manifest.public_digest
        {
            return Err(Error::Malformed);
        }
        let id = manifest.id()?;
        let p = profile(manifest.directory.rows)?;
        let client = IPIRClient::new(p);
        let setup = client
            .generate_public_query_setup_simplepir_from_seed(setup_seed(&manifest.directory)?);
        let public = ipir_sp::modulus_switch::recover_published_c1(
            public,
            p.rlwe().d,
            p.ypir().db_cols / p.rlwe().d,
            p.rlwe().q,
        );
        Ok(Self {
            manifest,
            id,
            client,
            setup,
            public,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Receiver and page influence only the encrypted row selection, never a public header.
    pub fn prepare(&self, receiver: Receiver, page: u32) -> Result<Query, Error> {
        let row = snapshot::row_for(&self.manifest.directory, &receiver, page)?;
        let (query, keys, seed) = self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body = MAGIC.to_vec();
        body.extend(self.id);
        body.extend(OsRng.gen::<[u8; 16]>());
        let p = profile(self.manifest.directory.rows)?;
        body.extend(
            ipir_sp::serialize::serialize_packing_keys(p.rlwe(), &keys).map_err(|_| Error::Pir)?,
        );
        body.extend(query.to_switched_bytes(p.rlwe().q, p.ypir().query_bits));
        Ok(Query {
            body,
            seed,
            receiver,
            page,
        })
    }

    pub fn decode(&self, query: Query, response: &[u8]) -> Result<Option<Record>, Error> {
        if query.body[4..36] != self.id {
            return Err(Error::Revision);
        }
        if response.len() != response_bytes(self.manifest.directory.rows)?
            || response[..HEADER_BYTES] != query.body[..HEADER_BYTES]
        {
            return Err(Error::Malformed);
        }
        let row = self.client.decode_response_simplepir(
            query.seed,
            &self.public,
            &response[HEADER_BYTES..],
        );
        Ok(snapshot::lookup_row(
            &self.manifest.directory,
            &query.receiver,
            query.page,
            &row,
        )?)
    }
}
