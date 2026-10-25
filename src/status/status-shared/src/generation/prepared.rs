//! Export preparation once and restore immutable serving material without
//! computing hints or packing matrices on query hosts.
use super::*;
use crate::publication::{self, Failure, Inventory};
use pir_publication::{ObjectCache, ObjectReference, VerifiedObjectFile};

/// Which prepared material a serving process holds beside the public
/// parameters: a worker evaluates over the serving units, a router packs
/// responses, and a combined provider does both over one inventory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedMaterial {
    Evaluation,
    Packing,
    Complete,
}

impl PreparedMaterial {
    /// Whether the serving units (the database) are loaded.
    pub fn evaluates(self) -> bool {
        self != Self::Packing
    }
    /// Whether the response packing material is loaded.
    pub fn packs(self) -> bool {
        self != Self::Evaluation
    }
}

impl Generation {
    pub fn export_prepared(
        &self,
        mut store: impl FnMut(&[u8]) -> Result<ObjectReference, Failure>,
    ) -> Result<Inventory, Failure> {
        if self.units.len() != ROWS / profile().rlwe().d {
            return Err("Status generation has no complete database".into());
        }
        let units = self
            .units
            .iter()
            .map(|unit| store(&unit.rows))
            .collect::<Result<Vec<_>, _>>()?;
        let mut packing = Vec::new();
        #[cfg(feature = "native-reinspiring")]
        for block in self.packing.iter() {
            let mut bytes = Vec::new();
            reinspiring::prepared_native::write(&mut bytes, std::slice::from_ref(block.as_ref()))?;
            packing.push(store(&bytes)?);
        }
        #[cfg(not(feature = "native-reinspiring"))]
        {
            let mut bytes = self.public.as_ref().clone();
            self.packing
                .as_ref()
                .ok_or("Status generation has no packing material")?
                .write(&mut bytes, profile().rlwe())?;
            packing.push(store(&bytes)?);
        }
        Ok(Inventory {
            schema: publication::SCHEMA.into(),
            manifest: self.manifest.clone(),
            units,
            packing,
            public: store(&self.public)?,
        })
    }

    /// Load from physical paths, verifying each object at the loader boundary.
    pub fn load_prepared(
        inventory: &Inventory,
        material: PreparedMaterial,
        object: impl Fn(&ObjectReference) -> std::io::Result<std::path::PathBuf>,
        previous: Option<&Self>,
    ) -> Result<Self, Failure> {
        Self::load_verified(
            inventory,
            material,
            |item| VerifiedObjectFile::open(object(item)?, item),
            previous,
        )
    }

    /// Load through the same cache that verified the transfer. Unchanged,
    /// settled inodes reuse that evidence instead of hashing their bodies at
    /// both the fetch and load boundaries. Restarted caches verify afresh.
    pub fn load_prepared_cached(
        inventory: &Inventory,
        material: PreparedMaterial,
        cache: &ObjectCache,
        previous: Option<&Self>,
    ) -> Result<Self, Failure> {
        Self::load_verified(
            inventory,
            material,
            |item| {
                cache.open_verified(item)?.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Status prepared object disappeared",
                    )
                })
            },
            previous,
        )
    }

    fn load_verified(
        inventory: &Inventory,
        material: PreparedMaterial,
        object: impl Fn(&ObjectReference) -> std::io::Result<VerifiedObjectFile>,
        previous: Option<&Self>,
    ) -> Result<Self, Failure> {
        inventory.validate_material()?;
        let read =
            |o: &ObjectReference| -> Result<Vec<u8>, Failure> { Ok(object(o)?.read_bytes()?) };
        let public = read(&inventory.public)?;
        if Sha256::digest(&public)[..] != inventory.manifest.public_digest {
            return Err("Status public parameters digest differs".into());
        }
        let mut generation = Self::public_only(inventory.manifest.clone(), Arc::new(public));
        if material.evaluates() {
            let local = ProductionSimplePirParams::new(
                profile().rlwe().d as u64,
                ITEM_BITS,
                SimplePirProfile::P16Q48,
            )?;
            let mut rows_hash = Sha256::new();
            for (i, item) in inventory.units.iter().enumerate() {
                let bytes = read(item)?;
                rows_hash.update(&bytes);
                // The opened object's complete bytes already matched this
                // digest. Hashing them again cannot add integrity evidence.
                let digest: Hash = hex::decode(&item.sha256)?
                    .try_into()
                    .map_err(|_| "Status unit digest length differs")?;
                if let Some(unit) = previous
                    .filter(|p| {
                        p.manifest.network == inventory.manifest.network
                            && p.manifest.salt == inventory.manifest.salt
                    })
                    .and_then(|p| p.units.get(i))
                    .filter(|u| u.bytes_digest == digest)
                {
                    generation.units.push(unit.clone());
                    continue;
                }
                let mut reader = bytes.as_slice();
                let db = build_from_le_u16(&mut reader, bytes.len() / 2, |values| {
                    IPIRServer::try_from_profile_with_backend(
                        &local,
                        values,
                        false,
                        true,
                        ipir_sp::server::MatvecBackend::Cpu,
                    )
                    .map(PreparedDatabase::new)
                })??;
                generation.units.push(Arc::new(Unit {
                    bytes_digest: digest,
                    rows: bytes,
                    db,
                    hint: Vec::new(),
                }));
            }
            if rows_hash.finalize()[..] != inventory.manifest.rows_digest {
                return Err("Status database commitment differs".into());
            }
        }
        if material.packs() {
            #[cfg(feature = "native-reinspiring")]
            let mut blocks = Vec::new();
            for item in &inventory.packing {
                let file = object(item)?;
                // SAFETY: downloads publish read-only immutable inodes with atomic
                // rename; collection only unlinks them. The mapping pins this inode.
                let map = unsafe { memmap2::Mmap::map(file.file())? };
                file.check()?;
                #[cfg(feature = "native-reinspiring")]
                {
                    let setup =
                        native_packing_setup(&inventory.manifest.network, &inventory.manifest.salt);
                    let block = reinspiring::prepared_native::read(map, 0, &setup, 1)?;
                    blocks.extend(block.into_iter().map(Arc::new));
                }
                #[cfg(not(feature = "native-reinspiring"))]
                {
                    let mapped = inspiring::prepared::MappedPrepared::new(
                        map,
                        profile().rlwe(),
                        profile().ypir().db_cols.div_ceil(profile().rlwe().d),
                        generation.public.len(),
                    )?;
                    if mapped.prefix() != generation.public.as_slice() {
                        return Err("Status prepared public parameters differ".into());
                    }
                    generation.mapped_packing = Some(Arc::new(mapped));
                }
            }
            #[cfg(feature = "native-reinspiring")]
            {
                let public = zakura_pir_query_server::native::publish(&blocks)?;
                if public != *generation.public {
                    return Err("Status prepared public mask binding differs".into());
                }
                generation.packing = Arc::new(blocks);
            }
        }
        Ok(generation)
    }
}
