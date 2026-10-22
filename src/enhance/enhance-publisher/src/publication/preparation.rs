//! Prepare each domain, reusing retained plans and installing immutable objects.
use super::{
    object_io::{install_object_bytes, install_packing_object, persist_metadata},
    read_metadata,
};
use crate::{prepared_packing, runtime, PackingBudget};
use enhance_query_server::{
    protocol::Coverage,
    publication::Domain,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

pub(super) fn prepare_domains(
    root: &Path,
    coverage: &Coverage,
    epochs: &BTreeMap<u64, String>,
    read: &mut impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<Vec<Domain>, String> {
    let mut engine = runtime::Engine::new(&root.join("preparation"));
    let mut domains = Vec::new();
    for shard in &coverage.shards {
        let mut plan = runtime::plan(shard.clone(), &mut *read)?;
        for unit in &mut plan.units {
            unit.recovery_epoch = epochs[&shard.id]
                .parse()
                .map_err(|_| "invalid domain epoch")?;
        }
        let mut rows = Vec::new();
        for (spec, unit) in plan.shard.units.iter().zip(&plan.units) {
            let bytes = runtime::unit_rows(&plan.shard, spec, &mut *read)?;
            if hex::encode(Sha256::digest(&bytes)) != unit.content_sha256 {
                return Err("canonical rows changed during publication preparation".into());
            }
            rows.push(install_object_bytes(root, &bytes)?);
        }
        // A previously verified domain avoids hint and packing preparation.
        let key = enhance_query_server::publication::inventory_digest(&plan);
        let retained = root.join("domains").join(format!("{key}.json"));
        if retained.exists() {
            let domain: Domain = read_metadata(&retained)?;
            if domain.plan != plan || domain.rows != rows {
                return Err("retained publication domain identity differs".into());
            }
            // The complete inventory is validated and its objects hashed below.
            domains.push(domain);
            continue;
        }
        let (evaluation, _) = engine
            .prepare(plan.clone(), &mut *read)
            .map_err(|e| e.to_string())?;
        let hint = evaluation.hint()?;
        let packing =
            runtime::Packing::new(shard.logical_rows, &hint, &PackingBudget::coordinator())?;
        let prepared = prepared_packing::persist(root, &packing)?;
        install_packing_object(
            root,
            &root.join(prepared_packing::DIRECTORY).join(&prepared.name),
            &prepared,
        )?;
        let mut encoded = Vec::new();
        crate::wire::write_crs_blocks(&mut encoded, &hint).map_err(|e| e.to_string())?;
        let domain = Domain {
            plan,
            rows,
            hint: install_object_bytes(root, &encoded)?,
            public_params_sha256: hex::encode(Sha256::digest(&packing.public)),
            public: install_object_bytes(root, &packing.public)?,
            packing: prepared,
        };
        persist_metadata(root, "domains", &format!("{key}.json"), &domain)?;
        domains.push(domain);
    }
    Ok(domains)
}
