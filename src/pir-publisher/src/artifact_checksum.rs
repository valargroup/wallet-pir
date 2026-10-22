//! Offline artifact conversion. Product adapters change only distribution
//! metadata; protocol manifests are carried through unchanged.
use clap::{Args, Subcommand, ValueEnum};
use pir_artifact::{catalog::Catalog, migration};
use serde_json::Value;
use std::{
    io::Read,
    path::{Path, PathBuf},
};
mod cache;
mod records;

#[derive(Args)]
pub struct Command {
    #[command(subcommand)]
    operation: Operation,
}

#[derive(Subcommand)]
enum Operation {
    /// Verify pinned rollback bodies without changing live publication names.
    VerifyPredecessors {
        #[arg(long)]
        stage: PathBuf,
    },
    /// Restore collected predecessor aliases from pinned verified bytes.
    RestorePredecessors {
        #[arg(long)]
        stage: PathBuf,
    },
    /// Check retained file identities without changing a preparation or authority.
    VerifyPrepared {
        #[arg(long)]
        stage: PathBuf,
    },
    /// Prepare locally cached objects against a verified publisher translation.
    PrepareCache {
        #[arg(long)]
        preparation: PathBuf,
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        stage: PathBuf,
    },
    /// Install only verified immutable aliases; the deployment tool owns the lock.
    InstallPrepared {
        #[arg(long)]
        stage: PathBuf,
        #[arg(long)]
        root: PathBuf,
    },
    /// Atomically replace one pinned, quiesced metadata record.
    InstallRecord {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        input_sha256: String,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        destination_sha256: String,
    },
    /// Hash exact bytes from a file, or stdin when no file is supplied.
    Hash {
        #[arg(long)]
        input: Option<PathBuf>,
    },
    /// Prepare an inventory and immutable objects in a separate staging tree.
    Prepare {
        #[arg(long, value_enum)]
        product: Product,
        #[arg(long)]
        root: PathBuf,
        /// Immutable source snapshot, retaining --root as the install location.
        #[arg(long)]
        source_root: Option<PathBuf>,
        #[arg(long)]
        inventory: PathBuf,
        #[arg(long)]
        inventory_sha256: String,
        #[arg(long)]
        stage: PathBuf,
        #[arg(long)]
        checkpoints: Option<PathBuf>,
    },
    /// Read a previously completed preparation without modifying it.
    Status {
        #[arg(long)]
        stage: PathBuf,
    },
    /// Convert one saved authority/restore record using verified preparations.
    /// The caller must atomically install this staged file under the deploy lock.
    PrepareRecord {
        /// Translate the latest record back; never restore an authority snapshot.
        #[arg(long)]
        reverse: bool,
        #[arg(long, value_enum)]
        kind: records::Kind,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        input_sha256: String,
        #[arg(long, required = true)]
        preparation: Vec<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Product {
    Enhance,
    Status,
    TransparentHistory,
    TransparentDisplay,
}

impl Command {
    pub fn run(self) -> crate::Result<()> {
        let result = match self.operation {
            Operation::VerifyPredecessors { stage } => {
                let prepared: pir_artifact::catalog::Prepared =
                    serde_json::from_slice(&std::fs::read(stage.join("prepared.json"))?)?;
                if prepared.stage != stage {
                    return Err("preparation stage differs".into());
                }
                prepared.verify_predecessors()?;
                serde_json::json!({"verified_predecessors":prepared.originals.len()})
            }
            Operation::RestorePredecessors { stage } => {
                let prepared: pir_artifact::catalog::Prepared =
                    serde_json::from_slice(&std::fs::read(stage.join("prepared.json"))?)?;
                if prepared.stage != stage {
                    return Err("preparation stage differs".into());
                }
                serde_json::json!({"restored_files":prepared.restore_predecessors()?})
            }
            Operation::VerifyPrepared { stage } => {
                let prepared: pir_artifact::catalog::Prepared =
                    serde_json::from_slice(&std::fs::read(stage.join("prepared.json"))?)?;
                if prepared.stage != stage {
                    return Err("preparation stage differs".into());
                }
                prepared.verify_files()?;
                serde_json::json!({"verified_files":prepared.files.len(),"inventory":prepared.inventory})
            }
            Operation::PrepareCache {
                preparation,
                root,
                stage,
            } => serde_json::to_value(cache::prepare(&preparation, &root, &stage)?)?,
            Operation::InstallPrepared { stage, root } => {
                let mut prepared: pir_artifact::catalog::Prepared =
                    serde_json::from_slice(&std::fs::read(stage.join("prepared.json"))?)?;
                if prepared.stage != stage {
                    return Err("preparation stage differs".into());
                }
                prepared.refresh_files()?;
                let installed = prepared.install(&root)?;
                #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
                for (path, proof) in &prepared.files {
                    transparent_shared::runtime::disk::ShippedRuntimes::refresh_migration_proof(
                        path,
                        &proof.checksum,
                        &proof.stamp,
                    )?;
                }
                serde_json::json!({"installed_files":installed,"inventory":prepared.inventory})
            }
            Operation::InstallRecord {
                input,
                input_sha256,
                destination,
                destination_sha256,
            } => {
                migration::install_record(
                    &input,
                    &migration::sha256(input_sha256)?,
                    &destination,
                    &migration::sha256(destination_sha256)?,
                )?;
                serde_json::json!({"installed_record":destination})
            }
            Operation::Hash { input } => {
                let source: Box<dyn Read> = match input {
                    Some(path) => Box::new(std::fs::File::open(path)?),
                    None => Box::new(std::io::stdin()),
                };
                let (checksum, _) = pir_artifact::hash_reader(source)?;
                serde_json::to_value(checksum)?
            }
            Operation::Prepare {
                product,
                root,
                source_root,
                inventory,
                inventory_sha256,
                stage,
                checkpoints,
            } => {
                let previous = migration::sha256(inventory_sha256)?;
                let bytes = std::fs::read(&inventory)?;
                previous.verify(&bytes)?;
                let mut catalog = Catalog::new(&root, &stage)?;
                if let Some(path) = source_root {
                    catalog = catalog.with_source_root(path)?;
                }
                if let Some(path) = checkpoints {
                    catalog = catalog.with_checkpoints(path)?;
                }
                migration::persist(&stage.join("predecessor-inventory.json"), &bytes)?;
                let value: Value = serde_json::from_slice(&bytes)?;
                pin_inputs(product, &mut catalog, &value)?;
                let (name, bytes) = convert_product(product, &mut catalog, value)?;
                serde_json::to_value(catalog.finish(name, previous, &bytes)?)?
            }
            Operation::Status { stage } => {
                serde_json::from_slice(&std::fs::read(stage.join("prepared.json"))?)?
            }
            Operation::PrepareRecord {
                reverse,
                kind,
                input,
                input_sha256,
                preparation,
                output,
            } => {
                if input == output {
                    return Err("record conversion requires a separate output file".into());
                }
                let old = std::fs::read(&input)?;
                migration::sha256(input_sha256.clone())?.verify(&old)?;
                let prepared = preparation
                    .iter()
                    .map(|path| Ok(serde_json::from_slice(&std::fs::read(path)?)?))
                    .collect::<crate::Result<Vec<pir_artifact::catalog::Prepared>>>()?;
                let value = serde_json::from_slice(&old)?;
                let converted = if reverse {
                    records::reverse(kind, value, &prepared)?
                } else {
                    records::convert(kind, value, &prepared)?
                };
                let bytes = serde_json::to_vec(&converted)?;
                migration::persist(&output, &bytes)?;
                serde_json::json!({"input":input,"input_sha256":input_sha256,"output":output,
                    "checksum":pir_artifact::checksum(&bytes), "bytes":bytes.len()})
            }
        };
        println!("{}", serde_json::to_string(&result)?);
        Ok(())
    }
}

fn pin_inputs(product: Product, catalog: &mut Catalog, value: &Value) -> crate::Result<()> {
    let mut objects = Vec::new();
    match product {
        Product::Enhance => {
            for domain in value["domains"]
                .as_array()
                .ok_or("missing Enhance domains")?
            {
                objects.extend(
                    domain["rows"]
                        .as_array()
                        .ok_or("missing Enhance rows")?
                        .iter(),
                );
                objects.extend([&domain["hint"], &domain["public"], &domain["packing"]]);
            }
        }
        Product::Status => {
            objects.extend(
                value["units"]
                    .as_array()
                    .ok_or("missing Status units")?
                    .iter(),
            );
            objects.extend(
                value["packing"]
                    .as_array()
                    .ok_or("missing Status packing")?
                    .iter(),
            );
            objects.push(&value["public"]);
        }
        Product::TransparentHistory | Product::TransparentDisplay => {
            objects.extend(
                value["artifacts"]
                    .as_array()
                    .ok_or("missing Transparent artifacts")?
                    .iter(),
            );
        }
    }
    let transparent = matches!(
        product,
        Product::TransparentHistory | Product::TransparentDisplay
    );
    let mut inputs = std::collections::BTreeMap::new();
    for object in objects {
        let checksum = migration::sha256(
            object["sha256"]
                .as_str()
                .ok_or("missing predecessor checksum")?
                .into(),
        )?;
        let bytes = object["bytes"]
            .as_u64()
            .ok_or("invalid predecessor length")?;
        let relative = if transparent {
            PathBuf::from(
                object["path"]
                    .as_str()
                    .ok_or("missing predecessor artifact path")?,
            )
        } else {
            Path::new("objects").join(&checksum.digest)
        };
        if let Some(previous) = inputs.insert(relative, (checksum.clone(), bytes)) {
            if previous != (checksum, bytes) {
                return Err("one source path has contradictory identities".into());
            }
        }
    }
    let inputs = inputs
        .into_iter()
        .map(|(path, (checksum, bytes))| (path, checksum, bytes))
        .collect::<Vec<_>>();
    catalog.pin_inputs(&inputs)?;
    Ok(())
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
fn text<'a>(value: &'a Value, field: &str) -> crate::Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing migration field {field}").into())
}

#[cfg(any(feature = "enhance", feature = "status"))]
fn array<'a>(value: &'a mut Value, field: &str) -> crate::Result<&'a mut Vec<Value>> {
    value
        .get_mut(field)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| format!("missing migration array {field}").into())
}

/// Convert only an explicitly selected object reference. Never recursively
/// rename similarly named commitments inside a product's protocol manifest.
#[cfg(any(feature = "enhance", feature = "status"))]
fn object(catalog: &mut Catalog, value: &mut Value) -> crate::Result<String> {
    let digest = text(value, "sha256")?.to_string();
    let bytes = value
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or("invalid legacy artifact length")?;
    let previous = migration::sha256(digest.clone())?;
    let converted = catalog.object(&Path::new("objects").join(&digest), &previous, bytes)?;
    let reference = value
        .as_object_mut()
        .ok_or("artifact reference must be an object")?;
    reference.remove("sha256");
    reference.insert("checksum".into(), serde_json::to_value(converted.checksum)?);
    Ok(digest)
}

fn convert_product(
    product: Product,
    catalog: &mut Catalog,
    value: Value,
) -> crate::Result<(&'static str, Vec<u8>)> {
    let _ = (&catalog, &value);
    #[allow(unreachable_patterns)]
    match product {
        #[cfg(feature = "enhance")]
        Product::Enhance => Ok(("enhance", enhance(catalog, value)?)),
        #[cfg(feature = "status")]
        Product::Status => Ok(("status", status(catalog, value)?)),
        #[cfg(feature = "transparent-history")]
        Product::TransparentHistory => {
            Ok(("transparent-history", transparent(catalog, value, false)?))
        }
        #[cfg(feature = "transparent-detail")]
        Product::TransparentDisplay => {
            Ok(("transparent-display", transparent(catalog, value, true)?))
        }
        _ => Err("requested product is not compiled into this executable".into()),
    }
}

#[cfg(feature = "enhance")]
fn enhance(catalog: &mut Catalog, mut value: Value) -> crate::Result<Vec<u8>> {
    if value["version"] != 1 {
        return Err("expected predecessor Enhance inventory".into());
    }
    for domain in array(&mut value, "domains")? {
        for rows in array(domain, "rows")? {
            object(catalog, rows)?;
        }
        object(catalog, &mut domain["hint"])?;
        let public = object(catalog, &mut domain["public"])?;
        object(catalog, &mut domain["packing"])?;
        domain["public_params_sha256"] = public.clone().into();
        let rows = domain["plan"]["shard"]["logical_rows"]
            .as_u64()
            .ok_or("missing Enhance logical rows")?;
        domain["packing"]["name"] =
            enhance_query_server::prepared_packing::artifact_name(rows, &public)?.into();
        domain["packing"]["format"] = enhance_query_server::prepared_packing::FORMAT.into();
    }
    value["version"] = enhance_query_server::publication::VERSION.into();
    let inventory: enhance_query_server::publication::Inventory = serde_json::from_value(value)?;
    inventory.validate()?;
    for domain in &inventory.domains {
        let name = enhance_query_server::publication::inventory_digest(&domain.plan);
        catalog.metadata_at(
            &Path::new("domains").join(format!("{name}.json")),
            &serde_json::to_vec(domain)?,
        )?;
        let previous = catalog
            .previous_for(&domain.packing.checksum)
            .ok_or("prepared packing object has no verified translation")?;
        catalog.install_at(
            &previous,
            &Path::new(enhance_query_server::prepared_packing::DIRECTORY)
                .join(&domain.packing.name),
        )?;
        catalog.metadata_at(
            &Path::new(enhance_query_server::prepared_packing::DIRECTORY)
                .join(format!("{}.json", domain.packing.name)),
            &serde_json::to_vec(&domain.packing)?,
        )?;
    }
    let bytes = serde_json::to_vec(&inventory)?;
    let name = pir_artifact::digest(&bytes);
    catalog.metadata_at(
        &Path::new(&pir_artifact::namespace("inventories")).join(format!("{name}.json")),
        &bytes,
    )?;
    Ok(bytes)
}

#[cfg(feature = "status")]
fn status(catalog: &mut Catalog, mut value: Value) -> crate::Result<Vec<u8>> {
    if value["schema"] != "status-prepared-distribution-v1" {
        return Err("expected predecessor Status inventory".into());
    }
    for unit in array(&mut value, "units")? {
        object(catalog, unit)?;
    }
    for packing in array(&mut value, "packing")? {
        object(catalog, packing)?;
    }
    object(catalog, &mut value["public"])?;
    value["schema"] = status_query_server::publication::SCHEMA.into();
    let inventory: status_query_server::publication::Inventory = serde_json::from_value(value)?;
    inventory.validate_material()?;
    Ok(serde_json::to_vec(&inventory)?)
}

#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
fn transparent(catalog: &mut Catalog, value: Value, display: bool) -> crate::Result<Vec<u8>> {
    transparent_shared::publication::migration::prepare(catalog, value, display)
}
