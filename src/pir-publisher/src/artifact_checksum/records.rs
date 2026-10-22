//! Explicit adapters for saved authority and restore records. Fields unrelated
//! to artifact identities are preserved, including every recovery/replay fence.
use pir_artifact::catalog::Prepared;
use serde_json::Value;

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Kind {
    EnhanceState,
    StatusState,
    StatusRetained,
    TransparentState,
    TransparentRestore,
}

fn preparation<'a>(digest: &str, prepared: &'a [Prepared]) -> crate::Result<&'a Prepared> {
    prepared
        .iter()
        .find(|p| p.previous_inventory.digest == digest)
        .ok_or_else(|| {
            "saved inventory has no verified checksum translation; refresh preparation".into()
        })
}

fn object(value: &mut Value, prepared: &[Prepared], product: &str) -> crate::Result<()> {
    if value.is_null() {
        return Ok(());
    }
    let digest = value["sha256"]
        .as_str()
        .ok_or("invalid predecessor inventory reference")?;
    let p = preparation(digest, prepared)?;
    if p.product != product || value["bytes"].as_u64().is_none() {
        return Err("saved inventory reference and preparation products differ".into());
    }
    *value = serde_json::json!({"checksum": p.inventory.checksum, "bytes": p.inventory.bytes});
    Ok(())
}

fn transparent(value: &mut Value, prepared: &[Prepared]) -> crate::Result<()> {
    let fields = value
        .as_object_mut()
        .ok_or("saved current must be an object")?;
    let digest = fields
        .remove("inventory_sha256")
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or("saved Transparent current has no predecessor inventory")?;
    let p = preparation(&digest, prepared)?;
    let schema = match p.product.as_str() {
        "transparent-history" => "transparent-prepared-distribution-v2",
        "transparent-display" => "transparent-display-prepared-distribution-v2",
        _ => return Err("saved current and preparation products differ".into()),
    };
    fields.insert("schema".into(), schema.into());
    fields.insert(
        "inventory_checksum".into(),
        serde_json::to_value(&p.inventory.checksum)?,
    );
    Ok(())
}

pub fn convert(kind: Kind, mut value: Value, prepared: &[Prepared]) -> crate::Result<Value> {
    if prepared
        .iter()
        .any(|p| p.schema != "artifact-checksum-preparation/1")
    {
        return Err("unsupported checksum preparation record".into());
    }
    match kind {
        Kind::StatusRetained => {
            for inventory in value
                .as_array_mut()
                .ok_or("retained inventories must be an array")?
            {
                let mut replacement = None;
                for p in prepared.iter().filter(|p| p.product == "status") {
                    let previous = std::fs::read(p.stage.join("predecessor-inventory.json"))?;
                    p.previous_inventory.verify(&previous)?;
                    if serde_json::from_slice::<Value>(&previous)? == *inventory {
                        let bytes = std::fs::read(&p.inventory_file)?;
                        p.inventory.checksum.verify(&bytes)?;
                        replacement = Some(serde_json::from_slice(&bytes)?);
                        break;
                    }
                }
                *inventory = replacement.ok_or("retained inventory has no verified translation")?;
            }
        }
        Kind::EnhanceState => {
            let current = if value.get("current").is_some() {
                &mut value["current"]
            } else {
                &mut value
            };
            if !current.is_null() {
                if current["version"] != 1 {
                    return Err("expected predecessor Enhance current".into());
                }
                object(&mut current["inventory"], prepared, "enhance")?;
                current["version"] = 2.into();
            }
            if let Some(retained) = value.get_mut("retained").and_then(Value::as_array_mut) {
                for entry in retained {
                    object(&mut entry["inventory"], prepared, "enhance")?;
                }
            }
        }
        Kind::StatusState => {
            let current = if value.get("current").is_some() {
                &mut value["current"]
            } else {
                &mut value
            };
            if !current.is_null() {
                if current["schema"] != "status-prepared-distribution-v1" {
                    return Err("expected predecessor Status current".into());
                }
                object(&mut current["inventory"], prepared, "status")?;
                current["schema"] = "status-prepared-distribution-v2".into();
            }
        }
        Kind::TransparentState => {
            let current = if value.get("current").is_some() {
                &mut value["current"]
            } else {
                &mut value
            };
            if !current.is_null() {
                transparent(current, prepared)?;
            }
        }
        Kind::TransparentRestore => {
            let digest = value["inventory_sha256"]
                .as_str()
                .ok_or("missing predecessor restore inventory")?;
            let p = preparation(digest, prepared)?;
            if p.product != "transparent-history"
                || value["schema"] != "transparent-history-restore-v1"
            {
                return Err("expected predecessor History restore record".into());
            }
            let old = value["inventory"]
                .as_str()
                .ok_or("missing predecessor inventory bytes")?;
            p.previous_inventory.verify(old.as_bytes())?;
            let bytes = std::fs::read(&p.inventory_file)?;
            p.inventory.checksum.require_current()?;
            p.inventory.checksum.verify(&bytes)?;
            if bytes.len() as u64 != p.inventory.bytes {
                return Err("prepared inventory length differs".into());
            }
            let inventory: Value = serde_json::from_slice(&bytes)?;
            if value["map_sha256"] != inventory["map_sha256"] {
                return Err("restore migration changes its protocol map".into());
            }
            value
                .as_object_mut()
                .ok_or("restore record must be an object")?
                .remove("inventory_sha256");
            value["inventory_checksum"] = serde_json::to_value(&p.inventory.checksum)?;
            value["inventory"] = String::from_utf8(bytes)?.into();
            value["runtime"] = inventory["runtime"].clone();
            value["schema"] = "transparent-history-restore-v2".into();
        }
    }
    Ok(value)
}

/// Translate the latest record back, preserving all authority and incident
/// fields. Unknown publications fail closed instead of restoring a snapshot.
pub fn reverse(kind: Kind, mut value: Value, prepared: &[Prepared]) -> crate::Result<Value> {
    fn previous(p: &Prepared) -> crate::Result<Vec<u8>> {
        let bytes = std::fs::read(p.stage.join("predecessor-inventory.json"))?;
        p.previous_inventory.verify(&bytes)?;
        Ok(bytes)
    }
    fn find<'a>(checksum: &Value, prepared: &'a [Prepared]) -> crate::Result<&'a Prepared> {
        let checksum: pir_artifact::Checksum = serde_json::from_value(checksum.clone())?;
        prepared
            .iter()
            .find(|p| p.inventory.checksum == checksum)
            .ok_or_else(|| "latest publication has no verified reverse translation".into())
    }
    fn object(value: &mut Value, prepared: &[Prepared]) -> crate::Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let p = find(&value["checksum"], prepared)?;
        let bytes = previous(p)?;
        *value = serde_json::json!({"sha256":p.previous_inventory.digest,"bytes":bytes.len()});
        Ok(())
    }
    match kind {
        Kind::StatusRetained => {
            for inventory in value
                .as_array_mut()
                .ok_or("retained inventories must be an array")?
            {
                let mut replacement = None;
                for p in prepared.iter().filter(|p| p.product == "status") {
                    let bytes = std::fs::read(&p.inventory_file)?;
                    p.inventory.checksum.verify(&bytes)?;
                    if serde_json::from_slice::<Value>(&bytes)? == *inventory {
                        replacement = Some(serde_json::from_slice(&previous(p)?)?);
                        break;
                    }
                }
                *inventory = replacement.ok_or("retained inventory has no reverse translation")?;
            }
        }
        Kind::EnhanceState | Kind::StatusState => {
            let current = if value.get("current").is_some() {
                &mut value["current"]
            } else {
                &mut value
            };
            if !current.is_null() {
                object(&mut current["inventory"], prepared)?;
                match kind {
                    Kind::EnhanceState => current["version"] = 1.into(),
                    _ => current["schema"] = "status-prepared-distribution-v1".into(),
                }
            }
            if let Some(retained) = value.get_mut("retained").and_then(Value::as_array_mut) {
                for entry in retained {
                    object(&mut entry["inventory"], prepared)?;
                }
            }
        }
        Kind::TransparentState | Kind::TransparentRestore => {
            let current = if value.get("current").is_some() {
                &mut value["current"]
            } else {
                &mut value
            };
            if !current.is_null() {
                let p = find(&current["inventory_checksum"], prepared)?;
                let bytes = previous(p)?;
                let inventory: Value = serde_json::from_slice(&bytes)?;
                let fields = current
                    .as_object_mut()
                    .ok_or("saved record must be an object")?;
                fields.remove("inventory_checksum");
                fields.insert(
                    "inventory_sha256".into(),
                    p.previous_inventory.digest.clone().into(),
                );
                let schema = match (kind, p.product.as_str()) {
                    (Kind::TransparentRestore, "transparent-history") => {
                        fields.insert("inventory".into(), String::from_utf8(bytes)?.into());
                        fields.insert("runtime".into(), inventory["runtime"].clone());
                        "transparent-history-restore-v1"
                    }
                    (Kind::TransparentState, "transparent-history") => {
                        "transparent-prepared-distribution-v1"
                    }
                    (Kind::TransparentState, "transparent-display") => {
                        "transparent-display-prepared-distribution-v1"
                    }
                    _ => return Err("reverse translation product differs".into()),
                };
                fields.insert("schema".into(), schema.into());
            }
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pir_artifact::{catalog::Object, Algorithm, Checksum};
    use std::collections::BTreeMap;

    fn prepared(product: &str) -> Prepared {
        Prepared {
            schema: "artifact-checksum-preparation/1".into(),
            product: product.into(),
            previous_inventory: Checksum {
                algorithm: Algorithm::Sha256,
                digest: "11".repeat(32),
            },
            inventory: Object {
                checksum: pir_artifact::checksum(b"new inventory"),
                bytes: 13,
            },
            inventory_file: "/unused/inventory.json".into(),
            objects: BTreeMap::new(),
            files: BTreeMap::new(),
            install_objects: true,
            root: "/unused/root".into(),
            stage: "/unused/stage".into(),
            checkpoints: "/unused/checkpoints".into(),
            records: Vec::new(),
            originals: Vec::new(),
            root_owner: pir_artifact::catalog::DirectoryOwner {
                uid: 0,
                gid: 0,
                mode: 0o755,
            },
            root_parent: "/unused".into(),
        }
    }

    #[test]
    fn a_withdrawal_and_every_high_water_mark_survive_conversion() {
        let mut original = serde_json::json!({"origin":"https://publisher.example", "current":{
            "schema":"transparent-prepared-distribution-v1", "revision":92,"recovery_epoch":8,
            "inventory_sha256":"11".repeat(32), "eligibility":"withdrawn", "invalidated_from":100,
            "anchor_hash":"22".repeat(32), "anchor_height":150
        }});
        let mut result = convert(
            Kind::TransparentState,
            original.clone(),
            &[prepared("transparent-history")],
        )
        .unwrap();
        original["current"]
            .as_object_mut()
            .unwrap()
            .remove("inventory_sha256");
        result["current"]
            .as_object_mut()
            .unwrap()
            .remove("inventory_checksum");
        original["current"]
            .as_object_mut()
            .unwrap()
            .remove("schema");
        result["current"].as_object_mut().unwrap().remove("schema");
        assert_eq!(original, result);
    }

    #[test]
    fn retained_status_round_trip_preserves_canonical_inventory_identity() {
        let directory = tempfile::tempdir().unwrap();
        let mut p = prepared("status");
        p.stage = directory.path().into();
        p.inventory_file = p.stage.join("inventory.json");
        // Typed inventory serialization need not use Value's sorted key order.
        let old = br#"{"version":1,"manifest":{"z":9,"a":2}}"#;
        let new = br#"{"version":2,"manifest":{"z":9,"a":2}}"#;
        let mut hash = pir_artifact::Hasher::for_algorithm(Algorithm::Sha256);
        hash.update(old);
        p.previous_inventory = hash.finish();
        p.inventory = Object {
            checksum: pir_artifact::checksum(new),
            bytes: new.len() as u64,
        };
        std::fs::write(p.stage.join("predecessor-inventory.json"), old).unwrap();
        std::fs::write(&p.inventory_file, new).unwrap();
        let original = serde_json::json!([serde_json::from_slice::<Value>(old).unwrap()]);
        let converted = convert(
            Kind::StatusRetained,
            original.clone(),
            std::slice::from_ref(&p),
        )
        .unwrap();
        assert_eq!(
            reverse(
                Kind::StatusRetained,
                converted.clone(),
                std::slice::from_ref(&p)
            )
            .unwrap(),
            original
        );
        std::fs::write(&p.inventory_file, b"damaged").unwrap();
        assert!(reverse(Kind::StatusRetained, converted, &[p]).is_err());
    }

    #[test]
    fn unknown_saved_inventory_requires_a_new_preparation() {
        let state = serde_json::json!({"current":{"version":1,"inventory":{"sha256":"33".repeat(32),"bytes":4}},"revoked":["keep"]});
        assert!(convert(Kind::EnhanceState, state, &[prepared("enhance")]).is_err());
    }

    #[test]
    fn reverse_uses_latest_authority_fields_and_refuses_unknown_publications() {
        let directory = tempfile::tempdir().unwrap();
        let mut p = prepared("transparent-history");
        p.stage = directory.path().into();
        let bytes = br#"{"map_sha256":"unchanged"}"#;
        let mut hash = pir_artifact::Hasher::for_algorithm(pir_artifact::Algorithm::Sha256);
        hash.update(bytes);
        p.previous_inventory = hash.finish();
        std::fs::write(p.stage.join("predecessor-inventory.json"), bytes).unwrap();
        let latest = serde_json::json!({"current":{
            "schema":"transparent-prepared-distribution-v2", "revision":108,
            "recovery_epoch":12,"inventory_checksum":p.inventory.checksum,
            "eligibility":"withdrawn","invalidated_from":25},
            "incidents":[{"acknowledged":false}],"revoked":["retain"]});
        let old = reverse(
            Kind::TransparentState,
            latest.clone(),
            std::slice::from_ref(&p),
        )
        .unwrap();
        assert_eq!(old["current"]["revision"], 108);
        assert_eq!(old["current"]["recovery_epoch"], 12);
        assert_eq!(old["current"]["eligibility"], "withdrawn");
        assert_eq!(old["incidents"], latest["incidents"]);
        assert_eq!(old["revoked"], latest["revoked"]);
        let mut unknown = latest;
        unknown["current"]["inventory_checksum"] =
            serde_json::to_value(pir_artifact::checksum(b"newer publication")).unwrap();
        assert!(reverse(Kind::TransparentState, unknown, &[p]).is_err());
    }
}
