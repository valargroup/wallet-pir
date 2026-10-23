//! Observations a per-host sidecar serves at `GET /v1/observations`.
//!
//! The sidecar decides each check's status locally; the central APM only
//! validates the document and propagates status and reason. Anything this
//! module rejects is treated by the APM as unknown, never as healthy.
use serde::{Deserialize, Serialize};

/// Bumped on any incompatible change; the APM rejects every other version.
pub const SCHEMA_VERSION: u32 = 1;
/// Served path.
pub const PATH: &str = "/v1/observations";
/// Default HTTPS port, overridable per host in the monitoring inventory.
pub const DEFAULT_PORT: u16 = 9443;
/// Largest document the APM reads.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// A check observed longer ago than this is unknown.
pub const MAX_AGE_SECONDS: u64 = 45;
/// Clock skew tolerated for observations from the future.
pub const MAX_FUTURE_SECONDS: u64 = 5;
const MAX_CHECKS: usize = 512;
const MAX_VALUES: usize = 16;
const MAX_REASON: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Value {
    pub name: String,
    pub value: f64,
    pub unit: Unit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unit {
    Bytes,
    Ratio,
    Count,
    Seconds,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// Stable name such as `disk:/srv/data`; it becomes part of the incident key.
    pub name: String,
    pub status: Status,
    /// Short fixed-form explanation; never raw command output.
    pub reason: String,
    /// Unix seconds when this check was last measured.
    pub observed_at: u64,
    #[serde(default)]
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observations {
    pub schema_version: u32,
    pub host_id: String,
    /// Random per process start, so a restarted sidecar is distinguishable.
    pub incarnation: String,
    /// `sha256:<hex>` of the canonical sidecar configuration.
    pub config_digest: String,
    pub generated_at: u64,
    pub checks: Vec<Check>,
}

/// Identifier alphabet shared by host ids and check names.
pub fn valid_name(name: &str, max: usize) -> bool {
    (1..=max).contains(&name.len())
        && name.bytes().all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

fn valid_text(text: &str) -> bool {
    text.len() <= MAX_REASON && text.chars().all(|c| !c.is_control())
}

/// Why a served document was rejected; fixed strings, safe for alert text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Schema,
    HostMismatch,
    ConfigMismatch,
    Malformed,
}

impl Rejection {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Schema => "incompatible observation schema",
            Self::HostMismatch => "reported host identity does not match enrollment",
            Self::ConfigMismatch => "sidecar configuration digest does not match inventory",
            Self::Malformed => "malformed observation document",
        }
    }
}

impl Observations {
    /// Parse and validate a served document against the enrolled identity.
    pub fn parse(body: &[u8], host_id: &str, config_digest: &str) -> Result<Self, Rejection> {
        if body.len() > MAX_BODY_BYTES {
            return Err(Rejection::Malformed);
        }
        #[derive(Deserialize)]
        struct Version {
            schema_version: u32,
        }
        let version: Version = serde_json::from_slice(body).map_err(|_| Rejection::Malformed)?;
        if version.schema_version != SCHEMA_VERSION {
            return Err(Rejection::Schema);
        }
        let doc: Self = serde_json::from_slice(body).map_err(|_| Rejection::Malformed)?;
        if doc.host_id != host_id {
            return Err(Rejection::HostMismatch);
        }
        if doc.config_digest != config_digest {
            return Err(Rejection::ConfigMismatch);
        }
        doc.validate().map_err(|_| Rejection::Malformed)?;
        Ok(doc)
    }

    /// Structural limits shared by the producer and the consumer.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_name(&self.host_id, 80) || !valid_name(&self.incarnation, 64) {
            return Err("invalid identity");
        }
        if !self.config_digest.starts_with("sha256:") || self.config_digest.len() != 71 {
            return Err("invalid config digest");
        }
        if self.checks.len() > MAX_CHECKS {
            return Err("too many checks");
        }
        let mut names = std::collections::BTreeSet::new();
        for check in &self.checks {
            if !valid_name(&check.name, 128) || !names.insert(check.name.as_str()) {
                return Err("invalid or duplicate check name");
            }
            if !valid_text(&check.reason) || check.values.len() > MAX_VALUES {
                return Err("invalid check detail");
            }
            if check
                .values
                .iter()
                .any(|v| !valid_name(&v.name, 64) || !v.value.is_finite())
            {
                return Err("invalid check value");
            }
        }
        Ok(())
    }

    /// A check's status as of `now`: stale or future observations are unknown.
    pub fn status_at(check: &Check, now: u64) -> Status {
        if check.observed_at > now + MAX_FUTURE_SECONDS
            || now.saturating_sub(check.observed_at) > MAX_AGE_SECONDS
        {
            Status::Unknown
        } else {
            check.status
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc() -> Observations {
        Observations {
            schema_version: SCHEMA_VERSION,
            host_id: "coordinator-01".into(),
            incarnation: "a1b2".into(),
            config_digest: format!("sha256:{}", "0".repeat(64)),
            generated_at: 100,
            checks: vec![Check {
                name: "disk:/".into(),
                status: Status::Healthy,
                reason: "50.0% available".into(),
                observed_at: 100,
                values: vec![Value {
                    name: "available_ratio".into(),
                    value: 0.5,
                    unit: Unit::Ratio,
                }],
            }],
        }
    }

    fn parse(d: &Observations) -> Result<Observations, Rejection> {
        Observations::parse(
            &serde_json::to_vec(d).unwrap(),
            "coordinator-01",
            &format!("sha256:{}", "0".repeat(64)),
        )
    }

    #[test]
    fn rejects_identity_schema_and_structure_failures() {
        assert!(parse(&doc()).is_ok());
        let mut d = doc();
        d.schema_version = 2;
        assert_eq!(parse(&d), Err(Rejection::Schema));
        let mut d = doc();
        d.host_id = "worker-01".into();
        assert_eq!(parse(&d), Err(Rejection::HostMismatch));
        let mut d = doc();
        d.config_digest = format!("sha256:{}", "1".repeat(64));
        assert_eq!(parse(&d), Err(Rejection::ConfigMismatch));
        let mut d = doc();
        d.checks.push(d.checks[0].clone());
        assert_eq!(parse(&d), Err(Rejection::Malformed));
        let mut d = doc();
        d.checks[0].reason = "line\nbreak".into();
        assert_eq!(parse(&d), Err(Rejection::Malformed));
        let body = br#"{"schema_version":1,"host_id":"coordinator-01","extra":1}"#;
        assert!(Observations::parse(body, "coordinator-01", "x").is_err());
        assert_eq!(
            Observations::parse(&vec![b' '; MAX_BODY_BYTES + 1], "h", "x"),
            Err(Rejection::Malformed)
        );
    }

    #[test]
    fn stale_and_future_checks_are_unknown() {
        let c = &doc().checks[0];
        assert_eq!(Observations::status_at(c, 145), Status::Healthy);
        assert_eq!(Observations::status_at(c, 146), Status::Unknown);
        assert_eq!(Observations::status_at(c, 94), Status::Unknown);
        assert_eq!(Observations::status_at(c, 95), Status::Healthy);
    }
}
